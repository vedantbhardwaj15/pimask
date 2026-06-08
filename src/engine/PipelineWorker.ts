import { parentPort, workerData } from 'worker_threads';
import { createAdapter } from '../adapters/DatabaseFactory';
import { RedisQueue } from '../queue/RedisQueue';
import { applyMask } from './MaskingFunctions';
import Ajv from 'ajv';

interface WorkerPayload {
  tableName: string;
  columns: string[];
  maskingRules: Record<string, string>;
  salt: string;
  redisHost: string;
  redisPort: number;
  targetDbConfig: any;
  targetDbType: string;
  batchSize: number;
}

const {
  tableName,
  columns,
  maskingRules,
  salt,
  redisHost,
  redisPort,
  targetDbConfig,
  targetDbType,
  batchSize,
} = workerData as WorkerPayload;

// Instantiate AJV validator for high-speed record validation
const ajv = new Ajv({ coerceTypes: true, useDefaults: true, allErrors: true });
// coercetypes automatically converts types
//allerrors shows all the errors instead of stopping at the first error

// Dynamically compile JSON schema based on the actual columns found in this table
const buildJsonSchema = (cols: string[]) => {
  const properties: Record<string, any> = {};
  for (const col of cols) {
    // We treat all fields as nullable/optional for validation simplicity,
    // allowing the database to enforce strict null constraints
    properties[col] = { type: ['string', 'number', 'boolean', 'null'] };
  }
  return {
    type: 'object',
    properties,
    required: [],
    additionalProperties: true,
  };
};

const schema = buildJsonSchema(columns);
const validate = ajv.compile(schema);

async function startWorker() {
  if (!parentPort) throw new Error('Worker must be run as a Thread');

  // Initialize Target Database Client
  const targetDb = createAdapter(targetDbType, targetDbConfig);
  await targetDb.connect();

  // Initialize Redis Connection
  const redisQueue = new RedisQueue(redisHost, redisPort);
  await redisQueue.connect();

  const queueKey = `queue:table:${tableName}`;
  const readerDoneKey = `status:reader:done:${tableName}`;

  try {
    let active = true;

    while (active) {
      // 1. Pop a batch from Redis
      const batch = await redisQueue.popBatch(queueKey, batchSize);

      if (batch.length === 0) {
        // Queue is empty. Check if reader has completed parsing raw tables.
        const isReaderDone = await redisQueue.getLength(readerDoneKey);
        if (isReaderDone > 0) {
          // Double check one last time that no items arrived during latency
          const lastCheck = await redisQueue.popBatch(queueKey, batchSize);
          if (lastCheck.length === 0) {
            active = false;
            break;
          } else {
            // Process the final lingering items
            await processBatch(lastCheck, targetDb);
          }
        }
        
        // Backoff slightly to prevent CPU spinning when queue is temporarily empty
        await new Promise((resolve) => setTimeout(resolve, 50));
        continue;
      }

      // 2. Process and write the batch
      await processBatch(batch, targetDb);
    }

    await redisQueue.disconnect();
    await targetDb.disconnect();
    
    parentPort.postMessage({ status: 'done', tableName });
    
  } catch (error: any) {
    console.error(`Worker Thread Error for table "${tableName}":`, error);
    
    try {
      await redisQueue.disconnect();
      await targetDb.disconnect();
    } catch {}

    parentPort.postMessage({ status: 'error', tableName, error: error.message });
    process.exit(1);
  }
}

async function processBatch(batch: any[], targetDb: any) {
  const processedRows: any[] = [];

  for (const row of batch) {
    const maskedRow: any = { ...row };

    // Apply masking rules dynamically based on configuration
    for (const col of columns) {
      const rule = maskingRules[col];
      if (rule && row[col] !== null && row[col] !== undefined) {
        maskedRow[col] = applyMask(rule, row[col], salt);
      }
    }

    const isValid = validate(maskedRow);
    if (!isValid) {
      throw new Error(
        `JSON Schema Validation failed for table ${tableName}: ${ajv.errorsText(validate.errors)}`
      );
    }

    processedRows.push(maskedRow);
  }

  // Parameterized bulk insert to target database
  await targetDb.writeBatch(tableName, processedRows);

  // Send progress back to parent thread
  parentPort!.postMessage({
    status: 'progress',
    tableName,
    rowsProcessed: processedRows.length,
  });
}

// Start processing immediately
startWorker();
