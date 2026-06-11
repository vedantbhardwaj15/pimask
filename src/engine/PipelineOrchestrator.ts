import { Worker } from 'worker_threads';
import * as path from 'path';
import { Readable } from 'stream';
import { AppConfig } from '../config';
import { createAdapter } from '../adapters/DatabaseFactory';
import { DatabaseAdapter } from '../adapters/DatabaseAdapter';
import { topologicalSort } from './TopologicalSort';
import { RedisQueue } from '../queue/RedisQueue';

// Ingestion Backpressure Thresholds
const QUEUE_HIGH_WATERMARK = 10000;
const QUEUE_LOW_WATERMARK = 2000;
const WORKER_CONCURRENCY = 2; 
const BATCH_SIZE = 100;

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

export class PipelineOrchestrator {
  private readonly config: AppConfig;
  private sourceDb!: DatabaseAdapter;
  private targetDb!: DatabaseAdapter;
  private redisQueue!: RedisQueue;

  constructor(config: AppConfig) {
    this.config = config;
  }

  /**
   * Main entrypoint initiating the static data masking pipeline
   */
  async run(): Promise<void> {
    console.log('[Orchestrator] Starting static data masking pipeline...');
    await this.initializeConnections();

    try {
      // Discover schema configuration
      const tables = await this.sourceDb.getTables();
      const foreignKeys = await this.sourceDb.getForeignKeys();
      console.log(`[Orchestrator] Discovered ${tables.length} tables and ${foreignKeys.length} foreign key relationships.`);

      // Compute processing sequence resolving relational dependencies
      const executionOrder = topologicalSort(tables, foreignKeys);
      console.log(`[Orchestrator] Resolved processing sequence: ${executionOrder.join(' -> ')}`);

      // Process tables sequentially according to the dependency graph
      for (const table of executionOrder) {
        await this.processTable(table);
      }

      console.log('\n[Orchestrator] Static data masking pipeline execution finished successfully.');
    } catch (error) {
      console.error('[Orchestrator] Critical error during execution:', error);
      throw error;
    } finally {
      await this.cleanupConnections();
    }
  }

  /**
   * Connects to all infrastructure services in parallel
   */
  private async initializeConnections(): Promise<void> {
    this.sourceDb = createAdapter(this.config.source.type, this.config.source.connection);
    this.targetDb = createAdapter(this.config.target.type, this.config.target.connection);
    this.redisQueue = new RedisQueue(this.config.redis.host, this.config.redis.port);

    await Promise.all([
      this.sourceDb.connect(),
      this.targetDb.connect(),
      this.redisQueue.connect(),
    ]);
    console.log('[Orchestrator] Connected to databases and Redis queue manager.');
  }

  /**
   * Orchestrates the stream-queue-worker pipeline for a single database table
   */
  private async processTable(tableName: string): Promise<void> {
    console.log(`\n--- Processing Table: ${tableName} ---`);

    const columns = await this.sourceDb.getColumns(tableName);
    const rulesForTable = this.config.masking.rules[tableName] || {};
    const queueKey = `queue:table:${tableName}`;
    const readerDoneKey = `status:reader:done:${tableName}`;

    // Reset volatile Redis states
    await Promise.all([
      this.redisQueue.clear(queueKey),
      this.redisQueue.clear(readerDoneKey),
    ]);

    const workerPayload: WorkerPayload = {
      tableName,
      columns,
      maskingRules: rulesForTable,
      salt: this.config.masking.salt,
      redisHost: this.config.redis.host,
      redisPort: this.config.redis.port,
      targetDbConfig: this.config.target.connection,
      targetDbType: this.config.target.type,
      batchSize: BATCH_SIZE,
    };

    // Spawn concurrent worker threads
    let totalProcessedByWorkers = 0;
    const workerPromises = Array.from({ length: WORKER_CONCURRENCY }, () =>
      this.spawnWorker(workerPayload, (progress) => {
        totalProcessedByWorkers += progress;
        process.stdout.write(`\r[Worker] Masked & Saved: ${totalProcessedByWorkers} rows`);
      })
    );

    try {
      // Stream raw source data to the queue
      const readStream = await this.sourceDb.readStream(tableName, columns);
      const totalRead = await this.streamDataToQueue(readStream, queueKey, readerDoneKey);
      console.log(`\n[Reader] Extracted ${totalRead} rows.`);

      // Await worker pool completion
      await Promise.all(workerPromises);
      console.log(`[Pipeline] Completed processing for table: ${tableName}`);
    } finally {
      // Clean up Redis keys
      await Promise.all([
        this.redisQueue.clear(queueKey),
        this.redisQueue.clear(readerDoneKey),
      ]);
    }
  }

  /**
   * Consumes database cursor stream, batching records into the Redis FIFO queue with backpressure flow-control
   */
  private streamDataToQueue(readStream: Readable, queueKey: string, readerDoneKey: string): Promise<number> {
    return new Promise((resolve, reject) => {
      let batch: any[] = [];
      let totalRead = 0;
      let isPaused = false;

      readStream.on('data', async (row: any) => {
        batch.push(row);
        totalRead++;

        if (batch.length >= BATCH_SIZE) {
          const currentBatch = [...batch];
          batch = [];

          // Implement Backpressure: pause reader stream if worker pool is falling behind
          const queueLength = await this.redisQueue.getLength(queueKey);
          if (queueLength >= QUEUE_HIGH_WATERMARK && !isPaused) {
            readStream.pause();
            isPaused = true;
          }

          await this.redisQueue.pushBatch(queueKey, currentBatch);

          // Resume reader stream once worker pool catches up
          if (isPaused) {
            const updatedLength = await this.redisQueue.getLength(queueKey);
            if (updatedLength <= QUEUE_LOW_WATERMARK) {
              readStream.resume();
              isPaused = false;
            }
          }
        }
      });

      readStream.on('end', async () => {
        try {
          // Flush remaining rows
          if (batch.length > 0) {
            await this.redisQueue.pushBatch(queueKey, batch);
          }
          // Notify worker threads that reading is complete
          await this.redisQueue.pushBatch(readerDoneKey, [{ done: true }]);
          resolve(totalRead);
        } catch (err) {
          reject(err);
        }
      });

      readStream.on('error', (err) => reject(err));
    });
  }

  /**
   * Spawns worker thread using development (ts-node) or production bootstrap configurations
   */
  private spawnWorker(payload: WorkerPayload, onProgress: (rows: number) => void): Promise<void> {
    return new Promise((resolve, reject) => {
      const isTsNode = process.execArgv.some(arg => arg.includes('ts-node')) || 
                       process.argv.some(arg => arg.includes('ts-node'));

      const workerFile = isTsNode
        ? path.resolve(__dirname, 'PipelineWorker.ts')
        : path.resolve(__dirname, 'PipelineWorker.js');

      let worker: Worker;

      if (isTsNode) {
        worker = new Worker(
          `
          require('ts-node').register();
          require(${JSON.stringify(workerFile)});
          `,
          {
            eval: true,
            workerData: payload,
          }
        );
      } else {
        worker = new Worker(workerFile, {
          workerData: payload,
        });
      }

      worker.on('message', (message) => {
        if (message.status === 'progress') {
          onProgress(message.rowsProcessed);
        } else if (message.status === 'done') {
          resolve();
        } else if (message.status === 'error') {
          reject(new Error(`Worker for table ${payload.tableName} failed: ${message.error}`));
        }
      });

      worker.on('error', (err) => reject(err));
      worker.on('exit', (code) => {
        if (code !== 0) {
          reject(new Error(`Worker for table ${payload.tableName} exited with code ${code}`));
        }
      });
    });
  }

  /**
   * Safely closes connections to all resources
   */
  private async cleanupConnections(): Promise<void> {
    try {
      await Promise.all([
        this.sourceDb ? this.sourceDb.disconnect() : Promise.resolve(),
        this.targetDb ? this.targetDb.disconnect() : Promise.resolve(),
        this.redisQueue ? this.redisQueue.disconnect() : Promise.resolve(),
      ]);
      console.log('[Orchestrator] All connections closed cleanly.');
    } catch (err) {
      console.error('[Orchestrator] Error during connection cleanup:', err);
    }
  }
}
