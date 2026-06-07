import Redis from 'ioredis';

export class RedisQueue {
  private client: Redis;

  constructor(host: string, port: number) {
    this.client = new Redis({
      host,
      port,
      maxRetriesPerRequest: 3,
      // Prevents process hanging on disconnect/error
      connectTimeout: 5000,
      lazyConnect: true,
    });

    this.client.on('error', (err) => {
      console.error('Redis Queue Client Error:', err);
    });
  }

  async connect(): Promise<void> {
    await this.client.connect();
  }

  async disconnect(): Promise<void> {
    await this.client.quit();
  }

  /**
   * Pushes a batch of objects onto the left of the list (FIFO ingestion)
   */
  async pushBatch(queueName: string, items: any[]): Promise<number> {
    if (items.length === 0) return 0;
    
    // Serialize each object to JSON
    const serialized = items.map(item => JSON.stringify(item));
    return await this.client.lpush(queueName, ...serialized);
  }

  /**
   * Pops up to `count` items from the right of the list (FIFO consumption)
   */
  async popBatch(queueName: string, count: number): Promise<any[]> {
    if (count <= 0) return [];

    // It returns an array of strings, or null if key does not exist.
    const results = await this.client.rpop(queueName, count);
    if (!results) return [];

    // If only one item is popped (older Redis or single RPOP result representation)
    const itemsArray = Array.isArray(results) ? results : [results];

    return itemsArray.map(item => JSON.parse(item));
  }

  /**
   * Gets the current queue length (useful to prevent buffer overflow)
   */
  // Useful for handling backpressure
  async getLength(queueName: string): Promise<number> {
    return await this.client.llen(queueName);
  }

  async clear(queueName: string): Promise<void> {
    await this.client.del(queueName);
  }
}
