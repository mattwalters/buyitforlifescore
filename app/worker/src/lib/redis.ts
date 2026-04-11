import { Redis } from "ioredis";

export async function waitForRedis(redisUrl: string) {
  const maxRetries = 20;
  const retryDelay = 2000;

  for (let i = 0; i < maxRetries; i++) {
    try {
      const client = new Redis(redisUrl, {
        maxRetriesPerRequest: 1, // Fail fast for this check
        retryStrategy: () => null, // Don't let ioredis retry internally for this check
      });

      await new Promise<void>((resolve, reject) => {
        client.once("ready", () => {
          client.disconnect();
          resolve();
        });
        client.once("error", (err) => {
          client.disconnect();
          reject(err);
        });
      });

      console.log("✅ Redis is ready!");
      return;
    } catch (err) {
      console.log(
        `⏳ Redis not ready yet, retrying in ${retryDelay / 1000}s... (${i + 1}/${maxRetries})`,
        err,
      );
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
    }
  }

  throw new Error("Could not connect to Redis after multiple attempts");
}
