import analysisProcessor from "./src/queues/analysis.js";
import { Queue, Worker, Job } from "bullmq";
import * as dotenv from "dotenv";

dotenv.config({ path: "../../.env" });

async function main() {
  const connection = {
    url: process.env.REDIS_URL,
  };
  console.log("Connecting to Redis...");
  const _q = new Queue("analysis", { connection });

  // Create a worker that only processes ONE job for test purposes
  const worker = new Worker(
    "analysis",
    async (job: Job) => {
      console.log(`Processing job ${job.id}`);
      await analysisProcessor(job);
      console.log(`Job ${job.id} completed successfully!`);
      worker.close();
    },
    { connection, concurrency: 1 },
  );

  worker.on("failed", (job, err) => {
    console.error(`Job ${job?.id} failed with error:`, err);
    worker.close();
  });
}

main().catch(console.error);
