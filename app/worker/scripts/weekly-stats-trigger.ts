import { Queue } from "bullmq";
import { env } from "../src/env.js";

const REDIS_URL = env.REDIS_URL;
const queue = new Queue("weekly-stats", {
  connection: { url: REDIS_URL },
});

async function trigger() {
  console.log("Adding manual trigger job to weekly-stats queue...");
  await queue.add("manual-trigger", {});
  console.log("Job added! Check the worker logs.");
  process.exit(0);
}

trigger().catch(console.error);
