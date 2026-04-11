import { Queue } from "bullmq";
import * as dotenv from "dotenv";

dotenv.config({ path: "../../.env" });

async function getFailedJob() {
  const queue = new Queue("analysis", {
    connection: { url: process.env.REDIS_URL },
  });

  const failedJobs = await queue.getFailed(0, 5);
  if (failedJobs.length > 0) {
    console.log(`Found ${failedJobs.length} failed jobs.`);
    for (const job of failedJobs) {
      console.log(`\n--- Job ${job.id} ---`);
      console.log("Failed Reason:", job.failedReason);
      console.log("Stacktrace:", job.stacktrace);
    }
  } else {
    console.log("No failed jobs found. Maybe you cleared them already.");
  }
  await queue.close();
}

getFailedJob().catch(console.error);
