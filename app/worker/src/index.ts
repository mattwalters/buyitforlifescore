import "dotenv/config";
import { env } from "./env.js";
import http from "node:http";
import { Worker, Job } from "bullmq";
import { waitForRedis } from "./lib/redis.js";

import analysisProcessor from "./queues/analysis.js";
import { getPostHogClient, captureWorkerException, shutdownPostHog } from "./lib/posthog.js";

const REDIS_URL = env.REDIS_URL;
const prefix = undefined;

console.log("🚀 Worker Service Starting...");
console.log(`🔌 Configuration: Redis at ${REDIS_URL}`);

async function start() {
  try {
    getPostHogClient();
    await waitForRedis(REDIS_URL);
    const connection = {
      url: REDIS_URL,
      retryStrategy: (times: number) => Math.min(times * 50, 2000),
    };

    if (prefix) {
      console.log(`🏷️  Using Redis Prefix: ${prefix}`);
    }

    const analysisWorker = new Worker("analysis", analysisProcessor, {
      connection,
      prefix,
      concurrency: 10,
      limiter: {
        max: 14, // 15 RPM is the Gemini 3 Flash free tier limit. 14 leaves a small buffer
        duration: 60000,
      },
    });

    analysisWorker.on("completed", (job) => {
      console.log(`🎉 Analysis Job ${job.id} has completed!`);
    });

    analysisWorker.on("failed", (job, err) => {
      console.log(`❌ Analysis Job ${job?.id} has failed with ${err.message}`);
      captureWorkerException(err, { queue: "analysis", jobId: job?.id, jobName: job?.name });
    });

    const testErrorWorker = new Worker(
      "test-error",
      async (_job: Job) => {
        throw new Error("[Test] Deliberate worker error from test-error page");
      },
      { connection, prefix },
    );
    testErrorWorker.on("failed", (job, err) => {
      console.error(`❌ Test Error Job ${job?.id} failed:`, err.message);
      captureWorkerException(err, { queue: "test-error", jobId: job?.id, jobName: job?.name });
    });

    const testSuccessWorker = new Worker(
      "test-success",
      async (_job: Job) => {
        return { success: true, timestamp: Date.now() };
      },
      { connection, prefix },
    );
    testSuccessWorker.on("completed", (job) => {
      console.log(`🎉 Test Success Job ${job.id} completed`);
    });
    testSuccessWorker.on("failed", (job, err) => {
      console.error(`❌ Test Success Job ${job?.id} failed:`, err.message);
      captureWorkerException(err, { queue: "test-success", jobId: job?.id, jobName: job?.name });
    });

    const testAiWorker = new Worker(
      "test-ai",
      async (_job: Job) => {
        const baseUrl = env.AI_MOCK_URL || "https://generativelanguage.googleapis.com";
        const url = `${baseUrl}/v1beta/models/gemini-2.5-flash:generateContent`;
        const res = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ contents: [{ parts: [{ text: "ping" }] }] }),
        });
        const json = (await res.json()) as {
          candidates?: { content?: { parts?: { text?: string }[] } }[];
        };
        const text = json.candidates?.[0]?.content?.parts?.[0]?.text || "{}";
        return { success: true, response: JSON.parse(text) };
      },
      { connection, prefix },
    );
    testAiWorker.on("completed", (job) => {
      console.log(`🎉 Test AI Job ${job.id} completed`);
    });
    testAiWorker.on("failed", (job, err) => {
      console.error(`❌ Test AI Job ${job?.id} failed:`, err.message);
      captureWorkerException(err, { queue: "test-ai", jobId: job?.id, jobName: job?.name });
    });

    const demoWorker = new Worker(
      "demo-queue",
      async (job: Job) => {
        const { a, b } = job.data;
        const sum = a + b;
        console.log(`\n\n========================================`);
        console.log(`🤖 DEMO JOB ACTIVATED! id: ${job.id}`);
        console.log(`📊 Processing simple math: ${a} + ${b} = ${sum}`);
        console.log(`========================================\n\n`);
        return sum;
      },
      { connection, prefix },
    );

    demoWorker.on("completed", (job) => {
      console.log(`🎉 Demo Math Job ${job.id} completed successfully`);
    });

    demoWorker.on("failed", (job, err) => {
      console.error(`❌ Demo Math Job ${job?.id} failed:`, err.message);
      captureWorkerException(err, { queue: "demo-queue", jobId: job?.id, jobName: job?.name });
    });

    console.log("👀 Worker is listening for jobs...");

    const port = env.PORT;
    const server = http.createServer((req, res) => {
      if (req.url === "/") {
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end("Worker is running");
      } else {
        res.writeHead(404, { "Content-Type": "text/plain" });
        res.end("Not Found");
      }
    });

    const host = env.HOST || "127.0.0.1";
    server.listen(port, host, () => {
      console.log(`🏥 Health check server listening on ${host}:${port}`);
    });
  } catch (error) {
    console.error("💀 Fatal error starting worker:", error);
    await shutdownPostHog();
    process.exit(1);
  }
}

start();
