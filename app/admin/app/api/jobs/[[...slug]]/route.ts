import { Hono } from "hono";
import { auth } from "@/auth";
import { handle } from "hono/vercel";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { HonoAdapter } from "@bull-board/hono";
import { serveStatic } from "@hono/node-server/serve-static";
import { Queue } from "bullmq";
import Redis from "ioredis";
import { env } from "@/env";

// Reuse the connection logic or create a new one
const REDIS_URL = env.REDIS_URL;

const connection = new Redis(REDIS_URL, {
  maxRetriesPerRequest: null,
});

// Define queues to monitor
const queueNames = [
  "ingest",
  "analysis",
  "email",
  "test-error",
  "test-success",
  "test-ai",
  "demo-queue",
];

const queues = queueNames.map((name) => new Queue(name, { connection }));

const app = new Hono().basePath("/api/jobs");

app.use("*", async (c, next) => {
  const session = await auth();
  if (!session?.user) {
    return c.text("Unauthorized", 401);
  }
  await next();
});

const serverAdapter = new HonoAdapter(serveStatic);
serverAdapter.setBasePath("/api/jobs");

createBullBoard({
  queues: queues.map((q) => new BullMQAdapter(q)),
  serverAdapter,
});

app.route("/", serverAdapter.registerPlugin());

export const GET = handle(app);
export const POST = handle(app);
