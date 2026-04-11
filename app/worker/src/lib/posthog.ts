import { PostHog } from "posthog-node";
import { env } from "../env.js";

let client: PostHog | null = null;

/**
 * Returns a singleton PostHog server client with exception autocapture enabled.
 * Returns null if env vars are missing (local dev).
 */
export function getPostHogClient(): PostHog | null {
  const token = env.NEXT_PUBLIC_POSTHOG_TOKEN;
  const host = env.NEXT_PUBLIC_POSTHOG_HOST;

  if (!token || !host) {
    return null;
  }

  if (!client) {
    client = new PostHog(token, {
      host,
      enableExceptionAutocapture: true,
    });
  }

  return client;
}

/**
 * Capture an exception from a BullMQ job failure.
 * Falls back to console.log in dev when PostHog is not configured.
 */
export function captureWorkerException(
  error: Error,
  context?: { queue?: string; jobId?: string; jobName?: string },
): void {
  const posthog = getPostHogClient();
  if (posthog) {
    posthog.captureException(error, "worker-service", {
      service: "worker",
      ...context,
    });
  } else {
    console.log(
      `[PostHog] Simulated captureException: ${error.message}`,
      context ? JSON.stringify(context) : "",
    );
  }
}

/**
 * Gracefully shut down PostHog (flush pending events).
 */
export async function shutdownPostHog(): Promise<void> {
  if (client) {
    await client.shutdown();
  }
}
