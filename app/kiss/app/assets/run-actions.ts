"use server";

import { prisma } from "@mono/db";
import { exec } from "child_process";
import { promisify } from "util";

const execAsync = promisify(exec);

/**
 * Server Action callable explicitly from the frontend to trigger a Job.
 * We invoke the CLI script in a child process to prevent DuckDB's native node addon
 * from deadlocking the Next.js Turbopack dev server thread pool!
 */
export async function runMaterializationAction(assetId: string, partitionKey?: string) {
  try {
    // 1. Create Job Entry
    const job = await prisma.kissJob.create({
      data: {
        assetId,
        partitionKey,
        status: "RUNNING",
        startedAt: new Date(),
      },
    });

    // 2. Invoke the CLI runner gracefully outside of Turbopack thread
    const partitionFlag = partitionKey ? ` --partition=${partitionKey}` : "";

    // We are inside app/kiss/app/assets, so we run inside the 'app/kiss' workspace
    await execAsync(`npm run summarize -- -w @mono/kiss --asset=${assetId}${partitionFlag}`);

    return {
      success: true,
      message: `Triggered materialization successfully.`,
    };
  } catch (error: any) {
    return { success: false, error: error.message };
  }
}
