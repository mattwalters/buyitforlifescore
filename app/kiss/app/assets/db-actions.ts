"use server";

import { prisma } from "@mono/db";

export async function getAssetHistory(assetId: string) {
  try {
    const materializations = await prisma.kissMaterialization.findMany({
      where: { assetId },
      orderBy: { createdAt: "desc" },
    });

    // We stringify/parse to handle Next.js Server Actions serialization (Dates, JsonB objects)
    return { success: true, data: JSON.parse(JSON.stringify(materializations)) };
  } catch (error: any) {
    console.error("Failed to fetch asset history:", error);
    return { success: false, error: error.message };
  }
}

export async function getJobs(assetId: string) {
  try {
    const jobs = await prisma.kissJob.findMany({
      where: { assetId },
      orderBy: { requestedAt: "desc" },
      take: 20,
    });
    return { success: true, data: JSON.parse(JSON.stringify(jobs)) };
  } catch (error: any) {
    return { success: false, error: error.message };
  }
}
