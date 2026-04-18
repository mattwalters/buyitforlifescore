/* eslint-disable @typescript-eslint/no-explicit-any */
import { prisma } from "@mono/db";
import { SpendChart } from "./spend-chart";

export async function SpendOverview() {
  const sevenDaysAgo = new Date();
  sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

  const spends = await prisma.aiSpend.findMany({
    where: {
      createdAt: {
        gte: sevenDaysAgo,
      },
    },
    select: {
      jobName: true,
      costInUsd: true,
      createdAt: true,
    },
    orderBy: {
      createdAt: "asc",
    },
  });

  const keysSet = new Set<string>();
  const dateMap = new Map<string, any>();

  // Pre-fill the last 7 days so we get a continuous X-axis even if a day has $0 spend
  for (let i = 6; i >= 0; i--) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    const dateStr = d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    dateMap.set(dateStr, { date: dateStr });
  }

  for (const row of spends) {
    if (!row.jobName) continue; // Safety guard

    // Format date as "Apr 5"
    const dateStr = row.createdAt.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    keysSet.add(row.jobName);

    const record = dateMap.get(dateStr) || { date: dateStr };
    const currentVal = record[row.jobName] || 0;
    record[row.jobName] = currentVal + row.costInUsd;
    dateMap.set(dateStr, record);
  }

  const chartData = Array.from(dateMap.values());
  const chartKeys = Array.from(keysSet);

  return (
    <div className="rounded-lg border bg-card text-card-foreground shadow-sm">
      <div className="flex flex-col space-y-1.5 p-6">
        <h3 className="font-semibold leading-none tracking-tight">AI Pipeline Execution Spend</h3>
        <p className="text-sm text-muted-foreground">
          Detailed generative API costs aggregated by pipeline layer over the last 7 days.
        </p>
      </div>
      <div className="p-6 pt-0">
        <SpendChart data={chartData} keys={chartKeys} />
      </div>
    </div>
  );
}
