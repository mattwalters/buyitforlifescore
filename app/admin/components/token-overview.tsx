import { prisma } from "@mono/db";
import { TokenChart } from "./token-chart";

export async function TokenOverview() {
  const sevenDaysAgo = new Date();
  sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

  const spends = await prisma.aiSpend.findMany({
    where: {
      createdAt: {
        gte: sevenDaysAgo,
      },
    },
    select: {
      promptTokens: true,
      thinkingTokens: true,
      responseTokens: true,
      createdAt: true,
    },
    orderBy: {
      createdAt: "asc"
    }
  });

  const dateMap = new Map<string, { date: string, Input: number, Thinking: number, Output: number }>();

  // Pre-fill the last 7 days so we get a continuous X-axis even if a day has 0 tokens
  for (let i = 6; i >= 0; i--) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    const dateStr = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    dateMap.set(dateStr, { date: dateStr, Input: 0, Thinking: 0, Output: 0 });
  }

  for (const row of spends) {
    const dateStr = row.createdAt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });

    const record = dateMap.get(dateStr) || { date: dateStr, Input: 0, Thinking: 0, Output: 0 };
    record.Input += row.promptTokens;
    record.Thinking += row.thinkingTokens;
    record.Output += row.responseTokens;
    dateMap.set(dateStr, record);
  }

  const chartData = Array.from(dateMap.values());

  return (
    <div className="rounded-lg border bg-card text-card-foreground shadow-sm">
      <div className="flex flex-col space-y-1.5 p-6">
        <h3 className="font-semibold leading-none tracking-tight">AI Token Footprint</h3>
        <p className="text-sm text-muted-foreground">Volume breakdown of tokens consumed globally by mutually exclusive payload type over the last 7 days.</p>
      </div>
      <div className="p-6 pt-0">
        <TokenChart data={chartData} />
      </div>
    </div>
  );
}
