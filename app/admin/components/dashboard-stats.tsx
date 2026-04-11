import { prisma } from "@mono/db";
import { Database, Package, Trophy, Activity, MessageSquare, Layers, Shapes } from "lucide-react";

export async function DashboardStats() {
  const [
    submissionCount,
    unprocessedSubmissions,
    commentCount,
    mentionCount,
    brandCount,
    lineCount,
    modelCount,
    orphanedModelCount,
    spendResult
  ] = await Promise.all([
    prisma.bronzeRedditSubmission.count(),
    prisma.bronzeRedditSubmission.count({ where: { isProcessed: false } }),
    prisma.bronzeRedditComment.count(),
    prisma.silverProductMention.count(),
    prisma.goldBrand.count(),
    prisma.goldProductLine.count(),
    prisma.goldProduct.count(),
    prisma.goldProduct.count({ where: { goldProductLineId: null } }),
    prisma.aiSpend.aggregate({ _sum: { costInUsd: true } }),
  ]);

  const totalSpend = spendResult._sum.costInUsd || 0;
  const processedSubmissions = Math.max(1, submissionCount - unprocessedSubmissions);
  const avgMentions = (mentionCount / processedSubmissions).toFixed(1);

  const avgCostPerSubmission = totalSpend / processedSubmissions;
  const estimatedCost = avgCostPerSubmission * unprocessedSubmissions;

  // Organized into tiered architectural layers
  const sections = [
    {
      title: "Bronze Layer (Raw Ingestion)",
      color: "border-orange-500/50",
      iconColor: "text-orange-500",
      metrics: [
        { label: "Total Submissions", value: submissionCount.toLocaleString(), icon: <MessageSquare className="h-4 w-4" /> },
        { label: "Total Comments", value: commentCount.toLocaleString(), icon: <MessageSquare className="h-4 w-4" /> },
        { label: "Unprocessed Threads", value: unprocessedSubmissions.toLocaleString(), icon: <Database className="h-4 w-4" /> },
      ]
    },
    {
      title: "Silver Layer (AI Extractions)",
      color: "border-slate-400/50",
      iconColor: "text-slate-400",
      metrics: [
        { label: "Total Mentions", value: mentionCount.toLocaleString(), icon: <Package className="h-4 w-4" /> },
        { label: "Mentions Per Thread", value: avgMentions, icon: <Activity className="h-4 w-4" /> },
      ]
    },
    {
      title: "Gold Layer (Canonical Taxonomy)",
      color: "border-yellow-500/50",
      iconColor: "text-yellow-500",
      metrics: [
        { label: "Canonical Brands", value: brandCount.toLocaleString(), icon: <Trophy className="h-4 w-4" /> },
        { label: "Product Lines", value: lineCount.toLocaleString(), icon: <Layers className="h-4 w-4" /> },
        { label: "Exact Models", value: modelCount.toLocaleString(), icon: <Shapes className="h-4 w-4" /> },
        { label: "Orphaned Models", value: orphanedModelCount.toLocaleString(), icon: <Activity className="h-4 w-4" /> },
      ]
    },
    {
      title: "System",
      color: "border-primary/20",
      iconColor: "text-primary",
      metrics: [
        { label: "Total AI Spend", value: `$${totalSpend.toFixed(4)}`, icon: <Activity className="h-4 w-4" /> },
        { label: "Est. Cost to Finish", value: `$${estimatedCost.toFixed(2)}`, icon: <Activity className="h-4 w-4" /> },
      ]
    }
  ];

  return (
    <div className="space-y-8">
      {sections.map((section) => (
        <div key={section.title} className="space-y-3">
          <h3 className={`text-lg font-bold tracking-tight flex items-center gap-2 ${section.iconColor}`}>
            {section.title}
          </h3>
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
            {section.metrics.map((stat) => (
              <div
                key={stat.label}
                className={`rounded-lg border bg-card p-6 text-card-foreground shadow-sm ${section.color} hover:bg-muted/10 transition-colors`}
              >
                <div className="flex flex-col space-y-2">
                  <div className="flex items-center gap-2">
                    <span className={section.iconColor}>{stat.icon}</span>
                    <h3 className="text-sm font-semibold leading-none tracking-tight text-muted-foreground">
                      {stat.label}
                    </h3>
                  </div>
                  <div className="text-3xl font-bold">{stat.value}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
