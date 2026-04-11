import { prisma } from "@mono/db";
import Link from "next/link";
import { notFound } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function GoldLinePage(props: {
  params: Promise<{ id: string }>
}) {
  const { id } = await props.params;

  const line = await prisma.goldProductLine.findUnique({
    where: { id },
    include: {
      goldBrand: true,
      products: true,
      mentions: {
        select: {
          id: true,
          submissionId: true,
        }
      }
    }
  });

  if (!line) {
    notFound();
  }

  // Cost Allocation Logic
  const submissionIds = Array.from(new Set(line.mentions.map(m => m.submissionId).filter(Boolean))) as string[];

  let totalApportionedSourceCost = 0;
  let totalApportionedRollupCost = 0;

  if (submissionIds.length > 0) {
     const allSiblingMentions = await prisma.silverProductMention.findMany({
       where: { submissionId: { in: submissionIds } },
       select: { id: true, submissionId: true }
     });

     const mentionCounts: Record<string, number> = {};
     for (const m of allSiblingMentions) {
       if (m.submissionId) {
         mentionCounts[m.submissionId] = (mentionCounts[m.submissionId] || 0) + 1;
       }
     }

     const spends = await prisma.aiSpend.findMany({
       where: { submissionId: { in: submissionIds } }
     });

     for (const spend of spends) {
        if (!spend.submissionId) continue;
        const denominator = mentionCounts[spend.submissionId] || 1;
        const myMentionsInThisSubmission = line.mentions.filter(m => m.submissionId === spend.submissionId).length;
        
        const apportionedCost = (spend.costInUsd / denominator) * myMentionsInThisSubmission;

        if (spend.jobName.startsWith("SILVER_")) {
           totalApportionedSourceCost += apportionedCost;
        } else if (spend.jobName.startsWith("GOLD_")) {
           totalApportionedRollupCost += apportionedCost;
        }
     }
  }

  return (
    <div className="container mx-auto p-8 space-y-8">
      <div className="flex flex-col gap-2">
        <Link href="/gold/lines" className="text-sm text-primary hover:underline">&larr; Back to Product Lines</Link>
        <div className="flex items-center gap-2">
           <h1 className="text-3xl font-bold tracking-tight">{line.canonicalName}</h1>
           <span className="text-xl text-muted-foreground font-normal">by</span>
           <Link href={`/gold/brands/${line.goldBrand.id}`} className="text-xl text-primary hover:underline font-semibold tracking-tight">
              {line.goldBrand.canonicalName}
           </Link>
        </div>
        <p className="text-muted-foreground text-sm font-mono">{line.id}</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
        <div className="space-y-6">
          <div className="rounded-xl border bg-card p-6 shadow-sm">
            <h2 className="font-semibold mb-4 border-b pb-2">Computed Taxonomy</h2>
            <dl className="grid grid-cols-2 gap-y-4 text-sm">
              <dt className="text-muted-foreground">Canonical Status</dt>
              <dd className="font-medium text-emerald-600">Gold Tier Verified</dd>
              
              <dt className="text-muted-foreground">Overall Sentiment</dt>
              <dd className="font-medium">{line.avgSentiment.toFixed(2)} / 10</dd>

              <dt className="text-muted-foreground">Child Exact Models</dt>
              <dd className="font-medium">{line.products.length}</dd>

              <dt className="text-muted-foreground">Total Gathered Mentions</dt>
              <dd className="font-medium">
                 <Link href={`/silver?goldProductLineId=${line.id}`} className="hover:underline text-primary">
                    {line.mentionCount} Mentions
                 </Link>
              </dd>
            </dl>
          </div>

          <div className="rounded-xl border bg-card p-6 shadow-sm">
            <h2 className="font-semibold mb-4 border-b pb-2">Pipeline State</h2>
            <dl className="grid grid-cols-2 gap-y-4 text-sm">
              <dt className="text-muted-foreground">Titled?</dt>
              <dd className="font-medium">{line.isTitled ? "Yes" : "No"}</dd>
              
              <dt className="text-muted-foreground">Discovered On</dt>
              <dd className="font-medium">{line.createdAt.toLocaleDateString()}</dd>
            </dl>
          </div>
        </div>

        <div className="space-y-6">
          <div className="rounded-xl border border-orange-500/20 bg-orange-500/5 p-6 shadow-sm">
            <h2 className="font-semibold text-orange-600 mb-4 border-b border-orange-500/10 pb-2">AI Total Investment</h2>
            <p className="text-xs text-muted-foreground mb-4">
              Represents the apportioned cost to surface {line.mentionCount} child mentions and deductually evaluate them into a canonical record.
            </p>
            <dl className="grid grid-cols-2 gap-y-4 text-sm">
              <dt className="text-muted-foreground">Apportioned Harvesting</dt>
              <dd className="font-medium font-mono text-amber-700">${totalApportionedSourceCost.toFixed(6)}</dd>
              
              <dt className="text-muted-foreground">LLM Classification Rollups</dt>
              <dd className="font-medium font-mono text-amber-700">${totalApportionedRollupCost.toFixed(6)}</dd>

              <dt className="text-muted-foreground font-semibold pt-2 border-t border-orange-500/10">Total Pipeline Spend</dt>
              <dd className="font-bold font-mono text-orange-700 pt-2 border-t border-orange-500/10 text-lg">${(totalApportionedSourceCost + totalApportionedRollupCost).toFixed(6)}</dd>
            </dl>
          </div>
        </div>
      </div>
    </div>
  );
}
