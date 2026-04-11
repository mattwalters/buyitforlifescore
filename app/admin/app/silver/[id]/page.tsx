import { prisma } from "@mono/db";
import Link from "next/link";
import { notFound } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function SilverMentionPage(props: {
  params: Promise<{ id: string }>
}) {
  const { id } = await props.params;

  const mention = await prisma.silverProductMention.findUnique({
    where: { id },
    include: {
      submission: {
        select: {
          id: true,
          title: true,
          postedAt: true,
        }
      },
      goldBrand: true,
      goldProductLine: true,
      goldProduct: true,
    }
  });

  if (!mention) {
    notFound();
  }

  let apportionedExtractionCost = 0;
  let apportionedEmbeddingCost = 0;
  let totalMentionsFromSource = 1;

  if (mention.submissionId) {
     totalMentionsFromSource = await prisma.silverProductMention.count({
       where: { submissionId: mention.submissionId }
     });

     const spends = await prisma.aiSpend.findMany({
       where: { submissionId: mention.submissionId }
     });

     const extractionSpend = spends.filter(s => s.jobName === "SILVER_EXTRACTION").reduce((sum, s) => sum + s.costInUsd, 0);
     const embeddingSpend = spends.filter(s => s.jobName === "SILVER_EMBEDDING").reduce((sum, s) => sum + s.costInUsd, 0);

     if (totalMentionsFromSource > 0) {
        apportionedExtractionCost = extractionSpend / totalMentionsFromSource;
        apportionedEmbeddingCost = embeddingSpend / totalMentionsFromSource;
     }
  }

  return (
    <div className="container mx-auto p-8 space-y-8">
      <div className="flex flex-col gap-2">
        <Link href="/silver" className="text-sm text-primary hover:underline">&larr; Back to Silver</Link>
        <h1 className="text-3xl font-bold tracking-tight">Mention: {mention.brand} {mention.productName}</h1>
        <p className="text-muted-foreground text-sm font-mono">{mention.id}</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
        <div className="space-y-6">
          <div className="rounded-xl border bg-card p-6 shadow-sm">
            <h2 className="font-semibold mb-4 border-b pb-2">Extracted Data</h2>
            <dl className="grid grid-cols-2 gap-y-4 text-sm">
              <dt className="text-muted-foreground">Original Brand</dt>
              <dd className="font-medium">{mention.brand || "-"}</dd>
              
              <dt className="text-muted-foreground">Original Product</dt>
              <dd className="font-medium">{mention.productName || "-"}</dd>

              <dt className="text-muted-foreground">Specificity Level</dt>
              <dd className="font-medium">{mention.specificityLevel}</dd>
              
              <dt className="text-muted-foreground">Overall Sentiment</dt>
              <dd className="font-medium">{mention.sentiment}</dd>

              <dt className="text-muted-foreground">Flaw or Caveat</dt>
              <dd className="font-medium text-destructive">{mention.flawOrCaveat || "None"}</dd>

              <dt className="text-muted-foreground">Price</dt>
              <dd className="font-medium">{mention.acquiredPrice ? `$${mention.acquiredPrice}` : "-"}</dd>

              <dt className="text-muted-foreground">Owned For (Months)</dt>
              <dd className="font-medium">{mention.ownershipDurationMonths || "-"}</dd>

              <dt className="text-muted-foreground">Usage Frequency</dt>
              <dd className="font-medium">{mention.usageFrequency || "-"}</dd>
            </dl>
          </div>

          <div className="rounded-xl border bg-card p-6 shadow-sm">
            <h2 className="font-semibold mb-4 border-b pb-2">Radar Feedback</h2>
            <dl className="grid grid-cols-2 gap-y-4 text-sm">
              <dt className="text-muted-foreground">Durability</dt>
              <dd className="font-medium">{mention.durability || "-"}</dd>
              
              <dt className="text-muted-foreground">Repairability</dt>
              <dd className="font-medium">{mention.repairability || "-"}</dd>

              <dt className="text-muted-foreground">Maintenance</dt>
              <dd className="font-medium">{mention.maintenance || "-"}</dd>
              
              <dt className="text-muted-foreground">Warranty</dt>
              <dd className="font-medium">{mention.warranty || "-"}</dd>

              <dt className="text-muted-foreground">Value</dt>
              <dd className="font-medium">{mention.value || "-"}</dd>
            </dl>
          </div>
        </div>

        <div className="space-y-6">
          <div className="rounded-xl border border-primary/20 bg-primary/5 p-6 shadow-sm">
            <h2 className="font-semibold text-primary mb-4 border-b border-primary/10 pb-2">AI Cost Profile (Pro-Rata)</h2>
            <p className="text-xs text-muted-foreground mb-4">
              This submission yielded {totalMentionsFromSource} mentions. Cost is apportioned mathematically.
            </p>
            <dl className="grid grid-cols-2 gap-y-4 text-sm">
              <dt className="text-muted-foreground">Apportioned Extraction</dt>
              <dd className="font-medium font-mono text-green-600">${apportionedExtractionCost.toFixed(6)}</dd>
              
              <dt className="text-muted-foreground">Apportioned Embedding</dt>
              <dd className="font-medium font-mono text-green-600">${apportionedEmbeddingCost.toFixed(6)}</dd>

              <dt className="text-muted-foreground font-semibold pt-2 border-t border-primary/10">Total Origin Cost</dt>
              <dd className="font-bold font-mono text-green-700 pt-2 border-t border-primary/10">${(apportionedExtractionCost + apportionedEmbeddingCost).toFixed(6)}</dd>
            </dl>
          </div>

          <div className="rounded-xl border bg-card p-6 shadow-sm">
            <h2 className="font-semibold mb-4 border-b pb-2">Pipeline Topology</h2>
            <dl className="gap-y-4 text-sm space-y-4 flex flex-col">
              <div>
                <dt className="text-muted-foreground text-xs mb-1">Bronze Source Submission</dt>
                {mention.submission ? (
                  <Link href={`/submissions/${mention.submission.id}`} className="text-primary hover:underline font-medium">
                    {mention.submission.title}
                  </Link>
                ) : <dd className="font-medium">-</dd>}
              </div>

              <div>
                <dt className="text-muted-foreground text-xs mb-1">Resolved Gold Brand</dt>
                {mention.goldBrand ? (
                  <Link href={`/gold/brands/${mention.goldBrand.id}`} className="text-orange-600 hover:underline font-medium">
                     {mention.goldBrand.canonicalName}
                  </Link>
                ) : <dd className="font-medium text-muted-foreground italic">Unresolved</dd>}
              </div>
              
              <div>
                <dt className="text-muted-foreground text-xs mb-1">Resolved Gold Product Line</dt>
                {mention.goldProductLine ? (
                  <Link href={`/gold/lines/${mention.goldProductLine.id}`} className="text-orange-600 hover:underline font-medium">
                     {mention.goldProductLine.canonicalName}
                  </Link>
                ) : <dd className="font-medium text-muted-foreground italic">Unresolved</dd>}
              </div>

              <div>
                <dt className="text-muted-foreground text-xs mb-1">Resolved Gold Product Model</dt>
                {mention.goldProduct ? (
                  <Link href={`/gold/models/${mention.goldProduct.id}`} className="text-orange-600 hover:underline font-medium">
                     {mention.goldProduct.canonicalName}
                  </Link>
                ) : <dd className="font-medium text-muted-foreground italic">Unresolved</dd>}
              </div>
            </dl>
          </div>
        </div>
      </div>
    </div>
  );
}
