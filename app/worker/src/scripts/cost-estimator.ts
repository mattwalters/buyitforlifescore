import { prisma } from "@mono/db";

async function estimateCosts() {
  console.log("📊 Running Cost Distribution Estimator...\n");

  // --- Step 1: Calculate Real Spend & Ratios ---
  console.log("Analyzing current AiSpend baseline...");
  const spends = await prisma.aiSpend.findMany({
    where: { model: "gemini-3-flash-preview" },
  });

  if (spends.length === 0) {
    console.error("No AiSpend data found for gemini-3-flash-preview. Cannot calculate ratio.");
    process.exit(1);
  }

  let totalActualSpend = 0;
  let totalCharsInSpend = 0;

  for (const spend of spends) {
    totalActualSpend += spend.costInUsd;

    // Fetch the text this spend processed
    const sub = await prisma.bronzeRedditSubmission.findUnique({
      where: { id: spend.submissionId! },
      select: { title: true, selftext: true, comments: { select: { body: true } } },
    });

    if (sub) {
      let charCount = (sub.title || "").length + (sub.selftext || "").length;
      for (const comment of sub.comments) {
        charCount += (comment.body || "").length;
      }
      totalCharsInSpend += charCount;
    }
  }

  const costPerThousandChars = totalActualSpend / (totalCharsInSpend / 1000);

  console.log(`- Total submissions processed: ${spends.length}`);
  console.log(`- Total characters processed: ${totalCharsInSpend.toLocaleString()}`);
  console.log(`- Total cost incurred: $${totalActualSpend.toFixed(4)}`);
  console.log(`- Calculated Ratio: $${costPerThousandChars.toFixed(5)} per 1,000 characters\n`);

  // --- Step 2: Distribution Analysis across ALL Data ---
  console.log("Analyzing total dataset scale and distribution...\n");

  // We fetch minimal data to sum it up quickly.
  const allSubmissions = await prisma.$queryRaw<{ score: number; total_length: bigint }[]>`
    SELECT 
      s.score,
      (
        COALESCE(LENGTH(s.title), 0) + 
        COALESCE(LENGTH(s.selftext), 0) + 
        COALESCE(SUM(LENGTH(c.body)), 0)
      ) AS total_length
    FROM "BronzeRedditSubmission" s
    LEFT JOIN "BronzeRedditComment" c ON s.id = c."submissionId"
    GROUP BY s.id
    ORDER BY s.score DESC
  `;

  const tiers = [
    { name: "Top 100", count: 100 },
    { name: "Top 1,000", count: 1000 },
    { name: "Top 5,000", count: 5000 },
    { name: "All Remaining (Long Tail)", count: allSubmissions.length },
  ];

  let totalGlobalChars = 0;
  const submissionCharCounts = allSubmissions.map((sub) => {
    const count = Number(sub.total_length || 0);
    totalGlobalChars += count;
    return count;
  });

  if (totalGlobalChars === 0) {
    console.log("No data found to project.");
    process.exit(0);
  }

  // Calculate cumulative tiers

  let lastIndex = 0;

  console.log("=====================================================");
  console.log("                  TIER BREAKDOWN                     ");
  console.log("=====================================================");

  for (let i = 0; i < tiers.length; i++) {
    const tier = tiers[i];
    let charVolumeForTier = 0;

    if (i === tiers.length - 1) {
      // The long tail (rest of array)
      for (let j = lastIndex; j < submissionCharCounts.length; j++) {
        charVolumeForTier += submissionCharCounts[j];
      }
    } else {
      const endIndex = Math.min(tier.count, submissionCharCounts.length);
      for (let j = 0; j < endIndex; j++) {
        charVolumeForTier += submissionCharCounts[j];
      }
      // Since it's cumulative (Top 100 vs Top 1000), we only want the *marginal* chars
      // if we were running them side by side, but the user expects the cost *total* if they cap at Top N.
      // Actually, let's show the Absolute metrics just for that Tier block!
      const startIdx = lastIndex;
      const endIdx = endIndex;
      charVolumeForTier = 0;
      for (let j = startIdx; j < endIdx; j++) {
        charVolumeForTier += submissionCharCounts[j];
      }
      lastIndex = endIdx;
      tier.name = i === 0 ? tier.name : `Next ${tier.count - tiers[i - 1].count} Posts`;
    }

    const percentageOfBase = ((charVolumeForTier / totalGlobalChars) * 100).toFixed(1);
    const estimatedCost = (charVolumeForTier / 1000) * costPerThousandChars;

    console.log(`Tier:      ${tier.name}`);
    console.log(`Chars:     ${charVolumeForTier.toLocaleString()} (${percentageOfBase}%)`);
    console.log(`Est. Cost: $${estimatedCost.toFixed(2)}`);
    console.log("-----------------------------------------------------");
  }

  const grandTotalCost = (totalGlobalChars / 1000) * costPerThousandChars;

  console.log("=====================================================");
  console.log("                GRAND TOTAL ESTIMATE                 ");
  console.log("=====================================================");
  console.log(`Total Posts:      ${allSubmissions.length.toLocaleString()}`);
  console.log(`Total Characters: ${totalGlobalChars.toLocaleString()}`);
  console.log(`Grand Total Cost: $${grandTotalCost.toFixed(2)}`);
  console.log("=====================================================\n");

  console.log("=====================================================");
  console.log("             SIGNAL GATEWAY (PRUNING)                ");
  console.log("=====================================================");
  console.log("If we prune posts with very little text (e.g. < 50 chars),");
  console.log("how many posts can we completely skip?");

  const lengthBuckets = { under50: 0, under100: 0, under500: 0, under1000: 0, over1000: 0 };

  allSubmissions.forEach((sub) => {
    const len = Number(sub.total_length || 0);
    if (len < 50) lengthBuckets.under50++;
    else if (len < 100) lengthBuckets.under100++;
    else if (len < 500) lengthBuckets.under500++;
    else if (len < 1000) lengthBuckets.under1000++;
    else lengthBuckets.over1000++;
  });

  const total = allSubmissions.length;
  console.log(
    `- Under 50 chars:   ${lengthBuckets.under50.toLocaleString()} posts (${((lengthBuckets.under50 / total) * 100).toFixed(1)}%)`,
  );
  console.log(
    `- 50 to 100 chars:  ${lengthBuckets.under100.toLocaleString()} posts (${((lengthBuckets.under100 / total) * 100).toFixed(1)}%)`,
  );
  console.log(
    `- 100 to 500 chars: ${lengthBuckets.under500.toLocaleString()} posts (${((lengthBuckets.under500 / total) * 100).toFixed(1)}%)`,
  );
  console.log(
    `- 500 to 1k chars:  ${lengthBuckets.under1000.toLocaleString()} posts (${((lengthBuckets.under1000 / total) * 100).toFixed(1)}%)`,
  );
  console.log(
    `- Over 1k chars:    ${lengthBuckets.over1000.toLocaleString()} posts (${((lengthBuckets.over1000 / total) * 100).toFixed(1)}%)`,
  );
  console.log("=====================================================\n");
}

estimateCosts()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
