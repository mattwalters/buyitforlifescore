import { prisma } from "@mono/db";
import { intro, outro, multiselect, spinner, cancel, confirm } from "@clack/prompts";

async function doClobber(tiers: Set<string>) {
  const spin = spinner();
  const summary: string[] = [];

  if (tiers.has("all")) {
    spin.start("☢️  Executing NUCLEAR Truncation (Instant Wipe)...");
    // Bypasses MVCC row scans; takes ~5ms instead of 30 seconds.
    await prisma.$executeRawUnsafe(`
      TRUNCATE TABLE "BronzeRedditSubmission", "BronzeRedditComment", "SilverProductMention", 
                     "GoldBrand", "GoldProductLine", "GoldProduct", 
                     "GoldDepartment", "GoldCategory", "SilverCategoryIdea", "AiSpend" CASCADE;
    `);
    summary.push(`[NUCLEAR]: Instantly dropped all tuples across 10 tables.`);
    spin.stop("Nuclear Clobber Complete");
    return summary;
  }

  if (tiers.has("ai") || tiers.has("all")) {
    spin.start("Clobbering AI Spend log tier...");
    const spendRes = await prisma.aiSpend.deleteMany({});
    summary.push(`[AI SPEND]: Deleted ${spendRes.count} logs.`);
    spin.stop("AI Spend Clobbered");
  }

  if (tiers.has("gold")) {
    spin.start("Clobbering GOLD tier...");
    const resProducts = await prisma.goldProduct.deleteMany({});
    const resLines = await prisma.goldProductLine.deleteMany({});
    const resBrands = await prisma.goldBrand.deleteMany({});
    summary.push(
      `[GOLD]: Deleted ${resBrands.count} Brands, ${resLines.count} Lines, and ${resProducts.count} Products.`,
    );
    spin.stop("GOLD Tier Clobbered");
  }

  if (tiers.has("taxonomy")) {
    spin.start("Clobbering TAXONOMY tier...");
    const resIdea = await prisma.silverCategoryIdea.deleteMany({});
    const resCat = await prisma.goldCategory.deleteMany({});
    const resDept = await prisma.goldDepartment.deleteMany({});
    summary.push(
      `[TAXONOMY]: Deleted ${resDept.count} Departments, ${resCat.count} Categories, and ${resIdea.count} Ideas.`,
    );
    spin.stop("TAXONOMY Tier Clobbered");
  }

  if (tiers.has("silver")) {
    spin.start("Clobbering SILVER tier...");
    const res = await prisma.silverProductMention.deleteMany({});

    // Always reset bronze if we clear silver so it can be re-run
    const subRes = await prisma.bronzeRedditSubmission.updateMany({
      where: { isProcessed: true },
      data: { isProcessed: false },
    });
    const comRes = await prisma.bronzeRedditComment.updateMany({
      where: { isProcessed: true },
      data: { isProcessed: false },
    });

    summary.push(
      `[SILVER]: Deleted ${res.count} Mentions. Un-processed ${subRes.count} Bronze Submissions and ${comRes.count} Comments.`,
    );
    spin.stop("SILVER Tier Clobbered");
  }

  if (tiers.has("bronze")) {
    spin.start("Clobbering BRONZE tier (Raw TRUNCATE)...");

    // Use TRUNCATE CASCADE to drop 300k+ rows instantly instead of Prisma row-scanning deleteMany
    await prisma.$executeRawUnsafe(`TRUNCATE TABLE "BronzeRedditSubmission" CASCADE;`);
    summary.push(`[BRONZE]: Truncated Submissions, Comments, and cascading dependencies.`);
    spin.stop("BRONZE Tier Clobbered");
  }

  return summary;
}

async function main() {
  const args = process.argv.slice(2);
  let tiers = new Set(args.map((a) => a.toLowerCase()));

  // Headless mode for pipeline execution
  if (tiers.size > 0) {
    const summary = await doClobber(tiers);
    console.log(`\n--- CLOBBER SUMMARY ---`);
    for (const msg of summary) {
      console.log(msg);
    }
    process.exit(0);
  }

  // Interactive Mode
  console.clear();
  intro("🧨 Database Clobber Tool 🧨");

  const selectedTiers = await multiselect({
    message: "Select which data tiers you would like to permanently delete:",
    options: [
      { value: "bronze", label: "Bronze (Submissions/Comments)", hint: "The absolute raw data." },
      {
        value: "silver",
        label: "Silver (Mentions)",
        hint: "The extracted mentions. Deleting will un-process Bronze.",
      },
      {
        value: "gold",
        label: "Gold (Brands/Lines/Products)",
        hint: "The rolled up hierarchical entities.",
      },
      {
        value: "taxonomy",
        label: "Taxonomy (Categories/Departments)",
        hint: "The organic mapping tables.",
      },
      { value: "ai", label: "AI Spend (Audit Logs)", hint: "The financial tracking records." },
      { value: "all", label: "Nuclear Option (ALL TIERS)", hint: "Wipe everything." },
    ],
    required: false,
  });

  if (
    !selectedTiers ||
    typeof selectedTiers === "symbol" ||
    (Array.isArray(selectedTiers) && selectedTiers.length === 0)
  ) {
    cancel("Operation cancelled by user.");
    process.exit(0);
  }

  tiers = new Set(selectedTiers as string[]);

  if (tiers.has("all")) {
    const isSure = await confirm({
      message:
        "☢️ WARNING: You selected the Nuclear Option. This will completely wipe all Bronze, Silver, Gold, Taxonomy, and AI data. Are you absolutely sure?",
      active: "Yes, Nuke it",
      inactive: "Cancel",
    });

    if (!isSure || typeof isSure === "symbol") {
      cancel("Nuclear clobber aborted. Database is safe.");
      process.exit(0);
    }
  }

  const summary = await doClobber(tiers);

  let formattedSummary = "Results:\n";
  summary.forEach((s) => (formattedSummary += `  -> ${s}\n`));

  outro(formattedSummary + "\n✅ Clobber Complete!");
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
    process.exit(0);
  });
