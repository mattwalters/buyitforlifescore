import { executeMaterialization } from "../lib/executor";
import { parseArgs } from "util";

/**
 * CLI Entrypoint for manually kicking off a local materialization run.
 * Usage: npm run kiss:summarize -- --asset=reddit_buyitforlife_comments --partition=2024-04
 */
async function main() {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      asset: {
        type: "string",
      },
      partition: {
        type: "string",
      },
      jobId: {
        type: "string", // Optionally pass a job ID to log to postgres
      },
    },
  });

  if (!values.asset) {
    console.error("❌ Error: Must provide --asset=<asset_id>");
    process.exit(1);
  }

  try {
    console.log(`🚀 Starting Materialization for Asset: ${values.asset}`);
    if (values.partition) console.log(`👉 Partition: ${values.partition}`);

    // We await the abstracted execute block.
    const result = await executeMaterialization(
      {
        assetId: values.asset,
        partitionKey: values.partition,
      },
      values.jobId,
    );

    console.log("\n✅ Materialization Successful!");
    console.log(`📊 Rows Analyzed: ${result.totalRows.toLocaleString()}`);
    console.log(`📊 Columns Handled: ${result.columns.length}`);

    if (result.qaResults.length > 0) {
      console.log(`\n🧪 QA Results:`);
      result.qaResults.forEach((qa) => {
        const icon = qa.passed ? "✅" : qa.severity === "error" ? "❌" : "⚠️";
        console.log(`${icon} [${qa.severity.toUpperCase()}] ${qa.ruleName}`);
      });
    }

    process.exit(0);
  } catch (error: any) {
    console.error("\n❌ Fatal Execution Error:", error.message);
    process.exit(1);
  }
}

main();
