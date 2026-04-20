import { fetchAssetSummary } from "./app/kiss/app/assets/actions";
import { getAssetHistory, getJobs } from "./app/kiss/app/assets/db-actions";

async function main() {
  console.log("Fetching jobs...");
  const jobs = await getJobs("reddit_buyitforlife_comments");
  console.log("Jobs:", jobs);

  console.log("Fetching DB history...");
  const history = await getAssetHistory("reddit_buyitforlife_comments");
  console.log("History:", history);

  console.log("Fetching duckdb summary...");
  const duckdbRes = await fetchAssetSummary("bronze", "reddit_buyitforlife_comments");
  console.log("DuckDB:", duckdbRes.success ? "Success" : duckdbRes.error);
}

main().catch(console.error);
