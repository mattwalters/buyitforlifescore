import { getDuckDB, execDuckDB } from "../lib/duckdb";
import { resolve } from "path";

/**
 * Example idempotent ingest script generating a dummy dataset 
 * and writing it as a partition to R2.
 */
async function main() {
  console.log("Starting Kiss Ingestion Pipeline...");
  
  const bucketName = process.env.R2_BUCKET_NAME || "kiss-data";
  
  const year = new Date().getFullYear();
  const month = new Date().getMonth() + 1;
  const partitionPath = `year=${year}/month=${month}`;
  
  console.log(`Targeting partition: ${partitionPath}`);
  
  const db = await getDuckDB();
  
  // Create a dummy table with some random metrics that would be analogous to our "quality checks"
  console.log("Generating dummy pipeline data...");
  await execDuckDB(db, `
    CREATE TABLE dummy_data AS
    SELECT 
      generate_series AS id,
      (RANDOM() * 100)::INT AS quality_score,
      (RANDOM() * 50)::INT AS processing_time_ms,
      CASE WHEN RANDOM() > 0.8 THEN 'error' ELSE 'success' END AS status,
      'node_' || (RANDOM() * 10)::INT AS source_node
    FROM generate_series(1, 1000);
  `);
  
  // Form the target path. If no credentials, we fall back to a local file for testing.
  let targetPath = `s3://${bucketName}/silver/dummy_metrics/${partitionPath}/data.parquet`;
  
  if (!process.env.CLOUDFLARE_ACCOUNT_ID) {
    console.warn("⚠️ No R2 Credentials found. Writing to local file instead.");
    const localPath = resolve(process.cwd(), `sample_${year}_${month}.parquet`);
    targetPath = localPath;
  }
  
  console.log(`Writing Parquet to ${targetPath}...`);
  try {
    // Write out optimized Parquet
    await execDuckDB(db, `
      COPY dummy_data TO '${targetPath}' (FORMAT PARQUET, COMPRESSION ZSTD);
    `);
    console.log(`✅ Successfully wrote partition to ${targetPath}`);
  } catch (error) {
    console.error("❌ Failed to write Parquet data:", error);
  }
}

main().catch(console.error);
