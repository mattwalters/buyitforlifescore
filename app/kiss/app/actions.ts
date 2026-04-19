"use server";

import { getDuckDB, queryDuckDB } from "../lib/duckdb";
import { resolve } from "path";

export async function fetchQualityMetrics() {
  const bucketName = process.env.R2_BUCKET_NAME || "kiss-data";

  // Try to find the file either locally or on R2 depending on env
  let dataPath = `s3://${bucketName}/silver/dummy_metrics/*/*/data.parquet`;

  if (!process.env.CLOUDFLARE_ACCOUNT_ID) {
    const year = new Date().getFullYear();
    const month = new Date().getMonth() + 1;
    dataPath = resolve(process.cwd(), `sample_${year}_${month}.parquet`);
  }

  const db = await getDuckDB();

  try {
    // We aggregate the data using DuckDB natively before sending JSON to the frontend Recharts
    const query = `
      SELECT 
        source_node,
        AVG(quality_score) as avg_quality,
        AVG(processing_time_ms) as avg_processing_time,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_count,
        COUNT(*) as total_runs
      FROM read_parquet('${dataPath}')
      GROUP BY source_node
      ORDER BY source_node ASC;
    `;

    type MetricRow = {
      source_node: string;
      avg_quality: number;
      avg_processing_time: number;
      error_count: number;
      total_runs: number;
    };

    const rows = await queryDuckDB<MetricRow>(db, query);
    return { success: true, data: rows };
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("DuckDB Query Error:", message);
    return { success: false, error: message };
  }
}
