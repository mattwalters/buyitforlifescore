"use server";

import { getDuckDB, queryDuckDB } from "../../lib/duckdb";
import { resolve } from "path";

type SchemaRow = {
  column_name: string;
  column_type: string;
  null: string;
  key: string;
  default: string;
  extra: string;
};

export async function fetchAssetSummary(layer: string, assetName: string) {
  const bucketName = process.env.R2_BUCKET_NAME || "kiss-data";

  // Construct R2 Object Storage Path
  // In Dagster-inspired formats, it could be flat: `s3://bucket/{layer}/{assetName}.parquet`
  // or wildcard: `s3://bucket/{layer}/{assetName}/**/*.parquet`
  let dataPath = `s3://${bucketName}/${layer}/${assetName}.parquet`;

  if (!process.env.CLOUDFLARE_ACCOUNT_ID) {
    // Graceful degrading locally if not connected to R2
    const year = new Date().getFullYear();
    const month = new Date().getMonth() + 1;
    // For pure testing we fall back to the dummy file we made
    dataPath = resolve(process.cwd(), `sample_${year}_${month}.parquet`);
  }

  const db = await getDuckDB();

  try {
    // 1. DuckDB: Super fast count without loading full columns
    const resCount = await queryDuckDB(
      db,
      `SELECT COUNT(*) as count FROM read_parquet('${dataPath}')`,
    );
    const totalRows: number = Number(resCount[0]?.count) || 0;

    // 2. DuckDB: Fetch native schema natively from Parquet file headers
    const schemaDefs = await queryDuckDB<SchemaRow>(
      db,
      `DESCRIBE SELECT * FROM read_parquet('${dataPath}')`,
    );

    // 3. DuckDB: Preview first 10 rows
    const preview = await queryDuckDB(db, `SELECT * FROM read_parquet('${dataPath}') LIMIT 10`);

    return {
      success: true,
      data: {
        totalRows,
        schema: schemaDefs,
        preview,
      },
    };
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`DuckDB Metadata Fetch Error [${layer}/${assetName}]:`, message);
    return { success: false, error: message };
  }
}
