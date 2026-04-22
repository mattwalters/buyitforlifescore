import { AssetRegistry } from "./registry";
import { getDuckDB, queryDuckDB } from "./duckdb";
import { resolve } from "path";
import { prisma } from "@mono/db";

export interface MaterializationTarget {
  assetId: string;
  partitionKey?: string;
}

export interface QaResult {
  ruleName: string;
  passed: boolean;
  severity: "warn" | "error";
}

export interface AssetSummaryPayload {
  totalRows: number;
  columns: any[];
  qaResults: QaResult[];
}

export async function executeMaterialization(
  target: MaterializationTarget,
  jobId?: string,
): Promise<AssetSummaryPayload> {
  const asset = AssetRegistry[target.assetId];
  if (!asset) {
    throw new Error(`Asset ${target.assetId} not found in registry.`);
  }

  // 1. Resolve R2 Path
  // e.g. "s3://[bucket]/bronze/reddit_comments/2024-04.parquet"
  let dataPath = "";
  if (asset.storagePathTemplate) {
    dataPath = asset.storagePathTemplate;
  } else {
    const bucket = process.env.R2_BUCKET_NAME || "kiss-data";
    const suffix = target.partitionKey ? `/${target.partitionKey}.parquet` : ".parquet";
    dataPath = `s3://${bucket}/${asset.layer}/${asset.id}${suffix}`;
  }

  // Fallback testing local logic if env is missing
  if (!process.env.CLOUDFLARE_ACCOUNT_ID) {
    // We gracefully default to the local test files for the developer machine
    const year = new Date().getFullYear();
    const month = new Date().getMonth() + 1;
    dataPath = resolve(process.cwd(), `sample_${year}_${month}.parquet`);
  }

  console.log(`[EXECUTOR] Executing Materialization for ${target.assetId} at ${dataPath}`);

  const db = await getDuckDB();

  try {
    // ----------------------------------------------------
    // Step A: DUCKDB SUMMARIZE
    // ----------------------------------------------------
    const resCount = await queryDuckDB(
      db,
      `SELECT COUNT(*) as count FROM read_parquet('${dataPath}')`,
    );
    const totalRows: number = Number(resCount[0]?.count) || 0;

    const summaryData = await queryDuckDB(
      db,
      `SUMMARIZE SELECT * FROM read_parquet('${dataPath}')`,
    );

    // ----------------------------------------------------
    // Step B: AUTOMATED QA ENGINE
    // ----------------------------------------------------
    const qaResults: QaResult[] = [];
    if (asset.qualityRules && asset.qualityRules.length > 0) {
      console.log(`[EXECUTOR] Running ${asset.qualityRules.length} QA Rules...`);
      for (const rule of asset.qualityRules) {
        try {
          const sql = rule.sqlTemplate.replace(/\{\{target\}\}/g, dataPath);
          const qaQuery = await queryDuckDB(db, sql);

          const passed = Boolean(qaQuery[0]?.passed); // assumes 'AS passed' output
          qaResults.push({
            ruleName: rule.name,
            severity: rule.severity,
            passed,
          });
        } catch (e: any) {
          console.error(`[EXECUTOR] QA Rule Failed: ${rule.name}`, e.message);
          qaResults.push({
            ruleName: rule.name,
            severity: rule.severity,
            passed: false,
          });
        }
      }
    }

    const payload: AssetSummaryPayload = {
      totalRows,
      columns: summaryData,
      qaResults,
    };

    // ----------------------------------------------------
    // Step C: LOG STATE TO POSTGRES (If job tracking runs)
    // ----------------------------------------------------
    if (jobId) {
      const anyCriticalError = qaResults.some((q) => !q.passed && q.severity === "error");
      const finalStatus = anyCriticalError ? "FAILED" : "COMPLETED";

      await prisma.kissJob.update({
        where: { id: jobId },
        data: {
          status: finalStatus,
          completedAt: new Date(),
        },
      });

      if (finalStatus === "COMPLETED") {
        await prisma.kissMaterialization.create({
          data: {
            jobId,
            assetId: target.assetId,
            partitionKey: target.partitionKey,
            summaryPayload: payload as any,
          },
        });
      }
    }

    return payload;
  } catch (error: any) {
    if (jobId) {
      await prisma.kissJob.update({
        where: { id: jobId },
        data: {
          status: "FAILED",
          completedAt: new Date(),
          errorTrace: error.message,
        },
      });
    }
    throw error;
  }
}
