/* eslint-disable @typescript-eslint/no-explicit-any */
 
 
 
import "dotenv/config";
import duckdb from "duckdb";
import { prisma } from "@mono/db";

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

const db = new duckdb.Database(":memory:");

function parseCreatedUtc(val: unknown): string {
  if (val == null) return new Date(0).toISOString();
  const str = val.toString().replace(/"/g, "");
  if (!isNaN(Number(str))) {
    return new Date(Number(str) * 1000).toISOString();
  }
  return new Date(0).toISOString();
}

const args = process.argv.slice(2);
const limitArg = args.find((a) => a.startsWith("--limit="));
const MAX_LIMIT = limitArg ? parseInt(limitArg.split("=")[1], 10) : Infinity;

const BATCH_SIZE = 1000;
const API_URL = "http://admin.buyitforlifeclub.localhost/api/ingest";

async function ingestType(type: "submissions" | "comments") {
  let offset = 0;
  const filepath =
    type === "submissions"
      ? "../../data/BuyItForLife_submissions.parquet"
      : "../../data/BuyItForLife_comments.parquet";

  console.log(`🚀 Starting ingest for ${type}...`);
  let totalProcessed = 0;

  while (totalProcessed < MAX_LIMIT) {
    const fetchLimit = Math.min(BATCH_SIZE, MAX_LIMIT - totalProcessed);
    console.log(`📥 Ingest [${type}] offset: ${offset}, limit: ${fetchLimit}`);

    const rows: Record<string, unknown>[] = await new Promise((resolve, reject) => {
      db.all(`SELECT * FROM '${filepath}' LIMIT ${fetchLimit} OFFSET ${offset}`, (err, res) => {
        if (err) reject(err);
        else resolve(res as Record<string, unknown>[]);
      });
    });

    if (rows.length === 0) {
      console.log(`✅ Finished processing ${type}! No more records found.`);
      break;
    }

    let payloadData: Record<string, unknown>[] = [];

    if (type === "submissions") {
      payloadData = rows
        .map((r) => ({
          redditId: r.id?.toString() || "",
          title: r.title?.toString().replace(/\0/g, "") || "",
          selftext: r.selftext ? r.selftext.toString().replace(/\0/g, "") : null,
          author: r.author ? r.author.toString().replace(/\0/g, "") : null,
          score: r.score != null ? Number(r.score) : r.downs != null ? Number(r.downs) : 0,
          url: r.url ? r.url.toString().replace(/\0/g, "") : null,
          permalink: r.permalink ? r.permalink.toString().replace(/\0/g, "") : null,
          numComments: r.num_comments != null ? Number(r.num_comments) : 0,
          postedAt: parseCreatedUtc(r.created_utc),
        }))
        .filter((x) => x.redditId !== "");
    } else if (type === "comments") {
      payloadData = rows
        .map((r) => {
          let pId = r.parent_id?.toString() || null;
          if (pId && pId.startsWith('"') && pId.endsWith('"')) {
            pId = pId.slice(1, -1);
          }
          return {
            redditId: r.id?.toString() || "",
            linkId: r.link_id?.toString() || "",
            parentId: pId,
            body: r.body?.toString().replace(/\0/g, "") || "",
            author: r.author ? r.author.toString().replace(/\0/g, "") : null,
            score: r.score != null ? Number(r.score) : r.ups != null ? Number(r.ups) : 0,
            postedAt: parseCreatedUtc(r.created_utc),
          };
        })
        .filter((x) => x.redditId !== "");
    }

    if (payloadData.length > 0) {
      try {
        const response = await fetch(API_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ type, data: payloadData }),
        });

        if (!response.ok) {
          const body = await response.text();
          throw new Error(`API returned ${response.status}: ${body}`);
        }

        const json = await response.json();
        console.log(`✅ Successfully sent ${json.count || payloadData.length} ${type} to the API.`);
      } catch (e) {
        console.error(`💥 Error pushing to API for ${type} at offset ${offset}:`, e);
        process.exit(1);
      }
    }

    offset += fetchLimit;
    totalProcessed += fetchLimit;
  }
}

async function run() {
  await ingestType("submissions");
  await ingestType("comments");

  console.log("📊 Computing global character densities...");
  await prisma.$executeRaw`
      WITH c_agg AS (
         SELECT "submissionId", SUM(LENGTH(body))::integer as total_len
         FROM "BronzeRedditComment"
         GROUP BY "submissionId"
      )
      UPDATE "BronzeRedditSubmission" s
      SET "charCount" = LENGTH(s.title) + COALESCE(LENGTH(s.selftext), 0) + COALESCE(c_agg.total_len, 0)
      FROM c_agg
      WHERE s.id = c_agg."submissionId";
  `;

  await prisma.$executeRaw`
      UPDATE "BronzeRedditSubmission" s
      SET "charCount" = LENGTH(s.title) + COALESCE(LENGTH(s.selftext), 0)
      WHERE "charCount" IS NULL;
  `;

  console.log("🎉 All ingestion and density computation complete!");
  process.exit(0);
}

run().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
