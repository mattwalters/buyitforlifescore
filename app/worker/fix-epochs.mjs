import { prisma } from "@mono/db";
import duckdb from "duckdb";

const db = new duckdb.Database(":memory:");

function chunkArray(array, size) {
  const result = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
}

async function fixTable(type, parquetPath, dbModel, idColParam = "redditId") {
  console.log(`\nStarting fix for ${type}...`);
  const rows = await new Promise((resolve, reject) => {
    db.all(
      `SELECT id, created_utc 
       FROM '${parquetPath}' 
       WHERE created_utc LIKE '"%"'`,
      (err, res) => {
        if (err) reject(err);
        else resolve(res);
      },
    );
  });

  console.log(`Found ${rows.length} rows in ${type} parquet with quoted dates.`);

  const updates = rows.map((r) => {
    const raw = r.created_utc.toString().replace(/"/g, "");
    return {
      id: r.id.toString(),
      date: new Date(Number(raw) * 1000).toISOString(),
    };
  });

  const chunks = chunkArray(updates, 1000);
  let processed = 0;

  for (const chunk of chunks) {
    const valuesQuery = chunk.map((u) => `('${u.id}', '${u.date}'::timestamp)`).join(", ");

    const sql = `
      UPDATE "${type}" AS t
      SET "postedAt" = v.date
      FROM (VALUES ${valuesQuery}) AS v(id, date)
      WHERE t."${idColParam}" = v.id;
    `;

    try {
      await prisma.$executeRawUnsafe(sql);
      processed += chunk.length;
      if (processed % 10000 === 0) {
        process.stdout.write(`\rProgress: ${processed}/${rows.length}`);
      }
    } catch (e) {
      console.error("\nError during executeRawUnsafe chunk: ", e.message);
    }
  }

  process.stdout.write(`\rProgress: ${processed}/${rows.length}\n`);
  console.log(`Finished ${type}!`);
}

async function main() {
  await fixTable(
    "RedditSubmission",
    "../../data/BuyItForLife_submissions.parquet",
    prisma.redditSubmission,
  );

  await fixTable("RedditComment", "../../data/BuyItForLife_comments.parquet", prisma.redditComment);
}

main()
  .catch(console.error)
  .finally(() => prisma.$disconnect());
