import { prisma } from "@mono/db";
import duckdb from "duckdb";

async function main() {
  const zeroEpochIds = await prisma.redditSubmission.findMany({
    where: { postedAt: new Date(0) },
    select: { redditId: true },
    take: 5,
  });

  const ids = zeroEpochIds.map((x) => `'${x.redditId}'`).join(", ");

  if (ids.length === 0) {
    console.log("No epoch 0 found in postgres.");
    return;
  }

  console.log("Finding these IDs in parquet:", ids);

  const db = new duckdb.Database(":memory:");
  db.all(
    `
    SELECT id, created_utc, typeof(created_utc) as type
    FROM '../../data/BuyItForLife_submissions.parquet' 
    WHERE id IN (${ids})
  `,
    (err, res) => {
      if (err) console.error(err);
      else console.table(res);
    },
  );
}

main().finally(() => prisma.$disconnect());
