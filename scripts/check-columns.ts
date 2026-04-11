import { prisma } from "../lib/db/prisma";

async function check() {
  const result = await prisma.$queryRawUnsafe(`
    SELECT column_name, data_type, udt_name, character_maximum_length, numeric_precision
    FROM information_schema.columns
    WHERE table_name = 'SilverProductMention' AND column_name LIKE 'embedding%';
  `);
  console.log("Postgres Columns for SilverProductMention:", result);
  process.exit(0);
}
check();
