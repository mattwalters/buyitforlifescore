/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */

import { prisma } from "@mono/db";
import { AiModel } from "../../worker/src/pricing.js";
import { embedBatchWithRetry } from "./local-embedder.js";
import * as dotenv from "dotenv";
dotenv.config({ path: "../../.env" });

async function main() {
  const CONCURRENCY = process.argv.includes("--concurrency")
    ? parseInt(process.argv[process.argv.indexOf("--concurrency") + 1], 10)
    : 10;

  console.log(`[Embedding] 🧹 Fetching Silver mentions without embeddings...`);

  const mentions = await prisma.silverProductMention.findMany({
    where: {
      // Prisma raw doesn't map Unsupported natively well in where clauses simply without being explicitly null,
      // Wait, Prisma can query Unsupported type if you use raw.
      // Actually we know we want anything that doesn't have an embedding.
      // But standard Prisma client `findMany` filters on unsupported types aren't allowed.
      // So let's fetch an array of IDs via raw SQL and then map them.
    },
  });

  // We actually need to use $queryRaw to safely get records where embedding IS NULL
  const rawNulls = await prisma.$queryRaw<{ id: string }[]>`
    SELECT id FROM "SilverProductMention" 
    WHERE embedding IS NULL
  `;

  if (rawNulls.length === 0) {
    console.log(`[Embedding] ✅ No Silver mentions found missing embeddings.`);
    return;
  }

  // Fetch the full records for those IDs
  const unmappedIds = rawNulls.map((r) => r.id);
  const items = await prisma.silverProductMention.findMany({
    where: { id: { in: unmappedIds } },
  });

  const BATCH_SIZE = Math.min(CONCURRENCY * 2, 64);
  console.log(
    `[Embedding] 📦 Embedding ${items.length} Silver Mentions (Batch Size: ${BATCH_SIZE})...`,
  );

  const total = items.length;
  let successCount = 0;

  for (let i = 0; i < items.length; i += BATCH_SIZE) {
    const chunk = items.slice(i, i + BATCH_SIZE);

    console.log(
      `\n[Embedding] 🔍 Batch [${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(total / BATCH_SIZE)}] Embedding ${chunk.length} items natively in ONNX...`,
    );

    const texts = chunk.map((m) => `${m.brand.trim()} ${m.productName.trim()}`.toLowerCase());

    try {
      const vectorValues = await embedBatchWithRetry(texts);

      if (vectorValues && vectorValues.length === chunk.length) {
        const dbWrites = chunk.map((m, idx) => {
          const vec = vectorValues[idx];
          if (vec && vec.length > 0) {
            const vectorLiteral = `[${vec.join(",")}]`;
            return prisma.$executeRaw`
                 UPDATE "SilverProductMention" 
                 SET embedding = ${vectorLiteral}::vector
                 WHERE id = ${m.id};
               `.then(() => {
              successCount++;
            });
          }
          return Promise.resolve();
        });

        await Promise.all(dbWrites);
      } else {
        console.warn(
          `   [Embedding] ⚠️ Batch output length mismatch or failure. Expected ${chunk.length}, got ${vectorValues?.length}.`,
        );
      }
    } catch (err: unknown) {
      console.error(
        `   [Embedding] ❌ Failed to generate or save batch. Aborting the chunk so it can retry later.`,
        (err as any)?.message || err,
      );
    }
  }

  console.log(
    `\n[Embedding] ✅ Silver Embedding Sweep complete! Successfully embedded ${successCount}/${total} products.`,
  );
}

main()
  .catch((e) => {
    console.error("Fatal error:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
