import { prisma } from "@mono/db";
import { GoogleGenAI } from "@google/genai";
import { env } from "../env.js";
const ai = new GoogleGenAI({ apiKey: env.GEMINI_API_KEY });
const SIMILARITY_THRESHOLD = 0.85;

async function embedText(text: string): Promise<{ vector: number[], estimatedTokens: number }> {
  const response = await ai.models.embedContent({
    model: "gemini-embedding-2-preview",
    contents: text,
    config: {
      outputDimensionality: 256,
    }
  });

  if (!response.embeddings || response.embeddings.length === 0 || !response.embeddings[0].values) {
    throw new Error("Failed to generate embedding");
  }

  // Gemini SDK might not expose usage on embedContent, so we'll estimate: ~4 chars per token
  const estimatedTokens = Math.ceil(text.length / 4);

  return { 
    vector: response.embeddings[0].values,
    estimatedTokens
  };
}

async function runResolution() {
  console.log("🚀 Starting Vector Resolution Pipeline...");

  // 1. Fetch Silver records needing an embedding or resolution
  const unmapped = await prisma.silverProductMention.findMany({
    where: { goldProductId: null },
    take: 1000 // Batch size
  });

  console.log(`Found ${unmapped.length} unmapped Silver mentions.`);

  for (const mention of unmapped) {
    try {
      // 2. Construct canonical identity string
      const identityString = `Brand: ${mention.rawBrand}. Product: ${mention.rawProductName}. Category: ${mention.seedCategory}.`;
      
      console.log(`\nProcessing [${mention.id}]: ${identityString}`);
      
      // Let's check via raw sql if the embedding exists first (since Prisma doesn't return `Unsupported` fields in standard `.findMany`)
      const rawQueryResult = await prisma.$queryRaw<{ embedding: string | null }[]>`SELECT embedding::text FROM "SilverProductMention" WHERE id = ${mention.id}`;
      const existingEmbeddingStr = rawQueryResult[0]?.embedding;
      
      let vector: number[] = [];

      if (!existingEmbeddingStr) {
        // We need to generate the vector
        console.log(`  -> Synthesizing vector...`);
        const result = await embedText(identityString);
        vector = result.vector;

        // Log spend
        const { calculateCost, AiModel } = await import("../pricing.js");
        const modelEnum = AiModel.GEMINI_EMBEDDING_2_PREVIEW;
        
        // Calculate cost manually based on token estimate
        // The calculateCost function takes a usage object `{ promptTokenCount: X }`.
        const usage = { promptTokenCount: result.estimatedTokens };
        const cost = calculateCost(modelEnum, usage);

        await prisma.aiSpend.create({
          data: {
            jobName: "WORKER_VECTOR_RESOLUTION",
            submissionId: mention.submissionId, // We map it back to the original source if possible
            model: "gemini-embedding-2-preview",
            promptTokens: result.estimatedTokens,
            costInUsd: cost,
          }
        });

        // Update the mention with the new embedding
        await prisma.$executeRaw`
          UPDATE "SilverProductMention"
          SET embedding = ${JSON.stringify(vector)}::vector
          WHERE id = ${mention.id}
        `;
      } else {
        vector = JSON.parse(existingEmbeddingStr) as number[];
      }

      // 3. Find closest GoldProduct using Cosine Distance <=>
      // <= (1 - THRESHOLD) because distance is 1 - similarity.
      // E.g. Similarity > 0.85 means Distance < 0.15
      const maxDistance = 1.0 - SIMILARITY_THRESHOLD;

      const candidates = await prisma.$queryRaw<{ id: string, name: string, dist: number }[]>`
        SELECT id, "canonicalName" as name, embedding <=> ${JSON.stringify(vector)}::vector AS dist
        FROM "GoldProduct"
        WHERE embedding <=> ${JSON.stringify(vector)}::vector <= ${maxDistance}
        ORDER BY dist ASC
        LIMIT 1
      `;

      if (candidates.length > 0) {
        const match = candidates[0];
        console.log(`  ✅ MATCHED: GoldProduct [${match.id}] '${match.name}' (Distance: ${match.dist.toFixed(4)})`);
        
        // Link it
        await prisma.silverProductMention.update({
          where: { id: mention.id },
          data: { goldProductId: match.id }
        });

        // Optionally, update mention counts
        await prisma.goldProduct.update({
          where: { id: match.id },
          data: {
            mentionCount: { increment: 1 }
          }
        });

      } else {
        console.log(`  🌟 NEW CANONICAL ENTRY: Creating GoldProduct...`);
        
        // Ensure canonical names are clean
        const newCanonicalName = `${mention.rawBrand} ${mention.rawProductName}`;
        
        // Insert GoldProduct with embedding via Raw SQL to support pgvector typing properly if Prisma doesn't map it.
        const pgId = (await prisma.goldProduct.create({
          data: {
            canonicalName: newCanonicalName,
            brand: mention.rawBrand,
            category: mention.seedCategory,
            mentionCount: 1,
            avgSentiment: mention.sentiment === 'POSITIVE' ? 1 : mention.sentiment === 'NEGATIVE' ? -1 : 0
          }
        })).id;
        
        // Update its vector
        await prisma.$executeRaw`
          UPDATE "GoldProduct"
          SET embedding = ${JSON.stringify(vector)}::vector
          WHERE id = ${pgId}
        `;
        
        // Link Silver
        await prisma.silverProductMention.update({
          where: { id: mention.id },
          data: { goldProductId: pgId }
        });
      }

    } catch (e) {
      console.error(`Error processing ${mention.id}:`, e);
    }
  }

  console.log("\n✅ Pipeline complete.");
}

runResolution()
  .catch(e => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
