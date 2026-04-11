 
 
 
import { prisma } from "@mono/db";
import { GoogleGenAI } from "@google/genai";
import { AiModel, calculateCost } from "../../worker/src/pricing.js";
// Use the shared ENV configuration from the worker or admin
import { z } from "zod";
import * as dotenv from "dotenv";
dotenv.config({ path: "../../.env" });

const envSchema = z.object({
  GEMINI_API_KEY: z.string().optional(),
});
const env = envSchema.parse(process.env);

const ai = env.GEMINI_API_KEY ? new GoogleGenAI({ apiKey: env.GEMINI_API_KEY }) : null;

// Convert sentiment string from Gemini to 0-10 scale
function getSentimentScore(sentiment: string): number {
  if (sentiment === "POSITIVE") return 10.0;
  if (sentiment === "NEGATIVE") return 0.0;
  return 5.0; // NEUTRAL or MIXED
}

async function main() {
  if (!ai) {
    console.error("❌ Cannot run rollup: no GEMINI_API_KEY found in .env");
    process.exit(1);
  }

  // 1. Fetch unmapped Silver Mentions
  const limit = process.argv.includes("--limit")
    ? parseInt(process.argv[process.argv.indexOf("--limit") + 1], 10)
    : 1000;

  console.log(`🧹 Sweeping for up to ${limit} unmapped Silver mentions...`);

  const mentions = await prisma.silverProductMention.findMany({
    where: {
      goldProductId: null,
      specificityLevel: { in: ["EXACT_MODEL", "PRODUCT_LINE"] },
    },
    take: limit,
  });

  if (mentions.length === 0) {
    console.log("✅ No unmapped exact/family mentions found.");
    return;
  }

  console.log(`📦 Processing ${mentions.length} mentions for Gold rollup...`);

  for (const m of mentions) {
    const canonicalStr = `${m.brand.trim()} ${m.productName.trim()}`.toLowerCase();
    console.log(`\n🔍 Evaluating: "${canonicalStr}"`);

    try {
      let vectorLiteral: string = "";

      // Try fetching existing embedding from Silver table first
      const rawSilver = await prisma.$queryRaw<Array<{ vec: string }>>`
        SELECT embedding::text as vec 
        FROM "SilverProductMention" 
        WHERE id = ${m.id}
      `;

      if (rawSilver.length > 0 && rawSilver[0].vec) {
        vectorLiteral = rawSilver[0].vec;
      } else {
        // Fallback: Generate if missing and save it
        await new Promise(r => setTimeout(r, 200));

        const embedRes3072 = await ai.models.embedContent({
          model: "gemini-embedding-2-preview",
          contents: `task: clustering | query: ${canonicalStr}`,
          config: { outputDimensionality: 3072 }
        });

        await new Promise(r => setTimeout(r, 200));

        const embedRes768 = await ai.models.embedContent({
          model: "gemini-embedding-2-preview",
          contents: `task: clustering | query: ${canonicalStr}`,
          config: { outputDimensionality: 768 }
        });

        const vectorValue3072 = embedRes3072.embeddings?.[0]?.values || [];
        const vectorValue768 = embedRes768.embeddings?.[0]?.values || [];

        if (!vectorValue3072 || vectorValue3072.length === 0 || !vectorValue768 || vectorValue768.length === 0) {
           console.warn(`⚠️ Failed to generate embedding for ${m.id}`);
           continue;
        }

        const vectorLiteral3072 = `[${vectorValue3072.join(",")}]`;
        const vectorLiteral768 = `[${vectorValue768.join(",")}]`;
        vectorLiteral = vectorLiteral3072; // The current system still relies on 3072 for calculations

        // Save fallbacks back to Silver table
        await prisma.$executeRaw`
          UPDATE "SilverProductMention" 
          SET embedding = ${vectorLiteral3072}::vector,
              embedding768 = ${vectorLiteral768}::vector 
          WHERE id = ${m.id};
        `;

        const promptTokens = Math.ceil(canonicalStr.length / 4);
        const costInUsd = calculateCost(AiModel.GEMINI_EMBEDDING_2_PREVIEW, {
          promptTokenCount: promptTokens,
          cachedContentTokenCount: 0,
          candidatesTokenCount: 0
        });
        
        await prisma.aiSpend.create({
          data: {
             jobName: "[Gold] Rollup: General",
             submissionId: m.submissionId,
             model: AiModel.GEMINI_EMBEDDING_2_PREVIEW,
             promptTokens,
             cachedTokens: 0,
             responseTokens: 0,
             totalTokens: promptTokens,
             costInUsd,
          }
        });
      }

      // 2. Perform Cosine Similarity Search against GoldProduct
      // Distance `<=>` gives 0 for identical, 2 for diametrically opposed.
      // We seek distance < 0.15 (which is similarity > 0.85) for "same product"
      interface MatchResult {
        id: string;
        distance: number;
      }
      
      const matches = await prisma.$queryRaw<MatchResult[]>`
        SELECT id, (embedding <=> ${vectorLiteral}::vector) as distance
        FROM "GoldProduct"
        ORDER BY embedding <=> ${vectorLiteral}::vector
        LIMIT 1;
      `;

      let goldId: string;

      if (matches.length > 0 && matches[0].distance < 0.15) {
        goldId = matches[0].id;
        console.log(`   🔗 MATCHED existing Gold product: ${goldId} (distance: ${matches[0].distance.toFixed(4)})`);
      } else {
        // Did not match or no GoldProducts exist at all
        const created = await prisma.goldProduct.create({
          data: {
            brand: m.brand,
            canonicalName: m.productName,
            mentionCount: 0,
            avgSentiment: 0,
          },
        });
        goldId = created.id;
        
        // Update embedding with raw SQL
        await prisma.$executeRaw`
          UPDATE "GoldProduct" 
          SET embedding = ${vectorLiteral}::vector 
          WHERE id = ${goldId};
        `;
        console.log(`   ✨ CREATED new Gold product: ${goldId}`);
      }

      // 3. Attach mention and update aggregates
      await prisma.$transaction(async (tx) => {
        // Connect mention to Gold
        await tx.silverProductMention.update({
           where: { id: m.id },
           data: { goldProductId: goldId },
        });

        // Recalculate stats for the Gold row
        const stats = await tx.silverProductMention.aggregate({
          where: { goldProductId: goldId },
          _count: { id: true },
        });

        const allMentions = await tx.silverProductMention.findMany({
          where: { goldProductId: goldId },
          select: { sentiment: true },
        });

        let totalScore = 0;
        for (const mention of allMentions) {
           totalScore += getSentimentScore(mention.sentiment);
        }
        const newAvg = allMentions.length > 0 ? (totalScore / allMentions.length) : 0;

        await tx.goldProduct.update({
          where: { id: goldId },
          data: {
            mentionCount: stats._count.id,
            avgSentiment: newAvg,
          },
        });
        console.log(`   📈 Updated GoldProduct stats -> mentions: ${stats._count.id}, avgSentiment: ${newAvg.toFixed(2)}`);
      });

    } catch (err: unknown) {
      console.error(`   ❌ Error processing mention ${m.id}:`, err?.message);
    }
  }

  console.log(`\n🎉 Rollup batch complete!`);
}

main()
  .catch((e) => {
    console.error("Fatal error:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
