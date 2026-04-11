/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */
 
import { prisma } from "@mono/db";
import { GoogleGenAI, Type, Schema } from "@google/genai";
import { AiModel, calculateCost, ThinkingLevel, getThinkingConfig } from "../../worker/src/pricing.js";
import { z } from "zod";
import * as dotenv from "dotenv";
dotenv.config({ path: "../../.env" });

const DEFAUL_LIMIT = 15;

const envSchema = z.object({
  GEMINI_API_KEY: z.string().optional(),
});
const env = envSchema.parse(process.env);

const ai = env.GEMINI_API_KEY ? new GoogleGenAI({ apiKey: env.GEMINI_API_KEY }) : null;

// The model configuration for this script run
const ACTIVE_MODEL = AiModel.GEMINI_2_5_FLASH_LITE;
const ACTIVE_THINKING_LEVEL: ThinkingLevel = "low";

// The Raw Identity Data
const MENTION_ITEM_SCHEMA: Schema = {
  type: Type.OBJECT,
  properties: {
    sourceId: {
      type: Type.INTEGER,
      description: "Extract the exact [SOURCE INDEX: X] integer from the comment where this product was found.",
    },
    quote: {
      type: Type.STRING,
      description: "Extract exactly 1 phrase or continuous sentence from the raw comment where the product or brand was explicitly named or heavily contextualized. Do not extract full paragraphs, limit to just enough to ground the exact context.",
    },
    brand: {
      type: Type.STRING,
      description:
        "The stated brand name. You MUST normalize misspellings and casing to the canonical proper spelling (e.g. 'All-Clad' instead of 'all clad', 'Allen Edmonds' instead of 'allen edmond'). NEVER return 'brand not specified', 'unknown', or 'unspecified'. If you don't know the exact brand, skip extracting the product entirely.",
    },
    productName: {
      type: Type.STRING,
      description:
        "The specific marketed product line or model name (e.g., 'Artisan', 'F-150', 'Aeron'). DO NOT extract generic product categories (e.g., 'mixer', 'backpack', 'pan'). If the mention is BRAND_ONLY, you MUST return an empty string \"\" for this field.",
    },
    specificityLevel: {
      type: Type.STRING,
      enum: ["EXACT_MODEL", "PRODUCT_LINE", "BRAND_ONLY"],
      description:
        "If a specific, identifiable unit is named (e.g. 'iPad 3 64GB', 'Higgins Mill boot') use EXACT_MODEL. If a marketed product family or series is named (e.g. 'Neuro Fuzzy', 'MacBook', 'Camry', 'Artisan') use PRODUCT_LINE. If they only mention the brand or a generic category (e.g. 'buy an Acura', 'Acura car', 'Apple computer', 'KitchenAid mixer') use BRAND_ONLY.",
    },
    acquiredPrice: {
      type: Type.NUMBER,
      nullable: true,
      description: "The price paid if mentioned. Only the numeric value.",
    },
    ownershipDurationMonths: {
      type: Type.INTEGER,
      nullable: true,
      description: "Standardize ownership time mentioned into months (e.g. '3 years' -> 36).",
    },
    usageFrequency: {
      type: Type.STRING,
      nullable: true,
      enum: ["DAILY", "WEEKLY", "MONTHLY", "SEASONAL", "RARELY"],
      description: "How often they mention using it.",
    },
    durability: {
      type: Type.STRING,
      nullable: true,
      enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"],
      description: "If durability is mentioned, what is the sentiment?",
    },
    repairability: {
      type: Type.STRING,
      nullable: true,
      enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"],
      description: "If repairability or fixing is mentioned, how easy/affordable is it?",
    },
    maintenance: {
      type: Type.STRING,
      nullable: true,
      enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"],
      description: "If maintenance/cleaning is mentioned, how easy is it?",
    },
    warranty: {
      type: Type.STRING,
      nullable: true,
      enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"],
      description: "If warranty/support is mentioned, how good is it?",
    },
    value: {
      type: Type.STRING,
      nullable: true,
      enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"],
      description: "If they discuss whether the product was worth the price.",
    },
    sentiment: {
      type: Type.STRING,
      enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"],
      description: "Overall sentiment about the product.",
    },
    flawOrCaveat: {
      type: Type.STRING,
      nullable: true,
      description: "Even if they love it, any specific flaws, quirks, or complaints they listed?",
    },
  },
  required: ["sourceId", "quote", "brand", "productName", "specificityLevel", "sentiment"],
};

const THREAD_EXTRACTION_SCHEMA: Schema = {
  type: Type.ARRAY,
  items: MENTION_ITEM_SCHEMA,
  description: "An array of all distinct product mentions found in the thread.",
};

// Helper for generating structure outputs
const generateWithRetry = async (prompt: string, retries = 4) => {
  if (!ai) return null;
  for (let i = 0; i < retries; i++) {
    try {
      await new Promise((r) => setTimeout(r, 1000));
      const response = await ai.models.generateContent({
        model: ACTIVE_MODEL,
        contents: prompt,
        config: {
          responseMimeType: "application/json",
          responseSchema: THREAD_EXTRACTION_SCHEMA,
          thinkingConfig: getThinkingConfig(ACTIVE_MODEL, ACTIVE_THINKING_LEVEL)
        },
      });
      return response;
    } catch (e: unknown) {
      console.warn(`   ⚠️ LLM Extraction failed (attempt ${i + 1}/${retries}):`, e.message || e);
      if (i === retries - 1) return null;
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
  return null;
};

// Helper for vector generation has been moved to embed-silvers.ts

async function main() {
  if (!ai) {
    console.error("❌ Cannot run silver generation: no GEMINI_API_KEY found in .env");
    process.exit(1);
  }

  const CONCURRENCY = process.argv.includes("--concurrency")
    ? parseInt(process.argv[process.argv.indexOf("--concurrency") + 1], 10)
    : 10;

  const limit = process.argv.includes("--limit")
    ? parseInt(process.argv[process.argv.indexOf("--limit") + 1], 10)
    : DEFAUL_LIMIT; // Hardcoded default for small test run

  const isRandom = process.argv.includes("--random");
  const seedArgIndex = process.argv.indexOf("--seed");
  const seed = seedArgIndex !== -1 ? parseFloat(process.argv[seedArgIndex + 1]) : 42;

  console.log(`[Extraction] 🧹 Fetching unprocessed Bronze Submissions (limit ${limit}, random: ${isRandom}, seed: ${seed})...`);

  // Grab the root submissions, include their comments
  let submissions: any[] = [];
  if (isRandom) {
    let idsResponse: {id: string}[];
    if (seed !== null) {
      // Postgres setseed requires -1 <= x <= 1. Map any number deterministically.
      const seedVal = Math.sin(seed);
      idsResponse = await prisma.$transaction(async (tx) => {
        await tx.$executeRaw`SELECT setseed(${seedVal})`;
        return tx.$queryRaw<{id: string}[]>`
          SELECT id FROM "BronzeRedditSubmission" 
          WHERE "isProcessed" = false 
          ORDER BY RANDOM() 
          LIMIT ${limit}
        `;
      });
    } else {
      idsResponse = await prisma.$queryRaw<{id: string}[]>`
        SELECT id FROM "BronzeRedditSubmission" 
        WHERE "isProcessed" = false 
        ORDER BY RANDOM() 
        LIMIT ${limit}
      `;
    }
    submissions = await prisma.bronzeRedditSubmission.findMany({
      where: { id: { in: idsResponse.map(r => r.id) } },
      include: { comments: { select: { id: true, body: true } } }
    });
  } else {
    submissions = await prisma.bronzeRedditSubmission.findMany({
      where: { isProcessed: false },
      take: limit,
      orderBy: { score: "desc" },
      include: { comments: { select: { id: true, body: true } } },
    });
  }

  if (submissions.length === 0) {
    console.log(`[Extraction] ✅ No unprocessed Submissions found in the Bronze layer.`);
    return;
  }

  console.log(
    `[Extraction] 📦 Churning ${submissions.length} Threads into Silver Mentions (concurrency: ${CONCURRENCY})...`,
  );

  const total = submissions.length;
  let successCount = 0;
  let processIndex = 0;

  const worker = async () => {
    while (processIndex < submissions.length) {
      const i = processIndex++;
      const sub = submissions[i];

      const baseContext = `[SOURCE INDEX: 0] Title: ${sub.title} | Body: ${sub.selftext || ""}`;
      const CHUNK_SIZE = 25;
      
      const chunks: string[] = [];
      if (sub.comments.length === 0) {
        chunks.push(baseContext);
      } else {
        for (let i = 0; i < sub.comments.length; i += CHUNK_SIZE) {
          const chunkComments = sub.comments.slice(i, i + CHUNK_SIZE).map((c, localIdx) => 
            `[SOURCE INDEX: ${i + localIdx + 1}] Body: ${c.body}`
          );
          chunks.push([baseContext, ...chunkComments].join("\n\n"));
        }
      }

      console.log(`\n[Extraction] 🔍 [${i + 1}/${total}] Analyzing Topic: "${sub.title}" | Comments: ${sub.comments.length} | Chunks: ${chunks.length}`);

      let allChunksSucceeded = true;
      let totalMentionsSaved = 0;

      await Promise.all(
        chunks.map(async (threadText, chunkIdx) => {
          const prompt = `You are a product analyst studying "Buy It For Life" patterns on Reddit. 
Extract every notable durable product being discussed, recommended, or reviewed in the following Reddit thread.
Include both products from the original submission and the comments.

CRITICAL INSTRUCTIONS:
- For each extracted product, you MUST specify the exact integer 'sourceId' from the text block where it was mentioned. 
- The sourceId will be the integer index from [SOURCE INDEX: X] (e.g. 0, 1, 2).
- Only extract physical, durable products.
- If the brand name of the product is unknown, completely unstated, or generic, DO NOT extract the product at all. Completely omit it. NEVER return phrases like "brand not specified" or "unspecified" or "unknown".
- Do NOT extract generic product categories or nouns (e.g., "mixer", "backpack", "pan", "car", "boots", "sweater") as a productName. If the user only says "I love my KitchenAid mixer", the specificityLevel MUST be BRAND_ONLY and the productName MUST be an empty string "". You MUST ONLY classify something as PRODUCT_LINE or EXACT_MODEL if the user uses a Proper Noun, marketing name, or specific model identifier (e.g., "Artisan", "F-150", "Aeron", "D5").

Thread to analyze:
${threadText}`;

          const promptTokensEst = Math.ceil(prompt.length / 4);
          console.log(`   [Extraction] 🧠 Sending ${promptTokensEst} estimated tokens to Gemini (Chunk ${chunkIdx + 1}/${chunks.length})...`);
          
          try {
            const response = await generateWithRetry(prompt);

            if (response && response.text) {
              const parsedArray = JSON.parse(response.text);

              if (Array.isArray(parsedArray) && parsedArray.length > 0) {
                const validMentions = parsedArray.filter(
                  (m: any) =>
                    m.sourceId !== undefined &&
                    m.sourceId !== null &&
                    m.brand &&
                    m.productName !== undefined &&
                    m.brand !== "null",
                );

                if (validMentions.length > 0) {
                  totalMentionsSaved += validMentions.length;
                  console.log(
                    `   [Extraction] ✨ Extracted ${validMentions.length} valid products in Chunk ${chunkIdx + 1}. Saving to Database...`,
                  );

                  const preparedItems = validMentions.map((parsed: any) => {
                    const sourceIndex =
                      typeof parsed.sourceId === "number"
                        ? parsed.sourceId
                        : parseInt(String(parsed.sourceId), 10);
                    
                    const isComment = sourceIndex > 0;
                    return {
                      submissionId: isComment ? null : sub.id,
                      commentId: isComment ? sub.comments[sourceIndex - 1].id : null,
                      brand: parsed.brand,
                      productName: parsed.productName,
                      quote: parsed.quote,
                      specificityLevel: parsed.specificityLevel || "UNKNOWN",
                      acquiredPrice: parsed.acquiredPrice || null,
                      ownershipDurationMonths: parsed.ownershipDurationMonths || null,
                      usageFrequency: parsed.usageFrequency || null,
                      durability: parsed.durability || null,
                      repairability: parsed.repairability || null,
                      maintenance: parsed.maintenance || null,
                      warranty: parsed.warranty || null,
                      value: parsed.value || null,
                      sentiment: parsed.sentiment,
                      flawOrCaveat: parsed.flawOrCaveat || null,
                    };
                  });

                  // 2. Perform DB Inserts (Embedding generation is now completely decoupled)
                  await prisma.silverProductMention.createMany({
                    data: preparedItems,
                  });

                  const usage = response.usageMetadata;
                  if (usage) {
                    const cost = calculateCost(ACTIVE_MODEL, {
                      promptTokenCount: usage.promptTokenCount,
                      cachedContentTokenCount: 0,
                      candidatesTokenCount: usage.candidatesTokenCount,
                    });
                    await prisma.aiSpend.create({
                      data: {
                        submissionId: sub.id,
                        model: ACTIVE_MODEL,
                        jobName: "[Silver] Extractor",
                        costInUsd: cost,
                        promptTokens: usage.promptTokenCount,
                        cachedTokens: 0,
                        responseTokens: usage.candidatesTokenCount || 0,
                        thinkingTokens: usage.thoughtsTokenCount || usage.thoughts_token_count || 0,
                        totalTokens: usage.totalTokenCount,
                      },
                    });
                  }
                } else {
                  console.log(`   [Extraction] 📉 Output parsed, but contained no valid/clean entities in Chunk ${chunkIdx + 1}. Skipped.`);
                }
              } else {
                console.log(`   [Extraction] 📉 Output array empty. No valid models detected in Chunk ${chunkIdx + 1}.`);
              }
            } else {
              console.log(`   [Extraction] ❌ LLM returned empty string or crashed entirely in Chunk ${chunkIdx + 1}.`);
              allChunksSucceeded = false;
            }
          } catch (err: unknown) {
            console.error(
              `   ❌ Failed to parse LLM JSON framework or API failed in Chunk ${chunkIdx + 1}.`,
            );
            allChunksSucceeded = false;
          }
        })
      );

      // We only mark as processed if ALL chunks within the thread were successfully queried and parsed
      if (allChunksSucceeded) {
        if (totalMentionsSaved > 0) {
           successCount++;
        }
        console.log(`[Extraction] 🎉 Finished Thread [${i + 1}/${total}]. Found ${totalMentionsSaved} Extracted Entities.`);
        await prisma.bronzeRedditSubmission.update({
          where: { id: sub.id },
          data: { 
            isProcessed: true
          },
        });
      } else {
        console.error(`   ❌ Thread aborting due to chunk failures. Will not mark as processed so it can retry.`);
      }
    }
  };

  const pool = Array.from({ length: Math.min(CONCURRENCY, submissions.length) }).map(() =>
    worker(),
  );
  await Promise.all(pool);

  console.log(
    `\n[Extraction] ✅ Silver Sweep complete! Fully processed ${total} threads, resulting in ${successCount} successful multi-product AI extraction runs.`,
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
