import { Job } from "bullmq";
import { prisma } from "@mono/db";
import { GoogleGenAI, Type } from "@google/genai";
import { env } from "../env.js";
import { calculateCost, AiModel } from "../pricing.js";
import { embedWithRetry } from "../local-embedder.js";

export interface AnalysisJobData {
  type: "thread";
  id: string;
}

const MOCK_OPTS = env.AI_MOCK_URL ? { apiKey: "mock-key", baseUrl: env.AI_MOCK_URL } : { apiKey: env.GEMINI_API_KEY || "missing_key" };
const ai = (env.GEMINI_API_KEY || env.AI_MOCK_URL) ? new GoogleGenAI(MOCK_OPTS) : null;

  // The Raw Identity Data
const MENTION_ITEM_SCHEMA = {
  type: Type.OBJECT,
  properties: {
    sourceId: { type: Type.INTEGER, description: "The EXACT integer source index from the text block where it was mentioned (e.g. 0, 1, 2)." },
    brand: { type: Type.STRING, description: "The stated brand name. You MUST normalize misspellings and casing to the canonical proper spelling (e.g. 'All-Clad' instead of 'all clad', 'Allen Edmonds' instead of 'allen edmond')." },
    productName: { type: Type.STRING, description: "The specific marketed product line or model name (e.g., 'Artisan', 'F-150', 'Aeron'). DO NOT extract generic product categories (e.g., 'mixer', 'backpack', 'pan'). If the mention is BRAND_ONLY, you MUST return an empty string \"\" for this field." },
    specificityLevel: { type: Type.STRING, enum: ["EXACT_MODEL", "PRODUCT_LINE", "BRAND_ONLY"], description: "If a specific, identifiable unit is named (e.g. 'iPad 3 64GB', 'Higgins Mill boot') use EXACT_MODEL. If a marketed product family or series is named (e.g. 'Neuro Fuzzy', 'MacBook', 'Camry', 'Artisan') use PRODUCT_LINE. If they only mention the brand or a generic category (e.g. 'buy an Acura', 'Acura car', 'Apple computer', 'KitchenAid mixer') use BRAND_ONLY." },
    acquiredPrice: { type: Type.NUMBER, nullable: true, description: "The price paid if mentioned. Only the numeric value." },
    ownershipDurationMonths: { type: Type.INTEGER, nullable: true, description: "Standardize ownership time mentioned into months (e.g. '3 years' -> 36)." },
    usageFrequency: { 
      type: Type.STRING, 
      nullable: true, 
      enum: ["DAILY", "WEEKLY", "MONTHLY", "SEASONAL", "RARELY"],
      description: "How often they mention using it." 
    },
    durability: { type: Type.STRING, nullable: true, enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"], description: "If durability is mentioned, what is the sentiment? (e.g. good=POSITIVE)" },
    repairability: { type: Type.STRING, nullable: true, enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"], description: "If repairability or fixing is mentioned, how easy/affordable is it? (e.g. easy=POSITIVE)" },
    maintenance: { type: Type.STRING, nullable: true, enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"], description: "If maintenance/cleaning is mentioned, how easy is it? (e.g. easy=POSITIVE)" },
    warranty: { type: Type.STRING, nullable: true, enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"], description: "If warranty/support is mentioned, how good is it? (e.g. excellent=POSITIVE)" },
    value: { type: Type.STRING, nullable: true, enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"], description: "If they discuss whether the product was worth the price. (e.g. worth it=POSITIVE)" },
    sentiment: { type: Type.STRING, enum: ["POSITIVE", "NEUTRAL", "NEGATIVE"], description: "Overall sentiment about the product." },
    flawOrCaveat: { type: Type.STRING, nullable: true, description: "Even if they love it, any specific flaws, quirks, or complaints they listed?" }
  },
  required: ["sourceId", "brand", "productName", "specificityLevel", "sentiment"]
};

const THREAD_EXTRACTION_SCHEMA = {
  type: Type.ARRAY,
  items: MENTION_ITEM_SCHEMA,
  description: "An array of all distinct product mentions found in the thread."
};

export default async function analysisProcessor(job: Job<AnalysisJobData>) {
  const { type, id } = job.data;
  if (!id || !type) {
    if ("batchSize" in job.data) {
      console.log(`[Worker] Ignoring legacy batch job ${job.id}`);
      return { processed: 0, reason: "legacy_job_ignored" };
    }
    throw new Error("Missing job id or type");
  }

  if (type !== ("thread" as unknown)) {
    console.log(`[Worker] Ignoring non-thread job ${job.id} (type: ${type})`);
    return { processed: 0, reason: "ignored_non_thread" };
  }

  const sub = await prisma.bronzeRedditSubmission.findUnique({ 
    where: { id },
    include: { comments: { select: { id: true, body: true } } }
  });

  if (!sub || sub.isProcessed) return { processed: 0, reason: "skip" };

  if (!ai) {
    throw new Error("GEMINI_API_KEY or AI_MOCK_URL is required to process analysis jobs");
  }

  const threadText = [
    `[SOURCE INDEX: 0] Title: ${sub.title} | Body: ${sub.selftext || ""}`,
    ...sub.comments.map((c, index) => `[SOURCE INDEX: ${index + 1}] Body: ${c.body}`)
  ].join("\n\n");

  // SIGNAL GATEWAY: Drop low-value threads to save AI token processing costs
  const wordCount = threadText.trim().split(/\s+/).length;
  if (wordCount < 100) {
    console.log(`[Worker] Skipping low-signal thread ${id} (${wordCount} words).`);
    // Still mark as processed so it isn't picked up by future batches
    await prisma.bronzeRedditSubmission.update({ where: { id }, data: { isProcessed: true } });
    return { processed: 0, reason: "low_signal_skipped" };
  }

  const prompt = `You are a product analyst studying "Buy It For Life" patterns on Reddit. 
Extract every notable durable product being discussed, recommended, or reviewed in the following Reddit thread.
Include both products from the original submission and the comments.

CRITICAL INSTRUCTIONS:
- For each extracted product, you MUST specify the exact integer 'sourceId' from the text block where it was mentioned. 
- The sourceId will be the integer index from [SOURCE INDEX: X] (e.g. 0, 1, 2).
- Only extract physical, durable products.
- If the brand name of the product is unknown, completely unstated, or generic, DO NOT extract the product at all. Completely omit it. Do not use placeholders like "Unknown".
- Do NOT extract generic product categories or nouns (e.g., "mixer", "backpack", "pan", "car", "boots", "sweater") as a productName. If the user only says "I love my KitchenAid mixer", the specificityLevel MUST be BRAND_ONLY and the productName MUST be an empty string "". You MUST ONLY classify something as PRODUCT_LINE or EXACT_MODEL if the user uses a Proper Noun, marketing name, or specific model identifier (e.g., "Artisan", "F-150", "Aeron", "D5").

Thread to analyze:
${threadText}`;

  // Direct generation using structured outputs
  const response = await ai.models.generateContent({
    model: "gemini-3-flash-preview",
    contents: prompt,
    config: {
      responseMimeType: "application/json",
      responseJsonSchema: THREAD_EXTRACTION_SCHEMA,
    }
  });

  const rawJson = response.text;
  if (rawJson) {
    try {
      const parsedArray = JSON.parse(rawJson);
      
      if (Array.isArray(parsedArray) && parsedArray.length > 0) {
        const validMentions = parsedArray.filter(m => m.sourceId !== undefined && m.sourceId !== null && m.brand && m.productName && m.brand !== "null");
        
        if (validMentions.length > 0) {
          // create a list of data models
          const dataToInsert = validMentions.map(parsed => {
            const sourceIndex = typeof parsed.sourceId === 'number' ? parsed.sourceId : parseInt(String(parsed.sourceId), 10);
            
            let commentId: string | null = null;
            
            // Map integer index back to actual comment ID. Index 0 is the submission itself.
            if (!isNaN(sourceIndex) && sourceIndex > 0 && sourceIndex <= sub.comments.length) {
              commentId = sub.comments[sourceIndex - 1].id;
            }

            return {
              submissionId: sub.id, // Always attribute to the root submission
              commentId: commentId, // Optionally attribute to the exact comment if it matches
              brand: parsed.brand,
              productName: parsed.productName,
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

          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const preparedItems: any[] = [];
          for (const item of dataToInsert) {
            const embedText = `${item.brand.trim()} ${item.productName.trim()}`.toLowerCase();
            const vectorValue = await embedWithRetry(embedText);

            preparedItems.push({
              item,
              vectorValue,
              embedText
            });
          }

          // 2. Perform the database inserts
          await prisma.$transaction(async (tx) => {
            for (const prepared of preparedItems) {
              const created = await tx.silverProductMention.create({
                data: prepared.item,
              });
              
              if (prepared.vectorValue.length > 0) {
                const vectorLiteral = `[${prepared.vectorValue.join(",")}]`;
                
                await tx.$executeRaw`
                  UPDATE "SilverProductMention" 
                  SET embedding = ${vectorLiteral}::vector
                  WHERE id = ${created.id};
                `;
              }
            }
          });
        }
      }
    } catch (err: unknown) {
      console.error("Failed to parse LLM structured output or generate embeddings. Failing job to prevent silent data drops.", err);
      throw err; // <--- This throws to BullMQ so the job actually fails and eventually retries!
    }
  }

  const usage = response.usageMetadata;
  if (usage) {
    const cost = calculateCost(AiModel.GEMINI_3_FLASH, usage);
    
    await prisma.aiSpend.create({
      data: {
        jobName: "SILVER_EXTRACTION",
        submissionId: sub.id,
        model: AiModel.GEMINI_3_FLASH,
        costInUsd: cost,
        promptTokens: usage.promptTokenCount || 0,
        cachedTokens: usage.cachedContentTokenCount || 0,
        responseTokens: usage.candidatesTokenCount || 0,
        totalTokens: usage.totalTokenCount || 0,
      }
    });
  }

  // Mark submission as processed
  await prisma.bronzeRedditSubmission.update({ where: { id }, data: { isProcessed: true } });

  return { processed: 1, action: "extracted" };
}
