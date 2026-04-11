 
/* eslint-disable @typescript-eslint/no-unused-vars */
 
import { prisma } from "@mono/db";
import { GoogleGenAI, Type, Schema } from "@google/genai";
import { AiModel, calculateCost, ThinkingLevel, getThinkingConfig } from "../../worker/src/pricing.js";
import { embedWithRetry } from "./local-embedder.js";
import * as dotenv from "dotenv";
import { z } from "zod";

dotenv.config({ path: "../../.env" });
const envSchema = z.object({ GEMINI_API_KEY: z.string().optional() });
const env = envSchema.parse(process.env);
const ai = env.GEMINI_API_KEY ? new GoogleGenAI({ apiKey: env.GEMINI_API_KEY }) : null;

const ACTIVE_MODEL = AiModel.GEMINI_2_5_FLASH_LITE;
const ACTIVE_THINKING_LEVEL: ThinkingLevel = "low";

const llmResponseSchema: Schema = {
  type: Type.ARRAY,
  items: {
    type: Type.OBJECT,
    properties: {
      canonicalName: { type: Type.STRING, description: "The single best, cleanest, capitalized version of this category (e.g. 'Leather Work Boots')." },
      rawNames: { 
        type: Type.ARRAY, 
        items: { type: Type.STRING },
        description: "The list of raw input names from the batch that map exactly to this canonical name." 
      }
    },
    required: ["canonicalName", "rawNames"]
  }
};

async function withRetry<T>(fn: () => Promise<T>, maxRetries = 3, delayMs = 2000): Promise<T> {
  let attempt = 0;
  while (attempt < maxRetries) {
    try {
      return await fn();
    } catch (err: unknown) {
      attempt++;
      if (attempt >= maxRetries) throw err;
      console.warn(`   [Retry] API Error: ${err?.message}. Retrying in ${delayMs}ms (Attempt ${attempt}/${maxRetries})...`);
      await new Promise(r => setTimeout(r, delayMs));
      delayMs *= 2; // exponential backoff
    }
  }
  throw new Error("Unreachable");
}

async function main() {
  if (!ai) {
    console.error("❌ Cannot run consolidation: no GEMINI_API_KEY found in .env");
    process.exit(1);
  }

  console.log(`[Consolidation] 🧹 Sweeping for unprocessed Silver Category Ideas...`);

  // Grab a batch of distinct raw names to deduplicate
  const ideas = await prisma.silverCategoryIdea.findMany({
    where: { isProcessed: false },
    select: { rawName: true },
    distinct: ['rawName'],
    take: 100
  });

  if (ideas.length === 0) {
    console.log("[Consolidation] ✅ No unprocessed ideas found.");
    return;
  }

  const rawNamesList = ideas.map(i => i.rawName);
  console.log(`[Consolidation] 📦 Consolidating a batch of ${rawNamesList.length} raw ideas...`);

  const prompt = `You are an expert e-commerce catalog manager.
I am providing you with a list of RAW category strings generated dynamically by users.
Your task is to aggressively group highly specific niche synonyms and long-tail variations into a single, perfectly formatted Canonical Category Hub.

# RAW LIST
${rawNamesList.map((n, i) => `${i+1}. ${n}`).join("\n")}

# RULES
1. MERGE long-tail variations into their broader Head Term hub. (e.g., "Airtight Glass Food Storage Containers", "Heat Resistant Glass Food Bowls", and "Microwave Safe Food Containers" should ALL be grouped into a single canonical hub called "Glass Food Storage").
2. NEVER over-consolidate functionally distinct items into a generic macro-category. You must preserve distinct form-factors, completely different use-cases, and specific tool shapes as separate Hubs.
3. Title Case the canonical name beautifully.
4. EVERY single raw name provided in the list MUST be included in the 'rawNames' array of ONE of the canonical output objects. Do not leave any behind!

# EXAMPLES OF BAD VS GOOD CONSOLIDATION
EXAMPLE 1 (Distinct Use-Cases)
Input: [Work Boots, Casual Boots, Hiking Boots, Leather Boots]
BAD Output: Boots
GOOD Output: Leave them separate. "Work Boots", "Casual Boots", and "Hiking Boots" serve entirely different functional purposes.

EXAMPLE 2 (Distinct Form-Factors)
Input: [Living Room Furniture, Recliners, Sectional Sofas, Accent Chairs]
BAD Output: Living Room Seating
GOOD Output: Leave them separate. A "Recliner" and a "Sectional Sofa" are completely different pieces of furniture.

EXAMPLE 3 (Tool Specificity)
Input: [Kitchen Tongs, Cooking Utensils, Spatulas]
BAD Output: Kitchen Utensils
GOOD Output: "Kitchen Tongs" and "Spatulas" must remain separate specific tool hubs.`;

  try {
    const response = await withRetry(() => ai.models.generateContent({
      model: ACTIVE_MODEL,
      contents: prompt,
      config: {
        responseMimeType: "application/json",
        responseSchema: llmResponseSchema,
        thinkingConfig: getThinkingConfig(ACTIVE_MODEL, ACTIVE_THINKING_LEVEL)
      }
    }));

    if (response.text) {
      const groupings: Array<{ canonicalName: string, rawNames: string[] }> = JSON.parse(response.text);
      let categoriesCreated = 0;

      for (const group of groupings) {
         if (!group.canonicalName || group.rawNames.length === 0) continue;
         
         // 1. Upsert Canonical Category
         const category = await prisma.goldCategory.upsert({
            where: { canonicalName: group.canonicalName },
            update: {},
            create: { canonicalName: group.canonicalName }
         });

         // 2. Add Vector Embedding
         // We do this immediately so Phase 3 matrix routing can find it
         try {
            const pureVec = await embedWithRetry(group.canonicalName);
            if (pureVec.length > 0) {
               const vParam = `[${pureVec.join(",")}]`;
               await prisma.$executeRaw`
                 UPDATE "GoldCategory" 
                 SET embedding = ${vParam}::vector 
                 WHERE id = ${category.id};
               `;
            }
         } catch(e) {
            console.error(`   [Consolidation] Failed to embed ${group.canonicalName}`);
         }

         // 3. Mark the raw ideas as processed
         await prisma.silverCategoryIdea.updateMany({
            where: {
               rawName: { in: group.rawNames }
            },
            data: { isProcessed: true }
         });

         categoriesCreated++;
         console.log(`   [Consolidation] ✨ Grouped [${group.rawNames.join(", ")}] -> ${group.canonicalName}`);
      }

      // Track spend
      const usage = response.usageMetadata;
      if (usage) {
        const cost = calculateCost(ACTIVE_MODEL, {
          promptTokenCount: usage.promptTokenCount,
          cachedContentTokenCount: 0,
          candidatesTokenCount: usage.candidatesTokenCount,
        });
        await prisma.aiSpend.create({
          data: {
            jobName: "[Taxonomy] Consolidation",
            model: ACTIVE_MODEL,
            promptTokens: usage.promptTokenCount,
            responseTokens: usage.candidatesTokenCount || 0,
            thinkingTokens: usage.thoughtsTokenCount || usage.thoughts_token_count || 0,
            totalTokens: usage.totalTokenCount,
            costInUsd: cost
          }
        });
      }
    }
  } catch (err: unknown) {
    console.error(`   [Consolidation] ❌ Error:`, err?.message);
  }
}

main()
  .catch((e) => {
    console.error("Fatal error:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
