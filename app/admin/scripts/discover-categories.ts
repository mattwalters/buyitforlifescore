/* eslint-disable @typescript-eslint/no-explicit-any */
 
 
 
import { prisma } from "@mono/db";
import { GoogleGenAI, Type, Schema } from "@google/genai";
import { AiModel, calculateCost, ThinkingLevel, getThinkingConfig } from "../../worker/src/pricing.js";
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
    type: Type.STRING
  },
  description: "An array of 2 to 3 broad, high-volume Head-Term e-commerce hub categories (2-3 words maximum)."
};

async function withRetry<T>(fn: () => Promise<T>, maxRetries = 3, delayMs = 2000): Promise<T> {
  let attempt = 0;
  while (attempt < maxRetries) {
    try {
      return await fn();
    } catch (err: unknown) {
      attempt++;
      if (attempt >= maxRetries) throw err;
      console.warn(`   [Retry] API Error: ${(err as any)?.message}. Retrying in ${delayMs}ms (Attempt ${attempt}/${maxRetries})...`);
      await new Promise(r => setTimeout(r, delayMs));
      delayMs *= 2; // exponential backoff
    }
  }
  throw new Error("Unreachable");
}

async function main() {
  if (!ai) {
    console.error("❌ Cannot run discovery: no GEMINI_API_KEY found in .env");
    process.exit(1);
  }

  console.log(`[Discovery] 🔎 Sweeping for uncategorized Gold Product Lines...`);

  // We find product lines that do not have any SilverCategoryIdeas yet
  const lines = await prisma.goldProductLine.findMany({
    where: {
      categoryIdeas: { none: {} }
    },
    include: {
      mentions: { 
        take: 3,
        include: {
          submission: { select: { title: true } }
        }
      }
    }
  });

  if (lines.length === 0) {
    console.log("[Discovery] ✅ No un-discovered product lines found.");
    return;
  }

  console.log(`[Discovery] 📦 Processing ${lines.length} lines for category idea generation...`);
  const total = lines.length;

  const args = process.argv.slice(2);
  const concurrencyIndex = args.indexOf("--concurrency");
  const CONCURRENCY = concurrencyIndex !== -1 ? parseInt(args[concurrencyIndex + 1], 10) || 10 : 10;
  
  console.log(`[Discovery] 🚀 Using concurrency: ${CONCURRENCY}`);

  const processLine = async (line: typeof lines[0], i: number) => {
    console.log(`[Discovery] 🧠 [${i + 1}/${total}] Brainstorming: ${line.brand} -> ${line.canonicalName}`);

    try {
      let contextStr = "No additional context.";
      if (line.mentions && line.mentions.length > 0) {
         contextStr = line.mentions.map(m => {
            const topic = m.submission?.title ? `Original Topic: "${m.submission.title}"\n` : "";
            return `${topic}Quote: "${m.quote}"`;
         }).join("\n\n");
      }

      const prompt = `You are a world-class e-commerce SEO strategist.
Your task is to analyze a "Buy It For Life" product and spontaneously generate 2 to 3 broad, high-volume Head-Term e-commerce hub categories that a buyer would type into Google to find it.

# PRODUCT IDENTITY
Brand: ${line.brand}
Product Line: ${line.canonicalName}
Context Evidence:
${contextStr}

# RULES
1. NEVER include the Brand Name or the Exact Model Name in your categories. You must return ONLY the generic functional classification. (e.g. return "Dive Watches", NOT "Seiko Dive Watches").
2. LIMIT categories to a maximum of 2 to 3 words. Target Head-Terms! (e.g. "Work Boots", "Food Storage").
3. DO NOT use descriptive durability adjectives like "Durable", "Heavy Duty", "BIFL", "Long Lasting", "Heirloom Quality", "Reliable". The entire site is Buy-It-For-Life, so these keywords are intrinsically assumed and redundant.
4. DO NOT return broad departments like "Shoes" or "Kitchen". 
5. DO return focused Head-Term hubs like "Work Boots", "Cast Iron Skillets", "Gaming Keyboards".
6. Return only the array of strings.`;

      const response = await withRetry(() => ai!.models.generateContent({
        model: ACTIVE_MODEL,
        contents: prompt,
        config: {
          responseMimeType: "application/json",
          responseSchema: llmResponseSchema,
          thinkingConfig: getThinkingConfig(ACTIVE_MODEL, ACTIVE_THINKING_LEVEL) as any
        }
      }));

      if (response.text) {
        const generatedCategories: string[] = JSON.parse(response.text);
        let savedCount = 0;

        for (const cat of generatedCategories) {
          if (!cat || typeof cat !== 'string') continue;
          const cleanName = cat.trim();
          if (cleanName.length < 3) continue;

          await prisma.silverCategoryIdea.create({
            data: {
              rawName: cleanName,
              goldProductLineId: line.id
            }
          });
          savedCount++;
        }

        console.log(`   [Discovery] ✨ Generated ${savedCount} SEO categories! (e.g. ${generatedCategories[0]})`);

        const usage = response.usageMetadata;
        if (usage) {
          const cost = calculateCost(ACTIVE_MODEL, {
            promptTokenCount: usage.promptTokenCount,
            cachedContentTokenCount: 0,
            candidatesTokenCount: usage.candidatesTokenCount,
          });
          await prisma.aiSpend.create({
            data: {
              jobName: "[Taxonomy] Discovery",
              model: ACTIVE_MODEL,
              promptTokens: usage.promptTokenCount,
              responseTokens: usage.candidatesTokenCount || 0,
              thinkingTokens: usage.thoughtsTokenCount || (usage as any).thoughts_token_count || 0,
              totalTokens: usage.totalTokenCount,
              costInUsd: cost
            }
          });
        }
      }
    } catch (err: unknown) {
      console.error(`   [Discovery] ❌ Error discovering ${line.id}:`, (err as Error)?.message);
    }
  };

  // Process in chunks
  for (let i = 0; i < lines.length; i += CONCURRENCY) {
    const chunk = lines.slice(i, i + CONCURRENCY);
    await Promise.all(chunk.map((line, idx) => processLine(line, i + idx)));
  }
}

main()
  .catch((err) => {
    console.error(`\n❌ Fatal Error:`, (err as any)?.message);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
