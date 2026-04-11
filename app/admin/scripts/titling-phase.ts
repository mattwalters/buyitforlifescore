/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */
 
import { prisma } from "@mono/db";
import { GoogleGenAI, Type, Schema } from "@google/genai";
import { AiModel, calculateCost, ThinkingLevel, getThinkingConfig } from "../../worker/src/pricing.js";
import { embedWithRetry } from "./local-embedder.js";
import { z } from "zod";
import * as dotenv from "dotenv";
dotenv.config({ path: "../../.env" });

const envSchema = z.object({
  GEMINI_API_KEY: z.string().optional(),
});
const env = envSchema.parse(process.env);

const ai = env.GEMINI_API_KEY ? new GoogleGenAI({ apiKey: env.GEMINI_API_KEY }) : null;

// The model configuration for this script run
const ACTIVE_MODEL = AiModel.GEMINI_2_5_FLASH_LITE;
const ACTIVE_THINKING_LEVEL: ThinkingLevel = "low";

const llmResponseSchema: Schema = {
  type: Type.OBJECT,
  properties: {
    canonicalName: { type: Type.STRING, description: "The single, precise, official marketing name." }
  },
  required: ["canonicalName"]
};

// Retry wrapper for Gemini Generation
const generateWithRetry = async (prompt: string, retries = 3) => {
  if (!ai) return null;
  for (let i = 0; i < retries; i++) {
    try {
      await new Promise(r => setTimeout(r, 1000)); // Rate limit buffer
      const response = await ai.models.generateContent({
        model: ACTIVE_MODEL,
        contents: prompt,
        config: {
          responseMimeType: "application/json",
          responseSchema: llmResponseSchema,
          thinkingConfig: getThinkingConfig(ACTIVE_MODEL, ACTIVE_THINKING_LEVEL)
        }
      });
      return response;
    } catch (e: unknown) {
      console.warn(`   ⚠️ LLM failed (attempt ${i + 1}/${retries}):`, e.message || e);
      if (i === retries - 1) return null;
      await new Promise(r => setTimeout(r, 2000));
    }
  }
  return null;
};


async function main() {
  if (!ai) {
    console.error("❌ Cannot run titling script: no GEMINI_API_KEY found in .env");
    process.exit(1);
  }

  const targetIdx = process.argv.indexOf("--target");
  if (targetIdx === -1 || !process.argv[targetIdx + 1]) {
    console.error("❌ Missing --target flag. Must be 'brands', 'lines', or 'models'");
    process.exit(1);
  }
  const target = process.argv[targetIdx + 1];

  console.log(`[Titling] 🧹 Fetching [${target}] for Titling phase...`);

  let items: any[] = [];
  
  if (target === "brands") {
     items = await prisma.goldBrand.findMany({
        where: { isTitled: false },
        include: {
           mentions: {
              take: 15,
              include: { submission: true, comment: true }
           }
        }
     });
  } else if (target === "lines") {
     items = await prisma.goldProductLine.findMany({
        where: { isTitled: false },
        include: {
           mentions: {
              take: 15,
              include: { submission: true, comment: true }
           }
        }
     });
  } else if (target === "models") {
     items = await prisma.goldProduct.findMany({
        where: { isTitled: false },
        include: {
           mentions: {
              take: 15,
              include: { submission: true, comment: true }
           }
        }
     });
  } else {
     console.error("❌ Invalid target.");
     process.exit(1);
  }

  if (items.length === 0) {
    console.log(`[Titling] ✅ No eligible [${target}] found.`);
    return;
  }

  console.log(`[Titling] 📦 Titling ${items.length} ${target}...`);
  const total = items.length;
  let renamedCount = 0;

  const CONCURRENCY = process.argv.includes("--concurrency")
    ? parseInt(process.argv[process.argv.indexOf("--concurrency") + 1], 10)
    : 10;

  let processIndex = 0;
  
  const worker = async () => {
    while (processIndex < items.length) {
      const i = processIndex++;
      const item = items[i];

      console.log(`\n[Titling] 🔍 [${i + 1}/${total}] Inspecting Canonical Name: "${item.canonicalName}"`);

      const extractText = (m: any) => m.quote || "";
      const quotes = item.mentions.map(extractText).filter((text: string) => text.trim().length > 0);

      if (quotes.length === 0) {
         console.log(`   [Titling] ⏭️ Skipping: No readable quote text found in attached mentions.`);
         continue;
      }

      const typeStr = target === "brands" ? "Corporate Brand" : target === "lines" ? "Product Line / Family" : "Exact Product Model";

      const brandRule = target !== "brands" && item.brand 
         ? `\nCRITICAL RULE: The parent brand is "${item.brand}". DO NOT include the brand name in your output! We already store the brand separately. Only output the pure ${typeStr} name.`
         : "";

      const skuRule = target === "models"
         ? `\nCRITICAL RULE: Actively strip meaningless SKUs, arbitrary alphanumeric tracking numbers, and raw physical sizing/weight measurements (e.g., oz, lbs, inches). However, you MUST PRESERVE defining feature metrics or version numbers that define the core product identity.`
         : "";

      const prompt = `You are a master e-commerce ontologist and brand marketer.
We have grouped several Reddit mentions into a product cluster currently labeled as: "${item.canonicalName}".
Some early users might have used slang, abbreviations, mispellings, or included descriptive adjectives (like 'pan', 'coat', 'drill') that attached to the name inappropriately.

Based on the context of the user quotes below, what is the SINGLE, precise, official marketing name for this ${typeStr}? 
Return only the corrected name string, normalized with correct casing and punctuation (e.g. "F-150" instead of "F150"). 
Try to avoid extremely long descriptive sentences, just output the product's official short title.
${brandRule}${skuRule}

## EXAMPLES OF BAD VS GOOD LLM TITLING
EXAMPLE 1 (Strip Meaningless SKUs)
Input: "Boronda Chesterfield 87 Sofa j000219647"
GOOD Output: "Boronda Chesterfield 87 Sofa"

EXAMPLE 2 (Strip Weights and Volumes)
Input: "Men Sensitive Skin Shave Cream 5.1 oz SKU 789664"
GOOD Output: "Men Sensitive Skin Shave Cream"

EXAMPLE 3 (Strip Lengths)
Input: "Good Grips 12-Inch Tongs with Silicone Head"
GOOD Output: "Good Grips Tongs with Silicone Head"

EXAMPLE 4 (Preserve Defining Features)
Input: "12-Hour Backpack"
GOOD Output: "12-Hour Backpack" (Preserve because "12-Hour" defines the class/style of the backpack, it is not an arbitrary dimension).

EXAMPLE 5 (Preserve Technical Versions)
Input: "Steelcase Leap V2"
GOOD Output: "Steelcase Leap V2" (Preserve "V2" as it is a crucial version identifier).

RAW USER QUOTES (Limited to 15):
${quotes.map((q: string) => `- "${q}"`).join("\n")}
`;

      const response = await generateWithRetry(prompt);

      if (response && response.text) {
         try {
            const result = JSON.parse(response.text);
            if (result.canonicalName && result.canonicalName.trim()) {
               const newName = result.canonicalName.trim();
               
               if (newName !== item.canonicalName) {
                  console.log(`   [Titling] ✨ RENAMING: "${item.canonicalName}"  ➡️  "${newName}"`);
                  renamedCount++;

                  const embedText = target === "brands" ? newName.toLowerCase() : `${item.brand} ${newName}`.toLowerCase();
                  const newVector = await embedWithRetry(embedText);
                  let vParam = "";
                  if (newVector.length > 0) {
                     vParam = `[${newVector.join(",")}]`;
                  }

                  if (target === "brands") {
                     await prisma.goldBrand.update({ where: { id: item.id }, data: { canonicalName: newName, isTitled: true }});
                     if (vParam) await prisma.$executeRaw`UPDATE "GoldBrand" SET embedding = ${vParam}::vector WHERE id = ${item.id};`;
                  } else if (target === "lines") {
                     await prisma.goldProductLine.update({ where: { id: item.id }, data: { canonicalName: newName, isTitled: true }});
                     if (vParam) await prisma.$executeRaw`UPDATE "GoldProductLine" SET embedding = ${vParam}::vector WHERE id = ${item.id};`;
                  } else if (target === "models") {
                     await prisma.goldProduct.update({ where: { id: item.id }, data: { canonicalName: newName, isTitled: true }});
                     if (vParam) await prisma.$executeRaw`UPDATE "GoldProduct" SET embedding = ${vParam}::vector WHERE id = ${item.id};`;
                  }
               } else {
                  console.log(`   [Titling] 👍 Name is already perfect. [${item.canonicalName}]`);
                  
                  if (target === "brands") {
                     await prisma.goldBrand.update({ where: { id: item.id }, data: { isTitled: true }});
                  } else if (target === "lines") {
                     await prisma.goldProductLine.update({ where: { id: item.id }, data: { isTitled: true }});
                  } else if (target === "models") {
                     await prisma.goldProduct.update({ where: { id: item.id }, data: { isTitled: true }});
                  }
               }

               // Log AI Spend
               const usage = response.usageMetadata;
               if (usage) {
                 const cost = calculateCost(ACTIVE_MODEL, {
                   promptTokenCount: usage.promptTokenCount,
                   cachedContentTokenCount: 0,
                   candidatesTokenCount: usage.candidatesTokenCount,
                 });
                 await prisma.aiSpend.create({
                   data: {
                     jobName: `[Gold] Titling: ${target.charAt(0).toUpperCase() + target.slice(1)}`,
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
            console.error(`   [Titling] ❌ Failed to parse LLM response:`, response.text);
         }
      }
    }
  };

  const pool = Array.from({ length: Math.min(CONCURRENCY, items.length) }).map(() => worker());
  await Promise.all(pool);

  console.log(`\n[Titling] ✅ Titling complete! Renamed ${renamedCount}/${total} items.`);
}

main()
  .catch((e) => {
    console.error("Fatal error:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
