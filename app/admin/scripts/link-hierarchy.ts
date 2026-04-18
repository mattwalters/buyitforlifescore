/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */

import { prisma } from "@mono/db";
import { GoogleGenAI, Type, Schema } from "@google/genai";
import {
  AiModel,
  calculateCost,
  ThinkingLevel,
  getThinkingConfig,
} from "../../worker/src/pricing.js";
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
    isLinked: {
      type: Type.BOOLEAN,
      description: "True if the model belongs to one of the candidate product lines.",
    },
    matchedLineId: {
      type: Type.STRING,
      nullable: true,
      description: "The ID of the matching product line, if any.",
    },
  },
  required: ["isLinked"],
};

const generateWithRetry = async (prompt: string, retries = 3) => {
  if (!ai) return null;
  for (let i = 0; i < retries; i++) {
    try {
      await new Promise((r) => setTimeout(r, 1000));
      const response = await ai.models.generateContent({
        model: ACTIVE_MODEL,
        contents: prompt,
        config: {
          responseMimeType: "application/json",
          responseSchema: llmResponseSchema,
          thinkingConfig: getThinkingConfig(ACTIVE_MODEL, ACTIVE_THINKING_LEVEL) as any,
        },
      });
      return response;
    } catch (e: unknown) {
      console.warn(`   ⚠️ LLM failed (attempt ${i + 1}/${retries}):`, (e as any).message || e);
      if (i === retries - 1) return null;
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  return null;
};

async function main() {
  if (!ai) {
    console.error("❌ Cannot run linker script: no GEMINI_API_KEY found in .env");
    process.exit(1);
  }

  const CONCURRENCY = process.argv.includes("--concurrency")
    ? parseInt(process.argv[process.argv.indexOf("--concurrency") + 1], 10)
    : 10;

  console.log(`[Hierarchy] 🧹 Fetching unlinked GoldProducts...`);

  // Query all exact models that are NOT YET LINKED to a line
  // We include the full brand and all its product lines so we have the candidate list natively!
  const items = await prisma.goldProduct.findMany({
    where: { goldProductLineId: null, isHierarchyAnalyzed: false },
    include: {
      goldBrand: {
        include: { productLines: true },
      },
    },
  });

  if (items.length === 0) {
    console.log(`[Hierarchy] ✅ No unlinked models found!`);
    return;
  }

  console.log(
    `[Hierarchy] 📦 Analyzing ${items.length} Extra Models for Hierarchy Links (concurrency: ${CONCURRENCY})...`,
  );

  const total = items.length;
  let linkedCount = 0;
  let processIndex = 0;

  const worker = async () => {
    while (processIndex < items.length) {
      const i = processIndex++;
      const model = items[i];
      const brand = model.goldBrand;
      const candidates = brand?.productLines || [];

      console.log(
        `\n[Hierarchy] 🔍 [${i + 1}/${total}] Inspecting Model: "${model.canonicalName}"`,
      );

      if (!brand) {
        console.log(`   [Hierarchy] ⏭️ Skipping: Model is orphaned (No Brand ID).`);
        await prisma.goldProduct.update({
          where: { id: model.id },
          data: { isHierarchyAnalyzed: true },
        });
        continue;
      }

      if (candidates.length === 0) {
        console.log(
          `   [Hierarchy] ⏭️ Skipping: No product lines exist for brand "${brand.canonicalName}".`,
        );
        await prisma.goldProduct.update({
          where: { id: model.id },
          data: { isHierarchyAnalyzed: true },
        });
        continue;
      }

      const candidateContext = candidates
        .map((l: any) => `- [ID: ${l.id}] ${l.canonicalName}`)
        .join("\n");

      const prompt = `You are an expert e-commerce catalog taxonomist. We are building a specific product hierarchy.

We have an EXACT PRODUCT MODEL and a short list of existing marketed PRODUCT LINES for the same brand.
Please determine if this exact model is a variant/member of one of these specific product lines.
If it is a standalone product that does NOT fit into any of these marketed families, return isLinked=false.

# CONTEXT
Brand: ${brand.canonicalName}
Exact Model Name: ${model.canonicalName}

# CANDIDATE PRODUCT LINES FOR THIS BRAND:
${candidateContext}

Return a JSON object indicating if the Exact Model is a member of any of the candidate Product Lines. If yes, return the ID.`;

      const response = await generateWithRetry(prompt);

      if (response && response.text) {
        try {
          const result = JSON.parse(response.text);
          if (result.isLinked && result.matchedLineId) {
            const verifiedLine = candidates.find((c: any) => c.id === result.matchedLineId);
            if (verifiedLine) {
              console.log(
                `   [Hierarchy] 🔗 LINKED: "${model.canonicalName}"  ➡️  [${verifiedLine.canonicalName}]`,
              );
              linkedCount++;

              await prisma.goldProduct.update({
                where: { id: model.id },
                data: { goldProductLineId: verifiedLine.id },
              });
            } else {
              console.log(`   [Hierarchy] ⚠️ LLM returned invalid ID: ${result.matchedLineId}`);
            }
          } else {
            console.log(`   [Hierarchy] 📉 Unlinked: Model stands alone.`);
          }

          // Always mark as analyzed so we don't query it again!
          await prisma.goldProduct.update({
            where: { id: model.id },
            data: { isHierarchyAnalyzed: true },
          });

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
                jobName: "[Gold] Hierarchy Linker",
                model: ACTIVE_MODEL,
                promptTokens: usage.promptTokenCount,
                responseTokens: usage.candidatesTokenCount || 0,
                thinkingTokens:
                  usage.thoughtsTokenCount || (usage as any).thoughts_token_count || 0,
                totalTokens: usage.totalTokenCount,
                costInUsd: cost,
              },
            });
          }
        } catch (err: unknown) {
          console.error(`   [Hierarchy] ❌ Failed to parse LLM response:`, response.text);
        }
      }
    }
  };

  const pool = Array.from({ length: Math.min(CONCURRENCY, items.length) }).map(() => worker());
  await Promise.all(pool);

  console.log(
    `\n[Hierarchy] ✅ Hierarchy sweep complete! Linked ${linkedCount}/${total} exact models to a parent line.`,
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
