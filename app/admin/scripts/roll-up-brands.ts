/* eslint-disable @typescript-eslint/no-explicit-any */

import { prisma } from "@mono/db";
import { GoogleGenAI, Type, Schema } from "@google/genai";
import {
  AiModel,
  calculateCost,
  ThinkingLevel,
  getThinkingConfig,
} from "../../worker/src/pricing.js";
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

function getSentimentScore(sentiment: string): number {
  if (sentiment === "POSITIVE") return 10.0;
  if (sentiment === "NEGATIVE") return 0.0;
  return 5.0; // NEUTRAL or MIXED
}

const llmResponseSchema: Schema = {
  type: Type.OBJECT,
  properties: {
    isMatch: {
      type: Type.BOOLEAN,
      description:
        "True if the mention is the exact same corporate brand as one of the candidates.",
    },
    matchId: {
      type: Type.STRING,
      nullable: true,
      description: "The ID of the matching candidate brand, if any.",
    },
  },
  required: ["isMatch"],
};

// Hybrid Matcher Helper Functions
function levenshtein(a: string, b: string): number {
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;
  const matrix = [];
  for (let i = 0; i <= b.length; i++) matrix[i] = [i];
  for (let j = 0; j <= a.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) == a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          Math.min(matrix[i][j - 1] + 1, matrix[i - 1][j] + 1),
        );
      }
    }
  }
  return matrix[b.length][a.length];
}

function normalize(str: string): string {
  return (str || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function getWords(str: string): Set<string> {
  return new Set(
    (str || "")
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter(Boolean),
  );
}

async function main() {
  if (!ai) {
    console.error("❌ Cannot run rollup: no GEMINI_API_KEY found in .env");
    process.exit(1);
  }

  console.log(`[Rollup] 🧹 Sweeping for unmapped Silver mentions...`);

  const mentions = await prisma.silverProductMention.findMany({
    where: {
      goldBrandId: null,
    },
  });

  if (mentions.length === 0) {
    console.log("[Rollup] ✅ No unmapped mentions found.");
    return;
  }

  console.log(`[Rollup] 📦 Processing ${mentions.length} mentions for BRAND rollup...`);

  const total = mentions.length;

  for (const [i, m] of mentions.entries()) {
    const cleanBrandName = m.brand.trim();
    const lowerBrand = cleanBrandName.toLowerCase();

    if (
      [
        "unknown",
        "generic",
        "unbranded",
        "homemade",
        "none",
        "n/a",
        "brand not specified",
        "unspecified",
        "no brand",
        "",
      ].includes(lowerBrand)
    ) {
      console.log(
        `\n[Rollup] ⏭️  [${i + 1}/${total}] Skipping excluded generic brand: "${cleanBrandName}"`,
      );
      continue;
    }

    console.log(`\n[Rollup] 🔍 [${i + 1}/${total}] Evaluating Brand: "${lowerBrand}"`);

    try {
      let finalGoldBrandId: string | null = null;
      let generateNewCentroid = false;

      // 1. GLOBAL LEXICAL MATCH BYPASS (Solves Unique Constraint collisions & skips PGVector)
      const exactMatch = await prisma.goldBrand.findFirst({
        where: {
          canonicalName: {
            equals: cleanBrandName,
            mode: "insensitive",
          },
        },
      });

      if (exactMatch) {
        console.log(
          `   [Rollup] ⚡ GLOBAL LEXICAL BYPASS: Perfectly matches Brand ${exactMatch.id}`,
        );
        finalGoldBrandId = exactMatch.id;
      }

      if (!finalGoldBrandId) {
        // 2. PGVector Nearest Neighbor Search
        let vectorLiteral: string = "";

        const rawSilver = await prisma.$queryRaw<Array<{ vec: string }>>`
          SELECT embedding::text as vec 
          FROM "SilverProductMention" 
          WHERE id = ${m.id}
        `;

        if (rawSilver.length > 0 && rawSilver[0].vec) {
          vectorLiteral = rawSilver[0].vec;
        } else {
          console.warn(`[Rollup] ⚠️ Skipped ${m.id} because it has no vector.`);
          continue;
        }

        interface MatchResult {
          id: string;
          canonicalName: string;
          distance: number;
        }

        const candidates = await prisma.$queryRaw<MatchResult[]>`
          SELECT id, "canonicalName", (embedding <=> ${vectorLiteral}::vector) as distance
          FROM "GoldBrand"
          ORDER BY embedding <=> ${vectorLiteral}::vector ASC
          LIMIT 25
        `;

        const validCandidatesMap = new Map<string, MatchResult>();
        const newWords = getWords(cleanBrandName);
        const norm1 = normalize(cleanBrandName);

        for (const c of candidates) {
          // 1. Vector Net
          if (c.distance <= 0.25) {
            validCandidatesMap.set(c.id, c);
            continue;
          }

          // 2. Lexical Subset Overlap
          const candWords = getWords(c.canonicalName);
          let intersectionCount = 0;
          for (const word of newWords) {
            if (candWords.has(word)) intersectionCount++;
          }
          if (
            intersectionCount > 0 &&
            (intersectionCount === newWords.size || intersectionCount === candWords.size)
          ) {
            validCandidatesMap.set(c.id, c);
            continue;
          }

          // 3. Levenshtein Fuzzy Match
          const norm2 = normalize(c.canonicalName);
          // Prevent tiny words from false matching (like "GE" and "GM") if lev is up to 3
          const levLimit = norm1.length <= 3 ? 1 : 3;
          if (levenshtein(norm1, norm2) <= levLimit) {
            validCandidatesMap.set(c.id, c);
          }
        }

        const validCandidates = Array.from(validCandidatesMap.values());
        validCandidates.sort((a, b) => a.distance - b.distance);

        if (validCandidates.length > 20) {
          console.warn(
            `   [Rollup] ⚠️ WARNING: Sending ${validCandidates.length} generated candidates to LLM! Context window/understanding may degrade.`,
          );
        }

        // 3. The Gemini Tiebreaker Check (For Typo matching against valid centroids)
        if (validCandidates.length > 0) {
          const candidateContext = validCandidates
            .map(
              (c, i) =>
                `Option ${i + 1}: [ID: ${c.id}] Brand Name: ${c.canonicalName} | Distance: ${c.distance.toFixed(3)}`,
            )
            .join("\n");

          const prompt = `You are a corporate brand deduplication expert.
A user has submitted a new "Buy It For Life" mention that refers ONLY to a brand name (e.g., "Patagonia", "KitchenAid", "Le Creuset").
We need to determine if this new mention refers to the EXACT SAME company as any of our existing database records.

# RULES FOR MATCHING
1. You are primarily fixing typos and slight variations. "KitchnAid" matches "KitchenAid". "L.L. Bean" matches "LL Bean".
2. Do not merge separate companies.

# NEW EXTRACTED BRAND:
Brand: ${cleanBrandName}

# CANDIDATE "GOLD" BRANDS FROM DATABASE:
${candidateContext}

Return a JSON object. If one of the candidate options is unambiguously the exact same brand as the new mention, set "isMatch" to true and return its ID. If none match perfectly, set "isMatch" to false and "matchId" to null.`;

          const response = await ai.models.generateContent({
            model: ACTIVE_MODEL,
            contents: prompt,
            config: {
              responseMimeType: "application/json",
              responseSchema: llmResponseSchema,
              thinkingConfig: getThinkingConfig(ACTIVE_MODEL, ACTIVE_THINKING_LEVEL) as any,
            },
          });

          if (response.text) {
            const result = JSON.parse(response.text);
            if (result.isMatch && result.matchId) {
              const verifiedId = validCandidates.find((c) => c.id === result.matchId);
              if (verifiedId) {
                finalGoldBrandId = verifiedId.id;
                console.log(
                  `   [Rollup] 🤖 LLM MATCHED TYPO: Option -> ${verifiedId.id} (${verifiedId.canonicalName})`,
                );
              }
            } else {
              console.log(`   [Rollup] 🤖 LLM REJECTED all candidates. Must be a novel brand.`);
            }

            const usage = response.usageMetadata;
            if (usage) {
              const cost = calculateCost(ACTIVE_MODEL, {
                promptTokenCount: usage.promptTokenCount,
                cachedContentTokenCount: 0,
                candidatesTokenCount: usage.candidatesTokenCount,
              });
              await prisma.aiSpend.create({
                data: {
                  jobName: "[Gold] Rollup: Brands",
                  submissionId: m.submissionId,
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
          }
        } else {
          console.log(
            `   [Rollup] 📉 No valid PGVector candidates found within distance threshold.`,
          );
        }

        if (!finalGoldBrandId) {
          generateNewCentroid = true;
        }
      }

      // 4. Execution Phase: Create Brand + Embed Pure Centroid
      if (generateNewCentroid || !finalGoldBrandId) {
        // Generate a PURE CENTROID embedding of just the Brand Name
        // This ensures typos naturally cluster to this pure vector!
        let pureVec: number[] = [];
        try {
          pureVec = await embedWithRetry(cleanBrandName);
        } catch (e: unknown) {
          console.error("   [Rollup] Failed to generate centroid vector:", e);
        }

        const created = await prisma.goldBrand.create({
          data: {
            canonicalName: cleanBrandName,
            mentionCount: 0,
            avgSentiment: 0,
          },
        });
        finalGoldBrandId = created.id;

        if (pureVec.length > 0) {
          const vParam = `[${pureVec.join(",")}]`;
          await prisma.$executeRaw`
              UPDATE "GoldBrand" 
              SET embedding = ${vParam}::vector 
              WHERE id = ${finalGoldBrandId};
            `;
        }
        console.log(
          `   [Rollup] ✨ CREATED new GoldBrand: ${finalGoldBrandId} (${cleanBrandName}) with pure centroid.`,
        );
      }

      // Link Mention and calculate stats
      await prisma.$transaction(async (tx) => {
        await tx.silverProductMention.update({
          where: { id: m.id },
          data: { goldBrandId: finalGoldBrandId },
        });

        const allMentions = await tx.silverProductMention.findMany({
          where: { goldBrandId: finalGoldBrandId },
          select: { sentiment: true },
        });

        let totalScore = 0;
        allMentions.forEach((mention) => (totalScore += getSentimentScore(mention.sentiment)));
        const newAvg = allMentions.length > 0 ? totalScore / allMentions.length : 0;

        await tx.goldBrand.update({
          where: { id: finalGoldBrandId },
          data: {
            mentionCount: allMentions.length,
            avgSentiment: newAvg,
          },
        });
      });
    } catch (err: unknown) {
      console.error(`   [Rollup] ❌ Error processing mention ${m.id}:`, (err as any)?.message);
    }
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
