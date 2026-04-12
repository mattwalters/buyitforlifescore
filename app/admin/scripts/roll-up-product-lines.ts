/* eslint-disable @typescript-eslint/no-explicit-any */
 
 
 
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

function getSentimentScore(sentiment: string): number {
  if (sentiment === "POSITIVE") return 10.0;
  if (sentiment === "NEGATIVE") return 0.0;
  return 5.0; // NEUTRAL or MIXED
}

const llmResponseSchema: Schema = {
  type: Type.OBJECT,
  properties: {
    isMatch: { type: Type.BOOLEAN, description: "True if the mention belongs to the exact same product line / family as one of the candidates." },
    matchId: { type: Type.STRING, nullable: true, description: "The ID of the matching candidate product line, if any." }
  },
  required: ["isMatch"]
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
        matrix[i][j] = Math.min(matrix[i - 1][j - 1] + 1, Math.min(matrix[i][j - 1] + 1, matrix[i - 1][j] + 1));
      }
    }
  }
  return matrix[b.length][a.length];
}

function normalize(str: string): string {
  return (str || "").toLowerCase().replace(/[^a-z0-9]/g, '');
}

function getWords(str: string): Set<string> {
  return new Set((str || "").toLowerCase().split(/[^a-z0-9]+/).filter(Boolean));
}

// No getOrCreateBrand. We strictly rely on the parent Brand script mapping first.


async function main() {
  if (!ai) {
    console.error("❌ Cannot run rollup: no GEMINI_API_KEY found in .env");
    process.exit(1);
  }

  console.log(`[Rollup] 🧹 Sweeping for unmapped PRODUCT_LINE Silver mentions...`);

  const mentions = await prisma.silverProductMention.findMany({
    where: {
      goldProductLineId: null,
      specificityLevel: "PRODUCT_LINE",
    },
  });

  if (mentions.length === 0) {
    console.log("[Rollup] ✅ No unmapped product line mentions found.");
    return;
  }

  console.log(`[Rollup] 📦 Processing ${mentions.length} mentions for PRODUCT LINE rollup...`);

  const total = mentions.length;

  for (const [i, m] of mentions.entries()) {
    const canonicalStr = `${m.brand.trim()} ${m.productName.trim()}`.toLowerCase();
    console.log(`\n[Rollup] 🔍 [${i + 1}/${total}] Evaluating Line: "${canonicalStr}"`);

    try {
      let finalGoldLineId: string | null = null;
      let generateNewCentroid = false;

      // START STRICT DEPENDENCY CHECK
      if (!m.goldBrandId) {
         console.warn(`   [Rollup] ⚠️ Skipped: Parent Brand not mapped. (Run brands script first, or it was an excluded generic brand.)`);
         continue;
      }
      const brandId = m.goldBrandId;

      // 1. GLOBAL LEXICAL MATCH BYPASS
      const exactMatch = await prisma.goldProductLine.findFirst({
        where: {
          goldBrandId: brandId,
          canonicalName: {
            equals: m.productName.trim(),
            mode: "insensitive"
          }
        }
      });

      if (exactMatch) {
         console.log(`   [Rollup] ⚡ GLOBAL LEXICAL BYPASS: Perfectly matches Product Line ${exactMatch.id}`);
         finalGoldLineId = exactMatch.id;
      }

      if (!finalGoldLineId) {
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
          brand: string;
          canonicalName: string;
          distance: number;
        }
        
        const candidates = await prisma.$queryRaw<MatchResult[]>`
          SELECT id, brand, "canonicalName", (embedding <=> ${vectorLiteral}::vector) as distance
          FROM "GoldProductLine"
          WHERE "goldBrandId" = ${brandId}
          ORDER BY embedding <=> ${vectorLiteral}::vector ASC
          LIMIT 25
        `;

        const validCandidatesMap = new Map<string, MatchResult>();
        const lineName = m.productName.trim();
        const newWords = getWords(lineName);
        const norm1 = normalize(lineName);

        for (const c of candidates) {
           if (c.distance <= 0.25) {
              validCandidatesMap.set(c.id, c);
              continue;
           }

           const candWords = getWords(c.canonicalName);
           let intersectionCount = 0;
           for (const word of newWords) {
             if (candWords.has(word)) intersectionCount++;
           }
           if (intersectionCount > 0 && (intersectionCount === newWords.size || intersectionCount === candWords.size)) {
              validCandidatesMap.set(c.id, c);
              continue;
           }

           const norm2 = normalize(c.canonicalName);
           const levLimit = norm1.length <= 3 ? 1 : 3;
           if (levenshtein(norm1, norm2) <= levLimit) {
              validCandidatesMap.set(c.id, c);
           }
        }

        const validCandidates = Array.from(validCandidatesMap.values());
        validCandidates.sort((a,b) => a.distance - b.distance);

        if (validCandidates.length > 20) {
           console.warn(`   [Rollup] ⚠️ WARNING: Sending ${validCandidates.length} generated candidates to LLM! Context window/understanding may degrade.`);
        }
        
        // 3. The Gemini Tiebreaker Check
        if (validCandidates.length > 0) {
           const candidateContext = validCandidates.map((c, i) => 
             `Option ${i+1}: [ID: ${c.id}] Brand: ${c.brand} | Line Name: ${c.canonicalName} | Distance: ${c.distance.toFixed(3)}`
           ).join("\n");

           const prompt = `You are a database classification expert organizing a product taxonomy.
A user has submitted a new "Buy It For Life" product mention that has been identified as a PRODUCT LINE (a family or series of goods, e.g., "All-Clad D5 Series", "Patagonia Baggies", "Red Wing Iron Ranger").
We need to determine if this new mention refers to the EXACT SAME Product Line as any of our existing database records.

# RULES FOR MATCHING
1. To match, the concepts must refer to the exact same product family. "KitchenAid Artisan Series" matches "KitchenAid Artisan". 
2. Do not match completely different product lines. "Patagonia Baggies" is completely different from "Patagonia Torrentshell".
3. We are mapping pluralities. "Darn Tough hiking socks" matches "Darn Tough hiking sock line".

# NEW EXTRACTED PRODUCT LINE:
Brand: ${m.brand.trim()}
Product Line Name: ${m.productName.trim()}

# CANDIDATE "GOLD" PRODUCT LINES FROM DATABASE:
${candidateContext}

Return a JSON object. If one of the candidate options is unambiguously the exact same core product line as the new mention, set "isMatch" to true and return its ID. If none match perfectly, set "isMatch" to false and "matchId" to null.`;

           const response = await ai.models.generateContent({
             model: ACTIVE_MODEL,
             contents: prompt,
             config: {
               responseMimeType: "application/json",
               responseSchema: llmResponseSchema,
               thinkingConfig: getThinkingConfig(ACTIVE_MODEL, ACTIVE_THINKING_LEVEL) as any
             }
           });

           if (response.text) {
             const result = JSON.parse(response.text);
             if (result.isMatch && result.matchId) {
                const verifiedId = validCandidates.find(c => c.id === result.matchId);
                if (verifiedId) {
                   finalGoldLineId = verifiedId.id;
                   console.log(`   [Rollup] 🤖 LLM MATCHED TYPO: Option -> ${verifiedId.id} (${verifiedId.canonicalName})`);
                }
             } else {
               console.log(`   [Rollup] 🤖 LLM REJECTED all candidates. Must be a novel product line.`);
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
                   jobName: "[Gold] Rollup: Product Lines",
                   submissionId: m.submissionId,
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
        } else {
           console.log(`   [Rollup] 📉 No valid PGVector candidates found within distance threshold.`);
        }

        if (!finalGoldLineId) {
           generateNewCentroid = true;
        }
      }

      // 4. Execution Phase: Create Product Line + Embed Pure Centroid
      if (generateNewCentroid || !finalGoldLineId) {
         let pureVec: number[] = [];
         try {
            pureVec = await embedWithRetry(canonicalStr);
         } catch(e: unknown) {
            console.error(`   [Rollup] ❌ Error processing mention ${m.id}:`, (e as any)?.message);
         }

         const created = await prisma.goldProductLine.create({
           data: {
             goldBrandId: brandId,
             brand: m.brand.trim(),
             canonicalName: m.productName.trim(),
             mentionCount: 0,
             avgSentiment: 0,
           },
         });
         finalGoldLineId = created.id;
         
         if (pureVec.length > 0) {
            const vParam = `[${pureVec.join(",")}]`;
            await prisma.$executeRaw`
              UPDATE "GoldProductLine" 
              SET embedding = ${vParam}::vector 
              WHERE id = ${finalGoldLineId};
            `;
         }
         console.log(`   [Rollup] ✨ CREATED new GoldProductLine: ${finalGoldLineId} (${m.productName.trim()}) with pure centroid.`);
      }

      // Link Mention and calculate stats
      await prisma.$transaction(async (tx) => {
         await tx.silverProductMention.update({
            where: { id: m.id },
            data: { goldProductLineId: finalGoldLineId },
         });

         const allMentions = await tx.silverProductMention.findMany({
           where: { goldProductLineId: finalGoldLineId },
           select: { sentiment: true },
         });

         let totalScore = 0;
         allMentions.forEach(mention => totalScore += getSentimentScore(mention.sentiment));
         const newAvg = allMentions.length > 0 ? (totalScore / allMentions.length) : 0;

         await tx.goldProductLine.update({
           where: { id: finalGoldLineId },
           data: {
             mentionCount: allMentions.length,
             avgSentiment: newAvg,
           },
         });
      });
      
    } catch (err: unknown) {
      console.error(`   ❌ Error processing mention ${m.id}:`, (err as any)?.message || err);
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
