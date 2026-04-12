 
/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable prefer-const */
import { prisma } from "@mono/db";
import { GoogleGenAI } from "@google/genai";
import { z } from "zod";
import * as dotenv from "dotenv";
dotenv.config({ path: "../../.env" });

const envSchema = z.object({
  GEMINI_API_KEY: z.string().optional(),
});
const env = envSchema.parse(process.env);
const ai = env.GEMINI_API_KEY ? new GoogleGenAI({ apiKey: env.GEMINI_API_KEY }) : null;

function cosineDistance(u: number[], v: number[]): number {
  let dot = 0, normU = 0, normV = 0;
  for (let i = 0; i < u.length; i++) {
    dot += u[i] * v[i];
    normU += u[i] * u[i];
    normV += v[i] * v[i];
  }
  return 1 - (dot / (Math.sqrt(normU) * Math.sqrt(normV)));
}

async function main() {
  if (!ai) {
    console.error("❌ No GEMINI_API_KEY found");
    return;
  }

  console.log("🔍 Running K-NN Distribution Analysis (3072 vs 768)...\n");
  
  // 1. Get a sample of Silver mentions that share a brand
  const popularBrands = await prisma.silverProductMention.groupBy({
    by: ['brand'],
    _count: { brand: true },
    having: { brand: { _count: { gt: 3 } } },
    orderBy: { _count: { brand: 'desc' } },
    take: 10
  });

  const cache3072 = new Map<string, number[]>();
  const cache768 = new Map<string, number[]>();
  let distances3072: number[] = [];
  let distances768: number[] = [];

  console.log("Fetching embeddings from the database for sample clusters...");

  for (const b of popularBrands) {
    const mentions = await prisma.$queryRaw<Array<{ brand: string, productName: string, emb3: string, emb7: string }>>`
      SELECT 
        brand, 
        "productName", 
        embedding::text as "emb3", 
        embedding768::text as "emb7"
      FROM "SilverProductMention"
      WHERE brand = ${b.brand}
        AND embedding IS NOT NULL
        AND embedding768 IS NOT NULL
    `;

    const uniqueStrings = new Set<string>();
    const vectors3072: number[][] = [];
    const vectors768: number[][] = [];

    for (const m of mentions) {
      const str = `${m.brand.trim()} ${m.productName.trim()}`.toLowerCase();
      if (!uniqueStrings.has(str)) {
        uniqueStrings.add(str);
        try {
          vectors3072.push(JSON.parse(m.emb3));
          vectors768.push(JSON.parse(m.emb7));
        } catch(e) {
          console.warn(`Failed to parse embeddings for ${str}`);
        }
      }
    }

    if (uniqueStrings.size < 2) continue;

    // Calculate all pairwise distances within this brand
    for (let i = 0; i < vectors3072.length; i++) {
      for (let j = i + 1; j < vectors3072.length; j++) {
        distances3072.push(cosineDistance(vectors3072[i], vectors3072[j]));
        distances768.push(cosineDistance(vectors768[i], vectors768[j]));
      }
    }
  }

  if (distances3072.length === 0) {
      console.log("Not enough varied data to plot.");
      return;
  }

  // 2. Bin the distances and plot histograms
  const binSize = 0.02;
  const maxDist = 0.40;
  const binCount = Math.ceil(maxDist / binSize) + 1; 
  
  const bins3072 = new Array(binCount).fill(0);
  const bins768 = new Array(binCount).fill(0);
  
  for (let i = 0; i < distances3072.length; i++) {
    const d3 = distances3072[i];
    const d7 = distances768[i];
    
    if (d3 <= 0.40) bins3072[Math.floor(d3 / binSize)]++;
    if (d7 <= 0.40) bins768[Math.floor(d7 / binSize)]++;
  }

  const maxBinValue = Math.max(...bins3072, ...bins768) || 1;
  const maxBarLength = 30;

  console.log("\n📊 DISTANCE DISTRIBUTION COMPARISON (Lower = closer semantics)\n");
  console.log("Range        | 3072 Dimensions                | 768 Dimensions");
  console.log("-------------|--------------------------------|--------------------------------");

  for (let i = 0; i < binCount; i++) {
    const binStart = (i * binSize).toFixed(2);
    const binEnd = ((i + 1) * binSize).toFixed(2);
    
    const count3072 = bins3072[i];
    const len3072 = Math.round((count3072 / maxBinValue) * maxBarLength);
    const bar3072 = "█".repeat(len3072).padEnd(maxBarLength, ' ');
    
    const count768 = bins768[i];
    const len768 = Math.round((count768 / maxBinValue) * maxBarLength);
    const bar768 = "█".repeat(len768).padEnd(maxBarLength, ' ');
    
    console.log(`[${binStart}-${binEnd}): | ${count3072.toString().padStart(3)} ${bar3072} | ${count768.toString().padStart(3)} ${bar768}`);
  }

  console.log("\n💡 Analysis:");
  console.log("- Look for the 'valley' separating matches from non-matches in each column.");
  console.log("- If the 768 column stretches items apart more clearly, it's removing noise.");
}

main()
  .catch((e) => {
    console.error("Fatal error:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
