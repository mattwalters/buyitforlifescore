/* eslint-disable @typescript-eslint/no-explicit-any */
 
 
 
import { prisma } from "@mono/db";

function cosineDistance(u: number[], v: number[]): number {
  let dot = 0, normU = 0, normV = 0;
  for (let i = 0; i < u.length; i++) {
    dot += u[i] * v[i];
    normU += u[i] * u[i];
    normV += v[i] * v[i];
  }
  return 1 - (dot / (Math.sqrt(normU) * Math.sqrt(normV)));
}

function shuffleArray<T>(array: T[]): T[] {
  for (let i = array.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}

async function main() {
  console.log("🔍 Fetching unique strings and their 768d vectors from the database...");
  
  // We'll analyze pairs across the most popular brands to get a good sample of real data
  const popularBrands = await prisma.silverProductMention.groupBy({
    by: ['brand'],
    _count: { brand: true },
    having: { brand: { _count: { gt: 3 } } },
    orderBy: { _count: { brand: 'desc' } },
    take: 15
  });

  const pairs: { str1: string, str2: string, dist: number, sim: number }[] = [];

  for (const b of popularBrands) {
    const mentions = await prisma.$queryRaw<Array<{ brand: string, productName: string, emb7: string }>>`
      SELECT brand, "productName", embedding768::text as "emb7"
      FROM "SilverProductMention"
      WHERE brand = ${b.brand} AND embedding768 IS NOT NULL
    `;

    const uniqueStrings = new Map<string, number[]>();
    for (const m of mentions) {
      if (m.emb7) {
        const str = `${m.brand.trim()} ${m.productName.trim()}`.toLowerCase();
        if (!uniqueStrings.has(str)) {
          uniqueStrings.set(str, JSON.parse(m.emb7));
        }
      }
    }

    const arr = Array.from(uniqueStrings.entries());
    // Calculate pairwise similarity
    for (let i = 0; i < arr.length; i++) {
      for (let j = i + 1; j < arr.length; j++) {
        const dist = cosineDistance(arr[i][1], arr[j][1]);
        pairs.push({
          str1: arr[i][0],
          str2: arr[j][0],
          dist,
          sim: 1 - dist
        });
      }
    }
  }

  // Sort by similarity descending
  pairs.sort((a, b) => b.sim - a.sim);

  console.log("\n=======================================================");
  console.log("🟢 HIGH SIMILARITY (0.90 - 1.00)");
  console.log("Should be practically identical variations of the same product.");
  console.log("=======================================================");
  const highTier = shuffleArray(pairs.filter(p => p.sim >= 0.90)).slice(0, 10);
  highTier.sort((a,b) => b.sim - a.sim).forEach(p => console.log(`[Sim: ${p.sim.toFixed(3)}] ${p.str1}  <==>  ${p.str2}`));

  console.log("\n=======================================================");
  console.log("🟡 THE BORDERLINE: AROUND 0.85 (0.83 - 0.87)");
  console.log("This is our current expected cutoff. Are these the same product?");
  console.log("=======================================================");
  const midTier = shuffleArray(pairs.filter(p => p.sim >= 0.83 && p.sim <= 0.87)).slice(0, 10);
  midTier.sort((a,b) => b.sim - a.sim).forEach(p => console.log(`[Sim: ${p.sim.toFixed(3)}] ${p.str1}  <==>  ${p.str2}`));

  console.log("\n=======================================================");
  console.log("🟠 LOWER BORDERLINE: AROUND 0.80 (0.78 - 0.82)");
  console.log("If we lower the threshold to 0.80, these will be clustered together.");
  console.log("=======================================================");
  const lowBorder = shuffleArray(pairs.filter(p => p.sim >= 0.78 && p.sim <= 0.82)).slice(0, 10);
  lowBorder.sort((a,b) => b.sim - a.sim).forEach(p => console.log(`[Sim: ${p.sim.toFixed(3)}] ${p.str1}  <==>  ${p.str2}`));

  console.log("\n=======================================================");
  console.log("🔴 CLEARLY DIFFERENT CATEGORIES (< 0.75)");
  console.log("Should represent distinct products within the same brand.");
  console.log("=======================================================");
  const lowTier = shuffleArray(pairs.filter(p => p.sim < 0.75)).slice(0, 10);
  lowTier.sort((a,b) => b.sim - a.sim).forEach(p => console.log(`[Sim: ${p.sim.toFixed(3)}] ${p.str1}  <==>  ${p.str2}`));
}

main()
  .catch(console.error)
  .finally(() => prisma.$disconnect());
