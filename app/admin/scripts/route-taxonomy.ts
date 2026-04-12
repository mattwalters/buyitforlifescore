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
  type: Type.OBJECT,
  properties: {
    departmentIndex: { type: Type.INTEGER, description: "The 1-based index (e.g. 1, 2, 3) of exactly ONE department from the list." },
    categoryIndices: { 
      type: Type.ARRAY, 
      items: { type: Type.INTEGER },
      description: "An array of 1-based indices representing the specific sub-categories this product applies to (choose 1 to 6)." 
    }
  },
  required: ["departmentIndex", "categoryIndices"]
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
    console.error("❌ Cannot run routing: no GEMINI_API_KEY found in .env");
    process.exit(1);
  }

  // Load Top-Down Departments
  const departments = await prisma.goldDepartment.findMany();
  if (departments.length === 0) {
     console.error("❌ No Departments found! Run seed-departments.ts first.");
     process.exit(1);
  }
  const deptContext = departments.map((d, i) => `${i + 1}. ${d.canonicalName}`).join("\n");

  console.log(`[Routing] 🔀 Sweeping for unrouted Gold Product Lines...`);

  // An unrouted line has no department assigned
  const lines = await prisma.goldProductLine.findMany({
    where: { goldDepartmentId: null },
    include: {
      categoryIdeas: { select: { rawName: true } },
      mentions: {
        take: 3,
        include: { submission: { select: { title: true } } }
      }
    },
    take: 100
  });

  if (lines.length === 0) {
    console.log("[Routing] ✅ No unrouted product lines found.");
    return;
  }

  console.log(`[Routing] 📦 Matrix routing ${lines.length} lines...`);
  const total = lines.length;

  const args = process.argv.slice(2);
  const concurrencyIndex = args.indexOf("--concurrency");
  const CONCURRENCY = concurrencyIndex !== -1 ? parseInt(args[concurrencyIndex + 1], 10) || 10 : 10;
  
  console.log(`[Routing] 🚀 Using concurrency: ${CONCURRENCY}`);

  const processLine = async (line: typeof lines[0], i: number) => {
    console.log(`\n[Routing] 🚦 [${i + 1}/${total}] Routing: ${line.brand} ${line.canonicalName}`);

    // Extract Context
    const ideas = line.categoryIdeas.map(c => c.rawName).filter(Boolean);
    const threadTitles = Array.from(new Set(
      line.mentions
        .map(m => m.submission?.title)
        .filter(Boolean)
    ));
    
    // Format them for the prompt
    const ideasContext = ideas.length > 0 ? ideas.join(", ") : "None";
    const titleContext = threadTitles.length > 0 ? threadTitles.map(t => `- "${t}"`).join("\n") : "None";

    // If it has no embedding, we can't vector search. So skip or fallback
    let vectorLiteral: string = "";
    const rawVec = await prisma.$queryRaw<Array<{ vec: string }>>`
      SELECT embedding::text as vec 
      FROM "GoldProductLine" 
      WHERE id = ${line.id}
    `;

    if (rawVec.length > 0 && rawVec[0].vec) {
      vectorLiteral = rawVec[0].vec;
    }

    let topCategories: Array<{ id: string, canonicalName: string }> = [];

    if (vectorLiteral) {
       // Find the closest 60 GoldCategories by cosine distance
       topCategories = await prisma.$queryRaw<Array<{ id: string, canonicalName: string }>>`
         SELECT id, "canonicalName"
         FROM "GoldCategory"
         WHERE embedding IS NOT NULL
         ORDER BY embedding <=> ${vectorLiteral}::vector ASC
         LIMIT 60
       `;
    } else {
       // Fallback: just grab 60 random ones or handle it.
       topCategories = await prisma.goldCategory.findMany({ select: { id: true, canonicalName: true }, take: 60 });
    }

    // --- Pin Category Idea Matches ---
    if (ideas.length > 0) {
      const ideaMatches = await prisma.goldCategory.findMany({
        where: { canonicalName: { in: ideas } },
        select: { id: true, canonicalName: true }
      });
      
      const existingIds = new Set(topCategories.map(c => c.id));
      for (const match of ideaMatches) {
        if (!existingIds.has(match.id)) {
          topCategories.push(match);
          existingIds.add(match.id);
        }
      }
    }

    if (topCategories.length === 0) {
       console.log(`   [Routing] ⚠️ No GoldCategories exist at all. Skip.`);
       return;
    }

    const catContext = topCategories.map((c, idx) => `${idx + 1}. ${c.canonicalName}`).join("\n");

    const prompt = `You are an expert e-commerce catalog architect.
Your job is to slot a product precisely into our taxonomy matrix.

# PRODUCT TO ROUTE
Brand: ${line.brand}
Product Line: ${line.canonicalName}

# ORIGINAL DISCOVERY CONTEXT
(Use these hints from the original source to understand what this product is, if the name is obscure)
Parent Conversational Threads:
${titleContext}
Top Category Guesses:
${ideasContext}

# TOP-DOWN DEPARTMENTS (Select EXACTLY ONE)
${deptContext}

# CANDIDATE BOTTOM-UP CATEGORIES (Select 1 to 6)
${catContext}

# INSTRUCTIONS
1. Evaluate the product.
2. Select the index number of the single best overarching Department.
3. Select an array of index numbers from the Candidate Categories list that this product acts as a member of.`;

    try {
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
        const result: { departmentIndex: number, categoryIndices: number[] } = JSON.parse(response.text);

        if (result.departmentIndex && result.categoryIndices.length > 0) {
           // Verify indices
           const validDept = departments[result.departmentIndex - 1];
           const validCats = result.categoryIndices
               .map(idx => topCategories[idx - 1])
               .filter(Boolean); // Drop undefined if LLM guesses out of bounds

           if (validDept && validCats.length > 0) {
              await prisma.goldProductLine.update({
                 where: { id: line.id },
                 data: {
                    goldDepartmentId: validDept.id,
                    categories: {
                       connect: validCats.map(c => ({ id: c.id }))
                    }
                 }
              });
              console.log(`   [Routing] 🎯 Routed to Department: '${validDept.canonicalName}' and ${validCats.length} Categories.`);
           } else {
              console.log(`   [Routing] ⚠️ LLM hallucinated indices. ${result.departmentIndex} and [${result.categoryIndices.join(",")}] are out of bounds. Skipping.`);
           }
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
              jobName: "[Taxonomy] Matrix Routing",
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
      console.error(`   [Routing] ❌ Error:`, (err as Error)?.message);
    }
  };

  // Process in chunks
  for (let i = 0; i < lines.length; i += CONCURRENCY) {
    const chunk = lines.slice(i, i + CONCURRENCY);
    await Promise.all(chunk.map((line, idx) => processLine(line, i + idx)));
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
