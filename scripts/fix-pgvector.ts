import { prisma } from "@mono/db";

async function main() {
  console.log("Forcing pgvector 1024-D constraints...");

  try {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SilverProductMention" ALTER COLUMN "embedding" TYPE vector(1024);`,
    );
    console.log("SilverProductMention altered.");
  } catch (e: any) {
    console.warn("Silver skip:", e.message);
  }

  try {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "GoldBrand" ALTER COLUMN "embedding" TYPE vector(1024);`,
    );
    console.log("GoldBrand altered.");
  } catch (e: any) {
    console.warn("GoldBrand skip:", e.message);
  }

  try {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "GoldProductLine" ALTER COLUMN "embedding" TYPE vector(1024);`,
    );
    console.log("GoldProductLine altered.");
  } catch (e: any) {
    console.warn("GoldProductLine skip:", e.message);
  }

  try {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "GoldProduct" ALTER COLUMN "embedding" TYPE vector(1024);`,
    );
    console.log("GoldProduct altered.");
  } catch (e: any) {
    console.warn("GoldProduct skip:", e.message);
  }

  console.log("Done!");
}

main().finally(() => prisma.$disconnect());
