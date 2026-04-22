import { prisma } from "@mono/db";

async function fix() {
  console.log("Forcibly re-creating vector columns with the correct sizes...");

  try {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SilverProductMention" ALTER COLUMN "embedding" TYPE vector(3072);`,
    );
    console.log("Altered Silver.embedding to vector(3072)");
  } catch (e: any) {
    console.warn("Could not alter Silver.embedding:", e.message);
  }

  try {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SilverProductMention" ALTER COLUMN "embedding768" TYPE vector(768);`,
    );
    console.log("Altered Silver.embedding768 to vector(768)");
  } catch (e: any) {
    console.warn("Could not alter Silver.embedding768:", e.message);
  }

  try {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "GoldProduct" ALTER COLUMN "embedding" TYPE vector(3072);`,
    );
    console.log("Altered Gold.embedding to vector(3072)");
  } catch (e: any) {
    console.warn("Could not alter Gold.embedding:", e.message);
  }

  try {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "GoldProduct" ALTER COLUMN "embedding768" TYPE vector(768);`,
    );
    console.log("Altered Gold.embedding768 to vector(768)");
  } catch (e: any) {
    console.warn("Could not alter Gold.embedding768:", e.message);
  }

  console.log("\nDone! Start your worker!");
}

fix()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
