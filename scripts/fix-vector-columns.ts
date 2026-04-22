import { prisma } from "../lib/db/prisma";

async function fix() {
  console.log("Forcibly dropping vector columns to allow clean rebuild...");

  try {
    await prisma.$executeRawUnsafe(`ALTER TABLE "SilverProductMention" DROP COLUMN "embedding";`);
    console.log("Dropped Silver.embedding");
  } catch (e: any) {
    console.warn("Could not drop Silver.embedding (might not exist)");
  }

  try {
    await prisma.$executeRawUnsafe(
      `ALTER TABLE "SilverProductMention" DROP COLUMN "embedding768";`,
    );
    console.log("Dropped Silver.embedding768");
  } catch (e: any) {
    console.warn("Could not drop Silver.embedding768 (might not exist)");
  }

  try {
    await prisma.$executeRawUnsafe(`ALTER TABLE "GoldProduct" DROP COLUMN "embedding";`);
    console.log("Dropped Gold.embedding");
  } catch (e: any) {
    console.warn("Could not drop Gold.embedding (might not exist)");
  }

  try {
    await prisma.$executeRawUnsafe(`ALTER TABLE "GoldProduct" DROP COLUMN "embedding768";`);
    console.log("Dropped Gold.embedding768");
  } catch (e: any) {
    console.warn("Could not drop Gold.embedding768 (might not exist)");
  }

  console.log("\nDone! Now please run: npm run db:push -w @mono/db");
}

fix()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
