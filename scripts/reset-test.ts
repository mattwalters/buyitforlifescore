import { prisma } from "@mono/db";

async function main() {
  console.log("Deleting all SilverProductMentions...");
  const deleteResult = await prisma.silverProductMention.deleteMany({});
  console.log(`Deleted ${deleteResult.count} SilverProductMentions.`);

  console.log("Resetting isProcessed flag on BronzeRedditSubmissions...");
  const updateResult = await prisma.bronzeRedditSubmission.updateMany({
    data: { isProcessed: false },
  });
  console.log(`Reset ${updateResult.count} submissions.`);
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
