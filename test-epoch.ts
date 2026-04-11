import { prisma } from "./lib/db/src/index.js";

async function main() {
  const total = await prisma.redditSubmission.count();
  const zeroEpoch = await prisma.redditSubmission.count({
    where: {
      postedAt: new Date(0),
    },
  });

  const percentage = total === 0 ? 0 : (zeroEpoch / total) * 100;
  console.log(`Total Submissions: ${total}`);
  console.log(`Submissions with epoch 0: ${zeroEpoch}`);
  console.log(`Percentage: ${percentage.toFixed(2)}%`);

  const commentsWithZeroEpoch = await prisma.redditComment.count({
    where: {
      postedAt: new Date(0),
    },
  });

  console.log(`\nBonus - Comments with epoch 0: ${commentsWithZeroEpoch}`);
}

main()
  .catch(console.error)
  .finally(() => prisma.$disconnect());
