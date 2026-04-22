import { prisma } from "@mono/db";

async function viewShortPosts() {
  console.log("🕵️  Investigating 'Low Signal' Posts (< 100 characters)...\n");

  // Fetch the top 10 scoring posts that are under 100 characters in total length
  const shortSubmissions = await prisma.$queryRaw<
    {
      id: string;
      title: string | null;
      selftext: string | null;
      score: number;
      total_length: bigint;
    }[]
  >`
    SELECT 
      s.id,
      s.title,
      s.selftext,
      s.score,
      (
        COALESCE(LENGTH(s.title), 0) + 
        COALESCE(LENGTH(s.selftext), 0) + 
        COALESCE(SUM(LENGTH(c.body)), 0)
      ) AS total_length
    FROM "BronzeRedditSubmission" s
    LEFT JOIN "BronzeRedditComment" c ON s.id = c."submissionId"
    GROUP BY s.id, s.title, s.selftext, s.score
    HAVING (
        COALESCE(LENGTH(s.title), 0) + 
        COALESCE(LENGTH(s.selftext), 0) + 
        COALESCE(SUM(LENGTH(c.body)), 0)
    ) < 100
    ORDER BY s.score DESC
    LIMIT 20
  `;

  if (shortSubmissions.length === 0) {
    console.log("None found!");
    return;
  }

  for (const sub of shortSubmissions) {
    // Fetch comments to display alongside
    const comments = await prisma.bronzeRedditComment.findMany({
      where: { submissionId: sub.id },
      select: { body: true, score: true },
    });

    console.log(`===============================================`);
    console.log(`Score: ${sub.score} | Total Chars: ${sub.total_length}`);
    console.log(`Title: ${sub.title}`);
    console.log(`Body:  ${sub.selftext ? sub.selftext : "(empty)"}`);

    if (comments.length > 0) {
      console.log(`\n--- Comments (${comments.length}) ---`);
      for (const c of comments) {
        console.log(`  [${c.score}]: ${c.body}`);
      }
    } else {
      console.log(`\n--- No Comments ---`);
    }
    console.log(`\n`);
  }
}

viewShortPosts()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
