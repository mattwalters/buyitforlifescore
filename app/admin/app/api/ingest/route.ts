import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { prisma, Prisma } from "@mono/db";

const submissionSchema = z.object({
  redditId: z.string().min(1),
  title: z.string(),
  selftext: z.string().nullable(),
  author: z.string().nullable(),
  score: z.number(),
  url: z.string().nullable(),
  permalink: z.string().nullable(),
  numComments: z.number(),
  postedAt: z.coerce.date(),
});

const commentSchema = z.object({
  redditId: z.string().min(1),
  linkId: z.string(),
  parentId: z.string().nullable(),
  body: z.string(),
  author: z.string().nullable(),
  score: z.number(),
  postedAt: z.coerce.date(),
});

const payloadSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("submissions"),
    data: z.array(submissionSchema),
  }),
  z.object({
    type: z.literal("comments"),
    data: z.array(commentSchema),
  }),
]);

export async function POST(req: NextRequest) {
  try {
    const rawBody = await req.json();
    const result = payloadSchema.safeParse(rawBody);

    if (!result.success) {
      return NextResponse.json(
        { error: "Invalid payload", details: result.error.format() },
        { status: 400 }
      );
    }

    const payload = result.data;

    if (payload.type === "submissions") {
      const submissions = payload.data.map(sub => ({
        ...sub,
        isProcessed: false,
      }));

      await prisma.bronzeRedditSubmission.createMany({
        data: submissions,
        skipDuplicates: true,
      });

      return NextResponse.json({ success: true, count: submissions.length });
    } else if (payload.type === "comments") {
      const redditIds = [...new Set(payload.data.map((d) => d.linkId.replace("t3_", "")))];
      
      const subs = await prisma.bronzeRedditSubmission.findMany({
        where: { redditId: { in: redditIds } },
        select: { id: true, redditId: true },
      });
      const subMap = Object.fromEntries(subs.map((s) => [s.redditId, s.id]));

      const validComments = payload.data
        .map((d) => {
          let pId = d.parentId;
          if (pId && pId.startsWith('"') && pId.endsWith('"')) {
            pId = pId.slice(1, -1);
          }
          const sid = subMap[d.linkId.replace("t3_", "")];
          return {
            redditId: d.redditId,
            submissionId: sid,
            linkId: d.linkId,
            parentId: pId,
            body: d.body,
            author: d.author,
            score: d.score,
            postedAt: d.postedAt,
            isProcessed: false,
          };
        })
        .filter((c) => c.submissionId) as Prisma.BronzeRedditCommentCreateManyInput[];

      if (validComments.length > 0) {
        await prisma.bronzeRedditComment.createMany({
          data: validComments,
          skipDuplicates: true,
        });
      }

      return NextResponse.json({ success: true, count: validComments.length });
    }

    return NextResponse.json({ error: "Unknown error type" }, { status: 400 });
  } catch (error) {
    console.error("Ingestion error:", error);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
