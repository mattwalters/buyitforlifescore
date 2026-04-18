import { prisma } from "@mono/db";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowLeft, ExternalLink, MessageCircle, ArrowUp } from "lucide-react";

export const dynamic = "force-dynamic";

export default async function SubmissionDetailPage(props: { params: Promise<{ id: string }> }) {
  const params = await props.params;
  const { id } = params;

  const submission = await prisma.bronzeRedditSubmission.findUnique({
    where: { id },
    include: {
      comments: {
        orderBy: { score: "desc" },
      },
      mentions: {
        include: {
          goldBrand: { select: { canonicalName: true } },
        },
      },
    },
  });

  if (!submission) {
    notFound();
  }

  const postUrl = submission.permalink
    ? `https://reddit.com${submission.permalink}`
    : submission.url
      ? submission.url
      : `https://reddit.com/r/BuyItForLife/comments/${submission.redditId}`;

  type CommentWithChildren = NonNullable<typeof submission>["comments"][0] & {
    children: CommentWithChildren[];
  };

  const commentsByRedditId: Record<string, CommentWithChildren> = {};
  const rootComments: CommentWithChildren[] = [];

  submission.comments.forEach((c) => {
    commentsByRedditId[c.redditId] = { ...c, children: [] };
  });

  submission.comments.forEach((c) => {
    const node = commentsByRedditId[c.redditId];
    if (c.parentId) {
      const parentRedditId = c.parentId.replace(/^(t1_|t3_)/, "");
      const parentNode = commentsByRedditId[parentRedditId];
      if (parentNode) {
        parentNode.children.push(node);
      } else {
        // If parent is not in our comment list it means it's a reply to the submission
        rootComments.push(node);
      }
    } else {
      rootComments.push(node);
    }
  });

  // Small helper to render recursively
  const CommentThread = ({ comment, depth }: { comment: CommentWithChildren; depth: number }) => {
    return (
      <div
        className={`space-y-3 ${depth > 0 ? "border-l-2 pl-4 ml-2 mt-4 border-muted" : "rounded-lg border bg-card p-4"}`}
      >
        <div className="flex items-center gap-3 text-sm text-muted-foreground">
          <span className="font-medium text-foreground">u/{comment.author || "Unknown"}</span>
          <span className="flex items-center gap-1">
            <ArrowUp className="w-3 h-3" /> {comment.score.toLocaleString()}
          </span>
          <span>{new Date(comment.postedAt).toLocaleString()}</span>
        </div>
        <div className="text-sm whitespace-pre-wrap leading-relaxed break-words">
          {comment.body}
        </div>

        {comment.children.length > 0 && (
          <div className="pt-2">
            {comment.children.map((child) => (
              <CommentThread key={child.id} comment={child} depth={depth + 1} />
            ))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="container mx-auto p-4 md:p-8 space-y-8 max-w-5xl">
      <div className="flex items-center gap-4 text-sm text-muted-foreground">
        <Link
          href="/submissions"
          className="flex items-center gap-1 hover:text-foreground transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Submissions
        </Link>
      </div>

      <div className="rounded-xl border bg-card text-card-foreground shadow-sm">
        <div className="p-6 md:p-8 space-y-6">
          <div className="space-y-4">
            <h1 className="text-2xl md:text-3xl font-bold tracking-tight">{submission.title}</h1>

            <div className="flex flex-wrap items-center gap-4 text-sm text-muted-foreground">
              <span className="font-medium text-foreground">
                u/{submission.author || "Unknown"}
              </span>
              <span className="flex items-center gap-1">
                <ArrowUp className="w-4 h-4" /> {submission.score.toLocaleString()}
              </span>
              <span className="flex items-center gap-1">
                <MessageCircle className="w-4 h-4" /> {submission.numComments.toLocaleString()}
              </span>
              <span>{new Date(submission.postedAt).toLocaleString()}</span>
              <a
                href={postUrl}
                target="_blank"
                rel="noreferrer"
                className="flex items-center gap-1 text-primary hover:underline ml-auto"
              >
                View on Reddit <ExternalLink className="w-4 h-4" />
              </a>
            </div>
          </div>

          {submission.selftext && (
            <div className="prose prose-sm md:prose-base dark:prose-invert max-w-none rounded-lg bg-muted/30 p-6 whitespace-pre-wrap font-sans break-words">
              {submission.selftext}
            </div>
          )}
        </div>
      </div>

      {submission.mentions.length > 0 && (
        <div className="space-y-4">
          <h2 className="text-xl font-semibold flex items-center gap-2">
            ✨ Extracted Silver Mentions ({submission.mentions.length})
          </h2>
          <div className="rounded-xl border bg-card text-card-foreground shadow-sm overflow-hidden">
            <table className="w-full text-sm text-left">
              <thead className="border-b bg-muted/50 text-muted-foreground font-medium whitespace-nowrap">
                <tr>
                  <th className="h-10 px-4 align-middle">Brand</th>
                  <th className="h-10 px-4 align-middle">Product</th>
                  <th className="h-10 px-4 align-middle">Sentiment</th>
                  <th className="h-10 px-4 align-middle">Gold Link</th>
                </tr>
              </thead>
              <tbody>
                {submission.mentions.map((m) => (
                  <tr
                    key={m.id}
                    className="border-b transition-colors hover:bg-muted/50 whitespace-nowrap"
                  >
                    <td className="p-4 align-middle font-medium">
                      <Link href={`/silver/${m.id}`} className="text-foreground hover:underline">
                        {m.brand}
                      </Link>
                    </td>
                    <td className="p-4 align-middle text-muted-foreground">{m.productName}</td>
                    <td className="p-4 align-middle">
                      <div
                        className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold ${m.sentiment === "POSITIVE" ? "bg-primary text-primary-foreground border-transparent" : m.sentiment === "NEGATIVE" ? "bg-destructive text-destructive-foreground border-transparent" : "bg-secondary text-secondary-foreground border-transparent"}`}
                      >
                        {m.sentiment}
                      </div>
                    </td>
                    <td className="p-4 align-middle text-muted-foreground">
                      {m.goldBrand ? (
                        <Link
                          href={`/gold/brands/${m.goldBrandId}`}
                          className="text-orange-600 hover:underline"
                        >
                          {m.goldBrand.canonicalName}
                        </Link>
                      ) : (
                        "-"
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div className="space-y-4">
        <h2 className="text-xl font-semibold flex items-center gap-2">
          <MessageCircle className="w-5 h-5" />
          Top Comments ({submission.comments.length})
        </h2>

        {rootComments.length === 0 ? (
          <div className="rounded-lg border border-dashed p-8 text-center text-muted-foreground">
            No comments have been ingested for this submission yet.
          </div>
        ) : (
          <div className="space-y-4">
            {rootComments.map((comment) => (
              <CommentThread key={comment.id} comment={comment} depth={0} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
