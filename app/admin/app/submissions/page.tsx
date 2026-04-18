/* eslint-disable @typescript-eslint/no-explicit-any */
import { prisma } from "@mono/db";
import Link from "next/link";
import TokenStatsChart from "./stats-client";

export const dynamic = "force-dynamic";

export default async function SubmissionsPage(props: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const searchParams = await props.searchParams;
  const tab = typeof searchParams.tab === "string" ? searchParams.tab : "table";

  const pageParam = typeof searchParams.page === "string" ? searchParams.page : "1";
  const page = parseInt(pageParam, 10) || 1;
  const sortBy = typeof searchParams.sortBy === "string" ? searchParams.sortBy : "score";
  const dir = typeof searchParams.dir === "string" ? searchParams.dir : "desc";
  const pageSize = 50;

  const validDirs: readonly ["asc", "desc"] = ["asc", "desc"];
  const orderDir = validDirs.includes(dir as "asc" | "desc") ? (dir as "asc" | "desc") : "desc";

  const total = await prisma.bronzeRedditSubmission.count();

  // Conditionally fetch data based on tab to avoid over-fetching
  let tableSubmissions: any[] = [];
  let tokenData: {
    chars: number;
    mentions: number;
    score: number;
    comments: number;
    isProcessed: boolean;
  }[] = [];
  let specificityData: { name: string; value: number }[] = [];
  let sentimentData: { name: string; value: number }[] = [];
  let totalPages = 1;

  if (tab === "table") {
    tableSubmissions = await prisma.bronzeRedditSubmission.findMany({
      orderBy: { [sortBy]: orderDir },
      take: pageSize,
      skip: Math.max(0, (page - 1) * pageSize),
    });
    totalPages = Math.ceil(total / pageSize);
  } else if (tab === "stats") {
    const submissionsWithTokens = await prisma.bronzeRedditSubmission.findMany({
      where: { charCount: { not: null } },
      select: {
        charCount: true,
        score: true,
        numComments: true,
        isProcessed: true,
        _count: { select: { mentions: true } },
      },
    });
    tokenData = submissionsWithTokens.map((s) => ({
      chars: s.charCount as number,
      mentions: s._count.mentions,
      score: s.score,
      comments: s.numComments,
      isProcessed: s.isProcessed,
    }));

    const rawSpecificity = await prisma.silverProductMention.groupBy({
      by: ["specificityLevel"],
      _count: { id: true },
    });
    specificityData = rawSpecificity.map((r: any) => ({
      name: r.specificityLevel,
      value: r._count.id,
    }));

    const rawSentiment = await prisma.silverProductMention.groupBy({
      by: ["sentiment"],
      _count: { id: true },
    });
    sentimentData = rawSentiment.map((r: any) => ({ name: r.sentiment, value: r._count.id }));
  }

  return (
    <div className="container mx-auto p-8 space-y-8">
      <div className="flex flex-col gap-2">
        <h1 className="text-3xl font-bold tracking-tight">Bronze: Raw Reddit Threads</h1>
        <p className="text-muted-foreground">
          View and sort through {total.toLocaleString()} unstructured source threads.
        </p>
      </div>

      <div className="border-b mb-6">
        <nav className="-mb-px flex space-x-8">
          <Link
            href="?tab=table"
            className={`whitespace-nowrap pb-4 px-1 border-b-2 font-medium text-sm ${
              tab === "table"
                ? "border-primary text-primary"
                : "border-transparent text-muted-foreground hover:text-foreground hover:border-border"
            }`}
          >
            Data Browser
          </Link>
          <Link
            href="?tab=stats"
            className={`whitespace-nowrap pb-4 px-1 border-b-2 font-medium text-sm ${
              tab === "stats"
                ? "border-primary text-primary"
                : "border-transparent text-muted-foreground hover:text-foreground hover:border-border"
            }`}
          >
            Density Distribution
          </Link>
        </nav>
      </div>

      {tab === "table" && (
        <>
          <div className="rounded-md border bg-card text-card-foreground">
            <div className="w-full overflow-auto">
              <table className="w-full text-sm text-left">
                <thead className="border-b bg-muted/50 text-muted-foreground font-medium">
                  <tr>
                    <th className="h-10 px-4 align-middle">
                      <Link
                        href={`?tab=table&sortBy=title&dir=${sortBy === "title" && dir === "asc" ? "desc" : "asc"}`}
                      >
                        Title
                      </Link>
                    </th>
                    <th className="h-10 px-4 align-middle">
                      <Link
                        href={`?tab=table&sortBy=author&dir=${sortBy === "author" && dir === "asc" ? "desc" : "asc"}`}
                      >
                        Author
                      </Link>
                    </th>
                    <th className="h-10 px-4 align-middle w-24">
                      <Link
                        href={`?tab=table&sortBy=score&dir=${sortBy === "score" && dir === "asc" ? "desc" : "asc"}`}
                      >
                        Score
                      </Link>
                    </th>
                    <th className="h-10 px-4 align-middle w-32">
                      <Link
                        href={`?tab=table&sortBy=numComments&dir=${sortBy === "numComments" && dir === "asc" ? "desc" : "asc"}`}
                      >
                        Comments
                      </Link>
                    </th>
                    <th className="h-10 px-4 align-middle w-40">
                      <Link
                        href={`?tab=table&sortBy=postedAt&dir=${sortBy === "postedAt" && dir === "asc" ? "desc" : "asc"}`}
                      >
                        Date
                      </Link>
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {tableSubmissions.map((sub) => (
                    <tr
                      key={sub.id}
                      className="border-b transition-colors hover:bg-muted/50 data-[state=selected]:bg-muted"
                    >
                      <td className="p-4 align-middle font-medium">
                        <Link
                          href={`/submissions/${sub.id}`}
                          className="hover:underline text-primary"
                        >
                          {sub.title.length > 80 ? sub.title.substring(0, 80) + "..." : sub.title}
                        </Link>
                      </td>
                      <td className="p-4 align-middle text-muted-foreground">
                        {sub.author || "Unknown"}
                      </td>
                      <td className="p-4 align-middle font-semibold">{sub.score}</td>
                      <td className="p-4 align-middle">{sub.numComments}</td>
                      <td className="p-4 align-middle whitespace-nowrap text-muted-foreground">
                        {new Date(sub.postedAt).toLocaleDateString()}
                      </td>
                    </tr>
                  ))}
                  {tableSubmissions.length === 0 && (
                    <tr>
                      <td colSpan={5} className="p-8 text-center text-muted-foreground">
                        No submissions found.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          <div className="flex items-center gap-4 justify-end">
            <span className="text-sm text-muted-foreground">
              Page {page} of {totalPages || 1}
            </span>
            <div className="flex gap-2">
              {page > 1 && (
                <Link
                  href={`?tab=table&page=${page - 1}&sortBy=${sortBy}&dir=${dir}`}
                  className="rounded-md border px-3 py-1 text-sm hover:bg-accent hover:text-accent-foreground"
                >
                  Previous
                </Link>
              )}
              {page < totalPages && (
                <Link
                  href={`?tab=table&page=${page + 1}&sortBy=${sortBy}&dir=${dir}`}
                  className="rounded-md border px-3 py-1 text-sm bg-primary text-primary-foreground hover:bg-primary/90"
                >
                  Next
                </Link>
              )}
            </div>
          </div>
        </>
      )}

      {tab === "stats" && (
        <TokenStatsChart
          data={tokenData}
          specificityData={specificityData}
          sentimentData={sentimentData}
        />
      )}
    </div>
  );
}
