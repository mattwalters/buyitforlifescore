import { prisma } from "@mono/db";
import Link from "next/link";
import CategoryIdeaClientTable from "./client-table";

export const dynamic = "force-dynamic";

export default async function CategoryIdeasPage(props: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const searchParams = await props.searchParams;
  const pageParam = typeof searchParams.page === "string" ? searchParams.page : "1";
  const page = parseInt(pageParam, 10) || 1;
  const sortBy = typeof searchParams.sortBy === "string" ? searchParams.sortBy : "createdAt";
  const dir = typeof searchParams.dir === "string" ? searchParams.dir : "desc";
  const pageSize = 50;

  const validDirs: readonly ["asc", "desc"] = ["asc", "desc"];
  const orderDir = validDirs.includes(dir as "asc" | "desc") ? (dir as "asc" | "desc") : "desc";

  const ideas = await prisma.silverCategoryIdea.findMany({
    orderBy: {
      [sortBy]: orderDir,
    },
    include: {
      goldProductLine: {
        select: { brand: true, canonicalName: true },
      },
    },
    take: pageSize,
    skip: Math.max(0, (page - 1) * pageSize),
  });

  const total = await prisma.silverCategoryIdea.count();
  const totalPages = Math.ceil(total / pageSize);

  return (
    <div className="container mx-auto p-8 space-y-8">
      <div className="flex items-start justify-between">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight text-slate-300 flex items-center gap-2">
            Silver: Category Hallucinations
          </h1>
          <p className="text-muted-foreground">
            View all {total.toLocaleString()} raw AI-generated category ideas awaiting
            deduplication.
          </p>
        </div>
      </div>

      <CategoryIdeaClientTable ideas={ideas} sortBy={sortBy} dir={dir} />

      <div className="flex items-center gap-4 justify-end">
        <span className="text-sm text-muted-foreground">
          Page {page} of {totalPages || 1}
        </span>
        <div className="flex gap-2">
          {page > 1 && (
            <Link
              href={`?page=${page - 1}&sortBy=${sortBy}&dir=${dir}`}
              className="rounded-md border px-3 py-1 text-sm hover:bg-accent hover:text-accent-foreground"
            >
              Previous
            </Link>
          )}
          {page < totalPages && (
            <Link
              href={`?page=${page + 1}&sortBy=${sortBy}&dir=${dir}`}
              className="rounded-md border px-3 py-1 text-sm bg-primary text-primary-foreground hover:bg-primary/90"
            >
              Next
            </Link>
          )}
        </div>
      </div>
    </div>
  );
}
