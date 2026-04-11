import { prisma } from "@mono/db";
import Link from "next/link";
import LineClientTable from "./client-table";

export const dynamic = "force-dynamic";

export default async function LinesPage(props: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const searchParams = await props.searchParams;
  const pageParam = typeof searchParams.page === "string" ? searchParams.page : "1";
  const page = parseInt(pageParam, 10) || 1;
  const sortBy = typeof searchParams.sortBy === "string" ? searchParams.sortBy : "mentionCount";
  const dir = typeof searchParams.dir === "string" ? searchParams.dir : "desc";
  const pageSize = 50;

  const validDirs: readonly ["asc", "desc"] = ["asc", "desc"];
  const orderDir = validDirs.includes(dir as "asc" | "desc") ? (dir as "asc" | "desc") : "desc";

  const lines = await prisma.goldProductLine.findMany({
    orderBy: {
      [sortBy]: orderDir,
    },
    take: pageSize,
    skip: Math.max(0, (page - 1) * pageSize),
  });

  const total = await prisma.goldProductLine.count();
  const totalPages = Math.ceil(total / pageSize);

  return (
    <div className="container mx-auto p-8 space-y-8">
      <div className="flex items-start justify-between">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight text-yellow-500 flex items-center gap-2">
            Gold: Product Lines
          </h1>
          <p className="text-muted-foreground">
            View all {total.toLocaleString()} product lines and series (e.g. All-Clad D5).
          </p>
        </div>
        <a
          href="/api/export?type=goldLines"
          className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors"
        >
          Export CSV
        </a>
      </div>

      <LineClientTable lines={lines} sortBy={sortBy} dir={dir} />

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
