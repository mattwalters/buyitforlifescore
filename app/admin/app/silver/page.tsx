import { prisma } from "@mono/db";
import Link from "next/link";
import SilverClientTable from "./silver-client-table";

export const dynamic = "force-dynamic";

export default async function SilverPage(props: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const searchParams = await props.searchParams;
  const pageParam = typeof searchParams.page === "string" ? searchParams.page : "1";
  const page = parseInt(pageParam, 10) || 1;
  const sortBy = typeof searchParams.sortBy === "string" ? searchParams.sortBy : "brand";
  const dir = typeof searchParams.dir === "string" ? searchParams.dir : "desc";
  const pageSize = 50;

  const validDirs: readonly ["asc", "desc"] = ["asc", "desc"];
  const orderDir = validDirs.includes(dir as "asc" | "desc") ? (dir as "asc" | "desc") : "desc";

  const goldProductId =
    typeof searchParams.goldProductId === "string" ? searchParams.goldProductId : undefined;
  const goldBrandId =
    typeof searchParams.goldBrandId === "string" ? searchParams.goldBrandId : undefined;
  const goldProductLineId =
    typeof searchParams.goldProductLineId === "string" ? searchParams.goldProductLineId : undefined;

  const where = {
    ...(goldProductId ? { goldProductId } : {}),
    ...(goldBrandId ? { goldBrandId } : {}),
    ...(goldProductLineId ? { goldProductLineId } : {}),
  };

  const mentions = await prisma.silverProductMention.findMany({
    where,
    orderBy: {
      [sortBy]: orderDir,
    },
    take: pageSize,
    skip: Math.max(0, (page - 1) * pageSize),
    include: {
      submission: {
        select: {
          title: true,
          postedAt: true,
        },
      },
    },
  });

  const total = await prisma.silverProductMention.count({ where });
  const totalPages = Math.ceil(total / pageSize);

  return (
    <div className="container mx-auto p-8 space-y-8">
      <div className="flex items-start justify-between">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight">Silver: Product Mentions</h1>
          <p className="text-muted-foreground">
            View and sort through {total.toLocaleString()} products extracted by the AI.
          </p>
        </div>
        <a
          href={`/api/export?type=silver${goldProductId ? `&goldProductId=${goldProductId}` : ""}${goldBrandId ? `&goldBrandId=${goldBrandId}` : ""}${goldProductLineId ? `&goldProductLineId=${goldProductLineId}` : ""}`}
          className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors"
        >
          Export CSV
        </a>
      </div>

      <SilverClientTable mentions={mentions} sortBy={sortBy} dir={dir} />

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
