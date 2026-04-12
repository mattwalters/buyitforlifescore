import { prisma } from "@mono/db";
import Link from "next/link";
import DepartmentClientTable from "./client-table";

export const dynamic = "force-dynamic";

export default async function DepartmentsPage(props: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const searchParams = await props.searchParams;
  const pageParam = typeof searchParams.page === "string" ? searchParams.page : "1";
  const page = parseInt(pageParam, 10) || 1;
  const sortBy = typeof searchParams.sortBy === "string" ? searchParams.sortBy : "canonicalName";
  const dir = typeof searchParams.dir === "string" ? searchParams.dir : "asc";
  const pageSize = 50;

  const validDirs: readonly ["asc", "desc"] = ["asc", "desc"];
  const orderDir = validDirs.includes(dir as "asc" | "desc") ? (dir as "asc" | "desc") : "asc";

  const validSorts = ["canonicalName", "productLines", "products", "createdAt"];
  const sortSafe = validSorts.includes(sortBy) ? sortBy : "canonicalName";

  const departments = await prisma.goldDepartment.findMany({
    orderBy: sortSafe === "productLines" 
      ? { productLines: { _count: orderDir } }
      : sortSafe === "products" 
      ? { products: { _count: orderDir } }
      : { [sortSafe]: orderDir },
    include: {
      _count: {
        select: { productLines: true, products: true }
      }
    },
    take: pageSize,
    skip: Math.max(0, (page - 1) * pageSize),
  });

  const total = await prisma.goldDepartment.count();
  const totalPages = Math.ceil(total / pageSize);

  return (
    <div className="container mx-auto p-8 space-y-8">
      <div className="flex items-start justify-between">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold tracking-tight text-yellow-500 flex items-center gap-2">
             Top-Down Departments
          </h1>
          <p className="text-muted-foreground">
            View all {total.toLocaleString()} core parent departments defining the taxonomy.
          </p>
        </div>
      </div>

      <DepartmentClientTable departments={departments} sortBy={sortBy} dir={dir} />

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
