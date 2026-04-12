"use client";

import Link from "next/link";

type DepartmentData = {
  id: string;
  canonicalName: string;
  createdAt: Date;
  _count: {
    productLines: number;
    products: number;
  };
};

export default function DepartmentClientTable({
  departments,
  sortBy,
  dir,
}: {
  departments: DepartmentData[];
  sortBy: string;
  dir: string;
}) {

  // We could add Client sorting like in the other tables if we want.
  // For now, these are usually fetched pre-sorted from Server Components.
  
  const getSortLink = (colKey: string) => {
    const p = new URLSearchParams();
    p.set("sortBy", colKey);
    p.set("dir", sortBy === colKey && dir === "desc" ? "asc" : "desc");
    return `?${p.toString()}`;
  };

  return (
    <div className="rounded-md border bg-card text-card-foreground shadow-sm overflow-hidden">
      <div className="relative w-full overflow-auto">
        <table className="w-full caption-bottom text-sm">
          <thead className="[&_tr]:border-b bg-muted/50">
            <tr className="border-b transition-colors hover:bg-muted/50 data-[state=selected]:bg-muted">
              <th className="h-12 px-4 text-left align-middle font-medium text-muted-foreground w-[100px]">
                <Link href={getSortLink("id")}>
                  ID {sortBy === "id" && (dir === "asc" ? "↑" : "↓")}
                </Link>
              </th>
              <th className="h-12 px-4 text-left align-middle font-medium text-muted-foreground hover:text-foreground">
                <Link href={getSortLink("canonicalName")}>
                  Canonical Name {sortBy === "canonicalName" && (dir === "asc" ? "↑" : "↓")}
                </Link>
              </th>
              <th className="h-12 px-4 text-center align-middle font-medium text-muted-foreground hover:text-foreground">
                <Link href={getSortLink("productLines")}>
                  Product Lines {sortBy === "productLines" && (dir === "asc" ? "↑" : "↓")}
                </Link>
              </th>
              <th className="h-12 px-4 text-center align-middle font-medium text-muted-foreground hover:text-foreground">
                <Link href={getSortLink("products")}>
                  Products {sortBy === "products" && (dir === "asc" ? "↑" : "↓")}
                </Link>
              </th>
              <th className="h-12 px-4 text-right align-middle font-medium text-muted-foreground hover:text-foreground">
                <Link href={getSortLink("createdAt")}>
                  Discovered {sortBy === "createdAt" && (dir === "asc" ? "↑" : "↓")}
                </Link>
              </th>
            </tr>
          </thead>
          <tbody className="[&_tr:last-child]:border-0 bg-background">
            {departments.map((dept) => (
              <tr key={dept.id} className="border-b transition-colors hover:bg-muted/50 data-[state=selected]:bg-muted">
                <td className="p-4 align-middle text-xs font-mono text-muted-foreground">
                  {dept.id.slice(-6)}
                </td>
                <td className="p-4 align-middle font-medium">
                  {dept.canonicalName}
                </td>
                <td className="p-4 align-middle text-center">
                  <span className="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold bg-primary/10 text-primary">
                    {dept._count.productLines}
                  </span>
                </td>
                <td className="p-4 align-middle text-center">
                  <span className="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold bg-secondary text-secondary-foreground">
                    {dept._count.products}
                  </span>
                </td>
                <td className="p-4 align-middle text-right text-muted-foreground text-sm">
                  {new Date(dept.createdAt).toLocaleDateString()}
                </td>
              </tr>
            ))}
            {departments.length === 0 && (
              <tr>
                <td colSpan={5} className="p-4 text-center text-muted-foreground h-24">
                  No departments found. Have you seeded them?
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
