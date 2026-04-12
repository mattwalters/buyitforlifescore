/* eslint-disable @typescript-eslint/no-explicit-any */
"use client";

import { useState } from "react";
import Link from "next/link";

export default function BrandClientTable({ 
  brands, 
  sortBy, 
  dir,
}: { 
  brands: Record<string, any>[],
  sortBy: string,
  dir: string,
}) {
  const allColumns = [
    { key: "id", label: "ID" },
    { key: "canonicalName", label: "Brand Name" },
    { key: "avgSentiment", label: "Avg Sentiment" },
    { key: "mentionCount", label: "Child Mentions" },
    { key: "createdAt", label: "Discovered" }
  ];

  const [visibleColumns, setVisibleColumns] = useState<Set<string>>(new Set([
    "canonicalName", "avgSentiment", "mentionCount", "createdAt"
  ]));

  const toggleCol = (key: string) => {
    const next = new Set(visibleColumns);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setVisibleColumns(next);
  };

  const formatSentiment = (val: number) => {
    return val?.toFixed(1) || "0.0";
  };

  const getSentimentVariant = (val: number) => {
    if (val >= 7.5) return "border-transparent bg-green-600 text-white";
    if (val >= 5) return "border-transparent bg-primary text-primary-foreground";
    if (val > 3) return "border-transparent bg-secondary text-secondary-foreground";
    return "border-transparent bg-destructive text-destructive-foreground";
  };

  const queryParams = new URLSearchParams();

  const getSortLink = (colKey: string) => {
    const p = new URLSearchParams(queryParams.toString());
    p.set("sortBy", colKey);
    p.set("dir", sortBy === colKey && dir === "asc" ? "desc" : "asc");
    return `?${p.toString()}`;
  };

  return (
    <div className="space-y-4">
      <details className="inline-block relative">
        <summary className="list-none cursor-pointer inline-flex items-center gap-2 rounded-md border px-3 py-1.5 text-sm font-medium hover:bg-muted bg-card">
          <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
          Toggle Columns
        </summary>
        <div className="absolute top-full left-0 z-50 mt-2 min-w-80 rounded-md border bg-popover p-4 shadow-md flex flex-wrap gap-3">
          {allColumns.map(col => (
            <label key={col.key} className="flex items-center gap-2 text-sm cursor-pointer whitespace-nowrap">
              <input 
                type="checkbox" 
                className="w-4 h-4"
                checked={visibleColumns.has(col.key)} 
                onChange={() => toggleCol(col.key)} 
              />
              {col.label}
            </label>
          ))}
        </div>
      </details>

      <div className="rounded-md border bg-card text-card-foreground">
        <div className="w-full overflow-auto">
          <table className="w-full text-sm text-left">
            <thead className="border-b bg-muted/50 text-muted-foreground font-medium whitespace-nowrap">
              <tr>
                {allColumns.map(col => visibleColumns.has(col.key) && (
                  <th key={col.key} className={`h-10 px-4 align-middle ${col.key === "mentionCount" ? "text-right w-24" : ""}`}>
                    <Link href={getSortLink(col.key)}>
                      {col.label} {sortBy === col.key && (dir === "asc" ? "↑" : "↓")}
                    </Link>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {brands.map((brand) => (
                <tr
                  key={brand.id}
                  className="border-b transition-colors hover:bg-muted/50 whitespace-nowrap"
                >
                  {allColumns.map(col => visibleColumns.has(col.key) && (
                    <td key={col.key} className={`p-4 align-middle text-muted-foreground ${col.key === "mentionCount" ? "text-right font-bold tabular-nums" : ""}`}>
                      {col.key === "canonicalName" ? (
                        <Link href={`/gold/brands/${brand.id}`} className="font-medium text-foreground hover:underline">
                          {brand.canonicalName}
                        </Link>
                      ) : col.key === "avgSentiment" ? (
                        <div className={`inline-flex items-center justify-center rounded-full border px-2.5 py-0.5 text-xs font-semibold ${getSentimentVariant(brand.avgSentiment)}`}>
                          {formatSentiment(brand.avgSentiment)} / 10
                        </div>
                      ) : col.key === "mentionCount" ? (
                        <Link href={`/silver?goldBrandId=${brand.id}`} className="hover:underline text-primary">
                          {brand.mentionCount}
                        </Link>
                      ) : col.key === "createdAt" ? (
                        new Date(brand.createdAt).toLocaleDateString()
                      ) : (
                        brand[col.key] ?? "-"
                      )}
                    </td>
                  ))}
                </tr>
              ))}
              {brands.length === 0 && (
                <tr>
                  <td colSpan={visibleColumns.size} className="p-8 text-center text-muted-foreground">
                    No brands found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
