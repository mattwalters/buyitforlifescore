/* eslint-disable @typescript-eslint/no-explicit-any */
"use client";

import { useState } from "react";
import Link from "next/link";

export default function SilverClientTable({ 
  mentions, 
  sortBy, 
  dir 
}: { 
  mentions: Record<string, any>[],
  sortBy: string,
  dir: string
}) {
  const allColumns = [
    { key: "brand", label: "Brand" },
    { key: "productName", label: "Product" },
    { key: "specificityLevel", label: "Specificity" },
    { key: "sentiment", label: "Sentiment" },
    { key: "durability", label: "Durability" },
    { key: "repairability", label: "Repairability" },
    { key: "maintenance", label: "Maintenance" },
    { key: "warranty", label: "Warranty" },
    { key: "value", label: "Value" },
    { key: "ownershipDurationMonths", label: "Owned For (Months)" },
    { key: "usageFrequency", label: "Usage Freq" },
    { key: "acquiredPrice", label: "Price" },
    { key: "flawOrCaveat", label: "Flaws" },
    { key: "submissionContext", label: "Source Context" }
  ];

  const [visibleColumns, setVisibleColumns] = useState<Set<string>>(new Set([
    "brand", "productName", "specificityLevel", "sentiment", "submissionContext"
  ]));

  const toggleCol = (key: string) => {
    const next = new Set(visibleColumns);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setVisibleColumns(next);
  };

  const getSentimentVariant = (sentiment: string) => {
    switch ((sentiment || "").toLowerCase()) {
      case "positive": return "border-transparent bg-primary text-primary-foreground";
      case "negative": return "border-transparent bg-destructive text-destructive-foreground";
      case "mixed": return "text-foreground";
      case "neutral": return "border-transparent bg-secondary text-secondary-foreground";
      default: return "border-transparent bg-secondary text-secondary-foreground";
    }
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
                  <th key={col.key} className="h-10 px-4 align-middle">
                    {col.key !== "submissionContext" && col.key !== "flawOrCaveat" ? (
                      <Link href={`?sortBy=${col.key}&dir=${sortBy === col.key && dir === "asc" ? "desc" : "asc"}`}>
                        {col.label} {sortBy === col.key && (dir === "asc" ? "↑" : "↓")}
                      </Link>
                    ) : (
                      col.label
                    )}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {mentions.map((mention) => (
                <tr
                  key={mention.id}
                  className="border-b transition-colors hover:bg-muted/50 data-[state=selected]:bg-muted whitespace-nowrap"
                >
                  {allColumns.map(col => visibleColumns.has(col.key) && (
                    <td key={col.key} className="p-4 align-middle text-muted-foreground">
                      {col.key === "brand" ? (
                        <Link href={`/silver/${mention.id}`} className="font-medium text-foreground hover:underline">
                          {mention.brand || "Unknown"}
                        </Link>
                      ) : col.key === "sentiment" ? (
                        <div className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold ${getSentimentVariant(mention.sentiment)}`}>
                          {mention.sentiment}
                        </div>
                      ) : col.key === "submissionContext" ? (
                        mention.submission ? (
                          <Link href={`/submissions/${mention.submissionId}`} className="hover:underline text-primary">
                            {mention.submission.title.length > 50 
                              ? mention.submission.title.substring(0, 50) + "..." 
                              : mention.submission.title}
                          </Link>
                        ) : "-"
                      ) : (
                        mention[col.key] ?? "-"
                      )}
                    </td>
                  ))}
                </tr>
              ))}
              {mentions.length === 0 && (
                <tr>
                  <td colSpan={visibleColumns.size} className="p-8 text-center text-muted-foreground">
                    No extracted products found.
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
