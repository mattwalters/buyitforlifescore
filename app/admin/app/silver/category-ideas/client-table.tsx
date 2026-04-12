"use client";

import { useState } from "react";

type SilverIdeaData = {
  id: string;
  rawName: string;
  isProcessed: boolean;
  createdAt: Date;
  goldProductLine: {
    brand: string;
    canonicalName: string;
  } | null;
};

export default function CategoryIdeaClientTable({
  ideas,
  sortBy: _sortBy,
  dir: _dir,
}: {
  ideas: SilverIdeaData[];
  sortBy: string;
  dir: string;
}) {
  const [data] = useState(ideas);

  return (
    <div className="rounded-md border bg-card text-card-foreground shadow-sm overflow-hidden">
      <div className="relative w-full overflow-auto">
        <table className="w-full caption-bottom text-sm">
          <thead className="[&_tr]:border-b bg-muted/50">
            <tr className="border-b transition-colors hover:bg-muted/50 data-[state=selected]:bg-muted">
              <th className="h-12 px-4 text-left align-middle font-medium text-muted-foreground w-[100px]">
                ID
              </th>
              <th className="h-12 px-4 text-left align-middle font-medium text-muted-foreground">
                Raw Phrase (Hallucination)
              </th>
              <th className="h-12 px-4 text-left align-middle font-medium text-muted-foreground">
                Originating Product Line
              </th>
              <th className="h-12 px-4 text-center align-middle font-medium text-muted-foreground">
                Status
              </th>
              <th className="h-12 px-4 text-right align-middle font-medium text-muted-foreground">
                Generated
              </th>
            </tr>
          </thead>
          <tbody className="[&_tr:last-child]:border-0 bg-background">
            {data.map((idea) => (
              <tr key={idea.id} className="border-b transition-colors hover:bg-muted/50 data-[state=selected]:bg-muted">
                <td className="p-4 align-middle text-xs font-mono text-muted-foreground">
                  {idea.id.slice(-6)}
                </td>
                <td className="p-4 align-middle font-medium text-slate-300">
                  "{idea.rawName}"
                </td>
                <td className="p-4 align-middle text-muted-foreground">
                  {idea.goldProductLine ? (
                    <span>
                      <span className="font-semibold text-foreground">{idea.goldProductLine.brand}</span> {idea.goldProductLine.canonicalName}
                    </span>
                  ) : (
                    <span className="italic">Orphaned</span>
                  )}
                </td>
                <td className="p-4 align-middle text-center">
                  {idea.isProcessed ? (
                     <span className="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold bg-emerald-500/10 text-emerald-500 border-emerald-500/20">
                     Consolidated
                    </span>
                  ) : (
                    <span className="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold bg-orange-500/10 text-orange-500 border-orange-500/20">
                      Pending
                    </span>
                  )}
                </td>
                <td className="p-4 align-middle text-right text-muted-foreground text-sm">
                  {new Date(idea.createdAt).toLocaleDateString()}
                </td>
              </tr>
            ))}
            {data.length === 0 && (
              <tr>
                <td colSpan={5} className="p-4 text-center text-muted-foreground h-24">
                  No ideas generated yet. Try running the taxonomy:discover pipeline.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
