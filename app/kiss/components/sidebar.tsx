"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Database, Filter, Table } from "lucide-react";

export const MEDALLION_ASSETS = {
  bronze: [
    {
      name: "reddit_buyitforlife_submissions",
      path: "/assets/bronze/reddit_buyitforlife_submissions",
    },
    { name: "reddit_buyitforlife_comments", path: "/assets/bronze/reddit_buyitforlife_comments" },
  ],
  silver: [
    // Placeholder for future silver assets
  ],
  gold: [
    // Placeholder for future gold assets
  ],
};

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-64 border-r bg-card/50 hidden md:flex flex-col h-full sticky top-0 overflow-y-auto">
      <div className="p-4 border-b">
        <h2 className="font-bold text-lg flex items-center gap-2">
          <Database className="w-5 h-5 text-primary" /> Data Assets
        </h2>
      </div>

      <nav className="flex-1 p-4 space-y-6">
        {/* BRONZE */}
        <div>
          <h3 className="text-xs font-semibold text-amber-600/80 uppercase tracking-wider mb-2 flex items-center gap-2">
            <Filter className="w-3.5 h-3.5" /> Bronze
          </h3>
          <ul className="space-y-1">
            {MEDALLION_ASSETS.bronze.map((asset) => {
              const isActive = pathname.startsWith(asset.path);
              return (
                <li key={asset.name}>
                  <Link
                    href={asset.path}
                    className={`flex items-center gap-2 text-sm px-2 py-1.5 rounded-md transition-colors ${
                      isActive
                        ? "bg-primary/10 text-primary font-medium"
                        : "text-muted-foreground hover:bg-muted hover:text-foreground"
                    }`}
                  >
                    <Table className="w-4 h-4 shrink-0" />
                    <span className="truncate">{asset.name}</span>
                  </Link>
                </li>
              );
            })}
          </ul>
        </div>

        {/* SILVER */}
        <div>
          <h3 className="text-xs font-semibold text-slate-400 capitalize tracking-wider mb-2 flex items-center gap-2">
            <Filter className="w-3.5 h-3.5" /> Silver
          </h3>
          <ul className="space-y-1">
            {MEDALLION_ASSETS.silver.length === 0 ? (
              <li className="text-xs text-muted-foreground px-2 italic">No assets mapped</li>
            ) : null}
          </ul>
        </div>

        {/* GOLD */}
        <div>
          <h3 className="text-xs font-semibold text-yellow-500/80 uppercase tracking-wider mb-2 flex items-center gap-2">
            <Filter className="w-3.5 h-3.5" /> Gold
          </h3>
          <ul className="space-y-1">
            {MEDALLION_ASSETS.gold.length === 0 ? (
              <li className="text-xs text-muted-foreground px-2 italic">No assets mapped</li>
            ) : null}
          </ul>
        </div>
      </nav>
    </aside>
  );
}
