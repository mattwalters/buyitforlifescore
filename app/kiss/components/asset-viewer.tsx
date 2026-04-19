"use client";

import { useEffect, useState } from "react";
import { fetchAssetSummary } from "../app/assets/actions";
import { Database, FileJson, Table2, Layers, AlertCircle } from "lucide-react";

type SchemaRow = {
  column_name: string;
  column_type: string;
};

type AssetSummary = {
  totalRows: number;
  schema: SchemaRow[];
// eslint-disable-next-line @typescript-eslint/no-explicit-any
  preview: any[];
};

export function AssetViewer({ layer, asset }: { layer: string; asset: string }) {
  const [data, setData] = useState<AssetSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      try {
        const response = await fetchAssetSummary(layer, asset);
        if (response.success && response.data) {
          setData(response.data);
        } else {
          setError(response.error || "Unknown Error reading parquet via DuckDB.");
        }
      } catch (e: unknown) {
        const message = e instanceof Error ? e.message : String(e);
        setError(message);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [layer, asset]);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center p-24 space-y-4 rounded-xl border bg-card shadow-sm mt-8">
        <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin"></div>
        <p className="text-muted-foreground animate-pulse text-sm">
          Querying S3/R2 objects with DuckDB...
        </p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="mt-8 rounded-xl border border-destructive/20 bg-destructive/10 p-6 flex flex-col gap-2">
        <h3 className="text-destructive font-bold flex items-center gap-2">
          <AlertCircle className="w-5 h-5" /> DuckDB R2 Execution Failed
        </h3>
        <p className="text-sm text-muted-foreground break-all bg-background p-4 rounded border mt-2 font-mono">
          {error}
        </p>
        <p className="text-xs text-muted-foreground mt-2 italic">
          Check if `bronze/{asset}.parquet` exists in your R2 Bucket matching the exact path
          structure in DuckDB HTTPFS.
        </p>
      </div>
    );
  }

  if (!data) return null;

  return (
    <div className="space-y-8 mt-6">
      {/* HUD Metrics */}
      <div className="grid gap-4 md:grid-cols-3">
        <div className="rounded-xl border bg-card text-card-foreground shadow p-5 flex items-center gap-4">
          <div className="p-3 bg-primary/10 rounded-lg shrink-0">
            <Layers className="w-6 h-6 text-primary" />
          </div>
          <div>
            <p className="text-sm font-medium text-muted-foreground">Total Rows</p>
            <h3 className="text-3xl font-bold font-mono tracking-tight">
              {data.totalRows.toLocaleString()}
            </h3>
          </div>
        </div>

        <div className="rounded-xl border bg-card text-card-foreground shadow p-5 flex items-center gap-4">
          <div className="p-3 bg-primary/10 rounded-lg shrink-0">
            <Table2 className="w-6 h-6 text-primary" />
          </div>
          <div>
            <p className="text-sm font-medium text-muted-foreground">Columns</p>
            <h3 className="text-3xl font-bold font-mono tracking-tight">
              {data.schema?.length || 0}
            </h3>
          </div>
        </div>
      </div>

      <div className="grid gap-8 lg:grid-cols-12">
        {/* Schema Sidebar */}
        <div className="lg:col-span-4 rounded-xl border bg-card text-card-foreground shadow overflow-hidden flex flex-col">
          <div className="p-4 border-b bg-muted/40">
            <h3 className="font-semibold flex items-center gap-2">
              <Database className="w-4 h-4" /> DuckDB Native Schema
            </h3>
          </div>
          <div className="p-0 overflow-y-auto max-h-[600px]">
            <table className="w-full text-sm text-left">
              <thead className="bg-muted text-muted-foreground sticky top-0">
                <tr>
                  <th className="px-4 py-2 font-medium">Column Name</th>
                  <th className="px-4 py-2 font-medium text-right">Datatype</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {data.schema?.map((col) => (
                  <tr key={col.column_name} className="hover:bg-muted/50 transition-colors">
                    <td className="px-4 py-2 font-medium text-primary">{col.column_name}</td>
                    <td className="px-4 py-2 text-right font-mono text-xs text-muted-foreground">
                      {col.column_type}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Data Preview */}
        <div className="lg:col-span-8 rounded-xl border bg-card text-card-foreground shadow overflow-hidden flex flex-col">
          <div className="p-4 border-b bg-muted/40 flex items-center justify-between">
            <h3 className="font-semibold flex items-center gap-2">
              <FileJson className="w-4 h-4" /> 10-Row Data Preview
            </h3>
            <span className="text-xs font-mono text-muted-foreground">LIMIT 10</span>
          </div>
          <div className="p-0 overflow-x-auto max-h-[600px]">
            <table className="w-full text-sm text-left">
              <thead className="bg-muted text-muted-foreground sticky top-0 whitespace-nowrap">
                <tr>
                  {data.schema?.map((col) => (
                    <th key={col.column_name} className="px-4 py-2 font-medium">
                      {col.column_name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y relative">
                {data.preview?.length === 0 && (
                  <tr>
                    <td
                      colSpan={data.schema?.length}
                      className="text-center p-8 text-muted-foreground"
                    >
                      No rows found in partition.
                    </td>
                  </tr>
                )}
                {data.preview?.map((row, i) => (
                  <tr key={i} className="hover:bg-muted/50 transition-colors">
                    {data.schema?.map((col) => {
                      let cellVal = row[col.column_name];
                      if (typeof cellVal === "object") cellVal = JSON.stringify(cellVal);
                      return (
                        <td
                          key={col.column_name}
                          className="px-4 py-2 max-w-[250px] truncate text-muted-foreground"
                          title={String(cellVal)}
                        >
                          {cellVal !== undefined && cellVal !== null ? (
                            String(cellVal)
                          ) : (
                            <span className="text-slate-500 italic">null</span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
