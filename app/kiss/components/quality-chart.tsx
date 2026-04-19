"use client";

import { useEffect, useState } from "react";
import { fetchQualityMetrics } from "../app/actions";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  LineChart,
  Line,
} from "recharts";

type MetricRow = {
  source_node: string;
  avg_quality: number;
  avg_processing_time: number;
  error_count: number;
  total_runs: number;
};

export function QualityChart() {
  const [data, setData] = useState<MetricRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      try {
        const response = await fetchQualityMetrics();
        if (response.success && response.data) {
          setData(response.data);
        } else {
          setError(response.error || "Failed to load");
        }
      } catch (e: unknown) {
        const message = e instanceof Error ? e.message : String(e);
        setError(message);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  if (loading)
    return (
      <div className="p-8 text-center text-muted-foreground animate-pulse">
        Loading Parquet Metrics via DuckDB...
      </div>
    );
  if (error)
    return (
      <div className="p-8 text-center text-destructive">
        Error: {error}. Hint: Did you run the ingest script to generate the Parquet file first?
      </div>
    );
  if (!data.length)
    return (
      <div className="p-8 text-center text-muted-foreground">
        No data available. Run the ingest script first.
      </div>
    );

  return (
    <div className="space-y-8">
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
          <p className="text-sm font-medium">Total Nodes</p>
          <h3 className="text-2xl font-bold">{data.length}</h3>
        </div>
        <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
          <p className="text-sm font-medium">Avg Quality Core</p>
          <h3 className="text-2xl font-bold">
            {(data.reduce((acc, curr) => acc + curr.avg_quality, 0) / data.length).toFixed(1)}
          </h3>
        </div>
      </div>

      <div className="grid gap-8 lg:grid-cols-2">
        <div className="rounded-xl border bg-card text-card-foreground shadow p-6 h-[400px]">
          <h3 className="text-lg font-medium mb-4">Average Quality Score by Node</h3>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" opacity={0.2} />
              <XAxis dataKey="source_node" />
              <YAxis />
              <Tooltip
                cursor={{ fill: "rgba(255,255,255,0.1)" }}
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  borderColor: "hsl(var(--border))",
                }}
              />
              <Legend />
              <Bar
                dataKey="avg_quality"
                fill="hsl(var(--primary))"
                name="Quality Score"
                radius={[4, 4, 0, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="rounded-xl border bg-card text-card-foreground shadow p-6 h-[400px]">
          <h3 className="text-lg font-medium mb-4">Error Count by Node</h3>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" opacity={0.2} />
              <XAxis dataKey="source_node" />
              <YAxis />
              <Tooltip
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  borderColor: "hsl(var(--border))",
                }}
              />
              <Legend />
              <Line
                type="monotone"
                dataKey="error_count"
                stroke="hsl(var(--destructive))"
                name="Errors"
                strokeWidth={2}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
