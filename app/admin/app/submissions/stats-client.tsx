/* eslint-disable @typescript-eslint/no-explicit-any, react-hooks/set-state-in-effect */
"use client";

import { useMemo, useState, useEffect } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  ZAxis,
  PieChart,
  Pie,
  Cell,
  Legend,
} from "recharts";

export type TrackerDatum = {
  chars: number;
  mentions: number;
  score: number;
  comments: number;
  isProcessed: boolean;
};
export type NameValue = { name: string; value: number };

export default function TokenStatsChart({
  data,
  specificityData,
  sentimentData,
}: {
  data: TrackerDatum[];
  specificityData: NameValue[];
  sentimentData: NameValue[];
}) {
  const [isMounted, setIsMounted] = useState(false);

  useEffect(() => {
    setIsMounted(true);
  }, []);

  const histogramData = useMemo(() => {
    if (data.length === 0) return [];

    // Simple bucketing algorithm
    const charValues = data.map((d) => d.chars);
    const maxChars = charValues.length > 0 ? charValues.reduce((a, b) => Math.max(a, b), 0) : 0;
    const bucketSize = 2500; // Group by 2500 chars (approx ~500-600 tokens)

    const buckets: Record<number, number> = {};
    for (let i = 0; i <= Math.ceil(maxChars / bucketSize); i++) {
      buckets[i * bucketSize] = 0;
    }

    for (const val of charValues) {
      const b = Math.floor(val / bucketSize) * bucketSize;
      buckets[b] = (buckets[b] || 0) + 1;
    }

    return Object.entries(buckets)
      .map(([bucket, count]) => ({
        range: `${bucket} - ${parseInt(bucket, 10) + bucketSize}`,
        count,
        sortValue: parseInt(bucket, 10),
      }))
      .sort((a, b) => a.sortValue - b.sortValue);
  }, [data]);

  const commentHistogramData = useMemo(() => {
    if (data.length === 0) return [];

    // Group comment counts by 50
    const bucketSize = 50;
    const commentValues = data.map((d) => d.comments);
    const maxComments =
      commentValues.length > 0 ? commentValues.reduce((a, b) => Math.max(a, b), 0) : 0;

    const buckets: Record<number, number> = {};
    for (let i = 0; i <= Math.ceil(maxComments / bucketSize); i++) {
      buckets[i * bucketSize] = 0;
    }

    for (const val of commentValues) {
      const b = Math.floor(val / bucketSize) * bucketSize;
      buckets[b] = (buckets[b] || 0) + 1;
    }

    return Object.entries(buckets)
      .map(([bucket, count]) => ({
        range: `${bucket} - ${parseInt(bucket, 10) + bucketSize}`,
        count,
        sortValue: parseInt(bucket, 10),
      }))
      .sort((a, b) => a.sortValue - b.sortValue);
  }, [data]);

  const scatterData = useMemo(() => data.filter((d) => d.isProcessed), [data]);

  const totalProcessed = data.length;
  const avgChars =
    totalProcessed > 0
      ? Math.round(data.map((d) => d.chars).reduce((a, b) => a + b, 0) / totalProcessed)
      : 0;

  const totalYield = scatterData.map((d) => d.mentions).reduce((a, b) => a + b, 0);
  const avgYield = scatterData.length > 0 ? (totalYield / scatterData.length).toFixed(1) : "0";

  const SPECIFICITY_COLORS: Record<string, string> = {
    EXACT_MODEL: "#10b981", // Emerald
    PRODUCT_LINE: "#3b82f6", // Blue
    BRAND_ONLY: "#f59e0b", // Amber
    UNKNOWN: "#6b7280", // Gray
  };

  const SENTIMENT_COLORS: Record<string, string> = {
    POSITIVE: "#10b981",
    NEUTRAL: "#9ca3af",
    NEGATIVE: "#ef4444",
  };

  if (!isMounted) {
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 animate-pulse">
          <div className="h-[120px] rounded-xl border bg-card p-6 shadow-sm" />
          <div className="h-[120px] rounded-xl border bg-card p-6 shadow-sm" />
          <div className="h-[120px] rounded-xl border bg-card p-6 shadow-sm" />
        </div>
        <div className="h-[500px] w-full animate-pulse rounded-lg bg-muted/10 mt-6" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="rounded-xl border bg-card p-6 shadow-sm">
          <h3 className="text-sm font-semibold text-muted-foreground">Chars / Thread (Avg)</h3>
          <p className="text-3xl font-bold mt-2 tabular-nums">{avgChars.toLocaleString()}</p>
        </div>
        <div className="rounded-xl border bg-card p-6 shadow-sm">
          <h3 className="text-sm font-semibold text-muted-foreground">Yield / Thread (Avg)</h3>
          <p className="text-3xl font-bold mt-2 tabular-nums">{avgYield}</p>
        </div>
        <div className="rounded-xl border bg-card p-6 shadow-sm">
          <h3 className="text-sm font-semibold text-muted-foreground">Processed Threads</h3>
          <p className="text-3xl font-bold mt-2 tabular-nums">{totalProcessed.toLocaleString()}</p>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        {/* Signal to Noise Map */}
        <div className="rounded-xl border bg-card p-6 shadow-sm xl:col-span-2">
          <h3 className="font-semibold mb-2">
            Signal-to-Noise: Thread Size vs Yield (Processed Only)
          </h3>
          <p className="text-sm text-muted-foreground mb-6">
            Observe if longer threads map linearly to higher product yields. Limited to threads that
            have successfully cleared the AI Extraction Pipeline.
          </p>
          {scatterData.length > 0 ? (
            <div className="h-[400px] w-full">
              <ResponsiveContainer width="100%" height={400}>
                <ScatterChart margin={{ top: 20, right: 30, left: 0, bottom: 30 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis
                    type="number"
                    dataKey="chars"
                    name="Characters"
                    tick={{ fill: "#9ca3af" }}
                    label={{
                      value: "Total Character Length (Window Size)",
                      position: "insideBottom",
                      offset: -25,
                      fill: "#d1d5db",
                      fontSize: 13,
                      fontWeight: "500",
                    }}
                  />
                  <YAxis
                    type="number"
                    dataKey="mentions"
                    name="Mentions Extracted"
                    tick={{ fill: "#9ca3af" }}
                    label={{
                      value: "Total Products Extracted",
                      angle: -90,
                      position: "insideLeft",
                      offset: 10,
                      fill: "#d1d5db",
                      fontSize: 13,
                      fontWeight: "500",
                    }}
                  />
                  <ZAxis type="number" dataKey="score" range={[20, 400]} name="Post Score" />
                  <Tooltip
                    cursor={{ strokeDasharray: "3 3" }}
                    contentStyle={{
                      backgroundColor: "hsl(var(--popover))",
                      borderColor: "hsl(var(--border))",
                      borderRadius: "8px",
                    }}
                    itemStyle={{ color: "hsl(var(--foreground))" }}
                    labelStyle={{ display: "none" }}
                  />
                  <Scatter name="Thread" data={scatterData} fill="#8b5cf6" fillOpacity={0.6} />
                </ScatterChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="h-96 w-full flex items-center justify-center text-muted-foreground border-2 border-dashed rounded-lg">
              No data found.
            </div>
          )}
        </div>

        {/* Specificity Donut */}
        <div className="rounded-xl border bg-card p-6 shadow-sm">
          <h3 className="font-semibold mb-6">Extraction Specificity</h3>
          {specificityData.length > 0 ? (
            <div className="h-72 w-full">
              <ResponsiveContainer width="100%" height={288}>
                <PieChart>
                  <Pie
                    data={specificityData}
                    cx="50%"
                    cy="50%"
                    innerRadius={70}
                    outerRadius={100}
                    paddingAngle={5}
                    dataKey="value"
                  >
                    {specificityData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={SPECIFICITY_COLORS[entry.name] || "#ccc"} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "hsl(var(--popover))",
                      borderColor: "hsl(var(--border))",
                      borderRadius: "8px",
                    }}
                    itemStyle={{ color: "hsl(var(--foreground))" }}
                  />
                  <Legend
                    verticalAlign="bottom"
                    height={36}
                    formatter={(value) => <span className="text-foreground">{value}</span>}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="h-72 flex items-center justify-center text-muted-foreground text-sm">
              No specificity data
            </div>
          )}
        </div>

        {/* Sentiment Distribution */}
        <div className="rounded-xl border bg-card p-6 shadow-sm">
          <h3 className="font-semibold mb-6">Sentiment Disposition</h3>
          {sentimentData.length > 0 ? (
            <div className="h-72 w-full">
              <ResponsiveContainer width="100%" height={288}>
                <BarChart
                  data={sentimentData}
                  layout="vertical"
                  margin={{ top: 5, right: 30, left: 30, bottom: 5 }}
                >
                  <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#374151" />
                  <XAxis type="number" tick={{ fill: "#9ca3af" }} />
                  <YAxis dataKey="name" type="category" tick={{ fill: "#d1d5db" }} width={80} />
                  <Tooltip
                    cursor={{ fill: "hsl(var(--muted)/0.5)" }}
                    contentStyle={{
                      backgroundColor: "hsl(var(--popover))",
                      borderColor: "hsl(var(--border))",
                      borderRadius: "8px",
                    }}
                  />
                  <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                    {sentimentData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={SENTIMENT_COLORS[entry.name] || "#ccc"} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="h-72 flex items-center justify-center text-muted-foreground text-sm">
              No sentiment data
            </div>
          )}
        </div>

        {/* Existing Histogram */}
        <div className="rounded-xl border bg-card p-6 shadow-sm xl:col-span-2">
          <h3 className="font-semibold mb-6">Pipeline Character Density</h3>
          {histogramData.length > 0 ? (
            <div className="h-96 w-full">
              <ResponsiveContainer width="100%" height={384}>
                <BarChart
                  data={histogramData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 30 }}
                >
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#374151" />
                  <XAxis
                    dataKey="range"
                    tick={{ fill: "#9ca3af" }}
                    tickLine={false}
                    axisLine={false}
                    angle={-45}
                    textAnchor="end"
                    height={80}
                    fontSize={12}
                    label={{
                      value: "Total Characters",
                      position: "insideBottom",
                      offset: -25,
                      fill: "#d1d5db",
                      fontSize: 14,
                      fontWeight: "500",
                    }}
                  />
                  <YAxis
                    tick={{ fill: "#9ca3af" }}
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={(val) => (val === 0 ? "0" : val)}
                    label={{
                      value: "Analyzed Threads",
                      angle: -90,
                      position: "insideLeft",
                      offset: -10,
                      fill: "#d1d5db",
                      fontSize: 14,
                      fontWeight: "500",
                    }}
                  />
                  <Tooltip
                    cursor={{ fill: "hsl(var(--muted)/0.5)" }}
                    contentStyle={{
                      backgroundColor: "hsl(var(--popover))",
                      borderColor: "hsl(var(--border))",
                      borderRadius: "8px",
                    }}
                    formatter={(value: any) => [value, "Threads"]}
                    labelStyle={{
                      color: "hsl(var(--foreground))",
                      fontWeight: "bold",
                      marginBottom: "8px",
                    }}
                  />
                  <Bar dataKey="count" name="Threads" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="h-96 w-full flex items-center justify-center text-muted-foreground border-2 border-dashed rounded-lg text-sm px-16 text-center">
              Global Character density is computed at Ingestion, not Extraction. If this space is
              blank, no threads exist in the Bronze layer. Run ingestion via CLI.
            </div>
          )}
        </div>

        {/* Comment Distribution Histogram */}
        <div className="rounded-xl border bg-card p-6 shadow-sm xl:col-span-2">
          <h3 className="font-semibold mb-6">Comment Density per Submission</h3>
          {commentHistogramData.length > 0 ? (
            <div className="h-96 w-full">
              <ResponsiveContainer width="100%" height={384}>
                <BarChart
                  data={commentHistogramData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 30 }}
                >
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#374151" />
                  <XAxis
                    dataKey="range"
                    tick={{ fill: "#9ca3af" }}
                    tickLine={false}
                    axisLine={false}
                    angle={-45}
                    textAnchor="end"
                    height={80}
                    fontSize={12}
                    label={{
                      value: "Total Comments",
                      position: "insideBottom",
                      offset: -25,
                      fill: "#d1d5db",
                      fontSize: 14,
                      fontWeight: "500",
                    }}
                  />
                  <YAxis
                    tick={{ fill: "#9ca3af" }}
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={(val) => (val === 0 ? "0" : val)}
                    label={{
                      value: "Analyzed Threads",
                      angle: -90,
                      position: "insideLeft",
                      offset: -10,
                      fill: "#d1d5db",
                      fontSize: 14,
                      fontWeight: "500",
                    }}
                  />
                  <Tooltip
                    cursor={{ fill: "hsl(var(--muted)/0.5)" }}
                    contentStyle={{
                      backgroundColor: "hsl(var(--popover))",
                      borderColor: "hsl(var(--border))",
                      borderRadius: "8px",
                    }}
                    formatter={(value: any) => [value, "Threads"]}
                    labelStyle={{
                      color: "hsl(var(--foreground))",
                      fontWeight: "bold",
                      marginBottom: "8px",
                    }}
                  />
                  <Bar dataKey="count" name="Threads" fill="#10b981" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="h-96 w-full flex items-center justify-center text-muted-foreground border-2 border-dashed rounded-lg text-sm">
              No tracked comment data found. Run Ingestion first!
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
