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
  Legend
} from "recharts";

interface SpendChartProps {
  data: any[];
  keys: string[];
}

// Ensure stable colors for tracking over time
const COLOR_PALETTE = [
  "#f59e0b", // yellow-500
  "#3b82f6", // blue-500
  "#10b981", // emerald-500
  "#8b5cf6", // violet-500
  "#ef4444", // red-500
  "#06b6d4", // cyan-500
  "#f97316", // orange-500
  "#6366f1", // indigo-500
];

export function SpendChart({ data, keys }: SpendChartProps) {
  const [isMounted, setIsMounted] = useState(false);

  useEffect(() => {
    setIsMounted(true);
  }, []);

  // Memoize formatter matching to prevent jitter
  const formatters = useMemo(() => {
    return {
      dollar: (value: number) => `$${value.toFixed(4)}`,
    };
  }, []);

  if (!isMounted) {
    return (
      <div className="h-[350px] w-full animate-pulse rounded-lg bg-muted/10 border border-dashed" />
    );
  }

  if (data.length === 0) {
    return (
      <div className="flex h-[350px] items-center justify-center rounded-lg border border-dashed bg-muted/20">
        <span className="text-muted-foreground text-sm">No recorded AI spend in the last 7 days.</span>
      </div>
    );
  }

  return (
    <div className="h-[350px] w-full">
      <ResponsiveContainer width="100%" height={350}>
        <BarChart
          data={data}
          margin={{
            top: 20,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--muted-foreground)/0.2)" />
          <XAxis 
            dataKey="date" 
            tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} 
            tickLine={false}
            axisLine={false}
          />
          <YAxis 
            tickFormatter={formatters.dollar} 
            tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }}
            tickLine={false}
            axisLine={false}
          />
          <Tooltip 
            formatter={(value: any) => [formatters.dollar(value as number), ""]}
            contentStyle={{ 
              backgroundColor: "hsl(var(--card))", 
              borderColor: "hsl(var(--border))",
              borderRadius: "0.5rem",
              color: "hsl(var(--foreground))"
            }}
            itemStyle={{ color: "hsl(var(--foreground))" }}
          />
          <Legend wrapperStyle={{ paddingTop: "20px", fontSize: "12px", color: "hsl(var(--muted-foreground))" }} />
          {keys.map((key, index) => (
            <Bar 
              key={key} 
              dataKey={key} 
              stackId="a" 
              fill={COLOR_PALETTE[index % COLOR_PALETTE.length]} 
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
