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

interface TokenChartProps {
  data: any[];
}

// Ensure stable colors for tracking over time. Distinct from spend tracking colors.
const COLOR_PALETTE = {
  Input: "#3b82f6",     // blue-500
  Thinking: "#f59e0b",  // yellow-500
  Output: "#10b981",    // emerald-500
};

export function TokenChart({ data }: TokenChartProps) {
  const [isMounted, setIsMounted] = useState(false);

  useEffect(() => {
    setIsMounted(true);
  }, []);

  // Memoize formatter matching to prevent jitter
  const formatters = useMemo(() => {
    return {
      number: (value: number) => value.toLocaleString(),
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
        <span className="text-muted-foreground text-sm">No recorded AI token usage in the last 7 days.</span>
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
            tickFormatter={formatters.number} 
            tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }}
            tickLine={false}
            axisLine={false}
            width={70}
          />
          <Tooltip 
            formatter={(value: any, name: any) => [formatters.number(value as number), name as string]}
            contentStyle={{ 
              backgroundColor: "hsl(var(--card))", 
              borderColor: "hsl(var(--border))",
              borderRadius: "0.5rem",
              color: "hsl(var(--foreground))"
            }}
            itemStyle={{ color: "hsl(var(--foreground))" }}
          />
          <Legend wrapperStyle={{ paddingTop: "20px", fontSize: "12px", color: "hsl(var(--muted-foreground))" }} />
          <Bar dataKey="Input" stackId="a" fill={COLOR_PALETTE.Input} />
          <Bar dataKey="Thinking" stackId="a" fill={COLOR_PALETTE.Thinking} />
          <Bar dataKey="Output" stackId="a" fill={COLOR_PALETTE.Output} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
