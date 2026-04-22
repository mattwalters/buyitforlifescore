"use client";

import { useMemo, useCallback } from "react";
import ReactFlow, {
  Background,
  Controls,
  Node,
  Edge,
  MarkerType,
  Handle,
  Position,
} from "reactflow";
import "reactflow/dist/style.css";
import { AssetRegistry } from "../lib/registry";
import { Database, Zap } from "lucide-react";
import { useRouter } from "next/navigation";

// Define a custom node styling mimicking Dagster/Airflow
const AssetNode = ({ data }: any) => {
  const router = useRouter();

  const getLayerColor = (layer: string) => {
    switch (layer) {
      case "bronze":
        return "border-amber-700/50 bg-amber-950/20 text-amber-500";
      case "silver":
        return "border-slate-500/50 bg-slate-900/40 text-slate-300";
      case "gold":
        return "border-yellow-500/50 bg-yellow-950/20 text-yellow-500";
      default:
        return "border-white/10 bg-black text-white";
    }
  };

  const nav = () => {
    router.push(`/assets/${data.layer}/${data.id}`);
  };

  return (
    <div
      onClick={nav}
      className={`px-4 py-3 rounded-lg border-2 shadow-lg backdrop-blur-sm min-w-[250px] cursor-pointer hover:-translate-y-1 transition-transform ${getLayerColor(data.layer)}`}
    >
      <Handle type="target" position={Position.Top} className="opacity-0" />

      <div className="flex items-center gap-2 mb-2">
        <Database className="w-4 h-4" />
        <span className="font-bold text-sm tracking-wide uppercase">{data.layer}</span>
      </div>
      <div>
        <p className="font-mono text-sm leading-tight text-white">{data.id}</p>
        <div className="flex gap-2 mt-3 text-xs text-muted-foreground font-mono">
          <span className="bg-background/50 px-2 py-0.5 rounded flex items-center gap-1">
            <Zap className="w-3 h-3" /> {data.rules} QA Rules
          </span>
          {data.isPartitioned && (
            <span className="bg-background/50 px-2 py-0.5 rounded">Partitioned</span>
          )}
        </div>
      </div>

      <Handle type="source" position={Position.Bottom} className="opacity-0" />
    </div>
  );
};

const nodeTypes = {
  assetNode: AssetNode,
};

export default function GlobalLineagePage() {
  const { initialNodes, initialEdges } = useMemo(() => {
    const nodes: Node[] = [];
    const edges: Edge[] = [];

    // Auto-layout parameters based on Medallion Layer
    const layerOffsets: Record<string, { x: number; y: number; col: number }> = {
      bronze: { x: 50, y: 100, col: 0 },
      silver: { x: 50, y: 350, col: 0 },
      gold: { x: 50, y: 600, col: 0 },
    };

    Object.values(AssetRegistry).forEach((asset) => {
      // Basic manual layout engine
      const layout = layerOffsets[asset.layer] || { x: 50, y: 100, col: 0 };
      const nodeX = layout.x + layout.col * 350;
      const nodeY = layout.y;
      layerOffsets[asset.layer].col += 1; // Increment column to stagger sibling nodes horizontally

      nodes.push({
        id: asset.id,
        type: "assetNode",
        position: { x: nodeX, y: nodeY },
        data: {
          id: asset.id,
          layer: asset.layer,
          rules: asset.qualityRules?.length || 0,
          isPartitioned: asset.isPartitioned,
        },
      });

      // Build Graph Edges matching Lineage Dependencies
      asset.dependencies?.forEach((depId) => {
        edges.push({
          id: `e-${depId}-${asset.id}`,
          source: depId,
          target: asset.id,
          animated: true,
          type: "smoothstep",
          style: { stroke: "#64748b", strokeWidth: 2 },
          markerEnd: { type: MarkerType.ArrowClosed, color: "#64748b" },
        });
      });
    });

    return { initialNodes: nodes, initialEdges: edges };
  }, []);

  return (
    <div className="w-full h-[calc(100vh-100px)] rounded-xl border overflow-hidden bg-background relative shadow-inner">
      <div className="absolute top-4 left-4 z-10 bg-card border px-4 py-2 rounded-lg shadow-lg">
        <h2 className="font-bold">Global Lineage Graph</h2>
        <p className="text-xs text-muted-foreground">
          Medallion architecture auto-discovered from registry.ts
        </p>
      </div>

      <ReactFlow
        nodes={initialNodes}
        edges={initialEdges}
        nodeTypes={nodeTypes}
        fitView
        className="bg-muted/10"
        minZoom={0.5}
      >
        <Background color="#ffffff" gap={16} />
        <Controls />
      </ReactFlow>
    </div>
  );
}
