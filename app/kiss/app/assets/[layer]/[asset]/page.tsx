import { AssetViewer } from "../../../../components/asset-viewer";

export default async function Page({
  params,
}: {
  params: Promise<{ layer: string; asset: string }>;
}) {
  const { layer, asset } = await params;

  return (
    <div className="max-w-7xl mx-auto space-y-2">
      <div>
        <div className="flex items-center gap-2 text-sm text-muted-foreground mb-2">
          <span className="capitalize">{layer}</span>
          <span className="opacity-50">/</span>
          <span className="font-mono text-xs">{asset}.parquet</span>
        </div>
        <h1 className="text-3xl font-bold tracking-tight capitalize">{asset.replace(/_/g, " ")}</h1>
        <p className="text-muted-foreground mt-2">
          Inspecting {layer} parquet datastore live out of Object Storage using native DuckDB.
        </p>
      </div>

      <AssetViewer layer={layer} asset={asset} />
    </div>
  );
}
