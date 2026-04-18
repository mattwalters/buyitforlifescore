import { QualityChart } from "../components/quality-chart";

export default function Page() {
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold tracking-tight">Data Quality Overview</h2>
        <p className="text-muted-foreground">
          Visualizing DuckDB Parquet aggregations directly from R2 partitions.
        </p>
      </div>
      
      <QualityChart />
    </div>
  );
}
