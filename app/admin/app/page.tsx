import { DashboardStats } from "../components/dashboard-stats";
import { SpendOverview } from "../components/spend-overview";
import { TokenOverview } from "../components/token-overview";

export const dynamic = "force-dynamic";

export default async function AdminDashboard() {
  return (
    <div className="container mx-auto p-8 space-y-8">
      <div className="flex flex-col gap-2">
        <h1 className="text-3xl font-bold tracking-tight">BuyItForLifeClub Dashboard</h1>
        <p className="text-muted-foreground">Overview of system metrics and tools.</p>
      </div>

      <section className="space-y-4">
        <h2 className="text-xl font-semibold">Metrics</h2>
        <DashboardStats />
      </section>

      <section className="space-y-4 pt-4">
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">
          <SpendOverview />
          <TokenOverview />
        </div>
      </section>
    </div>
  );
}
