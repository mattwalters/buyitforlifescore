"use client";

import { useEffect, useState } from "react";
import { getAssetHistory, getJobs } from "../app/assets/db-actions";
import { runMaterializationAction } from "../app/assets/run-actions";
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";
import { Loader2, PlayCircle, Clock, AlertCircle, CheckCircle2 } from "lucide-react";

export function HistoricalAssetViewer({ assetId }: { assetId: string }) {
  const [materializations, setMaterializations] = useState<any[]>([]);
  const [jobs, setJobs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);

  const loadData = async () => {
    setLoading(true);
    const [matRes, jobRes] = await Promise.all([
      getAssetHistory(assetId),
      getJobs(assetId)
    ]);
    if (matRes.success) setMaterializations(matRes.data);
    if (jobRes.success) setJobs(jobRes.data);
    setLoading(false);
  };

  useEffect(() => {
    loadData();
  }, [assetId]);

  const handleRun = async () => {
    setRunning(true);
    const partition = prompt("Run specific partition? (e.g. '2024-04' or leave empty for global)");
    await runMaterializationAction(assetId, partition || undefined);
    await loadData();
    setRunning(false);
  };

  // Reformat chart data over time
  const chartData = materializations
    .map(m => ({
      date: new Date(m.createdAt).toLocaleDateString(),
      rows: m.summaryPayload?.totalRows || 0,
      partition: m.partitionKey,
    }))
    .reverse();

  return (
    <div className="space-y-8 mt-6">
      
      <div className="flex justify-between items-center border-b pb-4">
        <div>
          <h2 className="text-lg font-semibold tracking-tight">Global Asset History</h2>
          <p className="text-sm text-muted-foreground">Persisted execution states tracking QA metrics across partitions.</p>
        </div>
        <button 
          onClick={handleRun}
          disabled={running}
          className="bg-primary text-primary-foreground px-4 py-2 flex items-center gap-2 rounded-md font-medium text-sm hover:opacity-90 disabled:opacity-50"
        >
          {running ? <Loader2 className="w-4 h-4 animate-spin"/> : <PlayCircle className="w-4 h-4"/>}
          {running ? "Executing..." : "Materialize Next"}
        </button>
      </div>

      {loading ? (
        <div className="h-48 flex items-center justify-center text-muted-foreground">
          <Loader2 className="w-6 h-6 animate-spin" />
        </div>
      ) : (
        <>
          {/* Row count trends */}
          <div className="h-64 mt-4 bg-card border rounded-xl p-4 shadow-sm">
            <h3 className="text-sm font-semibold mb-4 text-muted-foreground">Dataset Size Evolution (Rows)</h3>
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
                <XAxis dataKey="partition" fontSize={12} tickLine={false} axisLine={false} />
                <YAxis fontSize={12} tickLine={false} axisLine={false} tickFormatter={(val) => `${val / 1000}k`} />
                <Tooltip contentStyle={{ borderRadius: '8px', background: 'hsl(var(--card))', border: '1px solid hsl(var(--border))' }}/>
                <Area type="monotone" dataKey="rows" stroke="hsl(var(--primary))" fill="hsl(var(--primary))" fillOpacity={0.1} strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          <div className="grid md:grid-cols-2 gap-8">
            
            {/* Jobs Ledger */}
            <div className="border bg-card rounded-xl overflow-hidden shadow-sm">
              <div className="p-4 bg-muted/40 border-b">
                <h3 className="font-semibold text-sm flex items-center gap-2 font-mono"><Clock className="w-4 h-4"/> Execution Ledger</h3>
              </div>
              <ul className="divide-y max-h-96 overflow-y-auto">
                {jobs.length === 0 && <li className="p-4 text-sm text-muted-foreground text-center">No runs executed yet.</li>}
                {jobs.map(job => (
                  <li key={job.id} className="p-4 text-sm flex items-center justify-between hover:bg-muted/30">
                    <div>
                      <p className="font-semibold">{job.partitionKey || 'Global'}</p>
                      <p className="text-xs text-muted-foreground">{new Date(job.requestedAt).toLocaleString()}</p>
                      {job.errorTrace && <p className="text-xs text-destructive mt-1 font-mono truncate max-w-xs">{job.errorTrace}</p>}
                    </div>
                    <div className="flex flex-col items-end gap-1">
                      <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                        job.status === 'COMPLETED' ? 'bg-green-500/10 text-green-500' :
                        job.status === 'FAILED' ? 'bg-red-500/10 text-red-500' :
                        'bg-blue-500/10 text-blue-500'
                      }`}>
                        {job.status}
                      </span>
                    </div>
                  </li>
                ))}
              </ul>
            </div>

            {/* Latest Summaries */}
            <div className="border bg-card rounded-xl overflow-hidden shadow-sm">
              <div className="p-4 bg-muted/40 border-b">
                <h3 className="font-semibold text-sm flex items-center gap-2 font-mono"><CheckCircle2 className="w-4 h-4"/> Validated Materializations</h3>
              </div>
              <ul className="divide-y max-h-96 overflow-y-auto">
                 {materializations.length === 0 && <li className="p-4 text-sm text-muted-foreground text-center">No materializations stored.</li>}
                 {materializations.map(m => {
                  const qaResults: Array<any> = m.summaryPayload?.qaResults || [];
                  const failedQA = qaResults.filter(q => !q.passed);
                  
                  return (
                    <li key={m.id} className="p-4 text-sm flex flex-col gap-2 hover:bg-muted/30 cursor-pointer">
                      <div className="flex justify-between">
                        <span className="font-semibold font-mono text-primary">{m.partitionKey || 'Global'}</span>
                        <span className="text-muted-foreground">{Number(m.summaryPayload?.totalRows || 0).toLocaleString()} rows</span>
                      </div>
                      
                      {/* QA Breakdown */}
                      <div className="flex flex-wrap gap-2">
                        {qaResults.length === 0 && <span className="text-xs text-slate-500">No QA Rules</span>}
                        {qaResults.map((qa, i) => (
                          <span key={i} className={`text-xs px-1.5 py-0.5 rounded flex gap-1 items-center ${qa.passed ? 'bg-green-500/10 text-green-500' : 'bg-red-500/10 text-red-500 font-bold'}`}>
                            {qa.passed ? <CheckCircle2 className="w-3 h-3"/> : <AlertCircle className="w-3 h-3"/>}
                            {qa.ruleName}
                          </span>
                        ))}
                      </div>
                    </li>
                  )
                 })}
              </ul>
            </div>

          </div>
        </>
      )}
    </div>
  );
}
