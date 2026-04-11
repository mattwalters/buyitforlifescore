export default function JobsPage() {
  return (
    <div className="h-[calc(100vh-65px)] w-full">
      <iframe src="/api/jobs" className="h-full w-full border-0" title="Job Queue Dashboard" />
    </div>
  );
}
