export default function HomePage() {
  return (
    <div className="flex flex-col items-center justify-center min-h-screen p-8 text-center bg-background text-foreground">
      <h1 className="text-4xl font-bold mb-4 text-primary">Welcome to Razor</h1>
      <p className="text-xl text-muted-foreground max-w-2xl">
        This is a minimal Next.js application, fully styled with Tailwind CSS and integrated into
        the monorepo architecture.
      </p>
    </div>
  );
}
