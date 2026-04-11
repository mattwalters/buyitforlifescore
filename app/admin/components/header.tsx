import { auth } from "@/auth";
import { handleSignOut } from "@/app/lib/actions";
import Link from "next/link";

export async function Header() {
  const session = await auth();

  if (!session?.user) return null;

  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container mx-auto flex h-16 items-center justify-between px-4 sm:px-6 lg:px-8">
        <div className="flex items-center gap-8">
          <Link
            href="/"
            className="text-lg font-bold tracking-tight text-foreground transition-colors hover:text-foreground/80"
          >
            BuyItForLifeClub
          </Link>
        </div>
        <div className="flex items-center gap-4">
          <div className="text-sm text-muted-foreground flex items-center gap-1">
            <span className="text-xs opacity-50">Signed in as </span>
            <span className="font-medium text-foreground">{session.user.email}</span>
          </div>
          <form action={handleSignOut}>
            <button
              type="submit"
              className="rounded-md bg-background px-3 py-2 text-sm font-semibold text-foreground shadow-sm ring-1 ring-inset ring-border hover:bg-accent hover:text-accent-foreground"
            >
              Sign out
            </button>
          </form>
        </div>
      </div>
    </header>
  );
}
