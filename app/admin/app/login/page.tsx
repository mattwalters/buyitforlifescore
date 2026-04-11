"use client";

import { useActionState } from "react";
import { authenticate } from "@/app/lib/actions";

export default function LoginPage() {
  const [errorMessage, dispatch, isPending] = useActionState(authenticate, undefined);

  return (
    <div className="flex h-screen items-center justify-center bg-background text-foreground">
      <div className="w-full max-w-md space-y-8 rounded-lg bg-card border border-border p-8 shadow-md text-center">
        <div>
          <h2 className="mt-6 text-3xl font-extrabold text-card-foreground">Admin Access</h2>
          <p className="mt-2 text-sm text-muted-foreground">
            Sign in with your administrative credentials to continue.
          </p>
        </div>

        <form action={dispatch} className="mt-8">
          <div className="space-y-4">
            <div>
              <label
                htmlFor="email"
                className="block text-sm font-medium text-muted-foreground text-left"
              >
                Email address
              </label>
              <div className="mt-1">
                <input
                  id="email"
                  name="email"
                  type="email"
                  autoComplete="email"
                  required
                  className="block w-full rounded-md border-border bg-background text-foreground shadow-sm focus:border-primary focus:ring-primary sm:text-sm px-3 py-2 border"
                  placeholder="admin@example.com"
                />
              </div>
            </div>

            <div>
              <label
                htmlFor="password"
                className="block text-sm font-medium text-muted-foreground text-left"
              >
                Password
              </label>
              <div className="mt-1">
                <input
                  id="password"
                  name="password"
                  type="password"
                  autoComplete="current-password"
                  required
                  className="block w-full rounded-md border-border bg-background text-foreground shadow-sm focus:border-primary focus:ring-primary sm:text-sm px-3 py-2 border"
                />
              </div>
            </div>

            <button
              type="submit"
              disabled={isPending}
              className="flex w-full justify-center rounded-md border border-transparent bg-primary py-2 px-4 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed mt-6"
            >
              {isPending ? "Signing in..." : "Sign in"}
            </button>
          </div>

          <div
            className="flex h-8 items-end justify-center space-x-1 mt-4"
            aria-live="polite"
            aria-atomic="true"
          >
            {errorMessage && <p className="text-sm text-red-500">{errorMessage}</p>}
          </div>
        </form>
      </div>
    </div>
  );
}
