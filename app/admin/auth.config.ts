import type { NextAuthConfig } from "next-auth";
import Credentials from "next-auth/providers/credentials";

export const authConfig = {
  trustHost: true,
  pages: {
    signIn: "/login", // We'll create a simple login page or use default if we omit this
    // If we omit this, it defaults to /api/auth/signin
    // Let's create a simple login page at /login
  },
  callbacks: {
    authorized({ auth, request: { nextUrl }, request }) {
      const isLoggedIn = !!auth?.user;
      const isOnLogin = nextUrl.pathname.startsWith("/login");
      const isApiAuth = nextUrl.pathname.startsWith("/api/auth");

      if (isOnLogin || isApiAuth) {
        if (isLoggedIn && isOnLogin) {
          return Response.redirect(new URL("/", request.url));
        }
        return true; // Allow access to login/api out for guests
      }

      // Deny access to everything else if not logged in
      if (!isLoggedIn) {
        return false;
      }

      return true;
    },
  },
  providers: [
    Credentials({
      credentials: {
        email: { label: "Email", type: "email" },
        password: { label: "Password", type: "password" },
      },
      async authorize() {
        return null; // authorize logic is in auth.ts
      },
    }),
  ],
} satisfies NextAuthConfig;
