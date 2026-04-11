import NextAuth from "next-auth";
import { authConfig } from "./auth.config";
import { type NextRequest, type NextFetchEvent } from "next/server";

const { auth } = NextAuth(authConfig);

export default async function proxy(req: NextRequest, event: NextFetchEvent) {
  // @ts-expect-error - auth expects NextAuthRequest but handles NextRequest at runtime
  return auth(req, event);
}

export const config = {
  // https://nextjs.org/docs/app/building-your-application/routing/middleware#matcher
  matcher: ["/((?!api|_next/static|_next/image|favicon.ico).*)"],
};
