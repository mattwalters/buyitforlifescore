import NextAuth from "next-auth";
import CredentialsProvider from "next-auth/providers/credentials";
import bcrypt from "bcryptjs";
import { env } from "./env";
import { authConfig } from "./auth.config";
import { prisma } from "@mono/db";

export const { handlers, auth, signIn, signOut } = NextAuth({
  secret: env.AUTH_SECRET,
  ...authConfig,
  providers: [
    CredentialsProvider({
      credentials: {
        email: { label: "Email", type: "email" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        const email = credentials?.email as string | undefined;
        const password = credentials?.password as string | undefined;

        if (!email || !password) {
          return null;
        }

        const adminUser = await prisma.admin.findUnique({
          where: { email },
        });

        if (!adminUser || !adminUser.passwordHash) {
          console.warn(`[Admin Auth] Access denied or admin not found for: ${email}`);
          return null;
        }

        const passwordMatch = await bcrypt.compare(password, adminUser.passwordHash);

        if (!passwordMatch) {
          console.warn(`[Admin Auth] Invalid password for: ${email}`);
          return null;
        }

        return {
          id: String(adminUser.id),
          email: adminUser.email,
          name: adminUser.name,
        };
      },
    }),
  ],
  callbacks: {
    async jwt({ token, user }) {
      if (user) {
        token.id = user.id;
      }
      return token;
    },
    async session({ session, token }) {
      if (session.user && token.id) {
        session.user.id = token.id as string;
      }
      return session;
    },
  },
});
