import { createEnv } from "@t3-oss/env-nextjs";
import { z } from "zod";

export const env = createEnv({
  skipValidation: !!process.env.SKIP_ENV_VALIDATION,
  server: {
    HOST: z.string().default("127.0.0.1"),
    DATABASE_URL: z.string().url(),
    AUTH_SECRET: z.string().min(1),
    AUTH_ADMIN_SECRET: z.string().min(1),
    AUTH_ADMIN_GOOGLE_ID: z.string().min(1),
    AUTH_ADMIN_GOOGLE_SECRET: z.string().min(1),
    REDIS_URL: z.string().url(),
    AGENT_SECRET: z.string().min(1),
    NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  },
  client: {
    NEXT_PUBLIC_APP_URL: z.string().url(),
  },
  experimental__runtimeEnv: {
    NEXT_PUBLIC_APP_URL: process.env.NEXT_PUBLIC_APP_URL,
  },
});
