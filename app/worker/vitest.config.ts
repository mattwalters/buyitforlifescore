import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    globals: true,
    include: ["src/**/*.test.ts"],
    exclude: ["dist/**", "node_modules/**"],
    env: {
      GOOGLE_CLOUD_PROJECT: "test-project",
      GOOGLE_APPLICATION_CREDENTIALS: "/dev/null",
      SKIP_ENV_VALIDATION: "true",
    },
  },
});
