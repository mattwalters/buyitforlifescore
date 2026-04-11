import { config } from "@mono/eslint-config/base";

/** @type {import("eslint").Linter.Config[]} */
export default [
  ...config,
  {
    rules: {
      "@typescript-eslint/no-explicit-any": "error",
    },
  },
];
