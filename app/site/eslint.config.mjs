import { config as baseConfig } from "@mono/eslint-config/base";

/** @type {import('eslint').Linter.Config[]} */
const eslintConfig = [
  ...baseConfig,
  {
    ignores: [".astro/**", "dist/**", "node_modules/**"],
  },
];

export default eslintConfig;
