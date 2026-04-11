import { config as baseConfig } from "./base.mjs";
import nextVitals from "eslint-config-next/core-web-vitals";
import nextTs from "eslint-config-next/typescript";
import prettier from "eslint-config-prettier";

/** @type {import('eslint').Linter.Config[]} */
export const config = [
  ...baseConfig,
  ...nextVitals,
  ...nextTs,
  {
    rules: {
      "react-hooks/exhaustive-deps": "off",
      "import-x/extensions": "off",
    },
  },
  prettier,
];
