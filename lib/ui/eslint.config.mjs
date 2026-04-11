import { config } from "@mono/eslint-config/base";

export default [
  ...config,
  {
    rules: {
      "import-x/extensions": "off",
    },
  },
];
