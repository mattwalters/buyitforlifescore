try {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  require("dotenv/config");
} catch {
  // Ignore in production
}

let config;
try {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const prismaConfig = require("prisma/config");
  config = prismaConfig.defineConfig({
    schema: "prisma/schema.prisma",
    migrations: {
      path: "prisma/migrations",
    },
    datasource: {
      url: process.env.DATABASE_URL,
    },
  });
} catch {
  // Graceful fallback for production standalone builds where dev dependencies are aggressively stripped.
  // We simply supply the raw object shape that Prisma 7 expects.
  config = {
    schema: "prisma/schema.prisma",
    migrations: {
      path: "prisma/migrations",
    },
    datasource: {
      url: process.env.DATABASE_URL,
    },
  };
}

export default config;
