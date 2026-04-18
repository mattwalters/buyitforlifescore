import type { NextConfig } from "next";
import "./env";

const nextConfig: NextConfig = {
  allowedDevOrigins: [
    "app.mono.localhost",
    "www.mono.localhost",
    "admin.mono.localhost",
    "kiss.mono.localhost",
    "app.buyitforlifeclub.localhost",
    "www.buyitforlifeclub.localhost",
    "admin.buyitforlifeclub.localhost",
    "kiss.buyitforlifeclub.localhost",
  ],
  transpilePackages: ["@mono/db"],
  serverExternalPackages: ["duckdb"],
};

export default nextConfig;
