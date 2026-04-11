import type { NextConfig } from "next";
import "./env";

const nextConfig: NextConfig = {
  allowedDevOrigins: [
    "app.mono.localhost",
    "www.mono.localhost",
    "admin.mono.localhost",
    "app.buyitforlifeclub.localhost",
    "www.buyitforlifeclub.localhost",
    "admin.buyitforlifeclub.localhost",
  ],
  transpilePackages: ["@mono/db"],
  serverExternalPackages: ["@bull-board/ui", "@bull-board/api"],
};

export default nextConfig;
