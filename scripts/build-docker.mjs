import { spawnSync } from "child_process";
import { existsSync } from "fs";
import { join } from "path";

// Parse command line arguments
const args = process.argv.slice(2);
const specificApp = args[0];

// Safe public variables that are allowed to be passed as --build-arg
// Do NOT add sensitive secrets to this list. Those belong in runtime .env only.
const allowedBuildArgs = [
  "NEXT_PUBLIC_APP_URL",
  "NEXT_PUBLIC_POSTHOG_HOST",
  "NEXT_PUBLIC_POSTHOG_TOKEN",
  "GOOGLE_CLOUD_PROJECT",
];

function buildDocker(app) {
  const dockerfilePath = join(process.cwd(), "app", app, "Dockerfile");

  if (!existsSync(dockerfilePath)) {
    console.error(`❌ Cannot find Dockerfile for '${app}' at ${dockerfilePath}`);
    process.exit(1);
  }

  console.log(`\n🐳 Building Docker image for ${app}...`);

  // Construct --build-arg flags based purely on what's available in the current env
  const buildArgFlags = allowedBuildArgs
    .filter((arg) => process.env[arg] !== undefined)
    .flatMap((arg) => ["--build-arg", `${arg}=${process.env[arg]}`]);

  const commandArgs = [
    "build",
    ...buildArgFlags,
    "-f",
    `app/${app}/Dockerfile`,
    "-t",
    `mono-${app}:latest`,
    ".",
  ];

  console.log(`> docker ${commandArgs.join(" ")}\n`);

  const result = spawnSync("docker", commandArgs, { stdio: "inherit" });

  if (result.status !== 0) {
    console.error(`❌ Docker build failed for ${app} with exit code ${result.status}`);
    process.exit(result.status);
  }

  console.log(`✅ Docker build succeeded for ${app}`);
}

// Either build a specific requested app, or build all known apps
if (specificApp) {
  buildDocker(specificApp);
} else {
  console.log("No specific service provided. Building all Docker services...");
  const apps = ["admin", "worker", "hocuspocus"];
  for (const app of apps) {
    buildDocker(app);
  }
  console.log("\n✅ All Docker builds completed securely.");
}
