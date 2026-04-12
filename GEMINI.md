# SaaS Template Monorepo Context

## Project Overview

`mono` is a monorepo containing a Next.js web application and support services. It utilizes the modern Next.js App Router architecture and is configured for high performance and scalability.

**Key Technologies:**

- **Monorepo:** npm workspaces
- **Deployment:** Railway

## Architecture & Structure

The project follows a Monorepo structure, split into:

- **`app/worker`**: Background worker service (Node.js).
- **`lib/db`**: Shared database client and Prisma schema.

### Middleware & Edge Routing

This monorepo rejects the traditional Next.js `middleware.ts` pattern. Instead, edge routing, session validation, and proxy rules are defined in `proxy.ts` files (e.g., `app/web/proxy.ts`, `app/admin/proxy.ts`). Do not look for or recreate `middleware.ts`.

### Development Ports

| Service           | Port   | Description                           |
| :---------------- | :----- | :------------------------------------ |
| **Worker**        | `3001` | Background worker health check server |
| **Email Preview** | `3004` | React Email preview server            |
| **Admin**         | `3007` | **[PLANNED]** Admin Dashboard         |
| **Postgres**      | `5432` | Database (Docker)                     |
| **Redis**         | `6379` | Key-value store (Docker)              |

### Key Global Commands

Run these commands from the **ROOT** using `npm run <command>`:

| Command                | Description                                                                  |
| :--------------------- | :--------------------------------------------------------------------------- |
| `lint`                 | Runs ESLint across all workspaces.                                           |
| `docker compose up -d` | Starts the local PostgreSQL database.                                        |
| `./setup.sh`           | Automates dependency installation, environment setup, and Prisma generation. |
| `db:generate`          | Generates the Prisma client from the schema.                                 |
| `db:migrate`           | Runs database migrations.                                                    |
| `db:studio`            | Opens Prisma Studio to view database content.                                |
| `db:status`            | Checks the sync status of migrations.                                        |
| `format`               | Checks code formatting with Prettier.                                        |
| `format:write`         | Fixes code formatting with Prettier.                                         |

### Agent Command Authority

The AI Agent is explicitly authorized to run any `npm` command defined in `package.json`.

- **Root Commands**: `npm run <command>`
- **Workspace Commands**: `npm run <command> -w <workspace>` (e.g., `npm run dev -w @mono/admin`)

**CRITICAL RULE: NEVER COMMIT CODE.**

- The AI Agent must **NEVER** run `git add`, `git commit`, `git push` or attempt to manage version control state.
- The AI Agent should only make code changes and leave the git management entirely up to the human developer.

## Python Environment (uv)

The monorepo contains Python workspaces (e.g., `app/pipeline`). We use **`uv`** as our Python package and project manager. 
- You MUST use `uv run <script>` instead of `.venv/bin/python` to ensure the correct virtual environment and dependencies are automatically resolved.

## Code Formatting

The project uses **Prettier** for code formatting.

- **Config**: `.prettierrc` (root)
- **Ignore**: `.prettierignore` (root)

All workspaces inherit this configuration.

## Workflow Standards

After completing any task or significant change, you **MUST**:

1.  Run `npm run lint` to ensure no linting errors.
2.  Run `npm run format:write` to ensure consistent code style.
3.  Run targeted tests such as `npm run test:unit`, `npm run test:components`, or `npm run test:storybook` within the respective workspace. **Do NOT run the full E2E test suite (e.g., `npm run test`) during standard development** as it takes too long. E2E tests are intentionally left for CI or highly targeted manual execution.
4.  **Walkthroughs**: When creating a `walkthrough.md`, include links to relevant pages (e.g. `http://localhost:3000`) to allow for immediate verification.

### Event Tracking (PostHog)

When implementing custom analytics events, we follow PostHog's recommended `[object] [verb]` format, where the object is the entity the action relates to, and the verb is the action itself (separated by a space).

- **Format**: `[object] [verb]`
- **Good**: `user signed up`, `welcome tour completed`, `book created`
- **Bad**: `user_signed_up`, `completed welcome tour`, `bookCreated`

## Deployment Strategy (Railway)

We recommend deploying this monorepo as separate services on Railway, all connected to this same repository.

1.  **Worker Service**: `npm run start:worker` (Tracks branch `deploy/worker`)

### Deployment Branches

Railway is configured to track specific branches for each service. When code is pushed to these branches, a deployment is triggered.

- `deploy/admin` -> Deploys the Admin App
- `deploy/worker` -> Deploys the Worker

**CI/CD Note**: CI jobs are configured to **skip** these branches to save resources, as they are strictly valid deployment targets.

## Domain Strategy

The project uses a split-domain strategy for SEO and clear separation of concerns (e.g., putting specific services on distinct subdomains).

## E2E Testing Architecture

The project uses Playwright for comprehensive End-to-End testing. The E2E environment is designed to be fully isolated from local development so that both can run concurrently without interference.

### 1. Port Isolation (`1300X`)

To prevent conflicts with local development (`300X` ports), E2E tests operate on the `1300X` port range. Playwright's `webServer` automatically spins up all necessary services:

- **Worker:** `13001` (`WORKER_PORT`)
- **Mock AI Server:** `13005` (`AI_MOCK_URL`)

### 2. Database & Redis Isolation

E2E tests use isolated data stores to prevent polluting or destroying development data. These are defined in `.env.test`:

- **Postgres:** `15432` (instead of `5432`)
- **Redis:** `16379` (instead of `6379`)

### 3. Background Jobs

### 4. Mocking AI Calls

To avoid hitting real Google GenAI endpoints during tests (saving costs and avoiding rate limits):

- A local **Mock AI Server** is spun up by Playwright on port `13005`.
- The worker is provided with an environment variable: `AI_MOCK_URL="http://127.0.0.1:13005"`.
- The `SaaS TemplateAI` client detects this variable and redirects all generation requests to the mock server.
- The mock server supports **Dynamic Mocking**: E2E tests can register dynamic responses for specific prompt patterns via `POST /__admin/mock` and clean up the queue via `POST /__admin/reset`.
- For requests that aren't dynamically mocked, the mock server uses **Automatic Fallback Inference** based on string matching the request body, returning safe schema-compliant responses for generic generation tasks or falling back to your specific JSON fixtures.
- If no inference matches, it falls back to a generic success fallback.

## Environment Management

- **Local**: Use root-level `docker compose up -d` for Postgres.
- **Secrets**:
  - **Centralized Strategy**: The monorepo uses a **single root `.env` file** as the source of truth for all environment variables in development. Sub-workspaces (e.g., `app/admin`, `app/worker`) do **NOT** have their own local `.env` files.
  - **Validation**: Each app workspace defines a strict Zod schema in its `env.ts` (e.g., using `@t3-oss/env-nextjs`). All environment variables must be accessed through this validated `env` object rather than directly from `process.env`.
  - **Fail-Fast**: Missing or incorrectly formatted variables will cause the build or service to crash immediately with a descriptive error.
  - **Libraries**: Libraries remain **stateless** and do not load their own `.env` files or validate environments directly. They receive configuration via arguments from the consuming app.
  - **Orchestration**: All workspace scripts (e.g., `dev`, `build`, `start`, tests) use the **Root** as the orchestrator via `dotenv-cli` to securely inject the root environment into the workspace.
    - Pattern: `dotenv -e .env -- npm run <script> -w <workspace>`
  - **Strict Service Isolation (Production)**: Each production service container (e.g., on Railway) MUST ONLY contain the variables it explicitly requires in its `env.ts` schema. We strictly oppose configuration bloat. If a service does not use a variable, it must not be injected into its environment.
  - **Phantom Variable Prevention**: We actively avoid "phantom variables" (variables that linger after being deprecated). Any variable removed from a service's `env.ts` must also be entirely purged from `turbo.json`, `Dockerfile`s, CI/CD environments, `.env.test`, `.env`, and production hosting configurations.
  - **Build-Time vs. Runtime Boundaries**: While `env.ts` ensures perfect runtime safety, framework configuration files like `next.config.ts` often run _before_ runtime and require raw `process.env` access (e.g., `POSTHOG_API_KEY` for uploading sourcemaps). Build-time variables require careful tracking in deployment systems.
  - **Environment-Specific Friction Reduction**: If a variable isn't strictly necessary for local work, use Zod's `.optional()` combined with conditionals (`process.env.NODE_ENV === "production" ? z.string() : z.string().optional()`) or defaults to reduce onboarding friction for developers without compromising production strictness.

## Module System

**All packages should use ESM (ES Modules) unless there is a specific technical reason requiring CommonJS.**

ESM is the modern JavaScript module standard. Using ESM consistently across the monorepo:

- Prevents duplicate module issues (especially with complex peer dependencies)
- Enables tree-shaking for smaller bundles
- Ensures compatibility with modern packages

To configure a package for ESM:

1. Add `"type": "module"` to `package.json`
2. Set `"module": "ESNext"` and `"moduleResolution": "bundler"` in `tsconfig.json`
3. Use `.js` extensions in relative imports (e.g., `import { foo } from './bar.js'`)

## Creating New Workspaces

When adding new packages to `lib/` or apps to `app/`, follow this strict checklist to ensure it integrates seamlessly with the monorepo pipeline:

1.  **`package.json` Initialization**:
    - **Scope**: Ensure the name follows the scope `@mono/<name>`.
    - **Privacy**: Set `"private": true` to prevent accidental publishing.
    - **ESM**: Set `"type": "module"` (unless CJS is strictly required).

2.  **Standard Scripts**: Every workspace MUST include these standard scripts to connect gracefully to TurboRepo's global orchestrations:
    - `"build": "tsc -b ."` (or Next.js/Vite equivalent).
    - `"lint": "eslint ."` (to hook into the root `eslint.config.mjs` checks).
    - `"clean": "rm -rf .turbo node_modules dist"` (highly recommended).

3.  **TypeScript Configuration (`tsconfig.json`)**:
    - **CRITICAL**: You MUST add a local `tsconfig.json`. Without it, `npm run build` from the root will throw errors.
    - DO NOT redefine compiler options manually. Instead, leverage our 3-tier config system:
      - **Libraries (`lib/*`)**: Extend `"../../tsconfig.lib.json"`. You MUST specify `"outDir": "./dist"` and `"rootDir": "./src"` locally to prevent artifact leakage.
      - **Next.js Apps (`app/*`)**: Extend `"../../tsconfig.next.json"`.
      - **Node Services/Workers**: Extend `"../../tsconfig.base.json"` and ensure you include path aliases where needed.

4.  **Dependencies**:
    - **Use Root Tooling**: Do NOT install `@types/node`, `typescript`, `eslint`, or `prettier` in your package's `devDependencies`. They are hoisted and managed at the monorepo root.

5.  **Environment Variables**:
    - If the package requires env vars (e.g., `DATABASE_URL`), it is extremely recommended to use a validated `env.ts` (using Zod) rather than raw `process.env`. Add logic to `./setup.sh` to automatically create/populate the `.env` file from a template or defaults.

## Future Context

- This file (`GEMINI.md`) serves as the **Root Context** for the AI agent.
- See specific `GEMINI.md` files in `app/` for detailed implementation contexts.
