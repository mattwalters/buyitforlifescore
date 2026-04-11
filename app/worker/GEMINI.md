# Mono Worker Context (`app/worker`)

## Project Overview

A dedicated Node.js background worker service for handling asynchronous tasks and job processing using **BullMQ**.

**Key Technologies:**

- **Runtime:** Node.js (TypeScript)
- **Queue:** BullMQ
- **Cache:** Redis (Docker)
- **Database:** PostgreSQL (Shared via `@mono/db`)

## Services

- **Email Queue**: Handles email dispatch.
- **AI Agents Queue**: Handles AI tasks (e.g., text generation) via registered Agents.

## Environment

- `REDIS_URL`: Connection string for Redis.
- `DATABASE_URL`: Connection string for Postgres.
- Run `./setup.sh` to configure environment automatically.

## Commands

- `npm run dev`: Start worker in watch mode.

## AI Agents Architecture

The worker implements a flexible AI Agent system designed to offload complex logic.

- **Queue**: `agents-queue`
- **Registry**: Agents are registered in `src/index.ts` using a `Map<string, Agent>`.
- **Interface**: All agents implement the `Agent` interface (`src/agents/base.ts`).
  ```typescript
  interface Agent {
    name: string;
    description: string;
    process(input: any): Promise<any>;
  }
  ```
- **Current Agents**:
  - `writer`: Uses Google Gemini used to generate creative text.

## AI Analysis Architecture

The worker also handles the **Editorial Analysis Pipeline**, which is distinct from the general generic Agent system.

- **Queue**: `analysis-ingest` (Ingestion), `analysis-pipeline` (Execution)
- **Ingest Strategy**:
  - Receives content snapshots from the client (`lib/prose` plugin).
  - Deduplicates based on SHA-256 hash (unless `force: true`).
  - Resolves applicable pipelines (e.g., Grammar, Style) based on scope type.
  - Applies debouncing (default 10s) to prevent over-triggering.
- **Pipeline Execution**:
  - Hydrates content if missing.
  - Calls LLM (Gemini Flash).
  - Connects to **Hocuspocus (Yjs)** to apply highlights directly to the document.
