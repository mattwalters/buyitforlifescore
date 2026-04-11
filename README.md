# SaaS Monorepo Template

A production-ready **Next.js Monorepo Template** designed for high performance and scalability. This repository is pre-configured for deployment on **Railway** and includes dedicated services for background processing.

## ⚡️ Features

- **Monorepo**: Managed via npm workspaces for modularity.
- **Frontend**: Next.js 16 (App Router) with React 19 and Tailwind CSS 4.
- **Worker**: Dedicated Node.js service for background jobs (`app/worker`).
- **Database**: PostgreSQL with Prisma ORM.
- **Auth**: NextAuth.js 5 (Beta).

## 🚀 Quick Start

We include a helper script to get you up and running in seconds.

```bash
# 1. Clone the repository
git clone <your-repo-url>
cd my-saas-app

# 2. Run the Setup Script
# This installs dependencies, sets up your local .env, and generates the Prisma Client.
./setup.sh

# 3. Start Development
# This starts the Docker containers, Web App, and Worker concurrently.
npm run dev
```

The application will be available at `http://localhost:3000`.

## 🏗️ Architecture

The project is split into three main workspaces in the `app/` directory:

| `app/admin`  | **Admin**  | The admin application.      |
| `app/worker` | **Worker**  | Background process for handling async tasks.      |

## 🛠️ Key Commands

Run these from the **root** of the repository:

- `npm run dev:admin`: Start Admin dev server.
- `npm run dev:worker`: Start Worker dev server.
- `npm run test`: Run tests across all workspaces.
- `npm run lint`: Lint all workspaces.

## 🚂 Deployment (Railway)

This repository includes a `railway.toml` and is optimized for Railway. We recommend deploying this as **2 separate services** connected to the same repository:

1.  **Admin**: `npm run start:admin`
2.  **Worker**: `npm run start:worker`

For more detailed context, see [GEMINI.md](./GEMINI.md).
