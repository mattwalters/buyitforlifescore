# Deploying to Railway

This project uses **Config as Code** with per-service `railway.json` files.

## 1. CLI Login (Troubleshooting)

If `railway login` fails with "Unauthorized" despite browser success, try the browserless flow:

```bash
railway login --browserless
```

## 2. Project Setup

Since we have a monorepo, you need to creating a project and linking it.

1.  **Create a New Project** (in Dashboard or CLI):
    - Go to [Railway Dashboard](https://railway.com/dashboard).
    - Click "New Project" -> "Empty Project".
    - Note the **Project ID** (from Settings).
    - OR run `railway init` and select "Empty Project".

2.  **Link Local Repo**:
    ```bash
    railway link <project-id>
    ```

## 3. Deployment

Once linked, deploy your services:

```bash
railway up
```

Railway will detect your code. You may need to add the 3 services manually in the CLI prompt or Dashboard if it's the very first run, pointing them to their respective root directories:

- **Service Name**: `web` -> **Root Directory**: `app/web`
- **Service Name**: `worker` -> **Root Directory**: `app/worker`

Once the services exist and point to those roots, Railway will automatically load the `railway.json` from inside those folders to configure the build/start commands.

## Configuration

We use a root-level `railway.toml` to manage all services.

### Environment Variables

If your frontend has external dependencies, set them here.

### Services

The project defines the following services:

- **web**: `npm run start:web` (Next.js Application)
- **worker**: `npm run start:worker` (Background Job Processor)
