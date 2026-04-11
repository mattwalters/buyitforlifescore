FROM node:22.22.2-bookworm-slim AS base

FROM base AS builder
WORKDIR /app
RUN npm install -g turbo
COPY . .
RUN turbo prune --scope=@mono/worker --docker

FROM base AS installer
WORKDIR /app

# First install the dependencies (as they change less often)
COPY .gitignore .gitignore
COPY --from=builder /app/out/json/ .
COPY --from=builder /app/out/package-lock.json ./package-lock.json
COPY tsconfig.base.json tsconfig.lib.json tsconfig.next.json ./
RUN npm install

# Build the project
COPY --from=builder /app/out/full/ .
ENV SKIP_ENV_VALIDATION="true"
RUN echo "SKIP_ENV_VALIDATION=true" > .env
RUN npm run build --filter=@mono/worker

FROM base AS runner
WORKDIR /app

# 1. Setup the unprivileged appuser first
RUN addgroup --system --gid 1001 nodejs && \
    useradd -m -s /bin/bash -u 1001 -g nodejs appuser

# 2. Set a GLOBAL browser path outside the workspace
# This removes the dependency on specific monorepo folder structures
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# 3. Copy node_modules from installer so npx can find the local playwright
COPY --from=installer /app/node_modules ./node_modules
COPY --from=installer /app/app/worker/node_modules ./app/worker/node_modules

# 4. DEBUG: Log the worker-specific version of Playwright
# By changing directory to app/worker, npx resolves the exact version 
# specified in the worker's package.json, not the globally hoisted one.
RUN cd app/worker && npx playwright --version

# 5. USE THE WORKER BINARY to install to the global path.
# This prevents downloading version 1200 when the worker needs 1208.
RUN cd app/worker && npx playwright install-deps chromium
RUN cd app/worker && npx playwright install chromium

# 6. DEBUG: Verify it actually installed chromium-1208
RUN ls -R /ms-playwright

# 7. Set tight permissions for the browser folder and the app folder
RUN chown -R appuser:nodejs /ms-playwright && \
    chown -R appuser:nodejs /app

# 7. Copy the remainder of your application pieces
COPY --from=installer --chown=appuser:nodejs /app/lib ./lib
COPY --from=installer --chown=appuser:nodejs /app/app/worker/dist ./app/worker/dist
COPY --from=installer --chown=appuser:nodejs /app/app/worker/package.json ./app/worker/package.json

# Switch context to secure user
USER appuser

ENV NODE_ENV=production
# Force explicit global fallback one final time at runtime
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

CMD ["npm", "start", "-w", "@mono/worker"]
