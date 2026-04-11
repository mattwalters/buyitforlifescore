FROM node:22.22.2-alpine AS runner

RUN apk add --no-cache libc6-compat
WORKDIR /app

# Copy the entire monorepo so that scripts and all modules are available
COPY . .

# Install dependencies for all workspaces
RUN npm ci

# Build args required for next.js
ARG PORT
ARG NEXT_PUBLIC_APP_URL

# Build the admin service and its dependencies via turbo
ENV SKIP_ENV_VALIDATION="true"
RUN echo "SKIP_ENV_VALIDATION=true" > .env
RUN npx turbo build --filter=@mono/admin

# Setup environment for runtime
ENV AUTH_TRUST_HOST="true"
ENV NEXT_TELEMETRY_DISABLED="1"
ENV PORT="3007"
ENV HOSTNAME="0.0.0.0"

CMD ["npm", "run", "start:admin"]
