FROM python:3.12-slim

# Install system utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

WORKDIR /app

# Environment variables for uv
ENV UV_COMPILE_BYTECODE=1
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
# Ensure python prints unbuffered logs instantly to Railway UI
ENV PYTHONUNBUFFERED=1

# Install dependencies (without our source code yet, for docker caching)
COPY pyproject.toml uv.lock ./
# We purposely sync everything including dev (like dagster-webserver which is in the dev group)
RUN uv sync --frozen

# Copy our source code in
COPY . .

# Run a final sync to install the `pipeline` package itself
RUN uv sync --frozen

# Make the entrypoint executable
RUN chmod +x run.sh

# Railway exposes the $PORT variable natively
EXPOSE 3000

# Fire the orchestrator
ENTRYPOINT ["./run.sh"]
