#!/bin/bash
set -e

# Railway dynamically assigns $PORT at runtime, but we fallback to 3000 just in case
UI_PORT=${PORT:-3000}

# Enforce a shared persistent home directory for Dagster's SQLite database
export DAGSTER_HOME=/app/dagster_home
mkdir -p $DAGSTER_HOME

echo "Extracting Postgres Credentials from DATABASE_URL..."
export DAGSTER_PG_USER=$(python -c "import os, urllib.parse; url=urllib.parse.urlparse(os.environ.get('DATABASE_URL', '')); print(url.username or '')")
export DAGSTER_PG_PASSWORD=$(python -c "import os, urllib.parse; url=urllib.parse.urlparse(os.environ.get('DATABASE_URL', '')); print(url.password or '')")
export DAGSTER_PG_HOST=$(python -c "import os, urllib.parse; url=urllib.parse.urlparse(os.environ.get('DATABASE_URL', '')); print(url.hostname or '')")
export DAGSTER_PG_PORT=$(python -c "import os, urllib.parse; url=urllib.parse.urlparse(os.environ.get('DATABASE_URL', '')); print(url.port or '5432')")
export DAGSTER_PG_DB=$(python -c "import os, urllib.parse; url=urllib.parse.urlparse(os.environ.get('DATABASE_URL', '')); print(url.path.lstrip('/') or '')")

echo "Applying Production Dagster Configuration..."
cp dagster.production.yaml $DAGSTER_HOME/dagster.yaml

echo "Booting Dagster Daemon (Background orchestrator for sensors & schedules)..."
# Both webserver and daemon rely on workspace.yaml being implicitly present in the working directory
dagster-daemon run &

echo "Booting Dagster Webserver on port $UI_PORT..."
# exec replaces the shell with the webserver process, ensuring it receives termination signals cleanly
exec dagster-webserver -h 0.0.0.0 -p $UI_PORT -w workspace.yaml
