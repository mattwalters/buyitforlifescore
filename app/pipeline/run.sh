#!/bin/bash
set -e

# Railway dynamically assigns $PORT at runtime, but we fallback to 3000 just in case
UI_PORT=${PORT:-3000}

# Enforce a shared persistent home directory for Dagster's SQLite database
export DAGSTER_HOME=/app/dagster_home
mkdir -p $DAGSTER_HOME

echo "Applying Production Dagster Configuration..."
cp dagster.production.yaml $DAGSTER_HOME/dagster.yaml

echo "Booting Dagster Daemon (Background orchestrator for sensors & schedules)..."
# Both webserver and daemon rely on workspace.yaml being implicitly present in the working directory
dagster-daemon run &

echo "Booting Dagster Webserver on port $UI_PORT..."
# exec replaces the shell with the webserver process, ensuring it receives termination signals cleanly
exec dagster-webserver -h 0.0.0.0 -p $UI_PORT -w workspace.yaml
