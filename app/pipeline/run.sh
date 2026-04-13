#!/bin/bash
set -e

# Railway dynamically assigns $PORT at runtime, but we fallback to 3000 just in case
UI_PORT=${PORT:-3000}

echo "Booting Dagster Daemon (Background orchestrator for sensors & schedules)..."
dagster-daemon run &

echo "Booting Dagster Webserver on port $UI_PORT..."
# exec replaces the shell with the webserver process, ensuring it receives termination signals cleanly
exec dagster-webserver -h 0.0.0.0 -p $UI_PORT -m pipeline.definitions
