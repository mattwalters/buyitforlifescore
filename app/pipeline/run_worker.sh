#!/bin/bash
set -e

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

echo "Booting Celery Worker Node..."
# Tell Celery to spin up with concurrency bounded to what we mathematically proved earlier (20 runs = ~4GB peak natively).
# The container will securely process runs out of the Redis queue indefinitely.
exec dagster-celery worker start -A dagster_celery.app --concurrency=20
