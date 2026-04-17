import os

import duckdb


def get_duckdb_connection(database=":memory:", read_only=False, memory_limit="1GB"):
    """
    Creates a centralized DuckDB connection and configures the `httpfs` extension
    if R2/S3 environment variables are present. This allows seamless `s3://` queries.
    """
    con = duckdb.connect(database=database, read_only=read_only)

    # CRITICAL: We natively throttle concurrent workers dynamically.
    # Silver processes run at 1GB, Bronze (JSON Extraction) runs at 8GB.
    # con.execute("PRAGMA threads=1;")
    # con.execute(f"PRAGMA memory_limit='{memory_limit}';")

    r2_endpoint = os.getenv("R2_ENDPOINT_URL")
    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")

    if r2_endpoint and r2_access_key and r2_secret_key:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        # Strip https:// or http:// if provided in the endpoint URL because DuckDB
        # expects just the hostname.
        if r2_endpoint.startswith("https://"):
            r2_endpoint = r2_endpoint[8:]
        elif r2_endpoint.startswith("http://"):
            r2_endpoint = r2_endpoint[7:]

        # DuckDB 1.0+ prefers the Secret Manager over global SET s3_ variables,
        # which can fail on Cloudflare R2 authorization headers.
        secret_query = f"""
        CREATE OR REPLACE SECRET r2_secret (
            TYPE S3,
            ENDPOINT '{r2_endpoint}',
            KEY_ID '{r2_access_key}',
            SECRET '{r2_secret_key}',
            REGION 'auto',
            URL_STYLE 'path'
        );
        """
        con.execute(secret_query)

    return con
