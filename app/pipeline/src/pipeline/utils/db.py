import os

import duckdb


def get_duckdb_connection(database=":memory:", read_only=False):
    """
    Creates a centralized DuckDB connection and configures the `httpfs` extension
    if R2/S3 environment variables are present. This allows seamless `s3://` queries.
    """
    con = duckdb.connect(database=database, read_only=read_only)

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

        con.execute(f"SET s3_endpoint='{r2_endpoint}';")
        con.execute(f"SET s3_access_key_id='{r2_access_key}';")
        con.execute(f"SET s3_secret_access_key='{r2_secret_key}';")
        con.execute("SET s3_region='auto';")

        # Cloudflare R2 strongly recommends vhost routing or using fully qualified URLs.
        # But 'path' style is strictly required without custom domains on the base account URL.
        # Actually, Cloudflare now supports vhost. 'vhost' or 'path' depends on DuckDB version,
        # but 'path' is safest for the raw accountid.r2.cloudflarestorage.com endpoint
        con.execute("SET s3_url_style='path';")
        con.execute("SET s3_use_ssl=true;")

    return con


def load_bronze_threads_with_comments(
    submissions_path: str,
    comments_path: str,
    where_clause: str = "",
    having_clause: str = "",
    order_clause: str = "",
    limit: int | None = None,
    extra_group_cols: str = "",
    seed: float | None = None,
) -> list[tuple]:
    """
    The single source of truth for loading Reddit threads with their comments from Bronze parquet.

    Joins submissions ↔ comments by link_id, groups by submission, and aggregates
    comments into a struct list. Both production Dagster assets and evaluation
    scripts MUST call this function so that the query shape can never drift.

    Args:
        submissions_path: Path to the submissions parquet file.
        comments_path: Path to the comments parquet file.
        where_clause: Optional SQL WHERE clause (without the WHERE keyword).
        having_clause: Optional SQL HAVING clause (without the HAVING keyword).
        order_clause: Optional SQL ORDER BY clause (without the ORDER BY keyword).
        limit: Optional row limit.
        extra_group_cols: Extra columns to include in GROUP BY (comma-separated, with leading comma).
        seed: Optional deterministic seed for DuckDB's setseed() (value between 0 and 1).

    Returns:
        A list of tuples: (submission_id, title, body, created_utc, comments_list).
    """
    where = f"WHERE {where_clause}" if where_clause else ""
    having = f"HAVING {having_clause}" if having_clause else ""
    order = f"ORDER BY {order_clause}" if order_clause else ""
    limit_sql = f"LIMIT {limit}" if limit else ""

    query = f"""
        SELECT
            s.id as submission_id,
            s.title,
            s.selftext as body,
            s.created_utc,
            list({{
                'id': c.id, 'parent_id': c.parent_id,
                'body': c.body, 'author': c.author, 'created_utc': c.created_utc
            }} ORDER BY c.created_utc ASC) as comments
        FROM '{submissions_path}' s
        LEFT JOIN '{comments_path}' c ON c.link_id = 't3_' || s.id
        {where}
        GROUP BY s.id, s.title, s.selftext, s.created_utc{extra_group_cols}
        {having}
        {order}
        {limit_sql}
    """

    with get_duckdb_connection() as con:
        if seed is not None:
            con.execute(f"SELECT setseed({seed})")
        return con.execute(query).fetchall()
