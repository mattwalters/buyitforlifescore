from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from pipeline.defs.partitions import subreddit_partitions
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path


@asset(
    group_name="bronze",
    partitions_def=subreddit_partitions,
    description="Extract raw Reddit ZST dump for Comments and convert it into highly optimized columnar Parquet.",
)
def bronze_reddit_comments(context: AssetExecutionContext) -> MaterializeResult:
    subreddit_key = context.partition_key.lower()
    source_zst = get_read_path(f"ore/reddit_{subreddit_key}_comments.zst")
    target_parquet = get_write_path(f"bronze/reddit_{subreddit_key}_comments.parquet")

    context.log.info(f"Connecting to DuckDB and reading from {source_zst}")

    # We use sample_size=-1 to force DuckDB to scan the whole file to build the schema,
    # which prevents crashes if new keys (like 'likes') appear 200,000 rows deep.
    query = f"""
    COPY (
        SELECT *
        FROM read_json_auto('{source_zst}', compression='zstd', sample_size=-1)
    ) TO '{target_parquet}' (FORMAT PARQUET);
    """

    with get_duckdb_connection(memory_limit="8GB") as con:
        context.log.info("Executing DuckDB COPY command...")
        con.execute(query)

        # Generate Markdown Preview
        preview_df = con.execute(f"SELECT body, score, created_utc FROM '{target_parquet}' LIMIT 5").fetchdf()
        preview_md = preview_df.to_markdown()

        context.log.info("Successfully wrote parquet file and generated preview.")

    return MaterializeResult(
        metadata={
            "source_file": source_zst,
            "target_file": target_parquet,
            "data_preview": MetadataValue.md(preview_md),
        }
    )


@asset(
    group_name="bronze",
    partitions_def=subreddit_partitions,
    description="Extract raw Reddit ZST dump for Submissions and convert it into highly optimized columnar Parquet.",
)
def bronze_reddit_submissions(context: AssetExecutionContext) -> MaterializeResult:
    subreddit_key = context.partition_key.lower()
    source_zst = get_read_path(f"ore/reddit_{subreddit_key}_submissions.zst")
    target_parquet = get_write_path(f"bronze/reddit_{subreddit_key}_submissions.parquet")

    context.log.info(f"Connecting to DuckDB and reading from {source_zst}")

    import os
    access = os.getenv("R2_ACCESS_KEY_ID", "MISSING")
    secret = os.getenv("R2_SECRET_ACCESS_KEY", "MISSING")
    endpt = os.getenv("R2_ENDPOINT_URL", "MISSING")
    
    context.log.info(f"[DIAGNOSTICS] Access Key: {access[:5]}*** (Len: {len(access)})")
    context.log.info(f"[DIAGNOSTICS] Secret Key: {secret[:5]}*** (Len: {len(secret)})")
    context.log.info(f"[DIAGNOSTICS] Endpoint: {endpt}")

    # We use sample_size=-1 to infer full schema safely over mutations
    query = f"""
    COPY (
        SELECT *
        FROM read_json_auto('{source_zst}', compression='zstd', sample_size=-1)
    ) TO '{target_parquet}' (FORMAT PARQUET);
    """

    with get_duckdb_connection(memory_limit="8GB") as con:
        context.log.info("Executing DuckDB COPY command...")
        con.execute(query)

        # Generate Markdown Preview
        preview_df = con.execute(
            f"SELECT title, selftext, score, created_utc FROM '{target_parquet}' LIMIT 5"
        ).fetchdf()
        preview_md = preview_df.to_markdown()

        context.log.info("Successfully wrote parquet file and generated preview.")

    return MaterializeResult(
        metadata={
            "source_file": source_zst,
            "target_file": target_parquet,
            "data_preview": MetadataValue.md(preview_md),
        }
    )
