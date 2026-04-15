from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path


@asset(
    group_name="bronze",
    description=(
        "Extract raw Reddit ZST dump and convert specifically the BuyItForLife "
        "subreddit into highly optimized columnar Parquet."
    ),
)
def bronze_reddit_buyitforlife_comments(context: AssetExecutionContext) -> MaterializeResult:
    source_zst = get_read_path("ore/reddit_buyitforlife_comments.zst")
    target_parquet = get_write_path("bronze/reddit_buyitforlife_comments.parquet")

    context.log.info(f"Connecting to DuckDB and reading from {source_zst}")

    # and write to Parquet. DuckDB natively handles the zst decompression buffer.
    # We use sample_size=-1 to force DuckDB to scan the whole file to build the schema,
    # which prevents crashes if new keys (like 'likes') appear 200,000 rows deep.
    query = f"""
    COPY (
        SELECT *
        FROM read_json_auto('{source_zst}', compression='zstd', sample_size=-1)
    ) TO '{target_parquet}' (FORMAT PARQUET);
    """

    # NOTE: In production you would probably add: WHERE subreddit = 'BuyItForLife'
    # But since the file is literally named BuyItForLife_comments.zst, it's likely pre-filtered!

    with get_duckdb_connection() as con:
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
    description="Extract raw Reddit ZST dump for Submissions and convert it into highly optimized columnar Parquet.",
)
def bronze_reddit_buyitforlife_submissions(context: AssetExecutionContext) -> MaterializeResult:
    source_zst = get_read_path("ore/reddit_buyitforlife_submissions.zst")
    target_parquet = get_write_path("bronze/reddit_buyitforlife_submissions.parquet")

    context.log.info(f"Connecting to DuckDB and reading from {source_zst}")

    # We use sample_size=-1 to infer full schema safely over mutations
    query = f"""
    COPY (
        SELECT *
        FROM read_json_auto('{source_zst}', compression='zstd', sample_size=-1)
    ) TO '{target_parquet}' (FORMAT PARQUET);
    """

    with get_duckdb_connection() as con:
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
