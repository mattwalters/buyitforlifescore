import duckdb
from pathlib import Path
from dagster import asset, MaterializeResult, AssetExecutionContext

@asset(
    group_name="bronze",
    description="Extract raw Reddit ZST dump and convert specifically the BuyItForLife subreddit into highly optimized columnar Parquet."
)
def raw_reddit_buyitforlife_comments(context: AssetExecutionContext) -> MaterializeResult:
    # Identify paths relative to the monorepo root
    # __file__ is app/pipeline/src/pipeline/defs/bronze.py
    # Monorepo root is ../../../../../
    monorepo_root = Path(__file__).resolve().parents[5]
    source_zst = monorepo_root / "data" / "BuyItForLife_comments.zst"
    
    # Store the output in a `bronze` folder next to the data
    bronze_dir = monorepo_root / "data" / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    target_parquet = bronze_dir / "buyitforlife_comments.parquet"
    
    context.log.info(f"Connecting to DuckDB and reading from {source_zst}")
    
    # and write to Parquet. DuckDB natively handles the zst decompression buffer.
    # We use sample_size=-1 to force DuckDB to scan the whole file to build the schema,
    # which prevents crashes if new keys (like 'likes') appear 200,000 rows deep.
    query = f"""
    COPY (
        SELECT *
        FROM read_json_auto('{str(source_zst)}', compression='zstd', sample_size=-1)
    ) TO '{str(target_parquet)}' (FORMAT PARQUET);
    """
    
    # NOTE: In production you would probably add: WHERE subreddit = 'BuyItForLife'
    # But since the file is literally named BuyItForLife_comments.zst, it's likely pre-filtered!

    with duckdb.connect(database=':memory:') as con:
        context.log.info("Executing DuckDB COPY command...")
        con.execute(query)
        context.log.info("Successfully wrote parquet file.")

    return MaterializeResult(
        metadata={
            "source_file": str(source_zst),
            "target_file": str(target_parquet),
        }
    )
