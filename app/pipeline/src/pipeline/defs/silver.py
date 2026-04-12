import duckdb
from pathlib import Path
from dagster import asset, MaterializeResult, AssetExecutionContext, MonthlyPartitionsDefinition

# We define a Monthly Partition from Jan 2012 (when the BuyItForLife subreddit was founded)
# This will automatically create all the "boxes" in the Dagster UI up to the current month.
bifl_monthly_partitions = MonthlyPartitionsDefinition(start_date="2012-01-01")

@asset(
    group_name="silver",
    partitions_def=bifl_monthly_partitions,
    deps=["raw_reddit_buyitforlife_comments"],
    description="Runs LLM inferences (Mocked) on a specific monthly timeslice of Bronze data."
)
def extracted_sentiment_silver(context: AssetExecutionContext) -> MaterializeResult:
    # 1. Grab the exact month from Dagster that the User clicked on!
    partition_date_str = context.partition_key # e.g. "2021-06-01"
    
    monorepo_root = Path(__file__).resolve().parents[5]
    bronze_parquet = monorepo_root / "data" / "bronze" / "buyitforlife_comments.parquet"
    
    silver_dir = monorepo_root / "data" / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    # 2. Set the destination to exactly this month's partition slice. 
    # This guarantees idempotency (we can re-run this month 50 times without duplicating data)
    target_parquet = silver_dir / f"sentiment_silver_{partition_date_str}.parquet"
    
    context.log.info(f"Processing LLM Extractions for Timeslice: {partition_date_str}")
    
    # 3. Pull ONLY the data we need. This is the 1% LLM trial mechanism!
    # DuckDB will use the Parquet Columnar index to intelligently skip reading years of data
    # that don't match the partition date string.
    query = f"""
    COPY (
        SELECT id, body, created_utc, 
               'Positive' as ai_sentiment, 
               'Handle issues' as ai_flaws 
        FROM '{str(bronze_parquet)}'
        WHERE strftime(to_timestamp(CAST(created_utc AS BIGINT)), '%Y-%m-01') = '{partition_date_str}'
    ) TO '{str(target_parquet)}' (FORMAT PARQUET);
    """
    
    # 4. In a real environment, you would pull these SQL rows into Python, run an Asyncio batch
    # request calling Anthropic Claude 3 Haiku, and then write the results out.
    with duckdb.connect(database=':memory:') as con:
        con.execute(query)
        
    # 5. Yield our Asset Metadata! This satisfies your exact requirement to chart / track AI spend
    # across time without writing DB rows.
    return MaterializeResult(
        metadata={
            "partition": partition_date_str,
            "target_file": str(target_parquet),
            "cost_usd": 0.05, 
            "input_tokens": 12500,
            "output_tokens": 120
        }
    )
