import duckdb
import asyncio
from dagster import asset_check, AssetCheckResult, MetadataValue
from .silver import silver_entity_discovery
from ..utils.pricing import AiModel
from ..utils.judge import run_blind_canary_evaluation

@asset_check(asset=silver_entity_discovery)
def canary_validity_check(context) -> AssetCheckResult:
    """
    Randomly samples exactly 1,067 extractions across all materialized daily partitions
    and evaluates them using an un-anchored Gemini Blind Judge to compute a ~95% Confidence Interval.
    """
    from ..utils.paths import get_data_dir
    data_dir = get_data_dir()
    source_glob = f"{data_dir}/silver/entity_discovery_*.parquet"
    
    with duckdb.connect(database=':memory:') as con:
        if data_dir.startswith("s3://"):
            con.execute("INSTALL httpfs; LOAD httpfs;")
        try:
            # We use RESERVOIR sampling in DuckDB to pull an exact subset of the population longitudinally
            query = f"""
                SELECT brand, productName, body 
                FROM read_parquet('{source_glob}') 
                USING SAMPLE 1067 ROWS
            """
            sample_rows = con.execute(query).fetchall()
        except duckdb.IOException:
            context.log.info("No silver parquet files found to sample.")
            return AssetCheckResult(passed=True, metadata={"skipped": "No data"})

    if not sample_rows:
        return AssetCheckResult(passed=True, metadata={"skipped": "No data"})
        
    extractions_for_judge = [{"brand": r[0], "productName": r[1], "body": r[2]} for r in sample_rows]
    num_samples = len(extractions_for_judge)
    context.log.info(f"Running blind judge canary on {num_samples} longitudinally-mixed samples...")
    
    judge_semaphore = asyncio.Semaphore(10)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    chunk_size = 50 
    chunks = [extractions_for_judge[i:i + chunk_size] for i in range(0, num_samples, chunk_size)]
    
    async def process_chunk(chunk):
        res, cost = await run_blind_canary_evaluation(
            chunk, 
            judge_semaphore, 
            AiModel.GEMINI_3_PRO.value, 
            "high"
        )
        return res, cost

    async def run_all():
        tasks = [process_chunk(c) for c in chunks]
        results = await asyncio.gather(*tasks)
        
        all_res = []
        tot_cost = 0.0
        for r, c in results:
            all_res.extend(r)
            tot_cost += c
        return all_res, tot_cost

    validations_res, judge_cost = loop.run_until_complete(run_all())
    
    if not validations_res:
        return AssetCheckResult(passed=False, metadata={"error": "Judge failed to return valid outputs or encountered rate limits."})
        
    num_valid = sum(1 for v in validations_res if v.is_valid_durable_good)
    precision = num_valid / len(validations_res)
    
    passed = precision >= 0.90 # 90% floor to sound the alarm
    
    # Store top 20 failures for manual debugging
    failure_logs = [
        f"[{extractions_for_judge[i]['brand']}] -> {v.reasoning}"
        for i, v in enumerate(validations_res) if not getattr(v, "is_valid_durable_good", True)
    ]
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "precision": MetadataValue.float(round(precision, 4)),
            "cost_usd": MetadataValue.float(round(judge_cost, 4)),
            "samples_evaluated": len(validations_res),
            "failures": MetadataValue.json(failure_logs[:20])
        }
    )
