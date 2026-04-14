import duckdb
import asyncio
from dagster import asset, MaterializeResult, MetadataValue, Config
from .silver import silver_entity_discovery
from ..utils.pricing import AiModel
from ..utils.judge import run_blind_evaluation
from ..utils.db import get_duckdb_connection

class DiscoveryEvalConfig(Config):
    sample_size: int = 385  # 95% Confidence interval with a 5% margin of error

@asset(group_name="evaluations", deps=[silver_entity_discovery])
def silver_entity_discovery_eval(context, config: DiscoveryEvalConfig) -> MaterializeResult:
    """
    Randomly samples extracted rows across all materialized daily partitions
    and evaluates them using an un-anchored Gemini Blind Judge.
    """
    from ..utils.paths import get_read_path
    
    source_glob = get_read_path("silver/entity_discovery_[0-9]*.parquet")
    
    with get_duckdb_connection() as con:
        try:
            # We use RESERVOIR sampling in DuckDB to pull an exact subset of the population longitudinally
            query = f"""
                SELECT brand, productName, target_text, llm_generation_prompt 
                FROM read_parquet('{source_glob}') 
                USING SAMPLE {config.sample_size} ROWS
            """
            sample_rows = con.execute(query).fetchall()
        except duckdb.IOException:
            context.log.info("No silver parquet files found to sample.")
            return MaterializeResult(metadata={"skipped": "No data"})

    if not sample_rows:
        return MaterializeResult(metadata={"skipped": "No data"})
        
    extractions_for_judge = [{"brand": r[0], "productName": r[1], "body": r[2], "llm_generation_prompt": r[3]} for r in sample_rows]
    num_samples = len(extractions_for_judge)
    context.log.info(f"Running blind judge evaluation on {num_samples} longitudinally-mixed samples...")
    
    judge_semaphore = asyncio.Semaphore(10)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def process_single(item, idx):
        res, cost, it, ot, raw_json = await run_blind_evaluation(
            item, 
            judge_semaphore, 
            AiModel.GEMINI_3_FLASH.value, 
            "high"
        )
        return res, cost, it, ot, raw_json, idx

    async def run_all():
        tasks = [process_single(item, i) for i, item in enumerate(extractions_for_judge)]
        results = await asyncio.gather(*tasks)
        
        all_res = []
        tot_cost = 0.0
        
        payload_rows = []
        for res, c, it, ot, raw_json, idx in results:
            if res is not None:
                all_res.append(res)
            tot_cost += c
            payload_rows.append({
                "chunk_id": f"discovery_eval_{context.run_id}_item_{idx}",
                "model_used": AiModel.GEMINI_3_FLASH.value,
                "input_tokens": it,
                "output_tokens": ot,
                "cost_usd": c,
                "raw_json_output": raw_json
            })
            
        return all_res, tot_cost, payload_rows

    validations_res, judge_cost, payload_rows = loop.run_until_complete(run_all())
    
    # Save the native telemetry payloads immediately to file so the dashboard can harvest it
    import pandas as pd
    from ..utils.paths import get_write_path
    
    if payload_rows:
        target_parquet = get_write_path(f"evaluations/entity_discovery_eval_payloads_{context.run_id[:8]}.parquet")
        payload_df = pd.DataFrame(payload_rows)
        with get_duckdb_connection() as con:
            con.register('df_view', payload_df)
            con.execute(f"COPY (SELECT * FROM df_view) TO '{str(target_parquet)}' (FORMAT PARQUET)")
    
    if not validations_res:
        raise Exception("Judge failed to return valid outputs or encountered rate limits.")
        
    num_valid = sum(1 for v in validations_res if v.is_valid_durable_good)
    precision = num_valid / len(validations_res)
    
    passed = precision >= 0.90 # 90% floor to sound the alarm
    
    # Store top 20 failures for manual debugging
    failure_logs = [
        f"[{extractions_for_judge[i]['brand']}] -> {v.reasoning}"
        for i, v in enumerate(validations_res) if not getattr(v, "is_valid_durable_good", True)
    ]
    
    if not passed:
        context.log.error(f"Fidelity fell below 90% floor! Precision: {precision}")

    return MaterializeResult(
        metadata={
            "passed": MetadataValue.bool(passed),
            "precision": MetadataValue.float(round(precision, 4)),
            "cost_usd": MetadataValue.float(round(judge_cost, 4)),
            "samples_evaluated": len(validations_res),
            "failures": MetadataValue.json(failure_logs[:20])
        }
    )

from .silver import silver_entity_extraction
from ..utils.judge import run_extraction_blind_canary_evaluation

class ExtractionEvalConfig(Config):
    sample_size: int = 385

@asset(group_name="evaluations", deps=[silver_entity_extraction])
def silver_entity_extraction_eval(context, config: ExtractionEvalConfig) -> MaterializeResult:
    """
    Randomly samples exploded Extraction rows across all daily partitions
    and evaluates them using an un-anchored Gemini Blind Judge evaluating Sentiment and Lifespan coherence.
    """
    from ..utils.paths import get_read_path
    
    source_glob = get_read_path("silver/entity_extraction_[0-9]*.parquet")
    
    with get_duckdb_connection() as con:
        try:
            query = f"""
                SELECT brand, productName, quote, sentiment, ownershipDurationMonths 
                FROM read_parquet('{source_glob}') 
                USING SAMPLE {config.sample_size} ROWS
            """
            sample_rows = con.execute(query).fetchall()
        except duckdb.IOException:
            context.log.info("No silver extraction parquet files found to sample.")
            return MaterializeResult(metadata={"skipped": "No data"})

    if not sample_rows:
        return MaterializeResult(metadata={"skipped": "No data"})
        
    extractions_for_judge = [{"brand": r[0], "productName": r[1], "quote": r[2], "sentiment": r[3], "ownershipDurationMonths": r[4]} for r in sample_rows]
    num_samples = len(extractions_for_judge)
    context.log.info(f"Running extraction blind judge canary on {num_samples} samples...")
    
    judge_semaphore = asyncio.Semaphore(10)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    chunk_size = 50 
    chunks = [extractions_for_judge[i:i + chunk_size] for i in range(0, num_samples, chunk_size)]
    
    async def process_chunk(chunk):
        res, cost, it, ot, raw_json = await run_extraction_blind_canary_evaluation(
            chunk, 
            judge_semaphore, 
            AiModel.GEMINI_3_FLASH.value, 
            "high"
        )
        return res, cost, it, ot, raw_json

    async def run_all():
        tasks = [process_chunk(c) for c in chunks]
        results = await asyncio.gather(*tasks)
        
        all_res = []
        tot_cost = 0.0
        
        payload_rows = []
        for idx, (r, c, it, ot, raw_json) in enumerate(results):
            all_res.extend(r)
            tot_cost += c
            payload_rows.append({
                "chunk_id": f"extraction_eval_{context.run_id}_chunk_{idx}",
                "model_used": AiModel.GEMINI_3_FLASH.value,
                "input_tokens": it,
                "output_tokens": ot,
                "cost_usd": c,
                "raw_json_output": raw_json
            })
            
        return all_res, tot_cost, payload_rows

    validations_res, judge_cost, payload_rows = loop.run_until_complete(run_all())
    
    import pandas as pd
    from ..utils.paths import get_write_path
    
    if payload_rows:
        target_parquet = get_write_path(f"evaluations/entity_extraction_eval_payloads_{context.run_id[:8]}.parquet")
        payload_df = pd.DataFrame(payload_rows)
        with get_duckdb_connection() as con:
            con.register('df_view', payload_df)
            con.execute(f"COPY (SELECT * FROM df_view) TO '{str(target_parquet)}' (FORMAT PARQUET)")
    
    if not validations_res:
        raise Exception("Judge failed to return valid outputs.")
        
    num_valid = sum(1 for v in validations_res if v.is_sentiment_logical and v.is_lifespan_logical)
    precision = num_valid / len(validations_res)
    passed = precision >= 0.90 
    
    failure_logs = [
        f"[{extractions_for_judge[i]['brand']}] -> {v.reasoning}"
        for i, v in enumerate(validations_res) if not (v.is_sentiment_logical and v.is_lifespan_logical)
    ]
    
    if not passed:
        context.log.error(f"Fidelity fell below 90% floor! Precision: {precision}")

    return MaterializeResult(
        metadata={
            "passed": MetadataValue.bool(passed),
            "precision": MetadataValue.float(round(precision, 4)),
            "cost_usd": MetadataValue.float(round(judge_cost, 4)),
            "samples_evaluated": len(validations_res),
            "failures": MetadataValue.json(failure_logs[:20])
        }
    )
