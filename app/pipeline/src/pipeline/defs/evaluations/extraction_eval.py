import duckdb
import asyncio
from dagster import asset, MaterializeResult, MetadataValue, Config
from pipeline.utils.pricing import AiModel
from pipeline.utils.db import get_duckdb_connection
from pipeline.defs.silver.extraction import silver_entity_extraction_payloads

from pipeline.utils.judge import run_extraction_blind_canary_evaluation

class ExtractionEvalConfig(Config):
    sample_size: int = 385

@asset(group_name="evaluations", deps=[silver_entity_extraction_payloads])
def silver_entity_extraction_eval(context, config: ExtractionEvalConfig) -> MaterializeResult:
    """
    Randomly samples raw Extraction payload JSONs across all daily partitions
    and evaluates them using an un-anchored Gemini Blind Judge evaluating Sentiment and Lifespan coherence.
    """
    from pipeline.utils.paths import get_read_path
    import json
    
    source_glob = get_read_path("silver/entity_extraction_payloads_[0-9]*.parquet")
    
    with get_duckdb_connection() as con:
        try:
            query = f"""
                SELECT raw_json_output 
                FROM read_parquet('{source_glob}') 
                USING SAMPLE {config.sample_size} ROWS
            """
            sample_rows = con.execute(query).fetchall()
        except duckdb.IOException:
            context.log.info("No silver extraction parquet files found to sample.")
            return MaterializeResult(metadata={"skipped": "No data"})

    if not sample_rows:
        return MaterializeResult(metadata={"skipped": "No data"})
        
    extractions_for_judge = []
    for r in sample_rows:
        try:
            items = json.loads(r[0])
            if isinstance(items, list):
                extractions_for_judge.extend(items)
            elif isinstance(items, dict) and items:
                extractions_for_judge.append(items)
        except:
            pass
            
    num_samples = len(extractions_for_judge)
    context.log.info(f"Running extraction blind judge canary on {num_samples} samples from payloads...")
    
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
            "low"
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
    from pipeline.utils.paths import get_write_path
    
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
