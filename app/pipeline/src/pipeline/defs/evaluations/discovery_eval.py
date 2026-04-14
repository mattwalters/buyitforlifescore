import duckdb
import asyncio
from dagster import asset, MaterializeResult, MetadataValue, Config
from pipeline.utils.pricing import AiModel
from pipeline.utils.db import get_duckdb_connection
from pipeline.defs.silver.discovery import silver_entity_discovery_payloads
class DiscoveryEvalConfig(Config):
    sample_size: int = 385  # 95% Confidence interval with a 5% margin of error

@asset(group_name="evaluations", deps=[silver_entity_discovery_payloads])
def silver_entity_discovery_eval(context, config: DiscoveryEvalConfig) -> MaterializeResult:
    """
    Randomly samples extracted rows across all materialized daily partitions
    and evaluates them using an un-anchored Gemini Blind Judge.
    """
    from pipeline.utils.paths import get_read_path
    
    source_glob = get_read_path("silver/entity_discovery_payloads_[0-9]*.parquet")
    
    with get_duckdb_connection() as con:
        try:
            # We use RESERVOIR sampling in DuckDB to pull an exact subset of the population longitudinally
            query = f"""
                SELECT submission_id, chunk_index, content_blocks_json, raw_json_output 
                FROM read_parquet('{source_glob}') 
                USING SAMPLE {config.sample_size} ROWS
            """
            sample_rows = con.execute(query).fetchall()
        except duckdb.IOException:
            context.log.info("No silver parquet files found to sample.")
            return MaterializeResult(metadata={"skipped": "No data"})

    if not sample_rows:
        return MaterializeResult(metadata={"skipped": "No data"})
        
    payloads_for_judge = [{"submission_id": r[0], "chunk_index": r[1], "content_blocks_json": r[2], "raw_json_output": r[3]} for r in sample_rows]
    num_samples = len(payloads_for_judge)
    context.log.info(f"Running blind judge holistic payload evaluation on {num_samples} longitudinally-mixed samples...")
    
    judge_semaphore = asyncio.Semaphore(10)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    from pipeline.utils.judge import run_discovery_payload_evaluation
    
    async def process_single(payload, idx):
        res, cost, it, ot, raw_json = await run_discovery_payload_evaluation(
            payload["content_blocks_json"],
            payload["raw_json_output"],
            judge_semaphore, 
            AiModel.GEMINI_3_FLASH.value, 
            "low"
        )
        return res, cost, it, ot, raw_json, idx

    async def run_all():
        tasks = [process_single(payload, i) for i, payload in enumerate(payloads_for_judge)]
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
    from pipeline.utils.paths import get_write_path
    
    if payload_rows:
        target_parquet = get_write_path(f"evaluations/entity_discovery_eval_payloads_{context.run_id[:8]}.parquet")
        payload_df = pd.DataFrame(payload_rows)
        with get_duckdb_connection() as con:
            con.register('df_view', payload_df)
            con.execute(f"COPY (SELECT * FROM df_view) TO '{str(target_parquet)}' (FORMAT PARQUET)")
    
    if not validations_res:
        raise Exception("Judge failed to return valid outputs or encountered rate limits.")
        
    num_passed_items = 0
    total_extracted_items = 0
    num_misses = 0
    num_hallucinations = 0
    failure_logs = []
    
    for v in validations_res:
        total_extracted_items += len(v.item_validations)
        num_misses += len(v.missed_entities)
        num_hallucinations += len(v.hallucinated_entities)
        for iv in v.item_validations:
            if iv.is_valid_durable_good:
                num_passed_items += 1
            else:
                failure_logs.append(f"[{iv.brand}] -> {iv.reasoning}")
                
    precision = num_passed_items / total_extracted_items if total_extracted_items > 0 else 0.0
    passed = precision >= 0.90 # 90% floor to sound the alarm
    
    if not passed:
        context.log.error(f"Fidelity fell below 90% floor! Precision: {precision}")

    return MaterializeResult(
        metadata={
            "passed": MetadataValue.bool(passed),
            "precision": MetadataValue.float(round(precision, 4)),
            "cost_usd": MetadataValue.float(round(judge_cost, 4)),
            "payloads_evaluated": len(validations_res),
            "total_items_extracted": total_extracted_items,
            "total_missed_entities": num_misses,
            "total_hallucinated_entities": num_hallucinations,
            "failures": MetadataValue.json(failure_logs[:20])
        }
    )

