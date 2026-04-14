import os
import asyncio
import pandas as pd
from typing import Optional

from google import genai
from google.genai import types
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dagster import asset, MaterializeResult, AssetExecutionContext, MetadataValue

from pipeline.utils.pricing import calculate_gemini_cost
from pipeline.utils.paths import get_read_path, get_write_path
from pipeline.utils.db import get_duckdb_connection

from .shared import bifl_daily_partitions, SILVER_CODE_VERSION, PROMPT_VERSION, SilverLLMConfig
from pipeline.prompts.entity_extraction import EntityExtraction, get_extraction_prompt

async def _process_extraction_batch(items: list[dict], model_name: str, semaphore: asyncio.Semaphore, thinking: Optional[str] = None) -> tuple[list[dict], float, int, int]:
    client = genai.Client()
    results = []
    total_cost = 0.0
    total_input = 0
    total_output = 0
    
    async def _extract_item(item: dict):
        brand = item.get('brand')
        productName = item.get('productName', "")
        text = item.get('target_text')
        parent_text = item.get('parent_text', "")
        created_utc = item.get('target_authored_at')
        discovery_chunk_id = item.get('chunk_id')
        
        prompt = get_extraction_prompt(brand, productName, created_utc, text, parent_text)
        
        nonlocal total_cost, total_input, total_output
        gen_config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=EntityExtraction,
            temperature=0.1
        )
        if thinking:
            if thinking.lstrip('-').isdigit():
                gen_config.thinking_config = types.ThinkingConfig(thinking_budget=int(thinking))
            else:
                gen_config.thinking_config = types.ThinkingConfig(thinking_level=thinking)
                
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, min=4, max=10),
            retry=retry_if_exception_type(Exception),
            before_sleep=lambda rs: print(f"Retrying API call for extraction {brand} after error: {rs.outcome.exception()}")
        )
        async def api_call():
            async with semaphore:
                return await client.aio.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=gen_config
                )
                
        try:
            response = await api_call()
            cost = 0.0
            prompt_tokens = 0
            candidates_tokens = 0
            
            if response.usage_metadata:
                usage = response.usage_metadata
                cost = calculate_gemini_cost(model_name, usage)
                total_cost += cost
                prompt_tokens = usage.prompt_token_count or 0
                candidates_tokens = usage.candidates_token_count or 0
                total_input += prompt_tokens
                total_output += candidates_tokens
                
            raw_json = response.text if response.text else "{}"
            if raw_json.startswith("```json"):
                raw_json = raw_json[7:-3]
                
            results.append({
                "discovery_chunk_id": discovery_chunk_id,
                "author_id": item.get('author_id'),
                "brand": brand,
                "productName": productName,
                "target_authored_at": created_utc,
                "model_used": model_name,
                "thinking_level": thinking,
                "input_tokens": prompt_tokens,
                "output_tokens": candidates_tokens,
                "cost_usd": cost,
                "raw_json_output": raw_json,
                "full_prompt_text": prompt
            })

        except Exception as e:
            print(f"Skipping extraction for {brand} due to API Error: {e}")

    tasks = [_extract_item(item) for item in items]
    await asyncio.gather(*tasks)
    return results, total_cost, total_input, total_output


@asset(
    group_name="silver",
    partitions_def=bifl_daily_partitions,
    code_version=SILVER_CODE_VERSION,
    deps=["silver_entity_discovery"],
    description="Phase 2a: Submits extracted entities to Gemini for nuanced qualitative attribute and sentiment extraction. Saves raw LLM completions identically to discovery layer."
)
def silver_entity_extraction_payloads(context: AssetExecutionContext, config: SilverLLMConfig) -> MaterializeResult:
    partition_date_str = context.partition_key
    discovery_parquet = get_read_path(f"silver/entity_discovery_{partition_date_str}.parquet")
    target_parquet = get_write_path(f"silver/entity_extraction_payloads_{partition_date_str}.parquet")
    
    limit_clause = f"LIMIT {config.limit}" if config.limit else ""
    
    # Check if discovery output exists, otherwise skip
    import os
    if not os.path.exists(discovery_parquet):
        context.log.info(f"Skipping extraction, no Discovery outputs found for {partition_date_str}")
        return MaterializeResult(metadata={"status": "skipped", "reason": "No upstream partition data"})
        
    query = f"""
        SELECT 
            llm_chunk_id as chunk_id,
            author_id,
            brand,
            productName,
            target_text,
            target_authored_at,
            parent_text
        FROM '{str(discovery_parquet)}'
        {limit_clause}
    """
    
    with get_duckdb_connection() as con:
        df = con.execute(query).df()
        
    if df.empty:
        context.log.info(f"No BIFL entities discovered for this partition.")
        return MaterializeResult(metadata={"items_processed": 0})
        
    items = df.to_dict('records')
    context.log.info(f"Processing {len(items)} entity extractions natively.")
    
    # Restrict to 10 concurrent requests to respect rate limits
    semaphore = asyncio.Semaphore(10)
    
    # We must explicitly define the event loop runtime behavior for Dagster compatibility
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Run API Loop
    results, total_cost, total_input, total_output = loop.run_until_complete(
        _process_extraction_batch(items, config.model, semaphore, config.thinking)
    )
    
    if not results and len(items) > 0:
        raise Exception(f"CRITICAL: 0 out of {len(items)} LLM extractions succeeded after retries. Failing partition to prevent silent data loss.")
        
    out_df = pd.DataFrame(results)
    out_df['prompt_version'] = PROMPT_VERSION
    
    with get_duckdb_connection() as con:
        con.register('df_view', out_df)
        con.execute(f"COPY (SELECT * FROM df_view) TO '{str(target_parquet)}' (FORMAT PARQUET)")
        
    return MaterializeResult(metadata={
        "partition": partition_date_str,
        "payloads_processed": len(results),
        "total_cost_usd": float(total_cost),
        "input_tokens": total_input,
        "output_tokens": total_output,
        "model_used": config.model
    })

@asset(
    group_name="silver",
    partitions_def=bifl_daily_partitions,
    code_version="1",
    deps=["silver_entity_extraction_payloads"],
    description="Phase 2b: Idempotently un-nests Phase 2 JSON payloads into flattened attribute dimensions."
)
def silver_entity_extraction(context: AssetExecutionContext) -> MaterializeResult:
    partition_date_str = context.partition_key
    payloads_parquet = get_read_path(f"silver/entity_extraction_payloads_{partition_date_str}.parquet")
    target_parquet = get_write_path(f"silver/entity_extraction_{partition_date_str}.parquet")
    
    import os
    import json
    if not os.path.exists(payloads_parquet):
        context.log.info(f"Skipping unnest, no extraction payloads found for {partition_date_str}")
        return MaterializeResult(metadata={"status": "skipped"})
        
    query = f"SELECT * FROM '{str(payloads_parquet)}'"
    with get_duckdb_connection() as con:
        df = con.execute(query).df()
        
    extracted_items = []
    
    for _, row in df.iterrows():
        try:
            payload = json.loads(row['raw_json_output'])
            if not isinstance(payload, dict) or not payload:
                continue
                
            # Safely inherit complex JSON schema natively without manual mapping
            item = payload.copy()
            
            # Map canonical upstream identifiers
            item['discovery_chunk_id'] = row['discovery_chunk_id']
            item['author_id'] = row['author_id']
            item['brand'] = row['brand']
            item['productName'] = row['productName']
            item['target_authored_at'] = row['target_authored_at']
            item['llm_full_prompt_text'] = row.get('full_prompt_text')
            
            extracted_items.append(item)
            
        except json.JSONDecodeError as e:
            context.log.warning(f"Failed to unnest row due to invalid JSON: {row['raw_json_output'][:50]}... Error: {e}")
            
    if extracted_items:
        out_df = pd.DataFrame(extracted_items)
        out_df['prompt_version'] = PROMPT_VERSION
        with get_duckdb_connection() as con:
            con.register('df_view', out_df)
            con.execute(f"COPY (SELECT * FROM df_view) TO '{str(target_parquet)}' (FORMAT PARQUET)")
            
    metadata = {
        "partition": partition_date_str,
        "payloads_processed": len(df),
        "items_extracted": len(extracted_items)
    }
    
    if extracted_items:
        metadata["data_preview"] = MetadataValue.md(out_df.head(10).to_markdown())
        
    return MaterializeResult(metadata=metadata)
