import os
import asyncio
import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional, List, Literal, Any
from pydantic import BaseModel, Field
from dagster import asset, MaterializeResult, AssetExecutionContext, DailyPartitionsDefinition, Config, MetadataValue

from google import genai
from google.genai import types
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..utils.pricing import calculate_gemini_cost, AiModel
from ..utils.paths import get_read_path, get_write_path, get_ledger_path
from ..utils.db import get_duckdb_connection

PROMPT_VERSION = "v1.0.0"
# UPDATE THIS STRING manually whenever you make schema or prompt changes that invalidate older data.
# Do NOT update this for simple code cleanups, formatting, or comments.
SILVER_CODE_VERSION = "v1"

# We define a Daily Partition from Jan 2012. 
bifl_daily_partitions = DailyPartitionsDefinition(start_date="2012-01-01")

class SilverExtractionConfig(Config):
    limit: Optional[int] = None
    model: str = "gemini-2.5-flash-lite"
    thinking: Optional[str] = None

# --- PYDANTIC SCHEMA MAPPING --- 
# This exactly matches your TypeScript THREAD_EXTRACTION_SCHEMA so Gemini returns identical JSON.
from pipeline.prompts.entity_discovery import MentionItem, get_discovery_prompt

async def _process_thread_batch(threads: list[tuple], model_name: str, semaphore: asyncio.Semaphore, thinking: Optional[str] = None) -> tuple[list[dict], float, int, int]:
    import datetime
    client = genai.Client()
    results = []
    total_cost = 0.0
    total_input = 0
    total_output = 0
    
    async def _extract_chunk(submission_id: str, title: str, body: str, chunk: list, chunk_index: int, created_utc: Optional[str] = None):
        import json
        
        # Build Canonical ContentBlocks array
        content_blocks = []
        if chunk_index == 0:
            content_blocks.append({
                "block_id": 0,
                "author_id": "OP",
                "text": f"Title: {title}\nBody: {body or ''}",
                "created_utc": created_utc
            })
            
        if chunk:
            for idx, c_obj in enumerate(chunk):
                if c_obj and isinstance(c_obj, dict) and c_obj.get('body'):
                    true_idx = (chunk_index * 10) + idx + 1
                    content_blocks.append({
                        "block_id": true_idx,
                        "author_id": f"Commenter_{true_idx}",
                        "text": c_obj['body'],
                        "created_utc": c_obj.get('created_utc')
                    })
                    
        thread_text = json.dumps([{k: v for k, v in b.items() if k != 'created_utc'} for b in content_blocks], indent=2)

        prompt = get_discovery_prompt(thread_text)
        
        nonlocal total_cost, total_input, total_output
        gen_config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=list[MentionItem],
            temperature=0.1
        )
        if thinking:
            if thinking.lstrip('-').isdigit():
                gen_config.thinking_config = types.ThinkingConfig(
                    thinking_budget=int(thinking)
                )
            else:
                gen_config.thinking_config = types.ThinkingConfig(
                    thinking_level=thinking
                )
            
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, min=4, max=10),
            retry=retry_if_exception_type(Exception),
            reraise=True
        )
        async def call_api():
            return await client.aio.models.generate_content(
                model=model_name,
                contents=prompt,
                config=gen_config,
            )

        async with semaphore:
            try:
                response = await call_api()
                
                cost = 0.0
                prompt_tokens = 0
                candidates_tokens = 0
                
                # Aggregate costs
                if response.usage_metadata:
                    usage = response.usage_metadata
                    # We pass the raw python SDK object into your ported pricing utility
                    cost = calculate_gemini_cost(model_name, usage)
                    total_cost += cost
                    
                    prompt_tokens = usage.prompt_token_count or 0
                    candidates_tokens = usage.candidates_token_count or 0
                    
                    total_input += prompt_tokens
                    total_output += candidates_tokens
                
                import json
                raw_json = response.text if response.text else "[]"
                # Sometimes gemini wraps in markdown blocks
                if raw_json.startswith("```json"):
                    raw_json = raw_json[7:-3]
                    
                chunk_id = f"{submission_id}_chunk_{chunk_index}"
                
                results.append({
                    "chunk_id": chunk_id,
                    "submission_id": submission_id,
                    "chunk_index": chunk_index,
                    "target_authored_at": created_utc,
                    "model_used": model_name,
                    "thinking_level": thinking,
                    "input_tokens": prompt_tokens,
                    "output_tokens": candidates_tokens,
                    "cost_usd": cost,
                    "raw_json_output": raw_json,
                    "llm_generation_prompt": prompt,
                    "parent_text": title,
                    "content_blocks_json": json.dumps(content_blocks)
                })

            except Exception as e:
                # In Dagster, we fail gracefully per-item or log warnings, but for simplicity we skip failing texts.
                print(f"Skipping thread {submission_id} due to API Error: {e}")

    tasks = []
    chunk_size = 10
    for thread in threads:
        submission_id, title, body = thread[0], thread[1], thread[2]
        created_utc = thread[3] if len(thread) >= 4 else None
        comments_list = thread[4] if len(thread) >= 5 else []
        
        if not comments_list:
            comments_list = []
            
        chunks = [comments_list[i:i + chunk_size] for i in range(0, max(1, len(comments_list)), chunk_size)]
        
        for chunk_index, chunk in enumerate(chunks):
            tasks.append(_extract_chunk(submission_id, title, body, chunk, chunk_index, created_utc))
            
    await asyncio.gather(*tasks)
    return results, total_cost, total_input, total_output

@asset(
    group_name="silver",
    partitions_def=bifl_daily_partitions,
    code_version=SILVER_CODE_VERSION,
    deps=["bronze_reddit_buyitforlife_comments", "bronze_reddit_buyitforlife_submissions"],
    description="Phase 1: Generates LLM JSON payload inferences identifying entities in Bronze data."
)
def silver_entity_discovery_payloads(context: AssetExecutionContext, config: SilverExtractionConfig) -> MaterializeResult:
    partition_date_str = context.partition_key
    
    bronze_comments_parquet = get_read_path("bronze/reddit_buyitforlife_comments.parquet")
    bronze_submissions_parquet = get_read_path("bronze/reddit_buyitforlife_submissions.parquet")
        
    target_parquet = get_write_path(f"silver/entity_discovery_payloads_{partition_date_str}.parquet")
    
    limit_clause = f"LIMIT {config.limit}" if config.limit else ""
    
    # 1. READ FROM BRONZE INTO PYTHON MEMORY
    query = f"""
        SELECT 
            s.id as submission_id, 
            s.title, 
            s.selftext as body, 
            s.created_utc,
            list({{'body': c.body, 'created_utc': c.created_utc}} ORDER BY c.created_utc ASC) as comments
        FROM '{str(bronze_submissions_parquet)}' s
        LEFT JOIN '{str(bronze_comments_parquet)}' c ON c.link_id = 't3_' || s.id
        WHERE strftime(to_timestamp(CAST(s.created_utc AS BIGINT)), '%Y-%m-%d') = '{partition_date_str}'
        GROUP BY s.id, s.title, s.selftext, s.created_utc
        {limit_clause}
    """
    
    with get_duckdb_connection() as con:
            
        # returns [(id, title, body, created_utc, [c1, c2, c3]), ...]
        rows = con.execute(query).fetchall()

    if not rows:
        context.log.info(f"No threads found for {partition_date_str}. Skipping.")
        return MaterializeResult()

    context.log.info(f"Loaded {len(rows)} threads for extraction. Firing asyncio pool...")

    # 2. RUN PARALLEL INFERENCE
    model_name = config.model
    
    # Restrict to 10 concurrent requests to respect rate limits
    semaphore = asyncio.Semaphore(10)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Check for mock override or missing key
    if not os.environ.get("GEMINI_API_KEY") and not os.environ.get("AI_MOCK_URL"):
        raise Exception("GEMINI_API_KEY environment variable is missing for the Dagster worker.")

    extracted_items, total_cost, total_in, total_out = loop.run_until_complete(
        _process_thread_batch(rows, model_name, semaphore, config.thinking)
    )
    
    context.log.info(f"Extracted {len(extracted_items)} items. Total Cost: ${total_cost:.4f}")
    
    # 3. WRITE SILVER TO PARQUET
    if extracted_items:
        df = pd.DataFrame(extracted_items)
        df['prompt_version'] = PROMPT_VERSION
        
        # Using DuckDB to save DataFrame directly to partitioned Parquet!
        with get_duckdb_connection() as con:
            con.register('df_view', df)
            con.execute(f"COPY (SELECT * FROM df_view) TO '{str(target_parquet)}' (FORMAT PARQUET)")
            
    # Remove cost_ledger dual-write logic completely; telemetry is strictly saved in the row JSON payloads now!
        
    # 4. YIELD COST METADATA AND VIEWS!
    metadata = {
        "partition": partition_date_str,
        "threads_processed": len(rows),
        "payloads_generated": len(extracted_items),
        "model_used": model_name,
        "cost_usd": MetadataValue.float(float(round(total_cost, 6))),
        "input_tokens": total_in,
        "output_tokens": total_out
    }
    
    if extracted_items:
        metadata["data_preview"] = MetadataValue.md(df.head(10).to_markdown())

    return MaterializeResult(metadata=metadata)

@asset(
    group_name="silver",
    partitions_def=bifl_daily_partitions,
    code_version="1",
    deps=["silver_entity_discovery_payloads"],
    description="Phase 1b: Idempotently un-nests LLM discovery payloads into flattened distinct items."
)
def silver_entity_discovery(context: AssetExecutionContext) -> MaterializeResult:
    partition_date_str = context.partition_key
    payloads_parquet = get_read_path(f"silver/entity_discovery_payloads_{partition_date_str}.parquet")
    target_parquet = get_write_path(f"silver/entity_discovery_{partition_date_str}.parquet")
    
    import os
    if not os.path.exists(payloads_parquet):
        context.log.info(f"No payloads found for {partition_date_str}. Skipping.")
        return MaterializeResult()
        
    query = f"SELECT * FROM '{str(payloads_parquet)}'"
    
    with get_duckdb_connection() as con:
        df = con.execute(query).df()
        
    extracted_items = []
    import json
    
    for _, row in df.iterrows():
        try:
            items = json.loads(row['raw_json_output'])
            blocks = json.loads(row['content_blocks_json'])
            
            for item in items:
                # Core LLM Data Mapping
                item['submission_id'] = row.get('submission_id')
                item['llm_chunk_id'] = row.get('chunk_id')
                item['llm_chunk_total_cost_usd'] = row.get('cost_usd')
                item['llm_item_prorated_cost_usd'] = row.get('cost_usd') / len(items) if len(items) > 0 else 0.0
                item['llm_model'] = row.get('model_used')
                item['llm_thinking'] = row.get('thinking_level')
                item['llm_chunk_input_tokens'] = row.get('input_tokens')
                item['llm_chunk_output_tokens'] = row.get('output_tokens')
                item['llm_chunk_yield'] = len(items)
                
                # Contextual Data Mapping Idempotently
                matched_texts = []
                target_timestamp = None
                for bid in item.get('source_block_ids', []):
                    matching_block = next((b for b in blocks if b.get('block_id') == bid), None)
                    if matching_block:
                        matched_texts.append(matching_block.get('text', ''))
                        if target_timestamp is None:
                            target_timestamp = matching_block.get('created_utc')
                            
                item['target_text'] = "\n\n---\n\n".join(matched_texts)
                item['target_authored_at'] = target_timestamp
                item['parent_text'] = row.get('parent_text')
                item['llm_generation_prompt'] = row.get('llm_generation_prompt')
                
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
                "raw_json_output": raw_json
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
def silver_entity_extraction_payloads(context: AssetExecutionContext, config: SilverExtractionConfig) -> MaterializeResult:
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
    
    semaphore = asyncio.Semaphore(config.concurrency_limit)
    
    # We must explicitly define the event loop runtime behavior for Dagster compatibility
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Run API Loop
    results, total_cost, total_input, total_output = loop.run_until_complete(
        _process_extraction_batch(items, config.model_name, semaphore, config.thinking)
    )
    
    if not results:
        context.log.info("No successful extractions processed for {partition_date_str}.")
        return MaterializeResult(metadata={"items_processed": 0})
        
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
        "model_used": config.model_name
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
