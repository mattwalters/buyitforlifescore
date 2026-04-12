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

from ..utils.pricing import calculate_gemini_cost, AiModel

# We define a Daily Partition from Jan 2012. 
bifl_daily_partitions = DailyPartitionsDefinition(start_date="2012-01-01")

class SilverExtractionConfig(Config):
    limit: Optional[int] = None
    model: str = "gemini-2.5-flash-lite"
    thinking: Optional[str] = None

# --- PYDANTIC SCHEMA MAPPING --- 
# This exactly matches your TypeScript THREAD_EXTRACTION_SCHEMA so Gemini returns identical JSON.
class MentionItem(BaseModel):
    sourceId: int = Field(description="The EXACT integer source index from the text block.")
    brand: str = Field(description="The stated brand name. Normalize to canonical proper spelling.")
    productName: str = Field(description="The specific marketed product line or model name. DO NOT extract generic categories. Empty string if BRAND_ONLY.")
    specificityLevel: Literal["EXACT_MODEL", "PRODUCT_LINE", "BRAND_ONLY"]
    acquiredPrice: Optional[float] = Field(None)
    ownershipDurationMonths: Optional[int] = Field(None)
    usageFrequency: Optional[Literal["DAILY", "WEEKLY", "MONTHLY", "SEASONAL", "RARELY"]] = Field(None)
    durability: Optional[Literal["POSITIVE", "NEUTRAL", "NEGATIVE"]] = Field(None)
    repairability: Optional[Literal["POSITIVE", "NEUTRAL", "NEGATIVE"]] = Field(None)
    maintenance: Optional[Literal["POSITIVE", "NEUTRAL", "NEGATIVE"]] = Field(None)
    warranty: Optional[Literal["POSITIVE", "NEUTRAL", "NEGATIVE"]] = Field(None)
    value: Optional[Literal["POSITIVE", "NEUTRAL", "NEGATIVE"]] = Field(None)
    sentiment: Literal["POSITIVE", "NEUTRAL", "NEGATIVE"]
    flawOrCaveat: Optional[str] = Field(None)

async def _process_thread_batch(threads: list[tuple], model_name: str, semaphore: asyncio.Semaphore, thinking: Optional[str] = None) -> tuple[list[dict], float, int, int]:
    import datetime
    client = genai.Client()
    results = []
    total_cost = 0.0
    total_input = 0
    total_output = 0
    
    async def _extract_chunk(submission_id: str, title: str, body: str, chunk: list, chunk_index: int, created_utc: Optional[str] = None):
        # Build the exact TS string format based on chunk
        if chunk_index == 0:
            thread_parts = [f"[SOURCE INDEX: 0] Title: {title} | Body: {body or ''}"]
            instruction = ""
        else:
            thread_parts = [f"[PREVIOUSLY READ CONTEXT - DO NOT EXTRACT PRODUCTS FROM THIS HEADER. ONLY EXTRACT EXPLICIT NEW PRODUCT MENTIONS FROM THE COMMENTS LISTED BELOW.]:\nTitle: {title} | Body: {body or ''}"]
            instruction = "- CRITICAL: You have already read the Title and Body in a previous chunk. Do NOT extract those products again. Only extract new durable products found in the COMMENTS below."
            
        if chunk:
            for idx, c_body in enumerate(chunk):
                 if c_body:
                     true_idx = (chunk_index * 10) + idx + 1
                     thread_parts.append(f"[SOURCE INDEX: {true_idx}] Body: {c_body}")
                     
        thread_text = "\n\n".join(thread_parts)

        temporal_anchor = ""
        if created_utc:
            try:
                dt = datetime.datetime.fromtimestamp(float(created_utc))
                temporal_anchor = f"\nTEMPORAL ANCHOR: This thread was published in {dt.strftime('%B %Y')}. Use this temporal anchor to do chronological math if users say things like 'I've owned this for 5 years' or 'Bought in 1999'."
            except Exception:
                pass

        prompt = f"""You are a product analyst studying "Buy It For Life" patterns on Reddit.{temporal_anchor}
Extract every notable durable product being discussed, recommended, or reviewed in the following Reddit thread.
Include both products from the original submission and the comments.

CRITICAL INSTRUCTIONS:
- For each extracted product, you MUST specify the exact integer 'sourceId' from the text block where it was mentioned. 
- The sourceId will be the integer index from [SOURCE INDEX: X] (e.g. 0, 1, 2).
- Only extract physical, durable products.
- If the brand name of the product is unknown, completely unstated, or generic, DO NOT extract the product at all. Completely omit it. Do not use placeholders like "Unknown".
- Do NOT extract generic product categories or nouns (e.g., "mixer", "backpack", "pan", "car", "boots", "sweater") as a productName. If the user only says "I love my KitchenAid mixer", the specificityLevel MUST be BRAND_ONLY and the productName MUST be an empty string "". You MUST ONLY classify something as PRODUCT_LINE or EXACT_MODEL if the user uses a Proper Noun, marketing name, or specific model identifier (e.g., "Artisan", "F-150", "Aeron", "D5").
{instruction}

Thread to analyze:
{thread_text}"""
        
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
            
        async with semaphore:
            try:
                response = await client.aio.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=gen_config,
                )
                
                # Aggregate costs
                if response.usage_metadata:
                    usage = response.usage_metadata
                    # We pass the raw python SDK object into your ported pricing utility
                    cost = calculate_gemini_cost(model_name, usage)
                    total_cost += cost
                    total_input += usage.prompt_token_count or 0
                    total_output += usage.candidates_token_count or 0
                
                import json
                if response.text:
                    items = json.loads(response.text)
                    for item in items:
                        item['submission_id'] = submission_id
                        results.append(item)
            except Exception as e:
                # In Dagster, we fail gracefully per-item or log warnings, but for simplicity we skip failing texts.
                print(f"Skipping thread {submission_id} due to API Error: {e}")

    tasks = []
    chunk_size = 10
    for thread in threads:
        created_utc = thread[4] if len(thread) >= 5 else None
        submission_id, title, body, comments_list = thread[0], thread[1], thread[2], thread[3]
        
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
    deps=["raw_reddit_buyitforlife_comments", "raw_reddit_buyitforlife_submissions"],
    description="Runs LLM inferences (Gemini 2.5 Flash Lite) on a specific daily timeslice of Bronze data."
)
def extracted_sentiment_silver(context: AssetExecutionContext, config: SilverExtractionConfig) -> MaterializeResult:
    partition_date_str = context.partition_key
    
    monorepo_root = Path(__file__).resolve().parents[5]
    bronze_comments_parquet = monorepo_root / "data" / "bronze" / "buyitforlife_comments.parquet"
    bronze_submissions_parquet = monorepo_root / "data" / "bronze" / "buyitforlife_submissions.parquet"
    
    silver_dir = monorepo_root / "data" / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)
    target_parquet = silver_dir / f"sentiment_silver_{partition_date_str}.parquet"
    
    limit_clause = f"LIMIT {config.limit}" if config.limit else ""
    
    # 1. READ FROM BRONZE INTO PYTHON MEMORY
    query = f"""
        SELECT 
            s.id as submission_id, 
            s.title, 
            s.selftext as body, 
            s.created_utc,
            list(c.body ORDER BY c.created_utc ASC) as comments
        FROM '{str(bronze_submissions_parquet)}' s
        LEFT JOIN '{str(bronze_comments_parquet)}' c ON c.link_id = 't3_' || s.id
        WHERE strftime(to_timestamp(CAST(s.created_utc AS BIGINT)), '%Y-%m-%d') = '{partition_date_str}'
        GROUP BY s.id, s.title, s.selftext
        {limit_clause}
    """
    
    with duckdb.connect(database=':memory:') as con:
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
        # Using DuckDB to save DataFrame directly to partitioned Parquet!
        with duckdb.connect(database=':memory:') as con:
            con.register('df_view', df)
            con.execute(f"COPY (SELECT * FROM df_view) TO '{str(target_parquet)}' (FORMAT PARQUET)")
    
    # 4. YIELD COST METADATA AND VIEWS!
    import collections
    sentiment_counts = collections.Counter([item.get('sentiment') for item in extracted_items]) if extracted_items else {}
    
    metadata = {
        "partition": partition_date_str,
        "threads_processed": len(rows),
        "items_extracted": len(extracted_items),
        "model_used": model_name,
        "cost_usd": float(round(total_cost, 6)),
        "input_tokens": total_in,
        "output_tokens": total_out,
        "sentiment_distribution": dict(sentiment_counts)
    }
    
    if extracted_items:
        metadata["data_preview"] = MetadataValue.md(df.head(10).to_markdown())

    return MaterializeResult(metadata=metadata)
