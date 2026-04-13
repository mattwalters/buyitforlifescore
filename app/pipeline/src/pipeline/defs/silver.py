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
from ..utils.paths import get_data_dir, get_ledger_path

PROMPT_VERSION = "v1.0.0"

# We define a Daily Partition from Jan 2012. 
bifl_daily_partitions = DailyPartitionsDefinition(start_date="2012-01-01")

class SilverExtractionConfig(Config):
    limit: Optional[int] = None
    model: str = "gemini-2.5-flash-lite"
    thinking: Optional[str] = None

# --- PYDANTIC SCHEMA MAPPING --- 
# This exactly matches your TypeScript THREAD_EXTRACTION_SCHEMA so Gemini returns identical JSON.
class MentionItem(BaseModel):
    author_id: str = Field(description="The unique author identifier from the ContentBlock.")
    brand: str = Field(description="The stated brand name. Normalize to canonical proper spelling.")
    productName: str = Field(description="The specific marketed product line or model name. Leave as empty string if no specific model is mentioned.")
    source_block_ids: list[int] = Field(description="The list of block_ids where this author explicitly mentioned this product.")

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
                "text": f"Title: {title}\nBody: {body or ''}"
            })
            
        if chunk:
            for idx, c_body in enumerate(chunk):
                if c_body:
                    true_idx = (chunk_index * 10) + idx + 1
                    content_blocks.append({
                        "block_id": true_idx,
                        "author_id": f"Commenter_{true_idx}",
                        "text": c_body
                    })
                    
        thread_text = json.dumps(content_blocks, indent=2)

        prompt = f"""You are an Entity Discovery agent studying text blocks.
Your task is to identify every durable product mentioned.

CRITICAL INSTRUCTIONS:
- Aggregate your extractions by Author. Output exactly ONE extraction per unique 'author_id' and product combination. List all 'block_id's where they discussed it.
- Your goal is to identify explicit OPINIONS or REVIEWS of brands and products. 
- Do NOT extract a product if the user is simply stating that they bought it, are considering buying it, or are asking a question about it. There MUST be a qualitative opinion, endorsement, or explicit statement of experience attached.
- You can extract general brand mentions (e.g., "Georgia has dropped in quality") if an opinion is attached. Do not limit yourself strictly to "physical" models if the brand quality itself is being reviewed.
- If a commenter refers to a specific model name but omits the brand (e.g., "The SL-1200 is a tank"), you MUST use the preceding conversation blocks to infer the correct brand name ("Technics").
- CRITICAL BOUNDARY: You MUST be able to tie an opinion to a specific BRAND. If a user states an experience about a generic component (e.g. "side zippers fail") or a generic product (e.g. "I love my boots") but the BRAND is unknown and cannot be inferred from context, you MUST NOT extract it.
- Validation Gate 1 (Metaphors): Check if the statement is a rhetorical analogy (e.g., "asking for a Cadillac at a Chevy price"). If it is a metaphor, ABORT the extraction.
- Validation Gate 2 (Retailers): Check if the brand is actually a generic retailer (e.g., Costco, Home Depot, Amazon). Retailers are not product brands unless explicitly an in-house brand (e.g. Kirkland). If it is just a retailer, ABORT the extraction.
- Validation Gate 3 (Raw Materials): Check if the brand is actually a raw material or generic noun (e.g., teak, wooden, plastic, memory foam, goretex). If it is a material, ABORT the extraction. A brand must represent a named manufacturer.
- Validation Gate 4 (Unknown Identity): Check if the identity of the brand is vague or unnamed (e.g. "these showerheads"). If you cannot confidently identify the exact capitalized proper noun of the brand, YOU MUST ABORT the extraction. The brand field must always contain a specific, capitalized proper noun.
- Do NOT extract generic product nouns (e.g., "mixer", "backpack", "pan").

Thread to analyze (JSON ContentBlocks):
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
    description="Phase 1: Runs LLM inferences to discover entities in Bronze data."
)
def silver_entity_discovery(context: AssetExecutionContext, config: SilverExtractionConfig) -> MaterializeResult:
    partition_date_str = context.partition_key
    
    data_dir = get_data_dir()
    bronze_comments_parquet = f"{data_dir}/bronze/buyitforlife_comments.parquet"
    bronze_submissions_parquet = f"{data_dir}/bronze/buyitforlife_submissions.parquet"
    
    silver_dir_str = f"{data_dir}/silver"
    if not silver_dir_str.startswith("s3://"):
        Path(silver_dir_str).mkdir(parents=True, exist_ok=True)
        
    target_parquet = f"{silver_dir_str}/entity_discovery_{partition_date_str}.parquet"
    
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
        GROUP BY s.id, s.title, s.selftext, s.created_utc
        {limit_clause}
    """
    
    with duckdb.connect(database=':memory:') as con:
        if data_dir.startswith("s3://"):
            con.execute("INSTALL httpfs; LOAD httpfs;")
            
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
        with duckdb.connect(database=':memory:') as con:
            if data_dir.startswith("s3://"):
                con.execute("INSTALL httpfs; LOAD httpfs;")
                
            con.register('df_view', df)
            con.execute(f"COPY (SELECT * FROM df_view) TO '{str(target_parquet)}' (FORMAT PARQUET)")
    
    # 3.5. COST LEDGER PERSISTENCE
    ledger_path = get_ledger_path()
    with duckdb.connect(database=ledger_path) as ledger_con:
        ledger_con.execute("""
            CREATE TABLE IF NOT EXISTS cost_ledger (
                run_timestamp TIMESTAMP,
                partition_key VARCHAR,
                asset_name VARCHAR,
                model_used VARCHAR,
                input_tokens BIGINT,
                output_tokens BIGINT,
                cost_usd DOUBLE
            )
        """)
        ledger_con.execute("""
            INSERT INTO cost_ledger VALUES (
                current_timestamp, ?, ?, ?, ?, ?, ?
            )
        """, [partition_date_str, 'silver_entity_discovery', model_name, total_in, total_out, float(total_cost)])
        
    # 4. YIELD COST METADATA AND VIEWS!
    metadata = {
        "partition": partition_date_str,
        "threads_processed": len(rows),
        "items_extracted": len(extracted_items),
        "model_used": model_name,
        "cost_usd": float(round(total_cost, 6)),
        "input_tokens": total_in,
        "output_tokens": total_out
    }
    
    if extracted_items:
        metadata["data_preview"] = MetadataValue.md(df.head(10).to_markdown())

    return MaterializeResult(metadata=metadata)

@asset(
    group_name="silver",
    partitions_def=bifl_daily_partitions,
    deps=["silver_entity_discovery"],
    description="Phase 2: Reads the mapped entities and uses Gemini 3.1 Pro to extract nuanced sentiment and durability metrics."
)
def silver_nuance_extraction(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Phase 2 Nuance Extraction is a stub. It will read entity_discovery parquet and fan out.")
    return MaterializeResult(metadata={"status": "stub"})
