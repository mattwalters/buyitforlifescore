import asyncio
import json
import os
from typing import Optional

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.llm import run_entity_extraction
from pipeline.utils.paths import get_read_path, get_write_path

from .shared import PROMPT_VERSION, SILVER_CODE_VERSION, SilverLLMConfig, bifl_daily_partitions


async def _process_extraction_batch(
    items: list[dict], model_name: str, semaphore: asyncio.Semaphore, thinking: Optional[str] = None
) -> tuple[list[dict], float, int, int]:
    results = []
    total_cost = 0.0
    total_input = 0
    total_output = 0

    async def _extract_item(item: dict):
        nonlocal total_cost, total_input, total_output
        brand = item.get("brand")

        try:
            result = await run_entity_extraction(
                brand=brand,
                product_name=item.get("productName", ""),
                target_authored_at=item.get("target_authored_at"),
                text=item.get("target_text"),
                parent_text=item.get("parent_text", ""),
                model_name=model_name,
                thinking=thinking,
                semaphore=semaphore,
            )

            total_cost += result.cost
            total_input += result.input_tokens
            total_output += result.output_tokens

            results.append(
                {
                    "discovery_chunk_id": item.get("chunk_id"),
                    "author_id": item.get("author_id"),
                    "brand": brand,
                    "productName": item.get("productName", ""),
                    "target_authored_at": item.get("target_authored_at"),
                    "model_used": model_name,
                    "thinking_level": thinking,
                    "input_tokens": result.input_tokens,
                    "output_tokens": result.output_tokens,
                    "cost_usd": result.cost,
                    "raw_json_output": result.raw_json,
                    "full_prompt_text": result.prompt_text,
                }
            )

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
    description=(
        "Phase 2a: Submits extracted entities to Gemini for nuanced qualitative "
        "attribute and sentiment extraction. Saves raw LLM completions identically to discovery layer."
    ),
)
def silver_entity_extraction_payloads(context: AssetExecutionContext, config: SilverLLMConfig) -> MaterializeResult:
    partition_date_str = context.partition_key
    discovery_parquet = get_read_path(f"silver/entity_discovery_{partition_date_str}.parquet")
    target_parquet = get_write_path(f"silver/entity_extraction_payloads_{partition_date_str}.parquet")

    limit_clause = f"LIMIT {config.limit}" if config.limit else ""

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
        context.log.info("No BIFL entities discovered for this partition.")
        return MaterializeResult(metadata={"items_processed": 0})

    items = df.to_dict("records")
    context.log.info(f"Processing {len(items)} entity extractions natively.")

    semaphore = asyncio.Semaphore(10)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    results, total_cost, total_input, total_output = loop.run_until_complete(
        _process_extraction_batch(items, config.model, semaphore, config.thinking)
    )

    if not results and len(items) > 0:
        raise Exception(
            f"CRITICAL: 0 out of {len(items)} LLM extractions succeeded after retries. "
            f"Failing partition to prevent silent data loss."
        )

    out_df = pd.DataFrame(results)
    out_df["prompt_version"] = PROMPT_VERSION

    with get_duckdb_connection() as con:
        con.register("df_view", out_df)
        con.execute(f"COPY (SELECT * FROM df_view) TO '{str(target_parquet)}' (FORMAT PARQUET)")

    return MaterializeResult(
        metadata={
            "partition": partition_date_str,
            "payloads_processed": len(results),
            "total_cost_usd": float(total_cost),
            "input_tokens": total_input,
            "output_tokens": total_output,
            "model_used": config.model,
        }
    )


@asset(
    group_name="silver",
    partitions_def=bifl_daily_partitions,
    code_version="1",
    deps=["silver_entity_extraction_payloads"],
    description="Phase 2b: Idempotently un-nests Phase 2 JSON payloads into flattened attribute dimensions.",
)
def silver_entity_extraction(context: AssetExecutionContext) -> MaterializeResult:
    partition_date_str = context.partition_key
    payloads_parquet = get_read_path(f"silver/entity_extraction_payloads_{partition_date_str}.parquet")
    target_parquet = get_write_path(f"silver/entity_extraction_{partition_date_str}.parquet")

    if not os.path.exists(payloads_parquet):
        context.log.info(f"Skipping unnest, no extraction payloads found for {partition_date_str}")
        return MaterializeResult(metadata={"status": "skipped"})

    query = f"SELECT * FROM '{str(payloads_parquet)}'"
    with get_duckdb_connection() as con:
        df = con.execute(query).df()

    extracted_items = []

    for _, row in df.iterrows():
        try:
            payload = json.loads(row["raw_json_output"])
            if not isinstance(payload, dict) or not payload:
                continue

            item = payload.copy()
            item["discovery_chunk_id"] = row["discovery_chunk_id"]
            item["author_id"] = row["author_id"]
            item["brand"] = row["brand"]
            item["productName"] = row["productName"]
            item["target_authored_at"] = row["target_authored_at"]
            item["llm_full_prompt_text"] = row.get("full_prompt_text")

            extracted_items.append(item)

        except json.JSONDecodeError as e:
            context.log.warning(
                f"Failed to unnest row due to invalid JSON: {row['raw_json_output'][:50]}... Error: {e}"
            )

    if extracted_items:
        out_df = pd.DataFrame(extracted_items)
        out_df["prompt_version"] = PROMPT_VERSION
        with get_duckdb_connection() as con:
            con.register("df_view", out_df)
            con.execute(f"COPY (SELECT * FROM df_view) TO '{str(target_parquet)}' (FORMAT PARQUET)")

    metadata = {"partition": partition_date_str, "payloads_processed": len(df), "items_extracted": len(extracted_items)}

    if extracted_items:
        metadata["data_preview"] = MetadataValue.md(out_df.head(10).to_markdown())

    return MaterializeResult(metadata=metadata)
