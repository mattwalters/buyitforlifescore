import asyncio
import json
import os

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from pipeline.utils.db import get_duckdb_connection, load_bronze_threads_with_comments
from pipeline.utils.llm import process_thread_discovery
from pipeline.utils.paths import get_read_path, get_write_path
from pipeline.utils.tree import build_mention_context

from .shared import PROMPT_VERSION, SILVER_CODE_VERSION, SilverLLMConfig, bifl_daily_partitions


@asset(
    group_name="silver",
    partitions_def=bifl_daily_partitions,
    code_version=SILVER_CODE_VERSION,
    deps=["bronze_reddit_buyitforlife_comments", "bronze_reddit_buyitforlife_submissions"],
    description="Phase 1: Generates LLM JSON payload inferences identifying entities in Bronze data.",
)
def silver_entity_discovery_payloads(context: AssetExecutionContext, config: SilverLLMConfig) -> MaterializeResult:
    partition_date_str = context.partition_key

    bronze_comments_parquet = get_read_path("bronze/reddit_buyitforlife_comments.parquet")
    bronze_submissions_parquet = get_read_path("bronze/reddit_buyitforlife_submissions.parquet")

    target_parquet = get_write_path(f"silver/entity_discovery_payloads_{partition_date_str}.parquet")

    # 1. READ FROM BRONZE INTO PYTHON MEMORY
    rows = load_bronze_threads_with_comments(
        submissions_path=str(bronze_submissions_parquet),
        comments_path=str(bronze_comments_parquet),
        where_clause=f"strftime(to_timestamp(CAST(s.created_utc AS BIGINT)), '%Y-%m-%d') = '{partition_date_str}'",
        limit=config.limit,
    )

    if not rows:
        context.log.info(f"No threads found for {partition_date_str}. Skipping.")
        return MaterializeResult()

    context.log.info(f"Loaded {len(rows)} threads for extraction. Firing asyncio pool...")

    # 2. RUN PARALLEL INFERENCE via the shared thread runner
    model_name = config.model

    # Restrict to 10 concurrent requests to respect rate limits
    semaphore = asyncio.Semaphore(10)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Check for mock override or missing key
    if not os.environ.get("GEMINI_API_KEY") and not os.environ.get("AI_MOCK_URL"):
        raise Exception("GEMINI_API_KEY environment variable is missing for the Dagster worker.")

    async def _run_all():
        tasks = []
        for thread in rows:
            submission_id, title, body = thread[0], thread[1], thread[2]
            created_utc = thread[3] if len(thread) >= 4 else None
            comments_list = thread[4] if len(thread) >= 5 else []
            if not comments_list:
                comments_list = []

            tasks.append(
                process_thread_discovery(
                    submission_id=submission_id,
                    title=title,
                    body=body,
                    comments=comments_list,
                    model_name=model_name,
                    semaphore=semaphore,
                    thinking=config.thinking,
                    created_utc=created_utc,
                )
            )
        return await asyncio.gather(*tasks)

    thread_results = loop.run_until_complete(_run_all())

    # Aggregate all chunk payloads across threads
    extracted_items = []
    total_cost = 0.0
    total_in = 0
    total_out = 0

    for tr in thread_results:
        extracted_items.extend(tr.chunk_payloads)
        total_cost += tr.total_cost
        total_in += tr.total_input_tokens
        total_out += tr.total_output_tokens
        for err in tr.errors:
            context.log.warning(err)

    context.log.info(f"Extracted {len(extracted_items)} items. Total Cost: ${total_cost:.4f}")

    if len(extracted_items) == 0 and len(rows) > 0:
        context.log.warning(
            f"0 out of {len(rows)} LLM inferences produced payloads. "
            f"This may be expected for threads with no commercial mentions."
        )

    # 3. WRITE SILVER TO PARQUET
    if extracted_items:
        df = pd.DataFrame(extracted_items)
        df["prompt_version"] = PROMPT_VERSION

        # Using DuckDB to save DataFrame directly to partitioned Parquet!
        with get_duckdb_connection() as con:
            con.register("df_view", df)
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
        "output_tokens": total_out,
    }

    if extracted_items:
        metadata["data_preview"] = MetadataValue.md(df.head(10).to_markdown())

    return MaterializeResult(metadata=metadata)


@asset(
    group_name="silver",
    partitions_def=bifl_daily_partitions,
    code_version="1",
    deps=["silver_entity_discovery_payloads"],
    description="Phase 1b: Idempotently un-nests LLM discovery payloads into flattened distinct items.",
)
def silver_entity_discovery(context: AssetExecutionContext) -> MaterializeResult:
    partition_date_str = context.partition_key
    payloads_parquet = get_read_path(f"silver/entity_discovery_payloads_{partition_date_str}.parquet")
    target_parquet = get_write_path(f"silver/entity_discovery_{partition_date_str}.parquet")

    if not os.path.exists(payloads_parquet):
        context.log.info(f"No payloads found for {partition_date_str}. Skipping.")
        return MaterializeResult()

    query = f"SELECT * FROM '{str(payloads_parquet)}'"

    with get_duckdb_connection() as con:
        df = con.execute(query).df()

    extracted_items = []

    for _, row in df.iterrows():
        try:
            items = json.loads(row["raw_json_output"])
            blocks = json.loads(row["content_blocks_json"])

            for item in items:
                # Core LLM Data Mapping
                item["submission_id"] = row.get("submission_id")
                item["llm_chunk_id"] = row.get("chunk_id")
                item["llm_chunk_total_cost_usd"] = row.get("cost_usd")
                item["llm_item_prorated_cost_usd"] = row.get("cost_usd") / len(items) if len(items) > 0 else 0.0
                item["llm_model"] = row.get("model_used")
                item["llm_thinking"] = row.get("thinking_level")
                item["llm_chunk_input_tokens"] = row.get("input_tokens")
                item["llm_chunk_output_tokens"] = row.get("output_tokens")
                item["llm_chunk_yield"] = len(items)
                item["llm_full_prompt_text"] = row.get("full_prompt_text")

                # Contextual Data Mapping Idempotently
                source_block_ids = item.get("source_block_ids", [])
                target_text, parent_text = build_mention_context(
                    title=row.get("title", ""),
                    body=row.get("body"),
                    source_block_ids=source_block_ids,
                    content_blocks=blocks,
                )
                target_timestamp = next(
                    (b.get("created_utc") for bid in source_block_ids for b in blocks if b.get("block_id") == bid),
                    None,
                )
                item["target_text"] = target_text
                item["target_authored_at"] = target_timestamp
                item["parent_text"] = parent_text

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
