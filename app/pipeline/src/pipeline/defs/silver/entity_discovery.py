import asyncio
import json
import os
from typing import Optional

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.llm import run_entity_discovery
from pipeline.utils.paths import get_read_path, get_write_path
from pipeline.utils.tree import build_comment_tree, chunk_branches

from .shared import PROMPT_VERSION, SILVER_CODE_VERSION, SilverLLMConfig, bifl_daily_partitions


async def _process_discovery_batch(
    threads: list[tuple], model_name: str, semaphore: asyncio.Semaphore, thinking: Optional[str] = None
) -> tuple[list[dict], float, int, int]:
    results = []
    total_cost = 0.0
    total_input = 0
    total_output = 0

    async def _extract_chunk(
        submission_id: str, title: str, body: str, chunk: list, chunk_index: int, created_utc: Optional[str] = None
    ):

        # Build Canonical ContentBlocks array
        content_blocks = []
        if chunk_index == 0:
            content_blocks.append(
                {
                    "block_id": 0,
                    "author_id": "OP",
                    "text": f"Title: {title}\nBody: {body or ''}",
                    "created_utc": created_utc,
                }
            )

        for idx, c_obj in enumerate(chunk):
            if c_obj and isinstance(c_obj, dict) and c_obj.get("body"):
                block_id = len(content_blocks)
                content_blocks.append(
                    {
                        "block_id": block_id,
                        "author_id": f"Commenter_{block_id}",
                        "text": c_obj["body"],
                        "created_utc": c_obj.get("created_utc"),
                    }
                )

        thread_text = json.dumps([{k: v for k, v in b.items() if k != "created_utc"} for b in content_blocks], indent=2)

        nonlocal total_cost, total_input, total_output

        try:
            result = await run_entity_discovery(
                content_blocks_json=thread_text,
                model_name=model_name,
                thinking=thinking,
                semaphore=semaphore,
            )

            total_cost += result.cost
            total_input += result.input_tokens
            total_output += result.output_tokens

            chunk_id = f"{submission_id}_chunk_{chunk_index}"

            results.append(
                {
                    "chunk_id": chunk_id,
                    "submission_id": submission_id,
                    "chunk_index": chunk_index,
                    "target_authored_at": created_utc,
                    "model_used": model_name,
                    "thinking_level": thinking,
                    "input_tokens": result.input_tokens,
                    "output_tokens": result.output_tokens,
                    "cost_usd": result.cost,
                    "raw_json_output": result.raw_json,
                    "parent_text": title,
                    "content_blocks_json": json.dumps(content_blocks),
                    "full_prompt_text": result.prompt_text,
                }
            )

        except Exception as e:
            # In Dagster, we fail gracefully per-item or log warnings, but for simplicity we skip failing texts.
            print(f"Skipping thread {submission_id} due to API Error: {e}")

    tasks = []
    for thread in threads:
        submission_id, title, body = thread[0], thread[1], thread[2]
        created_utc = thread[3] if len(thread) >= 4 else None
        comments_list = thread[4] if len(thread) >= 5 else []

        if not comments_list:
            comments_list = []

        # Build the comment tree and chunk by complete conversational branches
        tree = build_comment_tree(comments_list, submission_id)
        chunks = chunk_branches(tree, max_chunk_size=20)

        # If there are no chunks (e.g., submission with no comments), still process the submission itself
        if not chunks:
            chunks = [[]]

        for chunk_index, chunk in enumerate(chunks):
            tasks.append(_extract_chunk(submission_id, title, body, chunk, chunk_index, created_utc))

    await asyncio.gather(*tasks)
    return results, total_cost, total_input, total_output


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

    limit_clause = f"LIMIT {config.limit}" if config.limit else ""

    # 1. READ FROM BRONZE INTO PYTHON MEMORY
    query = f"""
        SELECT
            s.id as submission_id,
            s.title,
            s.selftext as body,
            s.created_utc,
            list({{
                'id': c.id, 'parent_id': c.parent_id,
                'body': c.body, 'created_utc': c.created_utc
            }} ORDER BY c.created_utc ASC) as comments
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
        _process_discovery_batch(rows, model_name, semaphore, config.thinking)
    )

    context.log.info(f"Extracted {len(extracted_items)} items. Total Cost: ${total_cost:.4f}")

    if len(extracted_items) == 0 and len(rows) > 0:
        raise Exception(
            f"CRITICAL: 0 out of {len(rows)} LLM inferences succeeded after retries. "
            f"Failing partition to prevent silent data loss."
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
                matched_texts = []
                target_timestamp = None
                for bid in item.get("source_block_ids", []):
                    matching_block = next((b for b in blocks if b.get("block_id") == bid), None)
                    if matching_block:
                        matched_texts.append(matching_block.get("text", ""))
                        if target_timestamp is None:
                            target_timestamp = matching_block.get("created_utc")

                item["target_text"] = "\n\n---\n\n".join(matched_texts)
                item["target_authored_at"] = target_timestamp
                item["parent_text"] = row.get("parent_text")

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
