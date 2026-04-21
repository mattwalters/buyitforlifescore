import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    BackfillPolicy,
    asset,
    define_asset_job,
    multiprocess_executor,
)
from pydantic import TypeAdapter

from pipeline.defs.silver.chains import chains_partitions_def
from pipeline.schemas.reddit_entity_discovery import DiscoveryResult
from pipeline.schemas.reddit_llm_payloads import SilverRedditLlmPayload
from pipeline.utils.ai import get_client, invoke_entity_discovery
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path


@asset(
    group_name="silver",
    partitions_def=chains_partitions_def,
    description="Scans bundled LLM payloads for commercial products and brand entities using Gemini.",
    deps=[
        AssetDep(
            "silver_reddit_llm_payloads",
        ),
    ],
)
def silver_reddit_entity_discovery(context: AssetExecutionContext) -> MaterializeResult:
    partition_keys_dict = context.partition_key.keys_by_dimension
    date_key = partition_keys_dict["date"]
    subreddit_key = partition_keys_dict["subreddit"]
    sub_lower = subreddit_key.lower()

    context.log.info(f"[START] silver_reddit_entity_discovery — {subreddit_key} / {date_key}")

    source_payloads = get_read_path(f"silver/reddit_llm_payloads/subreddit={sub_lower}/date={date_key}/payloads.parquet")
    target_parquet = get_write_path(
        f"silver/reddit_entity_discovery/subreddit={sub_lower}/date={date_key}/entities.parquet"
    )

    # Fail-safe Idempotency: Do not re-run expensive AI inference if we already have the output.
    if os.path.exists(target_parquet):
        context.log.info("Physical Parquet file already exists! Skipping expensive LLM re-computation.")
        return MaterializeResult(
            metadata={
                "skipped": MetadataValue.bool(True),
                "target_file": target_parquet,
                "reason": MetadataValue.md("Manual idempotency check passed. To re-run, manually delete the target file."),
            }
        )

    with get_duckdb_connection() as con:
        try:
            df = con.execute(f"SELECT * FROM '{source_payloads}'").fetchdf()
        except Exception:
            context.log.info(f"No LLM payloads found for {subreddit_key} on {date_key}. Skipping.")
            empty_df = pd.DataFrame(columns=list(DiscoveryResult.model_fields.keys()))
            con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_parquet}' (FORMAT PARQUET)")
            return MaterializeResult(
                metadata={
                    "target_file": target_parquet,
                    "data_preview": MetadataValue.md("No data generated (empty upstream)."),
                }
            )

    records = df.to_dict("records")
    payloads = TypeAdapter(list[SilverRedditLlmPayload]).validate_python(records)

    client = get_client()

    results: list[DiscoveryResult] = []

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(invoke_entity_discovery, client, payload): payload for payload in payloads}
        for future in as_completed(futures):
            try:
                res = future.result()
                results.append(res)
            except Exception as e:
                context.log.error(f"Error processing payload: {e}")

    # Convert results back to dicts for duckdb
    out_records = [r.model_dump() for r in results]
    if not out_records:
        out_df = pd.DataFrame(columns=list(DiscoveryResult.model_fields.keys()))
    else:
        out_df = pd.DataFrame(out_records)

    total_cost = sum(r.cost_usd for r in results)

    with get_duckdb_connection() as con:
        con.execute(f"COPY (SELECT * FROM out_df) TO '{target_parquet}' (FORMAT PARQUET)")

        preview_df = con.execute(
            f"SELECT bundle_id, submission_id, cost_usd FROM '{target_parquet}' LIMIT 10"
        ).fetchdf()
        preview_md = preview_df.to_markdown()

    return MaterializeResult(
        metadata={
            "target_file": target_parquet,
            "cost_usd": MetadataValue.float(float(total_cost)),
            "data_preview": MetadataValue.md(preview_md),
        }
    )


silver_reddit_entity_discovery_job = define_asset_job(
    name="silver_reddit_entity_discovery_job",
    selection="silver_reddit_entity_discovery",
    executor_def=multiprocess_executor.configured({"max_concurrent": 8}),
)
