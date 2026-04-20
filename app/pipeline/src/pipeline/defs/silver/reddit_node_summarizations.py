from typing import Any

import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    BackfillPolicy,
    asset,
    define_asset_job,
    multiprocess_executor,
)
from pydantic import TypeAdapter
from tqdm import tqdm

from pipeline.defs.silver.chains import chains_partitions_def
from pipeline.schemas.reddit_node_summarizations import SilverRedditNodeSummarization
from pipeline.utils.ai import get_client, invoke_summarize_node
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path


@asset(
    group_name="silver",
    partitions_def=chains_partitions_def,
    description="Compresses overly large Reddit nodes (Submissions or Comments) into concise summaries using Gemini.",
    deps=[
        AssetDep(
            "silver_reddit_chain_bundles",
        ),
        AssetDep(
            "bronze_reddit_submissions",
            partition_mapping=MultiToSingleDimensionPartitionMapping(partition_dimension_name="subreddit"),
        ),
        AssetDep(
            "bronze_reddit_comments",
            partition_mapping=MultiToSingleDimensionPartitionMapping(partition_dimension_name="subreddit"),
        ),
    ],
)
def silver_reddit_node_summarizations(context: AssetExecutionContext) -> MaterializeResult:
    partition_keys_dict = context.partition_key.keys_by_dimension
    date_key = partition_keys_dict["date"]
    subreddit_key = partition_keys_dict["subreddit"]
    sub_lower = subreddit_key.lower()

    source_bundles = get_read_path(f"silver/chain_bundles/subreddit={sub_lower}/date={date_key}/bundles.parquet")
    source_submissions = get_read_path(f"bronze/reddit_{sub_lower}_submissions.parquet")
    source_comments = get_read_path(f"bronze/reddit_{sub_lower}_comments.parquet")
    target_parquet = get_write_path(
        f"silver/reddit_node_summarizations/subreddit={sub_lower}/date={date_key}/summarizations.parquet"
    )

    client = get_client()

    with get_duckdb_connection() as con:
        # Check if upstream file is completely empty before proceeding
        try:
            bundles_df = con.execute(f"SELECT * FROM '{source_bundles}' LIMIT 1").fetchdf()
            if bundles_df.empty:
                raise Exception("Empty dataframe")
        except Exception:
            context.log.info(f"No chain bundles found for {subreddit_key} on {date_key}. Skipping summarization.")
            empty_df = pd.DataFrame(columns=list(SilverRedditNodeSummarization.model_fields.keys()))
            con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_parquet}' (FORMAT PARQUET)")
            return MaterializeResult(
                metadata={
                    "target_file": target_parquet,
                    "data_preview": MetadataValue.md("No data generated (empty upstream)."),
                }
            )

        query = f"""
            WITH nodes_to_summarize AS (
                SELECT DISTINCT reddit_node_id 
                FROM read_parquet('{source_bundles}') 
                WHERE needs_summarization = true
            )
            SELECT n.reddit_node_id, COALESCE(s.title, '') || ' ' || COALESCE(s.selftext, '') AS full_text
            FROM nodes_to_summarize n
            JOIN read_parquet('{source_submissions}') s ON COALESCE(CAST(s.name AS VARCHAR), 't3_' || CAST(s.id AS VARCHAR)) = n.reddit_node_id
            UNION ALL
            SELECT n.reddit_node_id, COALESCE(c.body, '') AS full_text
            FROM nodes_to_summarize n
            JOIN read_parquet('{source_comments}') c ON COALESCE(CAST(c.name AS VARCHAR), 't1_' || CAST(c.id AS VARCHAR)) = n.reddit_node_id
        """

        nodes_df = con.execute(query).fetchdf()

    if nodes_df.empty:
        context.log.info("No nodes explicitly marked for summarization in this partition.")
        empty_df = pd.DataFrame(columns=list(SilverRedditNodeSummarization.model_fields.keys()))
        with get_duckdb_connection() as con:
            con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_parquet}' (FORMAT PARQUET)")
        return MaterializeResult(
            metadata={
                "target_file": target_parquet,
                "data_preview": MetadataValue.md("No summarizations needed."),
            }
        )

    records = nodes_df.to_dict("records")
    results: list[dict[str, Any]] = []

    context.log.info(f"Executing Gemini summarizations for {len(records)} nodes...")

    total_cost = 0.0

    for idx, row in enumerate(tqdm(records, desc="Summarizing nodes")):
        try:
            sum_res = invoke_summarize_node(client, row["full_text"])

            results.append({
                "reddit_node_id": row["reddit_node_id"],
                "summary": sum_res["summary"],
                "prompt_tokens": sum_res["prompt_tokens"],
                "completion_tokens": sum_res["completion_tokens"],
                "cost_usd": sum_res["cost_usd"],
            })
            total_cost += sum_res["cost_usd"]

        except Exception as e:
            context.log.error(f"Failed to summarize node {row['reddit_node_id']}: {e}")
            # If an error happens on one node due to API issue or safety blocking, insert empty but capture cost.
            # Here we might simply drop it, or insert a fallback. We'll fallback to original truncated.
            results.append({
                "reddit_node_id": row["reddit_node_id"],
                "summary": row["full_text"][:500] + "... [SUMMARIZATION_FAILED]",
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "cost_usd": 0.0,
            })

    context.log.info(f"Summarization complete. Total cost for this partition: ${total_cost:.6f}")

    # Pydantic Validation
    TypeAdapter(list[SilverRedditNodeSummarization]).validate_python(results)

    output_df = pd.DataFrame(results, columns=list(SilverRedditNodeSummarization.model_fields.keys())) # noqa: F841
    with get_duckdb_connection() as con:
        con.execute(f"COPY (SELECT * FROM output_df) TO '{target_parquet}' (FORMAT PARQUET)")
        preview_df = con.execute(f"SELECT reddit_node_id, summary, cost_usd FROM '{target_parquet}' LIMIT 10").fetchdf()
        preview_md = preview_df.to_markdown()

    return MaterializeResult(
        metadata={
            "target_file": target_parquet,
            "cost_usd": MetadataValue.float(total_cost),
            "data_preview": MetadataValue.md(preview_md),
        }
    )


silver_reddit_node_summarizations_job = define_asset_job(
    name="silver_reddit_node_summarizations_job",
    selection="silver_reddit_node_summarizations",
    executor_def=multiprocess_executor.configured({"max_concurrent": 8}),
)
