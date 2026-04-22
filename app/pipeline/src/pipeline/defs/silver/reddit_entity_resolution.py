import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    asset,
    define_asset_job,
    multiprocess_executor,
)

from pipeline.defs.silver.chains import chains_partitions_def
from pipeline.schemas.reddit_entity_resolution import EntityResolutionResult
from pipeline.utils.ai import get_client, invoke_entity_resolution
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path

# If more than this fraction of nodes fail, the asset raises instead of writing partial data.
FAILURE_THRESHOLD = 0.2


@asset(
    group_name="silver",
    partitions_def=chains_partitions_def,
    description=(
        "Classifies discovered entity verbatim quotes into structured"
        " brand / product_line / product_model fields with specificity levels."
    ),
    deps=[
        AssetDep("silver_reddit_entity_discovery_results"),
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
def silver_reddit_entity_resolution(context: AssetExecutionContext) -> MaterializeResult:
    partition_keys_dict = context.partition_key.keys_by_dimension
    date_key = partition_keys_dict["date"]
    subreddit_key = partition_keys_dict["subreddit"]
    sub_lower = subreddit_key.lower()

    context.log.info(f"[START] silver_reddit_entity_resolution — {subreddit_key} / {date_key}")

    source_discovery_results = get_read_path(
        f"silver/reddit_entity_discovery_results/subreddit={sub_lower}/date={date_key}/results.parquet"
    )
    source_submissions = get_read_path(f"bronze/reddit_{sub_lower}_submissions.parquet")
    source_comments = get_read_path(f"bronze/reddit_{sub_lower}_comments.parquet")
    target_parquet = get_write_path(
        f"silver/reddit_entity_resolution/subreddit={sub_lower}/date={date_key}/resolutions.parquet"
    )

    # Fail-safe Idempotency: Do not re-run expensive AI inference if we already have the output.
    if os.path.exists(target_parquet):
        context.log.info("Physical Parquet file already exists! Skipping expensive LLM re-computation.")
        return MaterializeResult(
            metadata={
                "skipped": MetadataValue.bool(True),
                "target_file": target_parquet,
                "reason": MetadataValue.md(
                    "Manual idempotency check passed. To re-run, manually delete the target file."
                ),
            }
        )

    empty_columns = list(EntityResolutionResult.model_fields.keys())

    with get_duckdb_connection() as con:
        # Check if upstream discovery results exist
        try:
            discovery_df = con.execute(f"SELECT * FROM read_parquet('{source_discovery_results}')").fetchdf()
        except Exception:
            context.log.info(
                f"No discovery results found for {subreddit_key} on {date_key}. Skipping entity resolution."
            )
            empty_df = pd.DataFrame(columns=empty_columns)  # noqa: F841
            con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_parquet}' (FORMAT PARQUET)")
            return MaterializeResult(
                metadata={
                    "target_file": target_parquet,
                    "data_preview": MetadataValue.md("No data generated (empty upstream)."),
                }
            )

        if discovery_df.empty:
            context.log.info(f"Empty discovery results for {subreddit_key} on {date_key}. Skipping.")
            empty_df = pd.DataFrame(columns=empty_columns)  # noqa: F841
            con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_parquet}' (FORMAT PARQUET)")
            return MaterializeResult(
                metadata={
                    "target_file": target_parquet,
                    "data_preview": MetadataValue.md("No data generated (empty upstream)."),
                }
            )

        # Group by node_id: collect all verbatim quotes and submission_id per node
        node_groups: dict[str, dict] = defaultdict(lambda: {"submission_id": "", "quotes": []})
        for _, row in discovery_df.iterrows():
            node_id = row["node_id"]
            node_groups[node_id]["submission_id"] = row["submission_id"]
            node_groups[node_id]["quotes"].append(row["verbatim_quote"])

        unique_node_ids = list(node_groups.keys())
        context.log.info(f"Grouped {len(discovery_df)} discovery results into {len(unique_node_ids)} unique nodes.")

        # Join back to original text for each node
        # Build a temporary table of target node IDs for the join
        node_ids_df = pd.DataFrame({"node_id": unique_node_ids})  # noqa: F841
        text_query = f"""
            WITH target_nodes AS (
                SELECT node_id FROM node_ids_df
            )
            SELECT n.node_id, COALESCE(s.title || ' ' || COALESCE(s.selftext, ''), c.body, '') AS full_text
            FROM target_nodes n
            LEFT JOIN read_parquet('{source_submissions}') s
                ON COALESCE(CAST(s.name AS VARCHAR), 't3_' || CAST(s.id AS VARCHAR)) = n.node_id
            LEFT JOIN read_parquet('{source_comments}') c
                ON COALESCE(CAST(c.name AS VARCHAR), 't1_' || CAST(c.id AS VARCHAR)) = n.node_id
        """
        text_df = con.execute(text_query).fetchdf()

    node_text_map = dict(zip(text_df["node_id"], text_df["full_text"]))

    client = get_client()
    results: list[EntityResolutionResult] = []
    total_nodes = len(unique_node_ids)
    total_cost = 0.0
    failed_count = 0

    context.log.info(f"Processing {total_nodes} nodes with ThreadPoolExecutor (max_workers=4)")

    def _resolve_node(node_id: str) -> EntityResolutionResult:
        """Invoke entity resolution for a single node. Thread-safe."""
        group = node_groups[node_id]
        node_text = node_text_map.get(node_id, "")
        return invoke_entity_resolution(
            client,
            node_id=node_id,
            submission_id=group["submission_id"],
            node_text=node_text,
            verbatim_quotes=group["quotes"],
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_resolve_node, nid): nid for nid in unique_node_ids}
        for i, future in enumerate(as_completed(futures), 1):
            nid = futures[future]
            try:
                result = future.result()
                results.append(result)
                total_cost += result.cost_usd
                context.log.info(
                    f"[{i}/{total_nodes}] ✅ {nid} — {result.resolved_count} entities, ${result.cost_usd:.6f}"
                )
            except Exception as e:
                failed_count += 1
                context.log.error(f"[{i}/{total_nodes}] ❌ {nid} — {e}")

    success_count = len(results)
    context.log.info(
        f"Entity resolution complete. {success_count} succeeded, {failed_count} failed. "
        f"Total cost: ${total_cost:.6f}"
    )

    # Threshold gate: fail the asset if too many nodes errored out
    if total_nodes > 0 and (failed_count / total_nodes) > FAILURE_THRESHOLD:
        raise RuntimeError(
            f"Entity resolution failure rate {failed_count}/{total_nodes} "
            f"({failed_count / total_nodes:.0%}) exceeds {FAILURE_THRESHOLD:.0%} threshold. "
            f"Aborting to prevent writing incomplete data."
        )

    if not results:
        out_df = pd.DataFrame(columns=empty_columns)  # noqa: F841
    else:
        out_records = [r.model_dump() for r in results]
        out_df = pd.DataFrame(out_records)  # noqa: F841

    with get_duckdb_connection() as con:
        con.execute(f"COPY (SELECT * FROM out_df) TO '{target_parquet}' (FORMAT PARQUET)")

        preview_df = con.execute(
            f"SELECT node_id, submission_id, resolved_count, cost_usd FROM '{target_parquet}' LIMIT 10"
        ).fetchdf()
        preview_md = preview_df.to_markdown()

    return MaterializeResult(
        metadata={
            "target_file": target_parquet,
            "cost_usd": MetadataValue.float(float(total_cost)),
            "success_nodes": MetadataValue.int(success_count),
            "failed_nodes": MetadataValue.int(failed_count),
            "data_preview": MetadataValue.md(preview_md),
        }
    )


silver_reddit_entity_resolution_job = define_asset_job(
    name="silver_reddit_entity_resolution_job",
    selection="silver_reddit_entity_resolution",
    executor_def=multiprocess_executor.configured({"max_concurrent": 8}),
)
