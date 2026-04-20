from collections import defaultdict
from typing import Any, Optional

import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    asset,
    define_asset_job,
    multiprocess_executor,
)
from pydantic import TypeAdapter

from pipeline.defs.silver.chains import chains_partitions_def
from pipeline.schemas.chain_bundles import SilverChainBundle
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path


class SilverChainBundlesConfig(Config):
    max_bundle_budget: int = 50000
    max_context_length: int = 2000
    summarized_context_length_estimate: int = 500
    validation_sample_size: Optional[int] = None


def build_chain_bundles(
    chains: list[dict[str, Any]], lengths_map: dict[str, int], config: SilverChainBundlesConfig
) -> list[dict[str, Any]]:
    # We must group chains structurally by Chain ID first
    chains_grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in chains:
        chains_grouped[row["submission_id"]][row["chain_id"]].append(row)

    all_bundles = []

    for sub_id, sub_chains_dict in chains_grouped.items():
        # Sort chains naturally to ensure deterministic processing
        sorted_chain_ids = sorted(sub_chains_dict.keys())

        current_bundle_nodes = []
        current_bundle_node_ids = set()
        current_bundle_budget = 0
        bundle_index = 0

        # Maintain a list of nodes that have already been considered "ANALYSIS" in ANY previous bundle
        # so they properly flip to "CONTEXT" in subsequent bundles for this submission
        seen_nodes_global = set()

        chain_idx = 0
        while chain_idx < len(sorted_chain_ids):
            chain_id = sorted_chain_ids[chain_idx]
            chain_rows = sorted(sub_chains_dict[chain_id], key=lambda x: x["sequence_order"])

            chain_marginal_cost = 0
            nodes_to_add = []

            for chain_node in chain_rows:
                node_id = chain_node["reddit_node_id"]
                if node_id in current_bundle_node_ids:
                    continue  # Already paid for in this bundle

                raw_length = lengths_map.get(node_id, 0)
                needs_summarization = False

                # Use our dynamic state to know if it's context or analysis
                is_analysis = node_id not in seen_nodes_global

                if is_analysis:
                    marginal_cost = raw_length
                    # We do NOT mark it seen here, only after it's officially added to the bundle
                else:
                    if raw_length > config.max_context_length:
                        marginal_cost = config.summarized_context_length_estimate
                        needs_summarization = True
                    else:
                        marginal_cost = raw_length

                chain_marginal_cost += marginal_cost
                nodes_to_add.append(
                    {
                        "bundle_id": f"{sub_id}_b{bundle_index}",
                        "submission_id": sub_id,
                        "chain_id": chain_id,
                        "reddit_node_id": node_id,
                        "sequence_order": chain_node["sequence_order"],
                        # It is canonical inside THIS bundle's context if it's up for analysis
                        "is_canonical": is_analysis,
                        "needs_summarization": needs_summarization,
                    }
                )

            # Check if adding this chain exceeds the budget
            if current_bundle_budget + chain_marginal_cost > config.max_bundle_budget and len(current_bundle_nodes) > 0:
                # The bundle is full! Yield it and start a new one
                all_bundles.extend(current_bundle_nodes)
                bundle_index += 1

                # Mark all newly analyzed nodes in that bundle as officially "seen" globally
                for n in current_bundle_nodes:
                    if n["is_canonical"]:
                        seen_nodes_global.add(n["reddit_node_id"])

                # Reset bundle state
                current_bundle_nodes = []
                current_bundle_node_ids = set()
                current_bundle_budget = 0

                # Note: We do NOT increment chain_idx so this chain gets re-evaluated in the empty bundle
                continue

            # Chain fits in the bundle (or bundle was empty so we MUST add it)
            current_bundle_nodes.extend(nodes_to_add)
            current_bundle_node_ids.update([n["reddit_node_id"] for n in nodes_to_add])
            current_bundle_budget += chain_marginal_cost
            chain_idx += 1

        # Yield any remaining nodes in the final bundle
        if current_bundle_nodes:
            all_bundles.extend(current_bundle_nodes)
            for n in current_bundle_nodes:
                if n["is_canonical"]:
                    seen_nodes_global.add(n["reddit_node_id"])

    return all_bundles


@asset(
    group_name="silver",
    partitions_def=chains_partitions_def,
    description="Bundle linear Reddit chains into LLM-sized chunks based on character length budgets.",
    deps=[
        AssetDep(
            "silver_reddit_chains",
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
def silver_reddit_chain_bundles(
    context: AssetExecutionContext, config: SilverChainBundlesConfig
) -> MaterializeResult:
    partition_keys_dict = context.partition_key.keys_by_dimension
    date_key = partition_keys_dict["date"]
    subreddit_key = partition_keys_dict["subreddit"]
    sub_lower = subreddit_key.lower()

    context.log.info(f"[START] silver_reddit_chain_bundles — {subreddit_key} / {date_key}")

    source_chains = get_read_path(f"silver/chains/subreddit={sub_lower}/date={date_key}/chains.parquet")
    source_submissions = get_read_path(f"bronze/reddit_{sub_lower}_submissions.parquet")
    source_comments = get_read_path(f"bronze/reddit_{sub_lower}_comments.parquet")
    target_parquet = get_write_path(f"silver/chain_bundles/subreddit={sub_lower}/date={date_key}/bundles.parquet")

    # Read chains
    with get_duckdb_connection() as con:
        chains_df = con.execute(
            f"SELECT * FROM read_parquet('{source_chains}') ORDER BY submission_id, chain_id, sequence_order"
        ).fetchdf()

        if chains_df.empty:
            context.log.info(
                f"No chains found for {subreddit_key} on {date_key}. Skipping bundle generation."
            )
            # Touch an empty parquet to satisfy downstream dependencies, ensuring correct schema
            empty_df = pd.DataFrame(columns=list(SilverChainBundle.model_fields.keys()))
            con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_parquet}' (FORMAT PARQUET)")
            return MaterializeResult(
                metadata={
                    "target_file": target_parquet,
                    "data_preview": MetadataValue.md("No data generated (empty upstream)."),
                }
            )

        lengths_query = f"""
            WITH target_nodes AS (
                SELECT DISTINCT reddit_node_id FROM read_parquet('{source_chains}')
            )
            SELECT n.reddit_node_id, CAST(length(COALESCE(s.selftext, '')) AS BIGINT) AS text_length
            FROM read_parquet('{source_submissions}') s
            JOIN target_nodes n ON COALESCE(CAST(s.name AS VARCHAR), 't3_' || CAST(s.id AS VARCHAR)) = n.reddit_node_id
            UNION ALL
            SELECT n.reddit_node_id, CAST(length(COALESCE(c.body, '')) AS BIGINT) AS text_length
            FROM read_parquet('{source_comments}') c
            JOIN target_nodes n ON COALESCE(CAST(c.name AS VARCHAR), 't1_' || CAST(c.id AS VARCHAR)) = n.reddit_node_id
        """
        raw_lengths_df = con.execute(lengths_query).fetchdf()

        lengths_map = dict(zip(raw_lengths_df["reddit_node_id"], raw_lengths_df["text_length"]))

        chains_records = chains_df.to_dict("records")

        context.log.info(f"Generating chain bundles for {subreddit_key} on {date_key}")

    records = build_chain_bundles(chains_records, lengths_map, config)

    # Pydantic Validation
    if config.validation_sample_size is not None:
        TypeAdapter(list[SilverChainBundle]).validate_python(records[: config.validation_sample_size])
    else:
        TypeAdapter(list[SilverChainBundle]).validate_python(records)

    output_df = pd.DataFrame(records, columns=list(SilverChainBundle.model_fields.keys()))  # noqa: F841
    with get_duckdb_connection() as con:
        con.execute(f"COPY (SELECT * FROM output_df) TO '{target_parquet}' (FORMAT PARQUET)")
        preview_df = con.execute(f"SELECT * FROM '{target_parquet}' LIMIT 10").fetchdf()
        preview_md = preview_df.to_markdown()

    return MaterializeResult(
        metadata={
            "target_file": target_parquet,
            "data_preview": MetadataValue.md(preview_md),
        }
    )


silver_chain_bundles_job = define_asset_job(
    name="silver_chain_bundles_job",
    selection="silver_reddit_chain_bundles",
    executor_def=multiprocess_executor.configured({"max_concurrent": 8}),
)
