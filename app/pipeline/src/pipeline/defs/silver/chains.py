import hashlib
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    Config,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    MultiPartitionsDefinition,
    MultiToSingleDimensionPartitionMapping,
    BackfillPolicy,
    asset,
    define_asset_job,
    multiprocess_executor,
)
from pydantic import TypeAdapter

from pipeline.defs.partitions import subreddit_partitions
from pipeline.schemas.chains import BronzeComment, BronzeSubmission, SilverChain
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path


class SilverRedditChainsConfig(Config):
    validation_sample_size: Optional[int] = None


date_partitions = DailyPartitionsDefinition(start_date="2011-01-01")

chains_partitions_def = MultiPartitionsDefinition(
    {
        "date": date_partitions,
        "subreddit": subreddit_partitions,
    }
)


def build_thread_chains(
    submissions: list[dict[str, Any]], comments: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    # Group comments by link_id (submission)
    comments_by_sub = defaultdict(list)
    for c in comments:
        comments_by_sub[c["link_id"]].append(c)

    all_chains = []

    for sub in submissions:
        sub_id = sub["submission_id"]
        sub_node_id = sub["reddit_node_id"]
        sub_comments = comments_by_sub.get(sub_node_id, [])

        # Build adjacency dictionary for this submission's comments
        children_map = defaultdict(list)
        for c in sub_comments:
            children_map[c["parent_id"]].append(c)

        # Use an iterative stack for DFS to prevent recursion limit errors on deep trees
        stack = [(sub_node_id, [sub_node_id])]
        while stack:
            curr_node, curr_path = stack.pop()
            children = children_map.get(curr_node, [])
            if not children:
                # Leaf node, save the path
                path_str = ",".join(curr_path)
                # Ensure a stable deterministic hash mimicking DuckDB's hash functionality
                chain_id = str(int(hashlib.sha256(path_str.encode("utf-8")).hexdigest()[:15], 16))
                all_chains.append({"submission_id": sub_id, "chain_id": chain_id, "path": curr_path})
            else:
                for child in children:
                    stack.append((child["reddit_node_id"], curr_path + [child["reddit_node_id"]]))

    # Now flatten paths into sequence items to match schema and assign is_canonical
    silver_chain_records = []

    # Sort chains deterministically to mimic SQL ORDER BY chain_id
    all_chains.sort(key=lambda x: x["chain_id"])

    for chain in all_chains:
        chain_id = chain["chain_id"]
        submission_id = chain["submission_id"]
        for idx, node_id in enumerate(chain["path"]):
            silver_chain_records.append(
                {
                    "chain_id": chain_id,
                    "submission_id": submission_id,
                    "reddit_node_id": node_id,
                    "sequence_order": idx + 1,  # 1-based indexing
                }
            )

    return silver_chain_records


@asset(
    group_name="silver",
    partitions_def=chains_partitions_def,
    description="Extract isolated linear paths from the Reddit submission-comment trees.",
    deps=[
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
def silver_reddit_chains(context: AssetExecutionContext, config: SilverRedditChainsConfig) -> MaterializeResult:
    # 1. Get partition keys
    partition_keys_dict = context.partition_key.keys_by_dimension
    date_key = partition_keys_dict["date"]
    subreddit_key = partition_keys_dict["subreddit"]

    sub_lower = subreddit_key.lower()

    # Calculate partition boundaries for DuckDB Parquet metadata pushdown
    dt = datetime.strptime(date_key, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_utc = int(dt.timestamp())
    end_utc = start_utc + 86400
    cutoff_utc = start_utc + (45 * 86400)  # 45 day comment window

    # Generate partitioned write path using Hive-style partitions
    target_parquet = get_write_path(f"silver/chains/subreddit={sub_lower}/date={date_key}/chains.parquet")

    source_submissions = get_read_path(f"bronze/reddit_{sub_lower}_submissions.parquet")
    source_comments = get_read_path(f"bronze/reddit_{sub_lower}_comments.parquet")

    sub_query = f"""
        SELECT
            CAST(s.id AS VARCHAR) AS submission_id,
            CAST(s.id AS VARCHAR) AS id,
            COALESCE(CAST(s.name AS VARCHAR), 't3_' || CAST(s.id AS VARCHAR)) AS name,
            CAST(s.created_utc AS BIGINT) AS created_utc,
            COALESCE(CAST(s.name AS VARCHAR), 't3_' || CAST(s.id AS VARCHAR)) AS reddit_node_id,
            CAST(s.subreddit AS VARCHAR) AS subreddit
        FROM read_parquet('{source_submissions}') s
        WHERE lower(CAST(s.subreddit AS VARCHAR)) = '{sub_lower}'
        AND CAST(s.created_utc AS BIGINT) >= {start_utc}
        AND CAST(s.created_utc AS BIGINT) < {end_utc}
    """

    com_query = f"""
        WITH target_submissions AS (
            {sub_query}
        )
        SELECT
            CAST(c.id AS VARCHAR) AS comment_id,
            CAST(c.id AS VARCHAR) AS id,
            COALESCE(CAST(c.name AS VARCHAR), 't1_' || CAST(c.id AS VARCHAR)) AS name,
            CAST(c.created_utc AS BIGINT) AS created_utc,
            COALESCE(CAST(c.name AS VARCHAR), 't1_' || CAST(c.id AS VARCHAR)) AS reddit_node_id,
            trim(CAST(c.parent_id AS VARCHAR), '"') AS parent_id,
            trim(CAST(c.link_id AS VARCHAR), '"') AS link_id,
            CAST(c.subreddit AS VARCHAR) AS subreddit
        FROM read_parquet('{source_comments}') c
        SEMI JOIN target_submissions s
          ON trim(CAST(c.link_id AS VARCHAR), '"') = s.reddit_node_id
        WHERE CAST(c.created_utc AS BIGINT) >= {start_utc}
        AND CAST(c.created_utc AS BIGINT) < {cutoff_utc}
    """

    context.log.info(f"Generating chains for {subreddit_key} on {date_key}")

    with get_duckdb_connection() as con:
        submissions_df = con.execute(sub_query).fetchdf()
        comments_df = con.execute(com_query).fetchdf()

    submissions = submissions_df.to_dict("records")
    comments = comments_df.to_dict("records")

    # Bronze Payload Validation Gate
    if config.validation_sample_size is not None:
        context.log.info(f"Validating upstream sample of {config.validation_sample_size} rows")
        TypeAdapter(list[BronzeSubmission]).validate_python(submissions[: config.validation_sample_size])
        TypeAdapter(list[BronzeComment]).validate_python(comments[: config.validation_sample_size])
    else:
        context.log.info("Validating 100% of upstream rows")
        TypeAdapter(list[BronzeSubmission]).validate_python(submissions)
        TypeAdapter(list[BronzeComment]).validate_python(comments)

    records = build_thread_chains(submissions, comments)

    # Pydantic Validation Gate
    if config.validation_sample_size is not None:
        context.log.info(f"Validating sample of {config.validation_sample_size} rows")
        TypeAdapter(list[SilverChain]).validate_python(records[: config.validation_sample_size])
    else:
        context.log.info("Validating 100% of generated rows")
        TypeAdapter(list[SilverChain]).validate_python(records)
    context.log.info("Schema validation strictly passed!")

    # Write back to Parquet via DuckDB
    output_df = pd.DataFrame(records, columns=list(SilverChain.model_fields.keys()))  # noqa: F841
    with get_duckdb_connection() as con:
        # Load into duckdb and export to partitioned parquet file
        con.execute(f"COPY (SELECT * FROM output_df) TO '{target_parquet}' (FORMAT PARQUET)")
        preview_df = con.execute(f"SELECT * FROM '{target_parquet}' LIMIT 10").fetchdf()
        preview_md = preview_df.to_markdown()

    return MaterializeResult(
        metadata={
            "target_file": target_parquet,
            "data_preview": MetadataValue.md(preview_md),
        }
    )


silver_chains_job = define_asset_job(
    name="silver_chains_job",
    selection="silver_reddit_chains",
    executor_def=multiprocess_executor.configured({"max_concurrent": 40}),
)
