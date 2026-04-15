from typing import Optional

from dagster import (
    AssetExecutionContext,
    Config,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    MultiPartitionsDefinition,
    asset,
)
from pydantic import TypeAdapter

from pipeline.defs.partitions import subreddit_partitions
from pipeline.schemas.chains import SilverChain
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


@asset(
    group_name="silver",
    partitions_def=chains_partitions_def,
    description="Extract isolated linear paths from the Reddit submission-comment trees.",
)
def silver_reddit_chains(context: AssetExecutionContext, config: SilverRedditChainsConfig) -> MaterializeResult:
    # 1. Get partition keys
    partition_keys_dict = context.partition_key.keys_by_dimension
    date_key = partition_keys_dict["date"]
    subreddit_key = partition_keys_dict["subreddit"]

    sub_lower = subreddit_key.lower()

    # Generate partitioned write path using Hive-style partitions
    target_parquet = get_write_path(f"silver/chains/subreddit={sub_lower}/date={date_key}/chains.parquet")

    source_submissions = get_read_path(f"bronze/reddit_{sub_lower}_submissions.parquet")
    source_comments = get_read_path(f"bronze/reddit_{sub_lower}_comments.parquet")

    # We use CAST(s.created_utc AS BIGINT) and to_timestamp to safely parse the Unix timestamp.
    query = f"""
    COPY (
        WITH RECURSIVE
        chains AS (
            SELECT
                CAST(s.id AS VARCHAR) AS submission_id,
                COALESCE(CAST(s.name AS VARCHAR), 't3_' || CAST(s.id AS VARCHAR)) AS reddit_node_id,
                CAST(s.subreddit AS VARCHAR) AS subreddit,
                [COALESCE(CAST(s.name AS VARCHAR), 't3_' || CAST(s.id AS VARCHAR))] AS path_nodes,
                COALESCE(CAST(s.name AS VARCHAR), 't3_' || CAST(s.id AS VARCHAR)) AS current_node,
                1 AS depth
            FROM read_parquet('{source_submissions}') s
            WHERE lower(CAST(s.subreddit AS VARCHAR)) = '{sub_lower}'
            AND CAST(to_timestamp(CAST(s.created_utc AS BIGINT)) AS DATE) = CAST('{date_key}' AS DATE)

            UNION ALL

            SELECT
                p.submission_id,
                COALESCE(CAST(c.name AS VARCHAR), 't1_' || CAST(c.id AS VARCHAR)) AS reddit_node_id,
                CAST(c.subreddit AS VARCHAR) AS subreddit,
                list_append(p.path_nodes, COALESCE(CAST(c.name AS VARCHAR), 't1_' || CAST(c.id AS VARCHAR))) AS path_nodes,
                COALESCE(CAST(c.name AS VARCHAR), 't1_' || CAST(c.id AS VARCHAR)) AS current_node,
                p.depth + 1 AS depth
            FROM read_parquet('{source_comments}') c
            JOIN chains p ON trim(CAST(c.parent_id AS VARCHAR), '"') = p.current_node
        ),
        leaf_nodes AS (
            SELECT c.*
            FROM chains c
            LEFT JOIN read_parquet('{source_comments}') child
              ON trim(CAST(child.parent_id AS VARCHAR), '"') = c.current_node
            WHERE child.name IS NULL
        ),
        unnested_paths AS (
            SELECT
                CAST(hash(CAST(path_nodes AS VARCHAR)) AS VARCHAR) AS chain_id,
                submission_id,
                UNNEST(path_nodes) AS reddit_node_id,
                path_nodes
            FROM leaf_nodes
        ),
        numbered_paths AS (
            SELECT
                chain_id,
                submission_id,
                CAST(reddit_node_id AS VARCHAR) AS reddit_node_id,
                list_position(path_nodes, reddit_node_id) as sequence_order
            FROM unnested_paths
        )
        SELECT
            chain_id,
            submission_id,
            reddit_node_id,
            sequence_order,
            CAST(ROW_NUMBER() OVER(PARTITION BY reddit_node_id ORDER BY chain_id) = 1 AS BOOLEAN) AS is_canonical
        FROM numbered_paths
        ORDER BY chain_id, sequence_order
    ) TO '{target_parquet}' (FORMAT PARQUET);
    """

    context.log.info(f"Generating chains for {subreddit_key} on {date_key}")

    with get_duckdb_connection() as con:
        con.execute(query)

        # Pydantic Validation Gate
        if config.validation_sample_size is not None:
            context.log.info(f"Validating sample of {config.validation_sample_size} rows")
            validation_df = con.execute(
                f"SELECT * FROM '{target_parquet}' LIMIT {config.validation_sample_size}"
            ).fetchdf()
        else:
            context.log.info("Validating 100% of generated rows")
            validation_df = con.execute(f"SELECT * FROM '{target_parquet}'").fetchdf()

        records = validation_df.to_dict("records")
        TypeAdapter(list[SilverChain]).validate_python(records)
        context.log.info("Schema validation strictly passed!")

        preview_df = con.execute(f"SELECT * FROM '{target_parquet}' LIMIT 10").fetchdf()
        preview_md = preview_df.to_markdown()

    return MaterializeResult(
        metadata={
            "target_file": target_parquet,
            "data_preview": MetadataValue.md(preview_md),
        }
    )
