import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    BackfillPolicy,
    asset,
    define_asset_job,
)
from pydantic import TypeAdapter

from pipeline.defs.silver.chains import chains_partitions_def
from pipeline.schemas.reddit_entity_discovery_results import RedditEntityDiscoveryResult
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path


@asset(
    group_name="silver",
    partitions_def=chains_partitions_def,
    description="Unwraps the bundled LLM entity extraction payloads into a highly normalized, normalized, relational table bridging directly to individual node IDs.",
    deps=[AssetDep("silver_reddit_entity_discovery")],
    backfill_policy=BackfillPolicy.single_run(),
)
def silver_reddit_entity_discovery_results(context: AssetExecutionContext) -> MaterializeResult:
    partition_keys_dict = context.partition_key.keys_by_dimension
    date_key = partition_keys_dict["date"]
    subreddit_key = partition_keys_dict["subreddit"]
    sub_lower = subreddit_key.lower()

    source_path = get_read_path(f"silver/reddit_entity_discovery/subreddit={sub_lower}/date={date_key}/entities.parquet")
    target_path = get_write_path(f"silver/reddit_entity_discovery_results/subreddit={sub_lower}/date={date_key}/results.parquet")

    with get_duckdb_connection() as con:
        # Check if upstream parquet exists and is queryable
        try:
            has_data = not con.execute(f"SELECT 1 FROM read_parquet('{source_path}') LIMIT 1").fetchdf().empty
        except Exception:
            has_data = False

        if not has_data:
            context.log.info(f"No LLM payload data found for {subreddit_key} on {date_key}. Outputting empty parquet.")
            empty_df = pd.DataFrame(columns=list(RedditEntityDiscoveryResult.model_fields.keys()))
            con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_path}' (FORMAT PARQUET)")
            return MaterializeResult(
                metadata={
                    "target_file": target_path,
                    "data_preview": MetadataValue.md("No upstream data observed."),
                }
            )

        query = f"""
            WITH unnested_items AS (
                SELECT 
                    bundle_id,
                    submission_id,
                    unnest(items) AS item
                FROM read_parquet('{source_path}')
            )
            SELECT 
                bundle_id,
                submission_id,
                item.verbatim_quote AS verbatim_quote,
                unnest(item.node_ids) AS node_id
            FROM unnested_items
        """

        results_df = con.execute(query).fetchdf()

        # Pydantic validation to ensure exactly the schema we expect
        records = results_df.to_dict("records")
        TypeAdapter(list[RedditEntityDiscoveryResult]).validate_python(records)

        # Write to target parquet
        con.execute(f"COPY (SELECT * FROM results_df) TO '{target_path}' (FORMAT PARQUET)")

        preview_md = results_df.head(10).to_markdown(index=False)

    return MaterializeResult(
        metadata={
            "target_file": target_path,
            "row_count": len(results_df),
            "data_preview": MetadataValue.md(preview_md),
        }
    )


silver_reddit_entity_discovery_results_job = define_asset_job(
    name="silver_reddit_entity_discovery_results_job",
    selection="silver_reddit_entity_discovery_results",
)
