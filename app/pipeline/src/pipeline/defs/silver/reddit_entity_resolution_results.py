import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
    define_asset_job,
)
from pydantic import TypeAdapter

from pipeline.defs.silver.chains import chains_partitions_def
from pipeline.schemas.reddit_entity_resolution_results import RedditEntityResolutionResult
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path


@asset(
    group_name="silver",
    partitions_def=chains_partitions_def,
    description=(
        "Flattens the raw JSON entity resolution output into a normalized relational table — "
        "one row per resolved entity per node."
    ),
    deps=[AssetDep("silver_reddit_entity_resolution")],
)
def silver_reddit_entity_resolution_results(context: AssetExecutionContext) -> MaterializeResult:
    partition_keys_dict = context.partition_key.keys_by_dimension
    date_key = partition_keys_dict["date"]
    subreddit_key = partition_keys_dict["subreddit"]
    sub_lower = subreddit_key.lower()

    context.log.info(f"[START] silver_reddit_entity_resolution_results — {subreddit_key} / {date_key}")

    source_path = get_read_path(
        f"silver/reddit_entity_resolution/subreddit={sub_lower}/date={date_key}/resolutions.parquet"
    )
    target_path = get_write_path(
        f"silver/reddit_entity_resolution_results/subreddit={sub_lower}/date={date_key}/results.parquet"
    )

    empty_columns = list(RedditEntityResolutionResult.model_fields.keys())

    with get_duckdb_connection() as con:
        # Check if upstream parquet exists and is queryable
        try:
            has_data = not con.execute(f"SELECT 1 FROM read_parquet('{source_path}') LIMIT 1").fetchdf().empty
        except Exception:
            has_data = False

        if not has_data:
            context.log.info(
                f"No resolution data found for {subreddit_key} on {date_key}. Outputting empty parquet."
            )
            empty_df = pd.DataFrame(columns=empty_columns)  # noqa: F841
            con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_path}' (FORMAT PARQUET)")
            return MaterializeResult(
                metadata={
                    "target_file": target_path,
                    "data_preview": MetadataValue.md("No upstream data observed."),
                }
            )

        # Parse raw_json and flatten into one row per entity per node
        json_schema = (
            '[{"verbatim_quote":"","brand":"",'
            '"product_line":"","product_model":"",'
            '"specificity_level":""}]'
        )
        query = f"""
            WITH parsed AS (
                SELECT
                    node_id,
                    submission_id,
                    unnest(from_json(raw_json, '{json_schema}')) AS item
                FROM read_parquet('{source_path}')
                WHERE resolved_count > 0
            )
            SELECT
                node_id,
                submission_id,
                item.verbatim_quote AS verbatim_quote,
                item.brand AS brand,
                item.product_line AS product_line,
                item.product_model AS product_model,
                item.specificity_level AS specificity_level
            FROM parsed
        """

        results_df = con.execute(query).fetchdf()

        if results_df.empty:
            context.log.info("Parsed results are empty (all nodes had zero entities). Writing empty parquet.")
            empty_df = pd.DataFrame(columns=empty_columns)  # noqa: F841
            con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_path}' (FORMAT PARQUET)")
            return MaterializeResult(
                metadata={
                    "target_file": target_path,
                    "data_preview": MetadataValue.md("No resolved entities found."),
                }
            )

        # Pydantic validation
        records = results_df.to_dict("records")
        TypeAdapter(list[RedditEntityResolutionResult]).validate_python(records)

        # Write to target parquet
        con.execute(f"COPY (SELECT * FROM results_df) TO '{target_path}' (FORMAT PARQUET)")

        preview_md = results_df.head(10).to_markdown(index=False)

    return MaterializeResult(
        metadata={
            "target_file": target_path,
            "row_count": MetadataValue.int(len(results_df)),
            "data_preview": MetadataValue.md(preview_md),
        }
    )


silver_reddit_entity_resolution_results_job = define_asset_job(
    name="silver_reddit_entity_resolution_results_job",
    selection="silver_reddit_entity_resolution_results",
)
