import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    MaterializeResult,
    AssetMaterialization,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    BackfillPolicy,
    asset,
    define_asset_job,
    multiprocess_executor,
)
from pydantic import TypeAdapter

from pipeline.defs.silver.chains import chains_partitions_def
from pipeline.schemas.reddit_llm_payloads import SilverRedditLlmPayload
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path


@asset(
    group_name="silver",
    partitions_def=chains_partitions_def,
    backfill_policy=BackfillPolicy.single_run(),
    description="Constructs the final, structurally-typed JSON payload context for the LLM to process.",
    deps=[
        AssetDep(
            "silver_reddit_chain_bundles",
        ),
        AssetDep(
            "silver_reddit_node_summarizations",
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
def silver_reddit_llm_payloads(context: AssetExecutionContext):
    failed_partitions = []

    for partition_key in context.partition_keys:
        try:
            date_key, subreddit_key = partition_key.split("|")
            sub_lower = subreddit_key.lower()

            source_bundles = get_read_path(f"silver/chain_bundles/subreddit={sub_lower}/date={date_key}/bundles.parquet")
            source_summarizations = get_read_path(
                f"silver/reddit_node_summarizations/subreddit={sub_lower}/date={date_key}/summarizations.parquet"
            )
            source_submissions = get_read_path(f"bronze/reddit_{sub_lower}_submissions.parquet")
            source_comments = get_read_path(f"bronze/reddit_{sub_lower}_comments.parquet")
            target_parquet = get_write_path(
                f"silver/reddit_llm_payloads/subreddit={sub_lower}/date={date_key}/payloads.parquet"
            )

            with get_duckdb_connection() as con:
                # Check if upstream chain bundles is completely empty
                try:
                    bundles_df = con.execute(f"SELECT * FROM '{source_bundles}' LIMIT 1").fetchdf()
                    if bundles_df.empty:
                        raise Exception("Empty dataframe")
                except Exception:
                    context.log.info(f"No chain bundles found for {subreddit_key} on {date_key}. Skipping LLM payloads.")
                    empty_df = pd.DataFrame(columns=list(SilverRedditLlmPayload.model_fields.keys()))
                    con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_parquet}' (FORMAT PARQUET)")
                    yield AssetMaterialization(
                        asset_key=context.asset_key,
                        partition=partition_key,
                        metadata={
                            "target_file": target_parquet,
                            "data_preview": MetadataValue.md("No data generated (empty upstream)."),
                        }
                    )
                    continue

                query = f"""
                    WITH nodes_expanded AS (
                        SELECT 
                            b.bundle_id,
                            b.submission_id,
                            b.chain_id,
                            CAST(b.sequence_order AS INT) AS sequence_order,
                            b.reddit_node_id,
                            COALESCE(s.author, c.author, '[deleted]') AS author,
                            CAST(COALESCE(s.created_utc, c.created_utc, 0) AS BIGINT) AS created_utc,
                            s.link_flair_text,
                            b.is_canonical,
                            b.needs_summarization,
                            COALESCE(s.title || ' ' || COALESCE(s.selftext, ''), c.body, '') AS text,
                            sum_tbl.summary
                        FROM read_parquet('{source_bundles}') b
                        LEFT JOIN read_parquet('{source_submissions}') s 
                            ON COALESCE(CAST(s.name AS VARCHAR), 't3_' || CAST(s.id AS VARCHAR)) = b.reddit_node_id
                        LEFT JOIN read_parquet('{source_comments}') c 
                            ON COALESCE(CAST(c.name AS VARCHAR), 't1_' || CAST(c.id AS VARCHAR)) = b.reddit_node_id
                        LEFT JOIN read_parquet('{source_summarizations}') sum_tbl 
                            ON sum_tbl.reddit_node_id = b.reddit_node_id
                    ),
                    bundled_nodes AS (
                        SELECT 
                            bundle_id,
                            MAX(submission_id) AS submission_id,
                            list({{
                                'chain_id': chain_id,
                                'sequence_order': sequence_order,
                                'reddit_node_id': reddit_node_id,
                                'author': author,
                                'created_utc': created_utc,
                                'link_flair_text': link_flair_text,
                                'is_canonical': is_canonical,
                                'needs_summarization': needs_summarization,
                                'text': text,
                                'summary': summary
                            }} ORDER BY chain_id, sequence_order) AS nodes
                        FROM nodes_expanded
                        GROUP BY bundle_id
                    )
                    SELECT * FROM bundled_nodes
                """

                results_df = con.execute(query).fetchdf()

            records = results_df.to_dict("records")

            context.log.info(f"Generated {len(records)} LLM payloads for {subreddit_key} on {date_key}")

            # Pydantic Validation
            TypeAdapter(list[SilverRedditLlmPayload]).validate_python(records)

            with get_duckdb_connection() as con:
                con.execute(f"COPY (SELECT * FROM results_df) TO '{target_parquet}' (FORMAT PARQUET)")

                # We don't want to dump the complete giant nested structs to Dagster markdown
                preview_df = con.execute(f"SELECT bundle_id, submission_id, length(nodes) as node_count FROM '{target_parquet}' LIMIT 10").fetchdf()
                preview_md = preview_df.to_markdown()

            yield AssetMaterialization(
                asset_key=context.asset_key,
                partition=partition_key,
                metadata={
                    "target_file": target_parquet,
                    "data_preview": MetadataValue.md(preview_md),
                }
            )

        except Exception as e:
            context.log.error(f"Partition {partition_key} failed: {e}")
            failed_partitions.append(partition_key)
            continue

    if failed_partitions:
        raise Exception(f"Failed to process partitions: {failed_partitions}")


silver_reddit_llm_payloads_job = define_asset_job(
    name="silver_reddit_llm_payloads_job",
    selection="silver_reddit_llm_payloads",
    executor_def=multiprocess_executor.configured({"max_concurrent": 64}),
)
