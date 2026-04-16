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

from pipeline.schemas.ops_ai_cost_summary import OpsAiCostSummary
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path, get_write_path


@asset(
    group_name="ops",
    description="Summarizes AI inference costs across all operational services. Provides a top-level rollup for observability.",
    deps=[
        AssetDep("silver_reddit_node_summarizations"),
        AssetDep("silver_reddit_entity_discovery"),
    ],
)
def ops_ai_cost_summary(context: AssetExecutionContext) -> MaterializeResult:
    target_parquet = get_write_path("ops/ai_cost_summary/summary.parquet")
    source_summarizations = get_read_path("silver/reddit_node_summarizations/*/*/*.parquet")
    source_discovery = get_read_path("silver/reddit_entity_discovery/*/*/*.parquet")

    with get_duckdb_connection() as con:
        # Check if upstream parquets exist
        try:
            has_sum_data = not con.execute(f"SELECT 1 FROM read_parquet('{source_summarizations}', union_by_name=True) LIMIT 1").fetchdf().empty
        except Exception:
            has_sum_data = False

        try:
            has_disc_data = not con.execute(f"SELECT 1 FROM read_parquet('{source_discovery}', union_by_name=True) LIMIT 1").fetchdf().empty
        except Exception:
            has_disc_data = False

        if not has_sum_data and not has_disc_data:
            context.log.info("No upstream AI inference data found. Outputting empty cost table.")
            empty_df = pd.DataFrame(columns=list(OpsAiCostSummary.model_fields.keys()))
            con.execute(f"COPY (SELECT * FROM empty_df) TO '{target_parquet}' (FORMAT PARQUET)")
            return MaterializeResult(
                metadata={
                    "target_file": target_parquet,
                    "data_preview": MetadataValue.md("No upstream data observed. Cost is $0.00."),
                }
            )

        queries = []
        if has_sum_data:
            queries.append(f"""
                SELECT 
                    'node_summarizations' AS service_name,
                    COALESCE(SUM(cost_usd), 0.0) AS total_cost_usd,
                    COALESCE(SUM(prompt_tokens), 0) AS total_prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0) AS total_completion_tokens,
                    COUNT(reddit_node_id) AS total_nodes_processed
                FROM read_parquet('{source_summarizations}', union_by_name=True)
            """)

        if has_disc_data:
            queries.append(f"""
                SELECT 
                    'entity_discovery' AS service_name,
                    COALESCE(SUM(cost_usd), 0.0) AS total_cost_usd,
                    COALESCE(SUM(prompt_tokens), 0) AS total_prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0) AS total_completion_tokens,
                    COUNT(bundle_id) AS total_nodes_processed
                FROM read_parquet('{source_discovery}', union_by_name=True)
            """)

        union_query = " UNION ALL ".join(queries)

        query = f"""
            WITH base_data AS (
                {union_query}
            )
            SELECT 
                service_name,
                total_cost_usd,
                total_prompt_tokens,
                total_completion_tokens,
                total_nodes_processed
            FROM base_data
            UNION ALL
            SELECT 
                'TOTAL' AS service_name,
                SUM(total_cost_usd) AS total_cost_usd,
                SUM(total_prompt_tokens) AS total_prompt_tokens,
                SUM(total_completion_tokens) AS total_completion_tokens,
                SUM(total_nodes_processed) AS total_nodes_processed
            FROM base_data
        """

        results_df = con.execute(query).fetchdf()

        # Pydantic validation
        records = results_df.to_dict("records")
        TypeAdapter(list[OpsAiCostSummary]).validate_python(records)

        # Write to parquet
        con.execute(f"COPY (SELECT * FROM results_df) TO '{target_parquet}' (FORMAT PARQUET)")

    preview_md = results_df.to_markdown(index=False)

    return MaterializeResult(
        metadata={
            "target_file": target_parquet,
            "data_preview": MetadataValue.md(preview_md),
        }
    )


ops_ai_cost_summary_job = define_asset_job(
    name="ops_ai_cost_summary_job",
    selection="ops_ai_cost_summary",
)
