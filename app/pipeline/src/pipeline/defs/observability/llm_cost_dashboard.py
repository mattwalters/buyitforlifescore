from dagster import MaterializeResult, MetadataValue, asset

from pipeline.utils.db import get_duckdb_connection


@asset(group_name="observability")
def llm_cost_dashboard(context) -> MaterializeResult:
    """
    Unpartitioned standalone asset. Queries the local duckdb Cost Ledger
    to sum all API spend across various models and tasks without waiting for entire backfills.
    """
    from pipeline.utils.paths import get_read_path

    silver_glob = str(get_read_path("silver/*_payloads_*.parquet"))
    eval_glob = str(get_read_path("evaluations/*_payloads_*.parquet"))

    with get_duckdb_connection() as con:
        dfs = []
        for g in [silver_glob, eval_glob]:
            try:
                query = f"""
                    SELECT
                        split_part(split_part(filename, '/', -1), '_payloads', 1) as asset_name,
                        model_used,
                        COUNT(chunk_id) as total_payloads,
                        SUM(input_tokens) as total_input_tokens,
                        SUM(output_tokens) as total_output_tokens,
                        SUM(cost_usd) as total_cost_usd
                    FROM read_parquet('{g}', filename=true)
                    GROUP BY 1, 2
                """
                dfs.append(con.execute(query).df())
            except Exception as e:
                if "No files found" in str(e):
                    continue
                raise e

        if not dfs:
            return MaterializeResult(metadata={"status": "No cost metrics found (No payload files yet)"})

        import pandas as pd

        df = pd.concat(dfs, ignore_index=True)

        # Aggregate across the merged dataframes
        df = df.groupby(["asset_name", "model_used"], as_index=False).sum()
        df = df.sort_values(by="total_cost_usd", ascending=False)

        grand_total = df["total_cost_usd"].sum()

        # Format the float explicitly for nicer markdown table rendering
        df["total_cost_usd"] = df["total_cost_usd"].apply(lambda x: f"${float(x):,.4f}")
        df["total_input_tokens"] = df["total_input_tokens"].apply(lambda x: f"{int(x):,}")
        df["total_output_tokens"] = df["total_output_tokens"].apply(lambda x: f"{int(x):,}")

    return MaterializeResult(
        metadata={
            "grand_total_usd": MetadataValue.float(float(round(grand_total, 4))),
            "cost_breakdown": MetadataValue.md(df.to_markdown(index=False)),
        }
    )
