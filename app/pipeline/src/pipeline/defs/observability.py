import os
from ..utils.db import get_duckdb_connection
from dagster import asset, MaterializeResult, MetadataValue

@asset(group_name="observability")
def llm_cost_dashboard(context) -> MaterializeResult:
    """
    Unpartitioned standalone asset. Queries the local duckdb Cost Ledger 
    to sum all API spend across various models and tasks without waiting for entire backfills.
    """
    from ..utils.paths import get_read_path
    
    silver_glob = str(get_read_path("silver/*_payloads_*.parquet"))
    eval_glob = str(get_read_path("evaluations/*_payloads_*.parquet"))
    
    with get_duckdb_connection() as con:
        try:
            query = f"""
                SELECT 
                    split_part(split_part(filename, '/', -1), '_payloads', 1) as asset_name,
                    model_used,
                    COUNT(chunk_id) as total_payloads,
                    SUM(input_tokens) as total_input_tokens,
                    SUM(output_tokens) as total_output_tokens,
                    SUM(cost_usd) as total_cost_usd
                FROM read_parquet(['{silver_glob}', '{eval_glob}'], filename=true)
                GROUP BY 1, 2
                ORDER BY total_cost_usd DESC
            """
            df = con.execute(query).df()
            
        except Exception as e:
            if "No files found" in str(e):
                return MaterializeResult(metadata={"status": "No cost metrics found (No payload files yet)"})
            raise e
            
        if df.empty:
            return MaterializeResult(metadata={"status": "No cost metrics found"})
            
        grand_total = df['total_cost_usd'].sum()
        
        # Format the float explicitly for nicer markdown table rendering
        df['total_cost_usd'] = df['total_cost_usd'].map(lambda x: f"${x:,.4f}")
        df['total_input_tokens'] = df['total_input_tokens'].map(lambda x: f"{int(x):,}")
        df['total_output_tokens'] = df['total_output_tokens'].map(lambda x: f"{int(x):,}")
        
    return MaterializeResult(
        metadata={
            "grand_total_usd": MetadataValue.float(float(round(grand_total, 4))),
            "cost_breakdown": MetadataValue.md(df.to_markdown(index=False))
        }
    )
