import os
from ..utils.db import get_duckdb_connection
from dagster import asset, MaterializeResult, MetadataValue

@asset(group_name="observability")
def llm_cost_dashboard(context) -> MaterializeResult:
    """
    Unpartitioned standalone asset. Queries the local duckdb Cost Ledger 
    to sum all API spend across various models and tasks without waiting for entire backfills.
    """
    from ..utils.paths import get_ledger_path
    ledger_path = get_ledger_path()
    
    if not os.path.exists(ledger_path):
        context.log.warning("Cost ledger database does not exist yet. No data to report.")
        return MaterializeResult(metadata={"status": "No ledger found"})
        
    with get_duckdb_connection(database=ledger_path, read_only=True) as con:
        tables = con.execute("SHOW TABLES").fetchall()
        if not any(t[0] == 'cost_ledger' for t in tables):
            return MaterializeResult(metadata={"status": "Ledger table empty"})
            
        query = """
            SELECT 
                asset_name,
                model_used,
                COUNT(DISTINCT partition_key) as partitions_run,
                SUM(input_tokens) as total_input_tokens,
                SUM(output_tokens) as total_output_tokens,
                SUM(cost_usd) as total_cost_usd
            FROM cost_ledger
            GROUP BY asset_name, model_used
            ORDER BY total_cost_usd DESC
        """
        df = con.execute(query).df()
        
        if df.empty:
            return MaterializeResult(metadata={"status": "No cost metrics found"})
            
        grand_total = df['total_cost_usd'].sum()
        
        # Format the float explicitly for nicer markdown table rendering
        df['total_cost_usd'] = df['total_cost_usd'].map(lambda x: f"${x:,.4f}")
        df['total_input_tokens'] = df['total_input_tokens'].map(lambda x: f"{int(x):,}")
        df['total_output_tokens'] = df['total_output_tokens'].map(lambda x: f"{int(x):,}")
        
    return MaterializeResult(
        metadata={
            "grand_total_usd": MetadataValue.float(round(grand_total, 4)),
            "cost_breakdown": MetadataValue.md(df.to_markdown(index=False))
        }
    )
