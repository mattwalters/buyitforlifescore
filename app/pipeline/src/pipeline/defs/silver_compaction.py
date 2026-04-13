import os
from ..utils.db import get_duckdb_connection
from dagster import asset, MonthlyPartitionsDefinition, MaterializeResult

bifl_monthly_partitions = MonthlyPartitionsDefinition(start_date="2012-01-01")

@asset(
    group_name="silver",
    partitions_def=bifl_monthly_partitions,
    deps=["silver_entity_discovery"],
)
def silver_entity_discovery_monthly(context) -> MaterializeResult:
    """Compacts the small daily Parquet files into a large monthly partition."""
    target_month_str = context.partition_key # e.g. "2012-01"
    
    from ..utils.paths import get_read_path, get_write_path
    
    source_glob = get_read_path(f"silver/entity_discovery_{target_month_str}-*.parquet")
    target_parquet = get_write_path(f"silver_monthly/entity_discovery_{target_month_str}.parquet")
    
    with get_duckdb_connection() as con:
        try:
            row_count = con.execute(f"SELECT count(*) FROM read_parquet('{source_glob}')").fetchone()[0]
        except duckdb.IOException:
            context.log.info(f"No daily files found for {target_month_str}. Skipping.")
            return MaterializeResult()
            
        if row_count > 0:
            con.execute(f"COPY (SELECT * FROM read_parquet('{source_glob}')) TO '{target_parquet}' (FORMAT PARQUET)")
            context.log.info(f"Compacted {row_count} rows into {target_parquet}")
            
            return MaterializeResult(
                metadata={
                    "partition": target_month_str,
                    "rows_compacted": row_count,
                    "file_path": target_parquet
                }
            )
            
    return MaterializeResult()
