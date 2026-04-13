import os

def get_data_dir() -> str:
    """
    Returns the target data directory for the pipeline operations.
    
    If the DATA_DIR environment variable is set (e.g. 's3://bifl-data' on Railway), 
    it returns that dynamically. Otherwise, it gracefully falls back to the 
    local Macbook filesystem so local development doesn't break.
    """
    data_dir = os.environ.get("DATA_DIR")
    if data_dir:
        # Prevent double-slashes if the user includes a trailing slash
        return str(data_dir).rstrip('/')
        
    return "/Users/matt/src/mattwalters/buyitforlifeclub/data"


def get_ledger_path() -> str:
    """
    Returns the absolute filepath for the Cost Ledger duckdb file.
    Because DuckDB cannot use s3:// to open a writable database file (it requires POSIX locks),
    this must always point to a local block storage folder. 
    We default to Railway's `/data` volume if in production, else your Macbook!
    """
    ledger_dir = os.environ.get("LEDGER_DIR", "/Users/matt/src/mattwalters/buyitforlifeclub/data/metrics")
    if not os.path.exists(ledger_dir):
        os.makedirs(ledger_dir, exist_ok=True)
    return os.path.join(ledger_dir, "ledger.duckdb")
