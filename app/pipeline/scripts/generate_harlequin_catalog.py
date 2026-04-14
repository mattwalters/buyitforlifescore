import os
import glob
from pathlib import Path
import duckdb
from dotenv import load_dotenv

# Load root monorepo env vars just in case we want to do R2 credentials later
load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")

def generate_catalog():
    data_dir = Path(__file__).parent.parent.parent.parent / "data"
    catalog_path = data_dir / "catalog.duckdb"
    
    print(f"Generating Harlequin DB Catalog at: {catalog_path}")
    
    # 1. Connect to or create a persistent duckdb file
    con = duckdb.connect(str(catalog_path))
    
    # Optional: If you want to view R2 remote tables, DuckDB can persist secrets so Harlequin inherits them!
    r2_endpoint = os.getenv("R2_ENDPOINT_URL")
    if r2_endpoint:
        print("R2 Credentials found, registering persistent secret for Harlequin...")
        if r2_endpoint.startswith("https://"): r2_endpoint = r2_endpoint[8:]
        con.execute(f"""
        CREATE PERSISTENT SECRET IF NOT EXISTS r2_creds (
            TYPE S3,
            KEY_ID '{os.getenv("R2_ACCESS_KEY_ID")}',
            SECRET '{os.getenv("R2_SECRET_ACCESS_KEY")}',
            ENDPOINT '{r2_endpoint}',
            URL_STYLE 'path'
        );
        """)
    
    # 2. Crawl local data directory for parquet files
    parquet_files = []
    parquet_files.extend(glob.glob(str(data_dir / "bronze" / "*.parquet")))
    parquet_files.extend(glob.glob(str(data_dir / "silver" / "*.parquet")))
    
    if not parquet_files:
        print(f"No parquet files found in {data_dir}/(bronze|silver).")
        return
        
    print(f"Found {len(parquet_files)} parquet files. Building Views...")
    
    # 3. Create a beautiful View for each file
    for filepath in parquet_files:
        path = Path(filepath)
        folder = path.parent.name # 'bronze' or 'silver'
        filename = path.stem # e.g. 'entity_extraction_2012-01-01'
        
        # Clean up view name (e.g. bronze_reddit_buyitforlife_comments)
        view_name = f"{folder}_{filename.replace('-', '_')}"
        
        # Harlequin will see these as First-Class tables in the Data Catalog
        query = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM '{filepath}';"
        con.execute(query)
        print(f"  + {view_name}")
        
    con.close()
    
    print("\n✅ Catalog successfully generated!")
    print("\nTo launch your new DB Studio, run:")
    print(f"    harlequin {catalog_path}")

if __name__ == "__main__":
    generate_catalog()
