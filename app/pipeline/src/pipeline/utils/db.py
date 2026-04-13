import os
import duckdb

def get_duckdb_connection(database=':memory:', read_only=False):
    """
    Creates a centralized DuckDB connection and configures the `httpfs` extension
    if R2/S3 environment variables are present. This allows seamless `s3://` queries.
    """
    con = duckdb.connect(database=database, read_only=read_only)
    
    r2_endpoint = os.getenv("R2_ENDPOINT_URL")
    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    
    if r2_endpoint and r2_access_key and r2_secret_key:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        
        # Strip https:// or http:// if provided in the endpoint URL because DuckDB
        # expects just the hostname.
        if r2_endpoint.startswith("https://"):
            r2_endpoint = r2_endpoint[8:]
        elif r2_endpoint.startswith("http://"):
            r2_endpoint = r2_endpoint[7:]
            
        con.execute(f"SET s3_endpoint='{r2_endpoint}';")
        con.execute(f"SET s3_access_key_id='{r2_access_key}';")
        con.execute(f"SET s3_secret_access_key='{r2_secret_key}';")
        con.execute("SET s3_region='auto';")
        
        # Cloudflare R2 strongly recommends vhost routing or using fully qualified URLs.
        # But 'path' style is strictly required without custom domains on the base account URL.
        # Actually, Cloudflare now supports vhost. 'vhost' or 'path' depends on DuckDB version, 
        # but 'path' is safest for the raw accountid.r2.cloudflarestorage.com endpoint
        con.execute("SET s3_url_style='path';")
        con.execute("SET s3_use_ssl=true;")
        
    return con
