import duckdb
import os
import sys

# manually grab from env
r2_endpoint = os.getenv("R2_ENDPOINT_URL")
r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")

if not r2_endpoint:
    print("MISSING R2_ENDPOINT_URL in environment!")
    sys.exit(1)

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_endpoint='{r2_endpoint.replace('https://', '')}';")
con.execute(f"SET s3_access_key_id='{r2_access_key}';")
con.execute(f"SET s3_secret_access_key='{r2_secret_key}';")
con.execute("SET s3_region='auto';")
con.execute("SET s3_url_style='path';")
con.execute("SET s3_use_ssl=true;")

try:
    df = con.execute("""
    SELECT brand, productName, text, parent_text 
    FROM read_parquet('s3://buyitforlifescore/silver/entity_discovery_*.parquet')
    LIMIT 20
    """).fetchdf()
    print(df.to_json(orient="records"))
except Exception as e:
    print(f"FAILED. Error: {e}")
