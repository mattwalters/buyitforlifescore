import duckdb
import os
import sys

r2_endpoint = os.getenv("R2_ENDPOINT_URL")
r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_endpoint='{r2_endpoint.replace('https://', '')}';")
con.execute(f"SET s3_access_key_id='{r2_access_key}';")
con.execute(f"SET s3_secret_access_key='{r2_secret_key}';")
con.execute("SET s3_region='auto';")
con.execute("SET s3_url_style='path';")
con.execute("SET s3_use_ssl=true;")

try:
    df = con.execute("SELECT * FROM glob('s3://buyitforlifescore/silver/*')").fetchdf()
    print(df)
except Exception as e:
    print(e)
