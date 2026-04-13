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

print(f"Loaded credentials. Endpoint: {r2_endpoint}")

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_endpoint='{r2_endpoint.replace('https://', '')}';")
con.execute(f"SET s3_access_key_id='{r2_access_key}';")
con.execute(f"SET s3_secret_access_key='{r2_secret_key}';")
con.execute("SET s3_region='auto';")
con.execute("SET s3_url_style='path';")
con.execute("SET s3_use_ssl=true;")

try:
    res = con.execute("SELECT * FROM read_json_auto('s3://buyitforlifescore/ore/reddit_buyitforlife_submissions.zst', compression='zstd', sample_size=1) LIMIT 1").fetchone()
    print("SUCCESS reading zst!")
except duckdb.IOException as e:
    print(f"FAILED with standard path style. Error: {e}")
    try:
        con.execute("SET s3_url_style='vhost';")
        res = con.execute("SELECT * FROM read_json_auto('s3://buyitforlifescore/ore/reddit_buyitforlife_submissions.zst', compression='zstd', sample_size=1) LIMIT 1").fetchone()
        print("SUCCESS reading zst with VHOST style!")
    except Exception as e2:
        print(f"FAILED AGAIN. Error: {e2}")

