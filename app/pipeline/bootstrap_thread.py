import asyncio
import duckdb
from pathlib import Path
import sys

sys.path.insert(0, "/Users/matt/src/mattwalters/buyitforlifeclub/app/pipeline/src")
from pipeline.defs.silver import _process_thread_batch
from google.genai import Client

async def main():
    conn = duckdb.connect()
    query = """
        WITH submissions AS (
            SELECT id, title, selftext as body, score, created_utc as postedAt
            FROM read_parquet('/Users/matt/src/mattwalters/buyitforlifeclub/data/BuyItForLife_submissions.parquet')
            WHERE id = 'ateshn'
        ),
        comments AS (
            SELECT link_id, body, score, author, created_utc
            FROM read_parquet('/Users/matt/src/mattwalters/buyitforlifeclub/data/BuyItForLife_comments.parquet')
        ),
        threads AS (
            SELECT 
                s.id,
                s.title,
                s.body,
                list(c.body ORDER BY c.created_utc ASC) as comments_list,
                s.postedAt
            FROM submissions s
            JOIN comments c ON 't3_' || s.id = c.link_id
            GROUP BY s.id, s.title, s.body, s.postedAt
        )
        SELECT * FROM threads;
    """
    rows = conn.execute(query).fetchall()
    thread = rows[0]
    
    semaphore = asyncio.Semaphore(10)
    
    print("Running batch extraction via Gemini 2.5 Flash Lite...")
    extracted, cost, inp, out = await _process_thread_batch([thread], "gemini-2.5-flash-lite", semaphore, None)
    
    import json
    with open("bootstrap_14.json", "w") as f:
        json.dump(extracted, f, indent=2)
    print(f"Extraction complete! Found {len(extracted)} items.")

asyncio.run(main())
