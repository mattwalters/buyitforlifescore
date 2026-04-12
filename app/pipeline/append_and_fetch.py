import json
import duckdb

conn = duckdb.connect()
query = """
    WITH submissions AS (
        SELECT id, title, selftext as body, score, author
        FROM read_parquet('/Users/matt/src/mattwalters/buyitforlifeclub/data/BuyItForLife_submissions.parquet')
        WHERE id = '2hg7yw'
    ),
    comments AS (
        SELECT link_id, body, score, author
        FROM read_parquet('/Users/matt/src/mattwalters/buyitforlifeclub/data/BuyItForLife_comments.parquet')
    ),
    threads AS (
        SELECT 
            s.id,
            s.title,
            s.body,
            list(c.body) as comments_list
        FROM submissions s
        JOIN comments c ON 't3_' || s.id = c.link_id
        GROUP BY s.id, s.title, s.body
    )
    SELECT * FROM threads;
"""
rows = conn.execute(query).fetchall()

thread_id, title, body, comments = rows[0]
print(f"--- THREAD ID: {thread_id} ---")
print(f"TITLE: {title}")
print(f"BODY:\n{body}")
print("-" * 20)
for i, c in enumerate(comments):
    print(f"COMMENT {i}:")
    print(c)
    print("-" * 20)
