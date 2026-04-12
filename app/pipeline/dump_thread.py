import json
import duckdb

conn = duckdb.connect()
query = """
    WITH submissions AS (
        SELECT id, title, selftext as body, score, author
        FROM read_parquet('/Users/matt/src/mattwalters/buyitforlifeclub/data/BuyItForLife_submissions.parquet')
        WHERE id = 'ateshn'
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
with open('thread_dump.txt', 'w') as f:
    f.write(f"TITLE: {title}\n")
    f.write(f"BODY:\n{body}\n")
    for i, c in enumerate(comments):
        f.write(f"COMMENT {i}:\n{c}\n")

print("Dumped to thread_dump.txt")
