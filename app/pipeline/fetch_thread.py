import duckdb
import random

conn = duckdb.connect()

query = """
    WITH submissions AS (
        SELECT id, title, selftext as body, score, author
        FROM read_parquet('/Users/matt/src/mattwalters/buyitforlifeclub/data/BuyItForLife_submissions.parquet')
        WHERE score > 200 AND length(title) > 10
    ),
    comments AS (
        SELECT link_id, body, score, author
        FROM read_parquet('/Users/matt/src/mattwalters/buyitforlifeclub/data/BuyItForLife_comments.parquet')
        WHERE score > 10
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
        HAVING length(list(c.body)) >= 3 AND length(list(c.body)) <= 8
    )
    SELECT * FROM threads USING SAMPLE 10;
"""

rows = conn.execute(query).fetchall()
row = random.choice(rows)

thread_id, title, body, comments = row

print(f"--- THREAD ID: {thread_id} ---")
print(f"TITLE: {title}")
print(f"BODY:\n{body}")
print("-" * 20)
for i, c in enumerate(comments):
    print(f"COMMENT {i}:")
    print(c)
    print("-" * 20)

