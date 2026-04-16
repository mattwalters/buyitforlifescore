import duckdb
import pandas as pd
from dagster import MultiPartitionKey, build_asset_context

from pipeline.defs.silver.reddit_llm_payloads import silver_reddit_llm_payloads


def test_silver_reddit_llm_payloads_execution(monkeypatch, tmp_path):
    silver_dir = tmp_path / "silver"
    bronze_dir = tmp_path / "bronze"
    target_bundles = silver_dir / "chain_bundles/subreddit=buyitforlife/date=2011-01-01"
    target_summarizations = silver_dir / "reddit_node_summarizations/subreddit=buyitforlife/date=2011-01-01"

    silver_dir.mkdir(parents=True)
    bronze_dir.mkdir(parents=True)
    target_bundles.mkdir(parents=True)
    target_summarizations.mkdir(parents=True)

    con = duckdb.connect()

    con.execute(
        f"""
        CREATE TABLE sub (id VARCHAR, name VARCHAR, subreddit VARCHAR, title VARCHAR, selftext VARCHAR, author VARCHAR, created_utc BIGINT, link_flair_text VARCHAR);
        INSERT INTO sub VALUES ('s1', 't3_s1', 'buyitforlife', 'Title', 'Body', 'auth1', 123456, 'Request');
        
        COPY sub TO '{bronze_dir}/reddit_buyitforlife_submissions.parquet' (FORMAT PARQUET);

        CREATE TABLE com (id VARCHAR, name VARCHAR, subreddit VARCHAR, body VARCHAR, author VARCHAR, created_utc BIGINT);
        INSERT INTO com VALUES ('c1', 't1_c1', 'buyitforlife', 'Comment text', 'auth2', 123457);

        COPY com TO '{bronze_dir}/reddit_buyitforlife_comments.parquet' (FORMAT PARQUET);

        CREATE TABLE bundles (
            bundle_id VARCHAR, 
            submission_id VARCHAR, 
            chain_id VARCHAR, 
            reddit_node_id VARCHAR, 
            sequence_order BIGINT, 
            is_canonical BOOLEAN, 
            needs_summarization BOOLEAN
        );
        INSERT INTO bundles VALUES ('s1_b0', 's1', 'ch_1', 't3_s1', 1, false, false);
        INSERT INTO bundles VALUES ('s1_b0', 's1', 'ch_1', 't1_c1', 2, true, true);
        
        COPY bundles TO '{target_bundles}/bundles.parquet' (FORMAT PARQUET);

        CREATE TABLE sum_tbl (reddit_node_id VARCHAR, summary VARCHAR);
        INSERT INTO sum_tbl VALUES ('t1_c1', 'Summarized comment text');

        COPY sum_tbl TO '{target_summarizations}/summarizations.parquet' (FORMAT PARQUET);
        """
    )

    def mock_get_read_path(filename):
        if "bronze" in filename:
            return str(bronze_dir / filename.split("/")[-1])
        return str(
            silver_dir
            / filename.split("/")[-4]
            / filename.split("/")[-3]
            / filename.split("/")[-2]
            / filename.split("/")[-1]
        )

    def mock_get_write_path(filename):
        target = tmp_path / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target)

    monkeypatch.setattr("pipeline.defs.silver.reddit_llm_payloads.get_read_path", mock_get_read_path)
    monkeypatch.setattr("pipeline.defs.silver.reddit_llm_payloads.get_write_path", mock_get_write_path)

    partition_key = MultiPartitionKey({"date": "2011-01-01", "subreddit": "buyitforlife"})
    context = build_asset_context(partition_key=partition_key)

    result = silver_reddit_llm_payloads(context)

    metadata = result.metadata
    assert metadata["target_file"] is not None

    output_parquet = tmp_path.joinpath(
        "silver/reddit_llm_payloads/subreddit=buyitforlife/date=2011-01-01/payloads.parquet"
    )
    assert output_parquet.exists()

    out_df = con.execute(f"SELECT * FROM read_parquet('{output_parquet}')").fetchdf()

    assert len(out_df) == 1
    row = out_df.iloc[0]

    assert row["bundle_id"] == "s1_b0"
    assert row["submission_id"] == "s1"

    nodes = row["nodes"]
    assert len(nodes) == 2

    # DuckDB list of structs reads as standard Python list of dicts in Pandas
    node0 = nodes[0]
    assert node0["chain_id"] == "ch_1"
    assert node0["sequence_order"] == 1
    assert node0["reddit_node_id"] == "t3_s1"
    assert node0["author"] == "auth1"
    assert node0["created_utc"] == 123456
    assert node0["link_flair_text"] == "Request"
    assert node0["is_canonical"] == False
    assert node0["needs_summarization"] == False
    assert node0["text"] == "Title Body"
    assert pd.isna(node0["summary"]) or node0["summary"] is None

    node1 = nodes[1]
    assert node1["chain_id"] == "ch_1"
    assert node1["sequence_order"] == 2
    assert node1["reddit_node_id"] == "t1_c1"
    assert node1["author"] == "auth2"
    assert node1["created_utc"] == 123457
    assert pd.isna(node1["link_flair_text"]) or node1["link_flair_text"] is None
    assert node1["is_canonical"] == True
    assert node1["needs_summarization"] == True
    assert node1["text"] == "Comment text"
    assert node1["summary"] == "Summarized comment text"
