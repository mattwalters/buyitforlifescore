import duckdb
import pandas as pd
from dagster import MultiPartitionKey, build_asset_context

from pipeline.defs.silver.chains import SilverRedditChainsConfig, build_thread_chains, silver_reddit_chains


def test_python_recursive_chain_logic():
    """Atomic test for recursive chain logic using the new Python builder."""
    submissions = [{"submission_id": "S1", "reddit_node_id": "t3_S1", "subreddit": "buyitforlife"}]

    comments = [
        {
            "comment_id": "C1",
            "reddit_node_id": "t1_C1",
            "parent_id": "t3_S1",
            "link_id": "t3_S1",
            "subreddit": "buyitforlife",
        },
        {
            "comment_id": "C2",
            "reddit_node_id": "t1_C2",
            "parent_id": "t1_C1",
            "link_id": "t3_S1",
            "subreddit": "buyitforlife",
        },
        {
            "comment_id": "C3",
            "reddit_node_id": "t1_C3",
            "parent_id": "t3_S1",
            "link_id": "t3_S1",
            "subreddit": "buyitforlife",
        },
    ]

    records = build_thread_chains(submissions, comments)
    df = pd.DataFrame(records)

    assert len(df) == 5

    # S1 should appear twice
    s1_rows = df[df["reddit_node_id"] == "t3_S1"]
    assert len(s1_rows) == 2

    # sequence_order tests
    assert df[df["reddit_node_id"] == "t1_C2"]["sequence_order"].iloc[0] == 3


def test_silver_reddit_chains_execution(monkeypatch, tmp_path):
    """Integration test using Dagster context."""
    # Create fake parquet files in tmp_path
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True)

    con = duckdb.connect()

    # Create mock parquets
    con.execute(
        f"""
        CREATE TABLE sub (id VARCHAR, name VARCHAR, subreddit VARCHAR, created_utc BIGINT);
        INSERT INTO sub VALUES ('mock_s1', 't3_mock', 'buyitforlife', 1293840000);
        -- 1293840000 is 2011-01-01

        COPY sub TO '{bronze_dir}/reddit_buyitforlife_submissions.parquet' (FORMAT PARQUET);

        CREATE TABLE com (
            id VARCHAR, name VARCHAR, parent_id VARCHAR, link_id VARCHAR, subreddit VARCHAR, created_utc BIGINT
        );
        INSERT INTO com VALUES ('mock_c1', 't1_mock', '"t3_mock"', '"t3_mock"', 'buyitforlife', 1293840050);

        COPY com TO '{bronze_dir}/reddit_buyitforlife_comments.parquet' (FORMAT PARQUET);
    """
    )

    # Mock paths
    def mock_get_read_path(filename):
        return str(bronze_dir / filename.split("/")[-1])

    def mock_get_write_path(filename):
        target = tmp_path / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target)

    monkeypatch.setattr("pipeline.defs.silver.chains.get_read_path", mock_get_read_path)
    monkeypatch.setattr("pipeline.defs.silver.chains.get_write_path", mock_get_write_path)

    partition_key = MultiPartitionKey({"date": "2011-01-01", "subreddit": "buyitforlife"})
    context = build_asset_context(partition_key=partition_key)

    # Execute asset with config requesting 100% data validation over the test files
    config = SilverRedditChainsConfig(validation_sample_size=None)

    result = silver_reddit_chains(context, config)

    # Assert result is generated
    assert result.metadata["target_file"] is not None
    assert tmp_path.joinpath("silver/chains/subreddit=buyitforlife/date=2011-01-01/chains.parquet").exists()
