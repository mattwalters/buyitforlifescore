import duckdb
from dagster import MultiPartitionKey, build_asset_context

from pipeline.defs.silver.chains import SilverRedditChainsConfig, silver_reddit_chains


def test_duckdb_recursive_chain_logic():
    """Atomic test for recursive CTE logic using in-memory DuckDB."""
    con = duckdb.connect()

    con.execute("""
    CREATE TABLE submissions (id VARCHAR, name VARCHAR, subreddit VARCHAR, created_utc BIGINT);
    CREATE TABLE comments (id VARCHAR, name VARCHAR, parent_id VARCHAR, subreddit VARCHAR, created_utc BIGINT);

    INSERT INTO submissions VALUES
    ('S1', 't3_S1', 'BuyItForLife', 1600000000);

    INSERT INTO comments VALUES
    ('C1', 't1_C1', '"t3_S1"', 'BuyItForLife', 1600000010),
    ('C2', 't1_C2', '"t1_C1"', 'BuyItForLife', 1600000020),
    ('C3', 't1_C3', '"t3_S1"', 'BuyItForLife', 1600000030);
    """)

    query = """
    WITH RECURSIVE chains AS (
        SELECT s.id AS submission_id, s.name AS reddit_node_id, s.subreddit,
               [s.name] AS path_nodes, s.name AS current_node, 1 AS depth
        FROM submissions s WHERE s.subreddit = 'BuyItForLife'
        UNION ALL
        SELECT p.submission_id, c.name AS reddit_node_id, c.subreddit,
               list_append(p.path_nodes, c.name) AS path_nodes, c.name AS current_node, p.depth + 1 AS depth
        FROM comments c JOIN chains p ON trim(c.parent_id, '"') = p.current_node
    ), leaf_nodes AS (
        SELECT c.* FROM chains c LEFT JOIN comments child ON trim(child.parent_id, '"') = c.current_node
        WHERE child.name IS NULL
    ), unnested_paths AS (
        SELECT
            hash(path_nodes::VARCHAR)::VARCHAR AS chain_id,
            submission_id,
            UNNEST(path_nodes) AS reddit_node_id,
            path_nodes
        FROM leaf_nodes
    ), numbered_paths AS (
        SELECT chain_id, submission_id, reddit_node_id, list_position(path_nodes, reddit_node_id) as sequence_order
        FROM unnested_paths
    )
    SELECT chain_id, submission_id, reddit_node_id, sequence_order,
           CAST(ROW_NUMBER() OVER(PARTITION BY reddit_node_id ORDER BY chain_id) = 1 AS BOOLEAN) AS is_canonical
    FROM numbered_paths ORDER BY chain_id, sequence_order;
    """

    df = con.execute(query).fetchdf()
    assert len(df) == 5

    # S1 should appear twice, once canonical and once not
    s1_rows = df[df["reddit_node_id"] == "t3_S1"]
    assert len(s1_rows) == 2
    assert s1_rows["is_canonical"].sum() == 1  # Exactly one true

    # sequence_order tests
    assert df[df["reddit_node_id"] == "t1_C2"]["sequence_order"].iloc[0] == 3


def test_silver_reddit_chains_execution(monkeypatch, tmp_path):
    """Integration test using Dagster context."""
    # Create fake parquet files in tmp_path
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True)

    con = duckdb.connect()

    # Create mock parquets
    con.execute(f"""
        CREATE TABLE sub (id VARCHAR, name VARCHAR, subreddit VARCHAR, created_utc BIGINT);
        INSERT INTO sub VALUES ('mock_s1', 't3_mock', 'BuyItForLife', 1293840000);
        -- 1293840000 is 2011-01-01

        COPY sub TO '{bronze_dir}/reddit_buyitforlife_submissions.parquet' (FORMAT PARQUET);

        CREATE TABLE com (id VARCHAR, name VARCHAR, parent_id VARCHAR, subreddit VARCHAR, created_utc BIGINT);
        INSERT INTO com VALUES ('mock_c1', 't1_mock', '"t3_mock"', 'BuyItForLife', 1293840050);

        COPY com TO '{bronze_dir}/reddit_buyitforlife_comments.parquet' (FORMAT PARQUET);
    """)

    # Mock paths
    def mock_get_read_path(filename):
        return str(bronze_dir / filename.split("/")[-1])

    def mock_get_write_path(filename):
        target = tmp_path / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target)

    monkeypatch.setattr("pipeline.defs.silver.chains.get_read_path", mock_get_read_path)
    monkeypatch.setattr("pipeline.defs.silver.chains.get_write_path", mock_get_write_path)

    partition_key = MultiPartitionKey({"date": "2011-01-01", "subreddit": "BuyItForLife"})
    context = build_asset_context(partition_key=partition_key)

    # Execute asset with config requesting 100% data validation over the test files
    config = SilverRedditChainsConfig(validation_sample_size=None)

    result = silver_reddit_chains(context, config)

    # Assert result is generated
    assert result.metadata["target_file"] is not None
    assert tmp_path.joinpath("silver/chains/subreddit=BuyItForLife/date=2011-01-01/chains.parquet").exists()
