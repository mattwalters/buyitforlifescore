import duckdb
import pandas as pd
from dagster import MultiPartitionKey, build_asset_context

from pipeline.defs.silver.chain_bundles import SilverChainBundlesConfig, build_chain_bundles, silver_reddit_chain_bundles


def test_build_chain_bundles_under_budget():
    """Test standard chunking where nodes easily fit within budget."""
    chains = [
        {"submission_id": "S1", "chain_id": "CH1", "reddit_node_id": "t3_S1", "sequence_order": 1, "is_canonical": True}, # 1000 cost. Total: 1000
        {"submission_id": "S1", "chain_id": "CH1", "reddit_node_id": "t1_C1", "sequence_order": 2, "is_canonical": True}, # 500 cost. Total: 1500
        {"submission_id": "S1", "chain_id": "CH2", "reddit_node_id": "t3_S1", "sequence_order": 1, "is_canonical": False}, # 0 cost (already in bundle)
        {"submission_id": "S1", "chain_id": "CH2", "reddit_node_id": "t1_C2", "sequence_order": 2, "is_canonical": True}, # 500 cost. Total 2000
    ]
    
    lengths_map = {"t3_S1": 1000, "t1_C1": 500, "t1_C2": 500}
    config = SilverChainBundlesConfig(max_bundle_budget=3000, max_context_length=5000, summarized_context_length_estimate=500)
    
    bundles = build_chain_bundles(chains, lengths_map, config)
    
    # Everything fits in 1 bundle
    assert len(set([b["bundle_id"] for b in bundles])) == 1
    assert len(bundles) == 3
    # All analysis nodes should not need summarization
    for b in bundles:
        assert not b["needs_summarization"]


def test_build_chain_bundles_over_budget():
    """Test that a large comment trips the bundle limit correctly."""
    chains = [
        {"submission_id": "S1", "chain_id": "CH1", "reddit_node_id": "t3_S1", "sequence_order": 1, "is_canonical": True}, # length=1000
        {"submission_id": "S1", "chain_id": "CH1", "reddit_node_id": "t1_C1", "sequence_order": 2, "is_canonical": True}, # length=1500. Total CH1=2500
        {"submission_id": "S1", "chain_id": "CH2", "reddit_node_id": "t3_S1", "sequence_order": 1, "is_canonical": False}, # length=1000 (re-evaluated for Bundle 2)
        {"submission_id": "S1", "chain_id": "CH2", "reddit_node_id": "t1_C2", "sequence_order": 2, "is_canonical": True}, # length=1500. Total CH2=2500
    ]
    
    lengths_map = {"t3_S1": 1000, "t1_C1": 1500, "t1_C2": 1500}
    config = SilverChainBundlesConfig(max_bundle_budget=3000, max_context_length=5000)
    
    bundles = build_chain_bundles(chains, lengths_map, config)
    df = pd.DataFrame(bundles)
    
    # Because CH1=2500, adding CH2(cost=1500, S1 is free) would push bundle to 4000. 
    # Wait, in CH2, S1 is already in bundle. So marginal cost is just 1500 for C2!
    # Wait! current_bundle_budget=2500. Adding C2 (+1500) makes it exactly 4000 > 3000 max.
    # Therefore it should split!
    
    assert list(df[df["chain_id"] == "CH1"]["bundle_id"].unique()) == ["S1_b0"]
    assert list(df[df["chain_id"] == "CH2"]["bundle_id"].unique()) == ["S1_b1"]
    
    # Verify S1 behavior in the second bundle.
    # When evaluated for Bundle 2, S1 is "seen_nodes_global", so is_analysis=False
    s1_b1 = df[(df["bundle_id"] == "S1_b1") & (df["reddit_node_id"] == "t3_S1")]
    assert len(s1_b1) == 1
    assert s1_b1.iloc[0]["is_canonical"] == False


def test_giant_root_context_summarization():
    """Test the constraint that a giant root gets summarized on subsequent bundles."""
    chains = [
        {"submission_id": "S1", "chain_id": "CH1", "reddit_node_id": "t3_S1", "sequence_order": 1, "is_canonical": True}, # length=35000!
        {"submission_id": "S1", "chain_id": "CH1", "reddit_node_id": "t1_C1", "sequence_order": 2, "is_canonical": True}, # length=500. 
        {"submission_id": "S1", "chain_id": "CH2", "reddit_node_id": "t3_S1", "sequence_order": 1, "is_canonical": False}, 
        {"submission_id": "S1", "chain_id": "CH2", "reddit_node_id": "t1_C2", "sequence_order": 2, "is_canonical": True}, # length=500. 
    ]
    
    lengths_map = {"t3_S1": 35000, "t1_C1": 500, "t1_C2": 500}
    # Using the 30k limit constraint, 500 length context constraint
    config = SilverChainBundlesConfig(max_bundle_budget=30000, max_context_length=1000, summarized_context_length_estimate=500)
    
    bundles = build_chain_bundles(chains, lengths_map, config)
    df = pd.DataFrame(bundles)

    # First chain: S1 (35000) + C1 (500) = 35500. Exceeds budget on first item!
    # Because current_bundle is empty, it adds S1+C1 as an oversized bundle.
    assert "S1_b0" in df["bundle_id"].values
    
    # Looking at S1 in bundle 0
    s1_b0 = df[(df["bundle_id"] == "S1_b0") & (df["reddit_node_id"] == "t3_S1")].iloc[0]
    assert s1_b0["needs_summarization"] == False, "Analysis nodes must never be summarized"

    # Second chain: S1 (35000). But wait, S1 was globally seen! is_analysis=False.
    # It evaluates S1 cost -> >1000 -> summarizes -> cost=500.
    # C2 -> cost=500.
    # Total cost for CH2 = 1000. It fits perfectly into Bundle 1!
    assert "S1_b1" in df["bundle_id"].values
    s1_b1 = df[(df["bundle_id"] == "S1_b1") & (df["reddit_node_id"] == "t3_S1")].iloc[0]
    
    assert s1_b1["is_canonical"] == False
    assert s1_b1["needs_summarization"] == True, "Excessive context nodes should be summarized"


def test_silver_reddit_chain_bundles_execution(monkeypatch, tmp_path):
    """Integration test using Dagster context."""
    silver_dir = tmp_path / "silver"
    bronze_dir = tmp_path / "bronze"
    target_chain = silver_dir / "chains/subreddit=buyitforlife/date=2011-01-01"
    silver_dir.mkdir(parents=True)
    bronze_dir.mkdir(parents=True)
    target_chain.mkdir(parents=True)

    con = duckdb.connect()

    # Create mock parquets
    con.execute(
        f"""
        CREATE TABLE sub (id VARCHAR, name VARCHAR, subreddit VARCHAR, title VARCHAR, selftext VARCHAR);
        INSERT INTO sub VALUES ('s1', 't3_s1', 'buyitforlife', 'the title', 'the text');

        COPY sub TO '{bronze_dir}/reddit_buyitforlife_submissions.parquet' (FORMAT PARQUET);

        CREATE TABLE com (id VARCHAR, name VARCHAR, subreddit VARCHAR, body VARCHAR);
        INSERT INTO com VALUES ('c1', 't1_c1', 'buyitforlife', 'the body');

        COPY com TO '{bronze_dir}/reddit_buyitforlife_comments.parquet' (FORMAT PARQUET);
        
        CREATE TABLE chains (submission_id VARCHAR, chain_id VARCHAR, reddit_node_id VARCHAR, sequence_order BIGINT, is_canonical BOOLEAN);
        INSERT INTO chains VALUES ('s1', 'chain_1', 't3_s1', 1, true);
        INSERT INTO chains VALUES ('s1', 'chain_1', 't1_c1', 2, true);
        
        COPY chains TO '{target_chain}/chains.parquet' (FORMAT PARQUET);
    """
    )
    
    # Mock paths
    def mock_get_read_path(filename):
        if "bronze" in filename:
            return str(bronze_dir / filename.split("/")[-1])
        return str(silver_dir / filename.split("/")[-4] / filename.split("/")[-3] / filename.split("/")[-2] / filename.split("/")[-1])

    def mock_get_write_path(filename):
        target = tmp_path / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target)

    monkeypatch.setattr("pipeline.defs.silver.chain_bundles.get_read_path", mock_get_read_path)
    monkeypatch.setattr("pipeline.defs.silver.chain_bundles.get_write_path", mock_get_write_path)

    partition_key = MultiPartitionKey({"date": "2011-01-01", "subreddit": "buyitforlife"})
    context = build_asset_context(partition_key=partition_key)

    config = SilverChainBundlesConfig(validation_sample_size=None)

    result = silver_reddit_chain_bundles(context, config)

    assert result.metadata["target_file"] is not None
    output_parquet = tmp_path.joinpath("silver/chain_bundles/subreddit=buyitforlife/date=2011-01-01/bundles.parquet")
    assert output_parquet.exists()
    
    # Read output and verify
    out_df = con.execute(f"SELECT * FROM read_parquet('{output_parquet}')").fetchdf()
    assert len(out_df) == 2
    assert 'needs_summarization' in out_df.columns
