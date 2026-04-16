import duckdb
import pytest
from dagster import MultiPartitionKey, build_asset_context

from pipeline.defs.silver.reddit_node_summarizations import silver_reddit_node_summarizations
from pipeline.utils.ai import AiModel, calculate_cost


def test_calculate_cost():
    """Verify standard logic for calculating cost matches flash-lite pricing standard."""
    prompt = 1_000_000
    completion = 1_000_000
    # Flash Lite should be $0.10 in, $0.40 out
    cost = calculate_cost(AiModel.GEMINI_2_5_FLASH_LITE, prompt, completion)
    assert cost == 0.50


def test_silver_reddit_node_summarizations_execution(monkeypatch, tmp_path):
    """Integration test using Dagster context, skipping GenAI explicitly."""
    silver_dir = tmp_path / "silver"
    bronze_dir = tmp_path / "bronze"
    target_bundles = silver_dir / "chain_bundles/subreddit=buyitforlife/date=2011-01-01"

    silver_dir.mkdir(parents=True)
    bronze_dir.mkdir(parents=True)
    target_bundles.mkdir(parents=True)

    con = duckdb.connect()

    # Mock Bronze Submissions & Comments
    con.execute(
        f"""
        CREATE TABLE sub (id VARCHAR, name VARCHAR, subreddit VARCHAR, title VARCHAR, selftext VARCHAR);
        INSERT INTO sub VALUES ('s1', 't3_s1', 'buyitforlife', 'Title S1', 'Giant body text 1');
        INSERT INTO sub VALUES ('s2', 't3_s2', 'buyitforlife', 'Title S2', 'Giant body text 2'); -- Not flagged

        COPY sub TO '{bronze_dir}/reddit_buyitforlife_submissions.parquet' (FORMAT PARQUET);

        CREATE TABLE com (id VARCHAR, name VARCHAR, subreddit VARCHAR, body VARCHAR);
        INSERT INTO com VALUES ('c1', 't1_c1', 'buyitforlife', 'Giant body text of comment c1');

        COPY com TO '{bronze_dir}/reddit_buyitforlife_comments.parquet' (FORMAT PARQUET);
        
        -- Mock Chain Bundles
        CREATE TABLE bundles (
            bundle_id VARCHAR, 
            submission_id VARCHAR, 
            chain_id VARCHAR, 
            reddit_node_id VARCHAR, 
            sequence_order BIGINT, 
            is_canonical BOOLEAN, 
            needs_summarization BOOLEAN
        );
        INSERT INTO bundles VALUES ('s1_b0', 's1', 'ch_1', 't3_s1', 1, true, true);
        INSERT INTO bundles VALUES ('s1_b0', 's1', 'ch_1', 't1_c1', 2, true, true);
        INSERT INTO bundles VALUES ('s2_b0', 's2', 'ch_2', 't3_s2', 1, true, false); -- should be ignored
        
        COPY bundles TO '{target_bundles}/bundles.parquet' (FORMAT PARQUET);
    """
    )

    def mock_get_read_path(filename):
        if "bronze" in filename:
            return str(bronze_dir / filename.split("/")[-1])
        return str(silver_dir / filename.split("/")[-4] / filename.split("/")[-3] / filename.split("/")[-2] / filename.split("/")[-1])

    def mock_get_write_path(filename):
        target = tmp_path / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target)

    def mock_invoke_summarize_node(client, text, model=AiModel.GEMINI_2_5_FLASH_LITE):
        return {
            "summary": f"[MOCKED SUMMARY OF: {text[:10]}]",
            "prompt_tokens": 100,
            "completion_tokens": 10,
            "cost_usd": 0.0001
        }

    def mock_get_client():
        return "mocked_client"

    monkeypatch.setattr("pipeline.defs.silver.reddit_node_summarizations.get_read_path", mock_get_read_path)
    monkeypatch.setattr("pipeline.defs.silver.reddit_node_summarizations.get_write_path", mock_get_write_path)
    monkeypatch.setattr("pipeline.defs.silver.reddit_node_summarizations.get_client", mock_get_client)
    monkeypatch.setattr("pipeline.defs.silver.reddit_node_summarizations.invoke_summarize_node", mock_invoke_summarize_node)

    partition_key = MultiPartitionKey({"date": "2011-01-01", "subreddit": "buyitforlife"})
    context = build_asset_context(partition_key=partition_key)

    result = silver_reddit_node_summarizations(context)

    metadata = result.metadata
    assert metadata["target_file"] is not None
    assert metadata["cost_usd"].value == pytest.approx(0.0002) # two items * 0.0001

    output_parquet = tmp_path.joinpath("silver/reddit_node_summarizations/subreddit=buyitforlife/date=2011-01-01/summarizations.parquet")
    assert output_parquet.exists()

    # Read output and verify
    out_df = con.execute(f"SELECT * FROM read_parquet('{output_parquet}') ORDER BY reddit_node_id").fetchdf()

    assert len(out_df) == 2 # Only the two with needs_summarization=True
    assert out_df.iloc[0]["reddit_node_id"] == "t1_c1"
    assert out_df.iloc[0]["summary"] == "[MOCKED SUMMARY OF: Giant body]"
    assert out_df.iloc[1]["reddit_node_id"] == "t3_s1"
    assert out_df.iloc[1]["summary"] == "[MOCKED SUMMARY OF: Title S1 G]"

