import duckdb
import pandas as pd
from dagster import build_asset_context

from pipeline.defs.ops.ai_cost_summary import ops_ai_cost_summary


def test_ops_ai_cost_summary_execution(monkeypatch, tmp_path):
    """Integration test using Dagster context."""
    silver_dir = tmp_path / "silver" / "reddit_node_summarizations" / "sub=test" / "date=2024"
    ops_dir = tmp_path / "ops" / "ai_cost_summary"

    silver_dir.mkdir(parents=True)
    ops_dir.mkdir(parents=True)

    con = duckdb.connect()

    # Mock Silver Summarizations - 2 rows
    test_df = pd.DataFrame([
        {
            "reddit_node_id": "n1",
            "summary": "text",
            "prompt_tokens": 1000,
            "completion_tokens": 100,
            "cost_usd": 0.50
        },
        {
            "reddit_node_id": "n2",
            "summary": "text",
            "prompt_tokens": 2000,
            "completion_tokens": 200,
            "cost_usd": 1.00
        }
    ])

    silver_path = silver_dir / "summarizations.parquet"
    con.execute(f"COPY (SELECT * FROM test_df) TO '{silver_path}' (FORMAT PARQUET)")

    def mock_get_read_path(pattern):
        # We know the pattern is "silver/reddit_node_summarizations/*/*/*.parquet"
        return str(tmp_path / "silver" / "reddit_node_summarizations" / "*" / "*" / "*.parquet")

    def mock_get_write_path(filename):
        return str(tmp_path / filename)

    monkeypatch.setattr("pipeline.defs.ops.ai_cost_summary.get_read_path", mock_get_read_path)
    monkeypatch.setattr("pipeline.defs.ops.ai_cost_summary.get_write_path", mock_get_write_path)

    context = build_asset_context()
    result = ops_ai_cost_summary(context)

    metadata = result.metadata
    assert metadata["target_file"] is not None

    # Read output and verify
    output_parquet = tmp_path.joinpath("ops/ai_cost_summary/summary.parquet")
    assert output_parquet.exists()

    out_df = con.execute(f"SELECT * FROM read_parquet('{output_parquet}')").fetchdf()

    # Expect 2 rows: node_summarization, and TOTAL
    assert len(out_df) == 2

    node_row = out_df.iloc[0]
    total_row = out_df.iloc[1]

    assert node_row["service_name"] == "node_summarization"
    assert node_row["total_cost_usd"] == 1.50
    assert node_row["total_prompt_tokens"] == 3000
    assert node_row["total_completion_tokens"] == 300
    assert node_row["total_nodes_processed"] == 2

    assert total_row["service_name"] == "TOTAL"
    assert total_row["total_cost_usd"] == 1.50
    assert total_row["total_prompt_tokens"] == 3000
    assert total_row["total_completion_tokens"] == 300
    assert total_row["total_nodes_processed"] == 2

def test_ops_ai_cost_summary_empty(monkeypatch, tmp_path):
    """Test when no data exists upstream."""
    ops_dir = tmp_path / "ops" / "ai_cost_summary"
    ops_dir.mkdir(parents=True)

    def mock_get_read_path(pattern):
        # Return a path that doesn't exist
        return str(tmp_path / "silver" / "nonexistent" / "*.parquet")

    def mock_get_write_path(filename):
        target = tmp_path / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target)

    monkeypatch.setattr("pipeline.defs.ops.ai_cost_summary.get_read_path", mock_get_read_path)
    monkeypatch.setattr("pipeline.defs.ops.ai_cost_summary.get_write_path", mock_get_write_path)

    context = build_asset_context()
    result = ops_ai_cost_summary(context)

    metadata = result.metadata
    assert "No upstream data observed" in metadata["data_preview"].value

    output_parquet = tmp_path.joinpath("ops/ai_cost_summary/summary.parquet")
    assert output_parquet.exists()

    con = duckdb.connect()
    out_df = con.execute(f"SELECT * FROM read_parquet('{output_parquet}')").fetchdf()
    assert len(out_df) == 0
