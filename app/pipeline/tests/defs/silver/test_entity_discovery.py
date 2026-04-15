import json
from unittest.mock import MagicMock

from dagster import build_asset_context

from pipeline.defs.silver.entity_discovery import silver_entity_discovery_payloads
from pipeline.defs.silver.shared import SilverLLMConfig
from pipeline.utils.llm import ThreadDiscoveryResult


def test_silver_entity_discovery_payloads_success(mocker, monkeypatch, tmp_path):
    # Overwrite the environment keys natively so the test avoids the API safety catch
    monkeypatch.setenv("GEMINI_API_KEY", "mock_key_for_testing")

    # 1. Mock the Data Sink (get_write_path) to use our pytest temp directory
    mock_write_path = mocker.patch("pipeline.defs.silver.entity_discovery.get_write_path")
    mock_write_path.return_value = tmp_path / "test_output.parquet"

    # 2. Mock the Data Source (DuckDB Queries)
    # We mock the context manager so it yields a mock connection
    mock_db_context = mocker.patch("pipeline.defs.silver.entity_discovery.get_duckdb_connection")
    mock_conn = MagicMock()
    mock_db_context.return_value.__enter__.return_value = mock_conn

    # The first call to conn.execute().fetchall() is the core data load
    mock_conn.execute.return_value.fetchall.return_value = [
        # (submission_id, title, body, created_utc, comments_list)
        (
            "t3_123",
            "Best Boots?",
            "I need BIFL boots",
            "2024-01-01",
            [{"id": "c1", "parent_id": "t3_t3_123", "body": "Get Red Wings", "created_utc": "2024-01-02"}],
        )
    ]

    # 3. Mock the shared thread discovery function (process_thread_discovery)
    mock_thread_discovery = mocker.patch(
        "pipeline.defs.silver.entity_discovery.process_thread_discovery", new_callable=mocker.AsyncMock
    )
    mock_thread_discovery.return_value = ThreadDiscoveryResult(
        chunk_payloads=[
            {
                "chunk_id": "t3_123_chunk_0",
                "submission_id": "t3_123",
                "chunk_index": 0,
                "target_authored_at": "2024-01-01",
                "model_used": "gemini-2.5-flash",
                "thinking_level": None,
                "input_tokens": 100,
                "output_tokens": 50,
                "cost_usd": 0.001,
                "raw_json_output": json.dumps([{"brand": "Red Wing", "productName": "Iron Ranger"}]),
                "title": "Best Boots?",
                "body": "I need BIFL boots",
                "content_blocks_json": json.dumps([{"block_id": 0, "author_id": "OP", "text": "Best Boots?"}]),
                "full_prompt_text": "mock prompt",
            }
        ],
        all_items=[{"brand": "Red Wing", "productName": "Iron Ranger"}],
        total_cost=0.001,
        total_input_tokens=100,
        total_output_tokens=50,
        errors=[],
    )

    # 4. Rig the Dagster Context & Config
    # Partitions match our expected daily partitions in Dagster
    context = build_asset_context(partition_key="2024-01-01")
    config = SilverLLMConfig(model="gemini-2.5-flash", limit=1, thinking=None)

    # 5. EXECUTE THE ASSET!
    result = silver_entity_discovery_payloads(context, config)

    # 6. ASSERTS
    # Verify the DuckDB pipeline was queried successfully
    assert mock_conn.execute.call_count >= 1

    # Verify the shared thread discovery function was called once (one thread)
    assert mock_thread_discovery.call_count == 1

    # Verify Dagster properly yielded its Metadata
    # (Dagster passes primitives natively, but wraps explicit MetadataValue types)
    assert result.metadata["threads_processed"] == 1
    assert result.metadata["payloads_generated"] == 1
    assert result.metadata["model_used"] == "gemini-2.5-flash"

    # Verify that your cost calculator worked automatically via our mocked tokens
    assert result.metadata["cost_usd"].value > 0.0

    # Verify it saved to our Pytest Temp Path
    # (DuckDB executes the Parquet save natively)
    save_call = mock_conn.execute.call_args_list[-1]
    assert "COPY (SELECT * FROM df_view) TO" in str(save_call)
    assert str(tmp_path) in str(save_call)
