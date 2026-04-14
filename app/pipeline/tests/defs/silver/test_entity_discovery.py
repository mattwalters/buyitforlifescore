import pytest
import json
from unittest.mock import MagicMock
from dagster import build_asset_context
from google.genai import types

from pipeline.defs.silver.shared import SilverLLMConfig
from pipeline.defs.silver.entity_discovery import silver_entity_discovery_payloads

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
        ("t3_123", "Best Boots?", "I need BIFL boots", "2024-01-01", [{"body": "Get Red Wings", "created_utc": "2024-01-02"}])
    ]
    
    # 3. Mock the LLM API (Google GenAI)
    # We mock the client instantiation inside _process_discovery_batch
    mock_genai_client = mocker.patch("pipeline.defs.silver.entity_discovery.genai.Client")
    
    # Set up the AsyncMock for the API call
    mock_api_call = mocker.AsyncMock()
    
    # Construct a valid fake response
    fake_response = MagicMock()
    fake_response.text = json.dumps([
        {"brand": "Red Wing", "productName": "Iron Ranger"}
    ])
    
    # Construct fake token usage to ensure your cost calculation triggers nicely!
    # By using a SimpleNamespace, we provide an object that supports standard dot-notation
    # (which the loop uses) and avoids MagicMock's nested mock returning issues.
    from types import SimpleNamespace
    fake_response.usage_metadata = SimpleNamespace(
        prompt_token_count=100,
        candidates_token_count=50,
        cached_content_token_count=0,
        thoughts_token_count=0
    )
    
    mock_api_call.return_value = fake_response
    mock_genai_client.return_value.aio.models.generate_content = mock_api_call
    
    # 4. Rig the Dagster Context & Config
    # Partitions match our expected daily partitions in Dagster
    context = build_asset_context(partition_key="2024-01-01")
    config = SilverLLMConfig(model="gemini-2.5-flash", limit=1, thinking=None)
    
    # 5. EXECUTE THE ASSET!
    result = silver_entity_discovery_payloads(context, config)
    
    # 6. ASSERTS
    # Verify the DuckDB pipeline was queried successfully
    assert mock_conn.execute.call_count >= 1
    
    # Verify the Google GenAI Endpoint was hit for prediction
    assert mock_api_call.call_count == 1
    
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
