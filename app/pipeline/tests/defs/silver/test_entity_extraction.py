import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
from dagster import build_asset_context

from pipeline.defs.silver.entity_extraction import silver_entity_extraction_payloads
from pipeline.defs.silver.shared import SilverLLMConfig


def test_silver_entity_extraction_payloads_success(mocker, tmp_path):
    # 1. Mock the File existence check to bypassed skipping
    mocker.patch("pipeline.defs.silver.entity_extraction.os.path.exists", return_value=True)

    # 2. Mock the Data Sink to use temp path
    mock_write_path = mocker.patch("pipeline.defs.silver.entity_extraction.get_write_path")
    mock_write_path.return_value = tmp_path / "test_extraction_output.parquet"

    # 3. Mock the DuckDB Connection to return a pandas DataFrame for .df()
    mock_db_context = mocker.patch("pipeline.defs.silver.entity_extraction.get_duckdb_connection")
    mock_conn = MagicMock()
    mock_db_context.return_value.__enter__.return_value = mock_conn

    # Simulate a single Discovery row passed upstream
    mock_conn.execute.return_value.df.return_value = pd.DataFrame(
        [
            {
                "chunk_id": "chunk_123",
                "author_id": "auth1",
                "brand": "Patagonia",
                "productName": "Synchilla",
                "target_text": "I love my jacket",
                "target_authored_at": "2024-01-01",
                "parent_text": "",
            }
        ]
    )

    # 4. Mock the LLM API (Google GenAI)
    mock_genai_client = mocker.patch("pipeline.defs.silver.entity_extraction.genai.Client")
    mock_api_call = mocker.AsyncMock()

    fake_response = MagicMock()
    fake_response.text = json.dumps(
        {"sentiment": "positive", "lifespan_years": 10, "quality_attributes": ["warm", "durable"]}
    )

    # Use SimpleNamespace to support dot-notation logic
    fake_response.usage_metadata = SimpleNamespace(
        prompt_token_count=150, candidates_token_count=20, cached_content_token_count=0, thoughts_token_count=0
    )

    mock_api_call.return_value = fake_response
    mock_genai_client.return_value.aio.models.generate_content = mock_api_call

    # 5. Build context & Config
    context = build_asset_context(partition_key="2024-01-01")
    config = SilverLLMConfig(model="gemini-2.5-flash", limit=1, thinking=None)

    # 6. Execute!
    result = silver_entity_extraction_payloads(context, config)

    # 7. Asserts
    # Verify DuckDB was queried for data and then saved to Parquet
    assert mock_conn.execute.call_count >= 2

    # Verify the LLM was called exactly once for our one simulated row
    assert mock_api_call.call_count == 1

    # Asset primitive metadata
    assert result.metadata["payloads_processed"] == 1
    assert result.metadata["model_used"] == "gemini-2.5-flash"

    # Verify explicit float calculation worked automatically and yielded properly
    assert result.metadata["total_cost_usd"] > 0.0

    # Check that disk writer successfully outputted to our tmp_path Parquet
    save_call = mock_conn.execute.call_args_list[-1]
    assert "COPY (SELECT * FROM df_view) TO" in str(save_call)
    assert str(tmp_path) in str(save_call)
