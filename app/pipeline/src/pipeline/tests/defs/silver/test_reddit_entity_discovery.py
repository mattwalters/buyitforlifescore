import json

import duckdb
import pytest
from dagster import MultiPartitionKey, build_asset_context

from pipeline.defs.silver.reddit_entity_discovery import silver_reddit_entity_discovery


def test_silver_reddit_entity_discovery_execution(monkeypatch, tmp_path):
    # Setup directories
    silver_dir = tmp_path / "silver"
    payload_dir = silver_dir / "reddit_llm_payloads/subreddit=buyitforlife/date=2011-01-01"
    payload_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # Mock Silver payloads upstream
    con.execute(
        f"""
        CREATE TABLE payload (
            bundle_id VARCHAR,
            submission_id VARCHAR,
            nodes STRUCT(
                chain_id VARCHAR,
                sequence_order BIGINT,
                reddit_node_id VARCHAR,
                author VARCHAR,
                created_utc BIGINT,
                link_flair_text VARCHAR,
                is_canonical BOOLEAN,
                needs_summarization BOOLEAN,
                text VARCHAR,
                summary VARCHAR
            )[]
        );
        INSERT INTO payload VALUES (
            'b1', 's1', 
            [
                {{'chain_id':'c1', 'sequence_order':1, 'reddit_node_id':'t3_1', 'author':'usr1', 'created_utc':1, 'link_flair_text':NULL, 'is_canonical':false, 'needs_summarization':false, 'text':'context text', 'summary':NULL}},
                {{'chain_id':'c1', 'sequence_order':2, 'reddit_node_id':'t1_2', 'author':'usr2', 'created_utc':2, 'link_flair_text':NULL, 'is_canonical':true, 'needs_summarization':false, 'text':'darn tough rulez', 'summary':NULL}}
            ]
        );
        
        COPY payload TO '{payload_dir}/payloads.parquet' (FORMAT PARQUET);
        """
    )

    def mock_get_read_path(filename):
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

    def mock_invoke_entity_discovery(client, payload):
        from pipeline.schemas.reddit_entity_discovery import DiscoveredEntity, DiscoveryResult

        return DiscoveryResult(
            bundle_id=payload.bundle_id,
            submission_id=payload.submission_id,
            items=[DiscoveredEntity(verbatim_quote="darn tough", node_ids=["t1_2"])],
            raw_json=json.dumps([{"verbatim_quote": "darn tough", "block_indexes": [1]}]),
            cost_usd=0.01,
            prompt_tokens=100,
            completion_tokens=20,
            prompt_text="mock",
        )

    def mock_get_client():
        return "mocked_client"

    monkeypatch.setattr("pipeline.defs.silver.reddit_entity_discovery.get_read_path", mock_get_read_path)
    monkeypatch.setattr("pipeline.defs.silver.reddit_entity_discovery.get_write_path", mock_get_write_path)
    monkeypatch.setattr("pipeline.defs.silver.reddit_entity_discovery.get_client", mock_get_client)
    monkeypatch.setattr(
        "pipeline.defs.silver.reddit_entity_discovery.invoke_entity_discovery", mock_invoke_entity_discovery
    )

    partition_key = MultiPartitionKey({"date": "2011-01-01", "subreddit": "buyitforlife"})
    context = build_asset_context(partition_key=partition_key)

    result = silver_reddit_entity_discovery(context)

    assert result.metadata["cost_usd"].value == pytest.approx(0.01)

    output_parquet = tmp_path.joinpath(
        "silver/reddit_entity_discovery/subreddit=buyitforlife/date=2011-01-01/entities.parquet"
    )
    assert output_parquet.exists()

    # Verify parqeut shape
    out_df = con.execute(f"SELECT * FROM read_parquet('{output_parquet}')").fetchdf()
    assert len(out_df) == 1
    assert out_df.iloc[0]["bundle_id"] == "b1"
    assert len(out_df.iloc[0]["items"]) == 1
    assert out_df.iloc[0]["items"][0]["verbatim_quote"] == "darn tough"
