import json
import os

import duckdb
from pydantic import TypeAdapter

from pipeline.schemas.reddit_llm_payloads import SilverRedditLlmPayload
from pipeline.utils.paths import get_read_path


def main():
    con = duckdb.connect()

    # Attempt to load a sample of existing payloads
    source_payloads = get_read_path("silver/reddit_llm_payloads/*/*/*.parquet")

    query = f"""
        SELECT * FROM read_parquet('{source_payloads}', union_by_name=true)
        WHERE length(nodes) > 0
        USING SAMPLE 20
    """

    try:
        df = con.execute(query).fetchdf()
    except Exception as e:
        print(f"Failed to load sample, ensure upstream assets are generated: {e}")
        return

    records = df.to_dict("records")
    payloads = TypeAdapter(list[SilverRedditLlmPayload]).validate_python(records)

    seed_data = []
    for p in payloads:
        seed_data.append(
            {
                "bundle_id": p.bundle_id,
                "submission_id": p.submission_id,
                "payload": p.model_dump(),
                "expected_entities": [],  # Human needs to populate this! e.g. [{"verbatim_quote": "Darn Tough", "block_indexes": [0]}]
            }
        )

    out_dir = os.path.join(os.path.dirname(__file__), "..", "evals", "datasets")
    os.makedirs(out_dir, exist_ok=True)

    out_file = os.path.join(out_dir, "offline_seed.json")
    with open(out_file, "w") as f:
        json.dump(seed_data, f, indent=2)

    print(f"Materialized {len(seed_data)} seed Payloads to {out_file}")
    print("Please manually edit the 'expected_entities' array to contain the Golden Answers for evaluation.")


if __name__ == "__main__":
    main()
