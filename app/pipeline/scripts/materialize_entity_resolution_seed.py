import argparse
import json
import os
import sys

import pandas as pd

# Allow imports from the src directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline.utils.db import get_duckdb_connection  # noqa: E402
from pipeline.utils.paths import get_read_path  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Materialize a seed dataset for entity resolution offline evals.")
    parser.add_argument("-n", "--num-samples", type=int, default=20, help="Number of unique nodes to sample.")
    args = parser.parse_args()

    # Read from entity discovery results (flat table: bundle_id, submission_id, verbatim_quote, node_id)
    source_results = get_read_path("silver/reddit_entity_discovery_results/*/*/*.parquet")
    source_submissions = get_read_path("bronze/reddit_*_submissions.parquet")
    source_comments = get_read_path("bronze/reddit_*_comments.parquet")

    print(f"Loading discovery results from {source_results}...")

    with get_duckdb_connection() as con:
        try:
            # Sample N unique node_ids, then pull all their quotes
            discovery_query = f"""
                WITH sampled_nodes AS (
                    SELECT DISTINCT node_id
                    FROM read_parquet('{source_results}', union_by_name=true)
                    USING SAMPLE {args.num_samples}
                )
                SELECT dr.*
                FROM read_parquet('{source_results}', union_by_name=true) dr
                SEMI JOIN sampled_nodes sn ON dr.node_id = sn.node_id
            """
            discovery_df = con.execute(discovery_query).fetchdf()
        except Exception as e:
            print(f"Failed to load discovery results: {e}")
            return

        if discovery_df.empty:
            print("No discovery results found. Ensure upstream assets are materialized.")
            return

        # Group by node_id
        grouped = (
            discovery_df.groupby("node_id")
            .agg(
                submission_id=("submission_id", "first"),
                verbatim_quotes=("verbatim_quote", list),
            )
            .reset_index()
        )

        unique_node_ids = grouped["node_id"].tolist()
        print(f"Sampled {len(unique_node_ids)} unique nodes with {len(discovery_df)} total quotes.")

        # Join to bronze for original text
        node_ids_df = pd.DataFrame({"node_id": unique_node_ids})  # noqa: F841
        text_query = f"""
            WITH target_nodes AS (
                SELECT node_id FROM node_ids_df
            )
            SELECT n.node_id, COALESCE(s.title || ' ' || COALESCE(s.selftext, ''), c.body, '') AS full_text
            FROM target_nodes n
            LEFT JOIN read_parquet('{source_submissions}') s
                ON COALESCE(CAST(s.name AS VARCHAR), 't3_' || CAST(s.id AS VARCHAR)) = n.node_id
            LEFT JOIN read_parquet('{source_comments}') c
                ON COALESCE(CAST(c.name AS VARCHAR), 't1_' || CAST(c.id AS VARCHAR)) = n.node_id
        """

        try:
            text_df = con.execute(text_query).fetchdf()
        except Exception as e:
            print(f"Failed to join node text: {e}")
            return

    node_text_map = dict(zip(text_df["node_id"], text_df["full_text"]))

    # Build seed data
    seed_data = []
    for _, row in grouped.iterrows():
        node_id = row["node_id"]
        seed_data.append(
            {
                "node_id": node_id,
                "submission_id": row["submission_id"],
                "node_text": node_text_map.get(node_id, ""),
                "verbatim_quotes": row["verbatim_quotes"],
                "expected_resolutions": [],  # Human annotator fills this in!
            }
        )

    out_dir = os.path.join(os.path.dirname(__file__), "..", "evals", "datasets")
    os.makedirs(out_dir, exist_ok=True)

    out_file = os.path.join(out_dir, "entity_resolution_seed.json")
    with open(out_file, "w") as f:
        json.dump(seed_data, f, indent=2)

    print(f"\nMaterialized {len(seed_data)} seed nodes to {out_file}")
    print("Please manually edit the 'expected_resolutions' array to contain golden labels.")
    print("Example format:")
    print(
        json.dumps(
            {
                "verbatim_quote": "Darn Tough",
                "brand": "Darn Tough",
                "product_line": None,
                "product_model": None,
                "specificity_level": "BRAND_ONLY",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
