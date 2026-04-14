#!/usr/bin/env python3
import sys
import subprocess
import hashlib
import os
from datetime import date, timedelta
import questionary

def get_stable_sample(start_date: date, end_date: date, percent: int) -> list[str]:
    """Generates a list of purely deterministic partition dates based on an MD5 hash modulo."""
    if percent <= 0:
        return []
    if percent >= 100:
        percent = 100
        
    partitions = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        
        # Consistent stable hash mapping algorithm
        hash_hex = hashlib.md5(date_str.encode("utf-8")).hexdigest()
        hash_int = int(hash_hex, 16)
        
        # Select the lowest X% of the uniform distribution
        if (hash_int % 100) < percent:
            partitions.append(date_str)
            
        current += timedelta(days=1)
    return partitions

def main():
    print("\n--- Dagster Deterministic Random Sampler ---\n")
    
    import requests
    port = os.environ.get("PORT", "3000")
    host = os.environ.get("HOST", "localhost")
    gql_url = f"http://{host}:{port}/graphql"
    
    try:
        query = """
        query {
          assetNodes {
            assetKey { path }
            partitionDefinition { description }
            dependencies {
              asset {
                assetKey { path }
              }
            }
          }
        }
        """
        res = requests.post(gql_url, json={"query": query}).json()
        raw_nodes = res.get('data', {}).get('assetNodes', [])
        
        asset_nodes = {node['assetKey']['path'][0]: node for node in raw_nodes}
        dynamic_assets = [key for key, node in asset_nodes.items() if node.get('partitionDefinition')]
        dynamic_assets.sort()
        
        if not dynamic_assets:
            print("[ERROR] No partitioned assets found. Is the repository loaded correctly?")
            return
            
    except Exception as e:
        print(f"\n[ERROR] Could not reach Dagster daemon at {gql_url}. Is `npm run dev` running?\n({e})")
        return

    asset_name = questionary.select(
        "Which partitioned asset do you want to safely sample?",
        choices=dynamic_assets
    ).ask()

    if not asset_name:
        print("Canceled.")
        return

    percent_str = questionary.text(
        "What percentage of the historical dates do you want to sample? (1-100)",
        default="1",
        validate=lambda text: text.isdigit() and 1 <= int(text) <= 100
    ).ask()
    
    if not percent_str:
        print("Canceled.")
        return
        
    percent = int(percent_str)

    upstream = questionary.confirm(
        "Resolve missing upstream dependencies lazily? (Dagster will auto-fill what it needs)",
        default=True
    ).ask()
    
    if upstream is None:
        print("Canceled.")
        return

    print("Generating deterministically stable partitions...")
    
    # Our data ranges from early reddit 2012 up to essentially current time (or archival date: 2024-01-01)
    # Using 'today' ensures it safely bounds any new partitions without fail.
    START_DATE = date(2012, 1, 1)
    END_DATE = date.today()
    
    partitions_to_run = get_stable_sample(START_DATE, END_DATE, percent)
    
    print(f"\n[INFO] Deterministic Hashing complete: Targeting {len(partitions_to_run)} specific date partitions.")
    
    # --- FILTER ALREADY MATERIALIZED PARTITIONS ---
    materialized_query = """
    query GetMaterialized($assetKey: String!) {
      assetNodeOrError(assetKey: {path: [$assetKey]}) {
        ... on AssetNode {
          assetPartitionStatuses {
            ... on TimePartitionStatuses {
              ranges {
                status
                startTime
              }
            }
          }
        }
      }
    }
    """
    try:
        res = requests.post(gql_url, json={
            "query": materialized_query, 
            "variables": {"assetKey": asset_name}
        }).json()
        
        node = res.get("data", {}).get("assetNodeOrError", {})
        statuses = node.get("assetPartitionStatuses", {}).get("ranges", [])
        
        import datetime
        materialized_dates = set()
        for rng in statuses:
            if rng.get("status") == "MATERIALIZED":
                dt = datetime.datetime.fromtimestamp(rng["startTime"], tz=datetime.timezone.utc)
                materialized_dates.add(dt.strftime("%Y-%m-%d"))
                
        original_count = len(partitions_to_run)
        partitions_to_run = [p for p in partitions_to_run if p not in materialized_dates]
        
        if len(partitions_to_run) < original_count:
            print(f"[INFO] Skipping {original_count - len(partitions_to_run)} already materialized partitions.")
            print(f"[INFO] Remaining missing partitions to build: {len(partitions_to_run)}")
            
    except Exception as e:
        print(f"[WARNING] Could not fetch materialized status from Dagster: {e}")

    if not partitions_to_run:
        print("All targeted partitions are already materialized. Nothing to do! Exiting.")
        return
        
    print(f"\n[EXEC] Enqueueing {len(partitions_to_run)} targeted executions into Dagster Daemon concurrently...\n")
    
    import requests

    ranges = [{"start": p, "end": p} for p in partitions_to_run]
    
    # -----------------------------------------------------
    # DAG RESOLUTION LOGIC
    # -----------------------------------------------------
    target_assets = {asset_name}
    
    if upstream:
        def get_upstream(asset_key):
            node = asset_nodes.get(asset_key)
            if not node: return
            for dep in node.get('dependencies', []):
                dep_key = dep['asset']['assetKey']['path'][0]
                dep_node = asset_nodes.get(dep_key)
                
                # We strictly only append partitioned assets to the backfill!
                if dep_node and dep_node.get('partitionDefinition'):
                    target_assets.add(dep_key)
                    get_upstream(dep_key)
                    
        get_upstream(asset_name)
    
    # Build array for LaunchPartitionBackfill
    partitions_by_assets = [
        {
            "assetKey": {"path": [a]},
            "partitions": {"ranges": ranges}
        } for a in target_assets
    ]
        
    query = """
    mutation LaunchPartitionBackfill($backfillParams: LaunchBackfillParams!) {
      launchPartitionBackfill(backfillParams: $backfillParams) {
        __typename
        ... on LaunchBackfillSuccess {
          backfillId
        }
        ... on PythonError {
          message
        }
      }
    }
    """
    
    variables = {
        "backfillParams": {
            "partitionsByAssets": partitions_by_assets
        }
    }
    
    try:
        port = os.environ.get("PORT", "3000")
        host = os.environ.get("HOST", "localhost")
        gql_url = f"http://{host}:{port}/graphql"
        
        print(f"Dispatching Native Root Backfill via GraphQL to {gql_url}...")
        res = requests.post(gql_url, json={"query": query, "variables": variables})
        data = res.json()
        
        if "errors" in data and data["errors"]:
            print(f"\n[ERROR] GraphQL Rejection: {data['errors']}")
            return

        result = data.get("data", {}).get("launchPartitionBackfill", {})
        
        if result.get("__typename") == "LaunchBackfillSuccess":
            print(f"\n[SUCCESS] Native dagster backfill queued successfully! Switch to the Dagster UI to natively monitor completion.")
            print(f"[ID] {result.get('backfillId')}")
        else:
            print(f"\n[ERROR] Failed to enqueue natively: {result.get('message', str(result))}")
            
    except requests.exceptions.ConnectionError:
        print(f"\n[ERROR] Could not connect to Dagster. Is it running locally on port {port}?")
    except Exception as e:
        print(f"\n[ERROR] Unknown error: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCanceled by user.")
