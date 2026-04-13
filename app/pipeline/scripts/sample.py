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
    
    asset_name = questionary.select(
        "Which asset do you want to safely sample?",
        choices=[
            "silver_entity_attributes",
            "silver_entity_discovery",
            "raw_reddit_buyitforlife_submissions",
            "raw_reddit_buyitforlife_comments"
        ]
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
    
    if not partitions_to_run:
        print("No partitions selected. Exiting.")
        return
        
    target_selector = f"*{asset_name}" if upstream else asset_name
    partition_string = ",".join(partitions_to_run)
    
    cmd = ["dagster", "asset", "materialize", "--select", target_selector, "--partition", partition_string]
    
    print(f"\n[EXEC] Executing: {' '.join(cmd)[:120]}...\n")
    try:
        # Since this script runs under `uv run python`, `dagster` will inherently resolve from the active `.venv`
        subprocess.run(cmd, check=True, env=os.environ.copy())
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Dagster command failed with exit code: {e.returncode}")
    except FileNotFoundError:
        print(f"\n[ERROR] 'dagster' executable not found on path! Did you run this script using `uv run`?")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCanceled by user.")
