import sys
import json
import asyncio
import time
import csv
import argparse
from pathlib import Path
from difflib import SequenceMatcher

# Allow import of the pipeline package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from pipeline.defs.silver import _process_thread_batch

def normalize_key(item):
    """Normalize brand and product name for matching."""
    brand = item.get('brand', '').lower().strip()
    product = item.get('productName', '').lower().strip()
    return f"{brand}|{product}"

def match_score(str1, str2):
    """Fuzzy match score between two strings."""
    return SequenceMatcher(None, str1, str2).ratio()

async def run_evaluation(model_name: str, thinking_budget: int, csv_out: str, verbose: bool):
    print(f"--- Starting Evaluation on: {model_name} (Thinking: {thinking_budget}) ---")
    
    fixture_path = Path(__file__).parent.parent / "fixtures" / "silver_mentions_benchmarks.json"
    with open(fixture_path, 'r') as f:
        fixtures = json.load(f)
        
    threads = []
    golden_map = {}
    
    for item in fixtures:
        t = item['thread']
        sid = t['submission_id']
        threads.append((sid, t['title'], t['body'], t['comments']))
        golden_map[sid] = item['expected_mentions']
        
    print(f"Loaded {len(threads)} golden threads.")
    
    # Run the exact production extraction batch function
    semaphore = asyncio.Semaphore(10)
    print("Firing inference layer...")
    
    start_time = time.time()
    extracted_items, total_cost, total_in, total_out = await _process_thread_batch(threads, model_name, semaphore, thinking_budget)
    latency = time.time() - start_time
    
    print(f"\nInference complete. Total Cost: ${total_cost:.6f} ({total_in} In / {total_out} Out)")
    
    # --- EVALUATION SCORING ---
    tp, fp, fn = 0, 0, 0
    total_attributes_expected = 0
    correct_attributes = 0
    
    attribute_keys = ["sentiment", "specificityLevel", "durability", "ownershipDurationMonths", "flawOrCaveat"]
    
    for sid, golden_mentions in golden_map.items():
        extracted = [e for e in extracted_items if e.get('submission_id') == sid]
        
        # We need to alignment match them
        matched_golden_indices = set()
        
        for ex in extracted:
            best_match_idx = -1
            best_score = 0.0
            ex_key = normalize_key(ex)
            
            for g_idx, g in enumerate(golden_mentions):
                if g_idx in matched_golden_indices:
                    continue
                g_key = normalize_key(g)
                
                score = match_score(ex_key, g_key)
                if score > best_score:
                    best_score = score
                    best_match_idx = g_idx
            
            # Threshold for alignment (e.g. 0.8)
            if best_score >= 0.8:
                tp += 1
                matched_golden_indices.add(best_match_idx)
                
                # Grade the Nuance!
                g_item = golden_mentions[best_match_idx]
                for key in attribute_keys:
                    if key in g_item:
                        total_attributes_expected += 1
                        if str(ex.get(key)) == str(g_item.get(key)):
                            correct_attributes += 1
                        elif verbose:
                            print(f"[NUANCE ERROR] Thread {sid} | {ex_key} | Expected '{key}' to be '{g_item.get(key)}', got '{ex.get(key)}'")
            else:
                fp += 1
                if verbose:
                    print(f"[HALLUCINATION] Extracted {ex_key} but no golden match found.")
                    
        # Any golden mentions not matched are False Negatives
        missed = len(golden_mentions) - len(matched_golden_indices)
        fn += missed
        if verbose and missed > 0:
            for g_idx, g in enumerate(golden_mentions):
                if g_idx not in matched_golden_indices:
                    print(f"[MISS] Failed to extract expected entity: {normalize_key(g)}")

    print("\n--- RESULTS ---")
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    nuance_accuracy = correct_attributes / total_attributes_expected if total_attributes_expected > 0 else 0
    
    print(f"Entity F1-Score:    {f1*100:.1f}%")
    print(f"  Precision:        {precision*100:.1f}% (TP: {tp}, FP: {fp})")
    print(f"  Recall:           {recall*100:.1f}% (TP: {tp}, FN: {fn})")
    print(f"Attribute Accuracy: {nuance_accuracy*100:.1f}% ({correct_attributes}/{total_attributes_expected})")
    print(f"Cost Economics:     ${total_cost:.6f} total")
    print(f"Latency:            {latency:.2f} seconds")
    
    if csv_out:
        out_path = Path(csv_out)
        file_exists = out_path.is_file()
        with open(out_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if not file_exists:
                writer.writerow(["Model", "ThinkingBudget", "F1Score", "Precision", "Recall", "AttributeAccuracy", "CostUSD", "LatencySeconds"])
            writer.writerow([model_name, thinking_budget or 0, round(f1, 4), round(precision, 4), round(recall, 4), round(nuance_accuracy, 4), round(total_cost, 6), round(latency, 2)])
        print(f"\nSaved eval metrics to {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Extraction Eval")
    parser.add_argument("-m", "--model", default="gemini-2.5-flash-lite", help="The Gemini model to test")
    parser.add_argument("-t", "--thinking", type=str, default=None, help="Thinking budget tokens (2.5) or level (3.X)")
    parser.add_argument("-c", "--csv", default=None, help="Append results to this CSV file for plotting")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print granular hallucination/miss details")
    args = parser.parse_args()
    
    import os
    if not os.environ.get("GEMINI_API_KEY"):
        print("ERROR: GEMINI_API_KEY is missing. Export it to run live evaluations.")
        sys.exit(1)
        
    asyncio.run(run_evaluation(args.model, args.thinking, args.csv, args.verbose))
