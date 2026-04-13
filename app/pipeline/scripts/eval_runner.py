import sys
import json
import asyncio
import time
import csv
import argparse
from pathlib import Path
from pydantic import BaseModel
from google import genai
from google.genai import types

# Allow import of the pipeline package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from pipeline.defs.silver import _process_thread_batch
from pipeline.utils.pricing import AiModel, calculate_gemini_cost

# --- Judge Pydantic Schemas ---
class AlignmentMapping(BaseModel):
    expected_index: int
    extracted_index: int

class AlignmentResult(BaseModel):
    mappings: list[AlignmentMapping]

async def run_judge_alignment(expected_mentions: list[dict], extracted_mentions: list[dict]) -> tuple[list[AlignmentMapping], float]:
    """Uses Gemini 3.1 Pro (High Thinking) as a Judge to semantically map Extractions to Expected entities. Returns (mappings, eval_cost)."""
    if not expected_mentions and not extracted_mentions:
        return [], 0.0

    client = genai.Client()
    
    # Strip down to just indices, author, brand, and productName for the Judge to avoid distraction
    expected_for_judge = [{"index": i, "author_id": e.get("author_id"), "brand": e.get("brand"), "productName": e.get("productName")} for i, e in enumerate(expected_mentions)]
    extracted_for_judge = [{"index": i, "author_id": e.get("author_id"), "brand": e.get("brand"), "productName": e.get("productName")} for i, e in enumerate(extracted_mentions)]

    prompt = f"""You are an expert taxonomy aligner. Your task is to match extracted product mentions to a ground-truth expected list. 
The items might have spelling errors, missing words, or different specificity, but you must map them if they obviously refer to the exact same product from the underlying conversation.
CRITICAL RULE: The extracted item and the expected item MUST have the exact same 'author_id' to be considered a match.

Expected Mentions:
{json.dumps(expected_for_judge, indent=2)}

Extracted Mentions:
{json.dumps(extracted_for_judge, indent=2)}

Return a list of mappings bridging the matching indices. Not all items will have a match."""

    # Hardcode Judge to 3.1 Pro High Thinking
    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=AlignmentResult,
        temperature=0.0,
        thinking_config=types.ThinkingConfig(thinking_level="high")
    )

    try:
        response = await client.aio.models.generate_content(
            model=AiModel.GEMINI_3_PRO.value,
            contents=prompt,
            config=gen_config,
        )
        
        cost = 0.0
        if response.usage_metadata:
            cost = calculate_gemini_cost(AiModel.GEMINI_3_PRO.value, response.usage_metadata)
            
        if response.text:
            result_dict = json.loads(response.text)
            return [AlignmentMapping(**m) for m in result_dict.get("mappings", [])], cost
    except Exception as e:
        print(f"Judge failed to assign or parse mappings: {e}")
        
    return [], 0.0


async def run_evaluation(model_name: str, thinking_budget: str | None, csv_out: str | None, verbose: bool, limit: int | None):
    print(f"--- Starting Evaluation on Candidate: {model_name} (Thinking: {thinking_budget}) ---")
    
    fixture_path = Path(__file__).parent.parent / "fixtures" / "silver_mentions_benchmarks.json"
    with open(fixture_path, 'r') as f:
        fixtures = json.load(f)
        
    if limit:
        fixtures = fixtures[:limit]
        print(f"Limited run to {limit} threads.")
        
    threads = []
    golden_map = {}
    
    for item in fixtures:
        t = item['thread']
        sid = t['submission_id']
        threads.append((sid, t['title'], t.get('body', ''), t.get('comments', [])))
        
        # Convert sourceId to author_id to support Phase 1 testing without rewriting the JSON
        mapped_expected = []
        for e in item['expected_mentions']:
            s_id = e.get('sourceId', 0)
            author = "OP" if s_id == 0 else f"Commenter_{s_id}"
            
            # Copy item and inject author_id
            new_e = e.copy()
            new_e['author_id'] = author
            mapped_expected.append(new_e)
            
        golden_map[sid] = mapped_expected
        
    print(f"Loaded {len(threads)} golden threads.")
    
    # Run the exact production extraction batch function
    semaphore = asyncio.Semaphore(10)
    print(f"Firing inference layer for candidate model {model_name}...")
    
    start_time = time.time()
    extracted_items, total_cost, total_in, total_out = await _process_thread_batch(threads, model_name, semaphore, thinking_budget)
    latency = time.time() - start_time
    
    print(f"\nCandidate Inference complete. Total Cost (Candidate Only): ${total_cost:.6f} ({total_in} In / {total_out} Out)")
    
    print("\nStarting Phase 1: Semantic Entity Alignment with Judge (gemini-3.1-pro-preview)...")
    # --- EVALUATION SCORING via LLM JUDGE ---
    tp, fp, fn = 0, 0, 0
    total_judge_cost = 0.0
    
    for sid, golden_mentions in golden_map.items():
        extracted = [e for e in extracted_items if e.get('submission_id') == sid]
        
        # Call the Judge
        mappings, judge_cost = await run_judge_alignment(golden_mentions, extracted)
        total_judge_cost += judge_cost
        
        matched_expected_indices = set([m.expected_index for m in mappings])
        matched_extracted_indices = set([m.extracted_index for m in mappings])
        
        # Calculate TP, FP, FN for this thread
        thread_tp = len(mappings)
        thread_fp = len(extracted) - len(matched_extracted_indices)  # Extractions not mapped
        thread_fn = len(golden_mentions) - len(matched_expected_indices) # Expected not mapped
        
        tp += thread_tp
        fp += thread_fp
        fn += thread_fn
        
        if verbose:
            if thread_fp > 0 or thread_fn > 0:
                print(f"\n--- Thread {sid} Mismatches ---")
            for ex_idx, ex in enumerate(extracted):
                if ex_idx not in matched_extracted_indices:
                    print(f"[HALLUCINATION (FP)]: Extracted {ex.get('brand')} {ex.get('productName')} but Judge rejected it.")
            for g_idx, g in enumerate(golden_mentions):
                if g_idx not in matched_expected_indices:
                    print(f"[MISS (FN)]: Failed to extract expected entity: {g.get('brand')} {g.get('productName')}")

    print("\n--- RESULTS PHASE 1 (Semantic Alignment) ---")
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    print(f"Entity F1-Score:    {f1*100:.1f}%")
    print(f"  Precision:        {precision*100:.1f}% (TP: {tp}, FP: {fp})")
    print(f"  Recall:           {recall*100:.1f}% (TP: {tp}, FN: {fn})")
    print(f"Cost Economics:     ${total_cost:.6f} total (Candidate Tracking Only)")
    print(f"Judge Overhead:     ${total_judge_cost:.6f} total (Excluded from candidate eval metrics)")
    print(f"Latency:            {latency:.2f} seconds")
    
    if csv_out:
        out_path = Path(csv_out)
        file_exists = out_path.is_file()
        with open(out_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if not file_exists:
                writer.writerow(["Model", "Thinking", "F1Score", "Precision", "Recall", "CostUSD", "LatencySeconds"])
                
            # Log the truthful default
            if not thinking_budget:
                if "gemini-3" in model_name:
                    display_thinking = "high (api default)"
                else:
                    display_thinking = "0 (api default)"
            else:
                display_thinking = thinking_budget
                
            writer.writerow([model_name, display_thinking, round(f1, 4), round(precision, 4), round(recall, 4), round(total_cost, 6), round(latency, 2)])
        print(f"\nSaved eval metrics to {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Extraction Eval")
    parser.add_argument("-m", "--model", default="gemini-2.5-flash-lite", help="The Gemini model to test")
    parser.add_argument("-t", "--thinking", type=str, default=None, help="Thinking budget tokens (2.5) or level (3.X)")
    parser.add_argument("-c", "--csv", default=None, help="Append results to this CSV file for plotting")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print granular hallucination/miss details")
    parser.add_argument("-l", "--limit", type=int, default=None, help="Limit number of threads to test (for quick iteration)")
    args = parser.parse_args()
    
    import os
    if not os.environ.get("GEMINI_API_KEY"):
        print("ERROR: GEMINI_API_KEY is missing. Export it to run live evaluations.")
        sys.exit(1)
        
    asyncio.run(run_evaluation(args.model, args.thinking, args.csv, args.verbose, args.limit))
