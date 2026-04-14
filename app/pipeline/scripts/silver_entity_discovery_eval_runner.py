import sys
import json
import asyncio
import time
import argparse
from pathlib import Path
from pydantic import BaseModel, Field
from google import genai
from google.genai import types
from tqdm import tqdm
import tqdm.asyncio

# Allow import of the pipeline package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from pipeline.utils.pricing import calculate_gemini_cost
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from pipeline.prompts.entity_discovery import MentionItem, get_entity_discovery_prompt

async def _extract_candidate(doc_id: str, content_blocks: list[dict], model_name: str, thinking: str | None, semaphore: asyncio.Semaphore) -> tuple[list[dict], float]:
    """Runs Phase 1 LLM extraction on flat Canonical Data."""
    client = genai.Client()
    
    thread_text = json.dumps(content_blocks, indent=2)

    prompt = get_entity_discovery_prompt(thread_text)
        
    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=list[MentionItem],
        temperature=0.1
    )
    
    if thinking:
        if thinking.lstrip('-').isdigit():
            gen_config.thinking_config = types.ThinkingConfig(thinking_budget=int(thinking))
        else:
            gen_config.thinking_config = types.ThinkingConfig(thinking_level=thinking)
            
    cost = 0.0
    results = []
    
    async with semaphore:
        @retry(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=2, min=2, max=10),
            retry=retry_if_exception_type(Exception),
            reraise=True
        )
        async def call_api():
            return await client.aio.models.generate_content(
                model=model_name,
                contents=prompt,
                config=gen_config,
            )
            
        try:
            response = await call_api()
            
            if response.usage_metadata:
                cost = calculate_gemini_cost(model_name, response.usage_metadata)
            
            if response.text:
                items = json.loads(response.text)
                for item in items:
                    item['document_id'] = doc_id
                    results.append(item)
                    
        except Exception as e:
            return [], cost, f"Skipping doc {doc_id} due to API Error: {e}"
            
    return results, cost, None


async def run_evaluation(model_name: str, thinking_budget: str | None, verbose: bool):
    print(f"--- Entity Discovery Phase 1 Eval: {model_name} (Thinking: {thinking_budget}) ---")
    
    fixture_path = Path(__file__).parent.parent / "fixtures" / "silver_entity_discovery_benchmark.json"
    with open(fixture_path, 'r') as f:
        fixtures = json.load(f)
        
    print(f"Loaded {len(fixtures)} synthetic benchmarks.")
    
    semaphore = asyncio.Semaphore(10)
    print("Firing inference layer...")
    
    start_time = time.time()
    
    # 1. GENERATE EXTRACTIONS
    extraction_tasks = []
    golden_map = {}
    
    for item in fixtures:
        doc_id = item["document"]["document_id"]
        blocks = item["document"]["content_blocks"]
        golden_map[doc_id] = item["expected_benchmark"]
        extraction_tasks.append(_extract_candidate(doc_id, blocks, model_name, thinking_budget, semaphore))
        
    extraction_results = await tqdm.asyncio.tqdm.gather(*extraction_tasks, desc="Extracting Entities")
    
    # Aggregate Extraction Results
    all_extracted_items = []
    all_errors = []
    total_candidate_cost = 0.0
    for items, cost, err in extraction_results:
        all_extracted_items.extend(items)
        total_candidate_cost += cost
        if err: all_errors.append(err)

    latency = time.time() - start_time
    print(f"Inference complete. Cost: ${total_candidate_cost:.5f}. Latency: {latency:.2f}s")
    
    print(f"\nFiring Deterministic Evaluation...")
    
    def _evaluate_doc(doc_id, expected_benchmark, doc_extractions):
        matched_expected_indices = set()
        matched_extracted_indices = set()
        
        for g_idx, g in enumerate(expected_benchmark):
            g_raw = g.get('raw_mention', '').lower()
            
            for ex_idx, ex in enumerate(doc_extractions):
                if ex_idx in matched_extracted_indices:
                    continue
                ex_raw = ex.get('raw_mention', '').lower()
                
                # Deterministic Greedy Match
                if g_raw in ex_raw or ex_raw in g_raw:
                    matched_expected_indices.add(g_idx)
                    matched_extracted_indices.add(ex_idx)
                    break
                    
        thread_tp = len(matched_expected_indices)
        thread_fp = len(doc_extractions) - len(matched_extracted_indices)
        thread_fn = len(expected_benchmark) - len(matched_expected_indices)

        return doc_id, expected_benchmark, doc_extractions, matched_expected_indices, matched_extracted_indices, thread_tp, thread_fp, thread_fn
        
    judge_results = [_evaluate_doc(doc_id, ec, [e for e in all_extracted_items if e.get('document_id') == doc_id]) for doc_id, ec in golden_map.items()]
    
    tp, fp, fn = 0, 0, 0
    
    for res in judge_results:
        doc_id, expected_benchmark, doc_extractions, matched_expected_indices, matched_extracted_indices, thread_tp, thread_fp, thread_fn = res
        
        tp += thread_tp
        fp += thread_fp
        fn += thread_fn
        if verbose:
            if thread_fp > 0 or thread_fn > 0:
                print(f"\n--- Document {doc_id} Mismatches ---")
            for ex_idx, ex in enumerate(doc_extractions):
                if ex_idx not in matched_extracted_indices:
                    print(f"[HALLUCINATION (FP)]: {ex.get('author_id')} -> {ex.get('raw_mention')}")
            for g_idx, g in enumerate(expected_benchmark):
                if g_idx not in matched_expected_indices:
                    print(f"[MISS (FN)]: {g.get('author_id')} -> {g.get('raw_mention')}")

    # 3. METRICS
    print("\n--- RESULTS PHASE 1 (Entity Discovery) ---")
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    print(f"Entity F1-Score:    {f1*100:.1f}%")
    print(f"  Precision:        {precision*100:.1f}% (TP: {tp}, FP: {fp})")
    print(f"  Recall:           {recall*100:.1f}% (TP: {tp}, FN: {fn})")
    print(f"Total API Cost:     ${total_candidate_cost:.5f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entity Discovery Phase 1 Eval")
    parser.add_argument("-m", "--model", default="gemini-2.5-flash-lite", help="Candidate Model")
    parser.add_argument("-t", "--thinking", type=str, default=None, help="Thinking constraint")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print mismatch details")
    args = parser.parse_args()
    
    import os
    if not os.environ.get("GEMINI_API_KEY"):
        print("ERROR: GEMINI_API_KEY is missing.")
        sys.exit(1)
        
    asyncio.run(run_evaluation(args.model, args.thinking, args.verbose))
