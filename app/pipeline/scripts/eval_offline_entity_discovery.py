import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

import tqdm.asyncio
from tqdm import tqdm

# Allow import of the pipeline package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.utils.llm import run_entity_discovery


async def _extract_candidate(
    doc_id: str, content_blocks: list[dict], model_name: str, thinking: str | None, semaphore: asyncio.Semaphore
) -> tuple[list[dict], float]:
    """Thin wrapper around the shared run_entity_discovery for offline eval context."""
    thread_text = json.dumps(content_blocks, indent=2)

    try:
        result = await run_entity_discovery(
            content_blocks_json=thread_text,
            model_name=model_name,
            thinking=thinking,
            semaphore=semaphore,
        )

        items = result.items
        for item in items:
            item["document_id"] = doc_id

        return items, result.cost, None

    except Exception as e:
        return [], 0.0, f"Skipping doc {doc_id} due to API Error: {e}"


async def run_evaluation(model_name: str, thinking_budget: str | None, verbose: bool):
    print(f"--- Entity Discovery Phase 1 Eval: {model_name} (Thinking: {thinking_budget}) ---")

    fixture_path = Path(__file__).parent.parent / "fixtures" / "silver_entity_discovery_benchmark.json"
    with open(fixture_path, "r") as f:
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
        if err:
            all_errors.append(err)

    latency = time.time() - start_time
    print(f"Inference complete. Cost: ${total_candidate_cost:.5f}. Latency: {latency:.2f}s")

    print("\nFiring Deterministic Evaluation...")

    def _evaluate_doc(doc_id, expected_benchmark, raw_doc_extractions):
        import re

        def norm(text):
            return re.sub(r"[^\w\s]", "", text.lower()).strip()

        expected_set = set(
            (norm(g.get("raw_mention", "")), g.get("author_id"), tuple(sorted(g.get("source_block_ids", []))))
            for g in expected_benchmark
        )
        extracted_set = set(
            (norm(ex.get("raw_mention", "")), ex.get("author_id"), tuple(sorted(ex.get("source_block_ids", []))))
            for ex in raw_doc_extractions
        )

        matched_expected = set()
        matched_extracted = set()

        for exp in expected_set:
            exp_mention, exp_author, exp_blocks = exp
            if not exp_mention:
                continue

            for ext in extracted_set:
                ext_mention, ext_author, ext_blocks = ext
                if not ext_mention:
                    continue

                # Strict matching: String must be a substring match, author and block array must be EXCACA matches.
                if (
                    (exp_mention in ext_mention or ext_mention in exp_mention)
                    and (exp_author == ext_author)
                    and (exp_blocks == ext_blocks)
                ):
                    matched_expected.add(exp)
                    matched_extracted.add(ext)

        missed = expected_set - matched_expected
        hallucinations = extracted_set - matched_extracted

        thread_tp = len(matched_expected)
        thread_fp = len(hallucinations)
        thread_fn = len(missed)

        return doc_id, missed, hallucinations, thread_tp, thread_fp, thread_fn

    judge_results = [
        _evaluate_doc(doc_id, ec, [e for e in all_extracted_items if e.get("document_id") == doc_id])
        for doc_id, ec in golden_map.items()
    ]

    tp, fp, fn = 0, 0, 0

    for res in judge_results:
        doc_id, missed, hallucinations, thread_tp, thread_fp, thread_fn = res

        tp += thread_tp
        fp += thread_fp
        fn += thread_fn
        if verbose:
            if thread_fp > 0 or thread_fn > 0:
                print(f"\n--- Document {doc_id} Mismatches ---")
            for h in hallucinations:
                print(f"[HALLUCINATION (FP)]: Brand '{h[0]}' (Author: {h[1]}, Blocks: {list(h[2])})")
            for m in missed:
                print(f"[MISS (FN)]: Brand '{m[0]}' (Author: {m[1]}, Blocks: {list(m[2])})")

    # 3. METRICS
    print("\n--- RESULTS PHASE 1 (Entity Discovery) ---")
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    print(f"Entity F1-Score:    {f1 * 100:.1f}%")
    print(f"  Precision:        {precision * 100:.1f}% (TP: {tp}, FP: {fp})")
    print(f"  Recall:           {recall * 100:.1f}% (TP: {tp}, FN: {fn})")
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
