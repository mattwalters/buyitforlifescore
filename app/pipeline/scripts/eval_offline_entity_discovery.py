import argparse
import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path

import tqdm.asyncio

# Allow import of the pipeline package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.utils.llm import process_thread_discovery
from pipeline.utils.metrics import calculate_eval_metrics


async def _extract_candidate(
    doc_id: str, content_blocks: list[dict], model_name: str, thinking: str | None, semaphore: asyncio.Semaphore
) -> tuple[list[dict], float, str | None]:
    """Wrapper around the shared process_thread_discovery for offline eval context."""
    title = ""
    body = ""
    comments = []

    for b in content_blocks:
        author = b.get("author_id", "")
        text = b.get("text", "")

        # In offline benchmarks, the OP block manually combined Title and Body.
        # We parse it apart so process_thread_discovery can rebuild the tree naturally.
        if str(author).lower() == "op" or b.get("block_id") == 0:
            if text.startswith("Title: "):
                try:
                    title_part, body_part = text.split("\nBody: ", 1)
                    title = title_part.replace("Title: ", "")
                    body = body_part
                except ValueError:
                    body = text
            else:
                body = text
        else:
            comments.append({
                "id": str(b.get("block_id", "")),
                "parent_id": f"t3_{doc_id}",
                "author": author,
                "body": text,
            })

    try:
        result = await process_thread_discovery(
            submission_id=doc_id,
            title=title,
            body=body,
            comments=comments,
            model_name=model_name,
            semaphore=semaphore,
            thinking=thinking,
        )

        items = result.all_items
        for item in items:
            item["document_id"] = doc_id

        err = "\n".join(result.errors) if result.errors else None
        return items, result.total_cost, err

    except Exception as e:
        return [], 0.0, f"Skipping doc {doc_id} due to API Error: {e}"


def _norm(text: str) -> str:
    return re.sub(r"[^\w\s]", "", text.lower()).strip()


def _evaluate_doc(doc_id: str, expected_benchmark: list[dict], raw_doc_extractions: list[dict]):
    expected_tuples = [
        (_norm(g.get("raw_mention", "")), g.get("author_id"), tuple(sorted(g.get("source_block_ids", []))))
        for g in expected_benchmark
    ]
    extracted_tuples = [
        (_norm(ex.get("raw_mention", "")), ex.get("author_id"), tuple(sorted(ex.get("source_block_ids", []))))
        for ex in raw_doc_extractions
    ]

    # 1-to-1 greedy matching: each expected item can only be consumed once.
    # This prevents multiple extracted items from inflating TP via substring overlap.
    remaining_expected = list(expected_tuples)
    matched_count = 0

    for ext in extracted_tuples:
        ext_mention, ext_author, ext_blocks = ext
        if not ext_mention:
            continue

        best_match_idx = None
        for i, exp in enumerate(remaining_expected):
            exp_mention, exp_author, exp_blocks = exp
            if not exp_mention:
                continue

            # Strict matching: string must be a substring match, author and block array must be exact matches.
            if (
                (exp_mention in ext_mention or ext_mention in exp_mention)
                and (exp_author == ext_author)
                and (exp_blocks == ext_blocks)
            ):
                best_match_idx = i
                break

        if best_match_idx is not None:
            remaining_expected.pop(best_match_idx)
            matched_count += 1

    thread_tp = matched_count
    thread_fn = len(remaining_expected)
    thread_fp = len(extracted_tuples) - matched_count

    missed = set((e[0], e[1], e[2]) for e in remaining_expected)
    # Hallucinations are any extracted items that didn't match
    hallucinations_list = []
    remaining_expected_for_fp = list(expected_tuples)
    for ext in extracted_tuples:
        ext_mention, ext_author, ext_blocks = ext
        if not ext_mention:
            hallucinations_list.append(ext)
            continue
        found = False
        for i, exp in enumerate(remaining_expected_for_fp):
            exp_mention, exp_author, exp_blocks = exp
            if (
                (exp_mention in ext_mention or ext_mention in exp_mention)
                and (exp_author == ext_author)
                and (exp_blocks == ext_blocks)
            ):
                remaining_expected_for_fp.pop(i)
                found = True
                break
        if not found:
            hallucinations_list.append(ext)

    hallucinations = set(hallucinations_list)

    return doc_id, missed, hallucinations, thread_tp, thread_fp, thread_fn


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
    m = calculate_eval_metrics(tp=tp, fp=fp, fn=fn)

    print(f"Entity F1-Score:    {m.f1 * 100:.1f}%")
    print(f"  Precision:        {m.precision * 100:.1f}% (TP: {m.tp}, FP: {m.fp})")
    print(f"  Recall:           {m.recall * 100:.1f}% (TP: {m.tp}, FN: {m.fn})")
    print(f"Total API Cost:     ${total_candidate_cost:.5f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entity Discovery Phase 1 Eval")
    parser.add_argument("-m", "--model", default="gemini-2.5-flash-lite", help="Candidate Model")
    parser.add_argument("-t", "--thinking", type=str, default=None, help="Thinking constraint")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print mismatch details")
    args = parser.parse_args()

    if not os.environ.get("GEMINI_API_KEY"):
        print("ERROR: GEMINI_API_KEY is missing.")
        sys.exit(1)

    asyncio.run(run_evaluation(args.model, args.thinking, args.verbose))
