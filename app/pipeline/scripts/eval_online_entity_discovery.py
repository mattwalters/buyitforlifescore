#!/usr/bin/env python3
import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from tqdm.asyncio import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from google import genai
from google.genai import types
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipeline.prompts.judge_online_discovery import JudgeDiscoveryResult, get_blind_judge_discovery_prompt
from pipeline.utils.db import load_bronze_threads_with_comments
from pipeline.utils.llm import process_thread_discovery
from pipeline.utils.metrics import calculate_eval_metrics
from pipeline.utils.paths import get_read_path
from pipeline.utils.pricing import calculate_gemini_cost

_judge_client = genai.Client()


def get_random_bronze_threads(count: int, seed: int = 42) -> list[dict]:
    """Loads random Bronze threads with their raw comment lists for evaluation.

    Returns thread dicts with document_id, title, body, and the raw comments
    list — the same shape the production pipeline receives from DuckDB.
    """
    subs_path = get_read_path("bronze/reddit_buyitforlife_submissions.parquet")
    coms_path = get_read_path("bronze/reddit_buyitforlife_comments.parquet")

    rows = load_bronze_threads_with_comments(
        submissions_path=str(subs_path),
        comments_path=str(coms_path),
        where_clause="s.score > 10",
        having_clause="len(comments) > 5 AND len(comments) < 20",
        order_clause="random()",
        limit=count,
        seed=seed / 100.0 if seed else None,
    )

    threads = []
    for row in rows:
        doc_id, title, body, _created_utc, comments = row
        threads.append(
            {
                "document_id": doc_id,
                "title": title,
                "body": body,
                "comments": comments or [],
            }
        )
    return threads


async def run_thread_extraction(thread: dict, model_name: str, thinking: str | None, semaphore: asyncio.Semaphore):
    """Runs the shared process_thread_discovery — identical to production."""
    doc_id = thread["document_id"]

    try:
        result = await process_thread_discovery(
            submission_id=doc_id,
            title=thread["title"],
            body=thread["body"],
            comments=thread["comments"],
            model_name=model_name,
            semaphore=semaphore,
            thinking=thinking,
        )

        # Build the content_blocks_json from the first chunk payload for the judge
        # (the judge needs to see the actual text the LLM saw)
        all_blocks_json = "[]"
        if result.chunk_payloads:
            # Merge content blocks from all chunks for the judge's full view
            all_blocks = []
            for payload in result.chunk_payloads:
                chunk_blocks = json.loads(payload["content_blocks_json"])
                all_blocks.extend(chunk_blocks)
            all_blocks_json = json.dumps(all_blocks, indent=2)

        return doc_id, all_blocks_json, result.all_items, result.total_cost

    except Exception as e:
        print(f"\n[🚨 FATAL] Extraction fully failed on {doc_id} after 3 attempts. Error: {e}")
        return doc_id, "[]", [], 0.0


async def fetch_judgment(doc_id: str, blocks_json: str, extractions: list[dict], model_name: str, thinking_tokens: int):
    prompt = get_blind_judge_discovery_prompt(blocks_json, extractions)

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json", response_schema=JudgeDiscoveryResult, temperature=0.1
    )
    if thinking_tokens:
        gen_config.thinking_config = types.ThinkingConfig(thinking_budget=thinking_tokens)

    # Accumulated across all attempts — if Google returned a response, we were charged,
    # regardless of whether our parsing succeeded.
    accumulated_cost = 0.0

    def log_retry(rs):
        print(f"\n[⚠️ RETRY] Judge failed on {doc_id} (Attempt {rs.attempt_number}/3). Error: {rs.outcome.exception()}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
        before_sleep=log_retry,
    )
    async def call_api():
        nonlocal accumulated_cost
        response = await _judge_client.aio.models.generate_content(model=model_name, contents=prompt, config=gen_config)
        # Capture cost before parsing — a response was received, so Google charged us.
        if response.usage_metadata:
            accumulated_cost += calculate_gemini_cost(model_name, response.usage_metadata)
        data = JudgeDiscoveryResult(**json.loads(response.text)) if response.text else None
        return data

    try:
        data = await call_api()
        return doc_id, data, accumulated_cost, len(extractions)
    except Exception as e:
        print(f"\n[🚨 FATAL] Judge fully failed on {doc_id} after 3 attempts. Error: {e}")
        return doc_id, None, accumulated_cost, len(extractions)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--count", type=int, default=100, help="Number of random bronze threads to sample")
    parser.add_argument(
        "-m1", "--extractor-model", type=str, default="gemini-2.5-flash-lite", help="Model for pipeline extraction"
    )
    parser.add_argument("-t1", "--extractor-think", type=int, default=1024, help="Thinking tokens for extraction")
    parser.add_argument("-m2", "--judge-model", type=str, default="gemini-2.5-flash", help="Model for Blind Judge")
    parser.add_argument("-t2", "--judge-think", type=int, default=1024, help="Thinking tokens for Judge")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print all judge reasoning")
    args = parser.parse_args()

    print("--- Entity Discovery Phase 1 [ONLINE/SHADOW] Eval ---")
    print(f"Sampling {args.count} real-world threads from Bronze layer.")
    threads = get_random_bronze_threads(args.count)
    if not threads:
        print("Could not load any bronze threads.")
        return

    # PHASE 1: EXTRACTION (via shared process_thread_discovery — identical to production)
    semaphore = asyncio.Semaphore(10)
    print(f"Firing Pipeline Extractor: {args.extractor_model} (Thinking: {args.extractor_think})")
    start_t1 = time.time()
    ext_tasks = [
        run_thread_extraction(
            t, args.extractor_model, str(args.extractor_think) if args.extractor_think else None, semaphore
        )
        for t in threads
    ]
    ext_results = await tqdm.gather(*ext_tasks, desc="Extracting Entities")
    ext_latency = time.time() - start_t1

    total_ext_cost = 0.0
    for res in ext_results:
        total_ext_cost += res[3]

    print(f"Extraction Pipeline Complete. Cost: ${total_ext_cost:.5f}. Latency: {ext_latency:.2f}s")

    # PHASE 2: BLIND JUDGE
    print(f"\nFiring Blind QA Judge: {args.judge_model} (Thinking: {args.judge_think})")
    start_t2 = time.time()
    judge_tasks = [fetch_judgment(res[0], res[1], res[2], args.judge_model, args.judge_think) for res in ext_results]
    judge_results = await tqdm.gather(*judge_tasks, desc="Judging Quality")
    judge_latency = time.time() - start_t2

    total_judge_cost = 0.0
    global_tp = 0
    global_fp = 0
    global_fn = 0

    for j_res in judge_results:
        doc_id, judge_data, cost, ext_count = j_res
        total_judge_cost += cost

        if judge_data:
            thread_fp = len(judge_data.hallucinations)
            thread_fn = len(judge_data.missed_brands)
            thread_tp = max(0, ext_count - thread_fp)

            global_fp += thread_fp
            global_fn += thread_fn
            global_tp += thread_tp

            if args.verbose and (thread_fp > 0 or thread_fn > 0):
                print(f"\n--- Thread {doc_id} Quality Report ---")
                if judge_data.missed_brands:
                    print(f"[MISS (FN)]: {judge_data.missed_brands}")
                if judge_data.hallucinations:
                    print(f"[HALLUCINATION (FP)]: {judge_data.hallucinations}")
                print(f"Judge Reasoning: {judge_data.reasoning}")

    m = calculate_eval_metrics(tp=global_tp, fp=global_fp, fn=global_fn)

    print("\n--- RESULTS ONLINE/SHADOW (Blind Evaluation) ---")
    print(f"Total Sample Size:  {len(judge_results)} Threads")
    print(f"Entity QA F1-Score: {m.f1 * 100:.1f}%")
    print(f"  QA Precision:     {m.precision * 100:.1f}% (TP: {m.tp}, FP: {m.fp})")
    print(f"  QA Recall:        {m.recall * 100:.1f}% (TP: {m.tp}, FN: {m.fn})")
    print(f"Extraction Latency: {ext_latency:.2f}s")
    print(f"Judge Latency:      {judge_latency:.2f}s")
    print(f"Extraction Cost:    ${total_ext_cost:.5f}")
    print(f"QA Judge Cost:      ${total_judge_cost:.5f}")


if __name__ == "__main__":
    asyncio.run(main())
