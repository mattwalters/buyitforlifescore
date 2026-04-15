import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import tqdm
import tqdm.asyncio
from google import genai
from google.genai import types

# Allow import of the pipeline package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.utils.pricing import AiModel, calculate_gemini_cost

# Allow fallback defaults
DEFAULT_MODEL = "gemini-2.5-flash-lite"

from pipeline.prompts.entity_extraction import EntityExtraction, get_extraction_prompt


async def semantically_equivalent(client, expected: str, actual: str, field: str, semaphore: asyncio.Semaphore) -> bool:
    """Uses a cheap LLM Judge to determine if an unstructured text matches the benchmark."""
    if expected is None and actual is None:
        return True, 0.0
    if expected is None or actual is None:
        return False, 0.0

    # Fast path for exact literal matches (bypasses LLM overhead)
    if str(expected).strip().lower() == str(actual).strip().lower():
        return True, 0.0

    prompt = f"""You are a strict data-science judge.
Determine if the 'Actual' unstructured string semantically means the exact same core concept as the 'Expected' benchmark for the field '{field}'.
Ignore differences in capitalization, punctuation, trailing words, or grammatical tense.
Focus strictly on the underlying subject matter.

Expected Benchmark: "{expected}"
Actual Extraction: "{actual}"

Respond exactly with the word MATCH or MISMATCH. Do not output anything else."""

    async with semaphore:
        try:
            gen_config = types.GenerateContentConfig(thinking_config=types.ThinkingConfig(thinking_level="low"))
            cost = 0.0
            response = await client.aio.models.generate_content(
                model=AiModel.GEMINI_3_FLASH.value, contents=prompt, config=gen_config
            )
            if response.usage_metadata:
                cost = calculate_gemini_cost(AiModel.GEMINI_3_FLASH.value, response.usage_metadata)
            return "MATCH" in (response.text or "").upper(), cost
        except:
            return False, 0.0


async def evaluate_extraction(
    client, fixture, model_name: str, thinking: str | None, judge_semaphore: asyncio.Semaphore
):
    brand = fixture.get("brand")
    productName = fixture.get("productName", "")
    text = fixture.get("text")
    parent_text = fixture.get("parent_text", "")
    expected = fixture.get("expected", {})
    target_authored_at = fixture.get("targetAuthoredAt", "2024-01-01")

    prompt = get_extraction_prompt(brand, productName, target_authored_at, text, parent_text)

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json", response_schema=EntityExtraction, temperature=0.1
    )

    if thinking:
        if thinking.lstrip("-").isdigit():
            gen_config.thinking_config = types.ThinkingConfig(thinking_budget=int(thinking))
        else:
            gen_config.thinking_config = types.ThinkingConfig(thinking_level=thinking)

    cost = 0.0
    try:
        response = await client.aio.models.generate_content(model=model_name, contents=prompt, config=gen_config)
        if response.usage_metadata:
            cost += calculate_gemini_cost(model_name, response.usage_metadata)

        actual = json.loads(response.text)
    except Exception as e:
        return {}, {}, cost, 0.0, f"Failed to parse LLM Response for {brand}: {e}"

    # SCORE THE RESULTS (Micro F1 Arrays)
    tps = []
    fps = []
    fns = []
    mismatch_logs = []

    # We define which fields need LLM Semantic Judging vs Exact Match
    semantic_fields = {"primaryFlawOrFailure", "alternativeBrandMentioned"}

    judge_tasks = {}
    for key, expected_val in expected.items():
        actual_val = actual.get(key)

        # True Negatives (Correctly identified as null) do not impact ABSA Micro F1
        if expected_val is None and actual_val is None:
            continue

        # False Negatives (Missed)
        if expected_val is not None and actual_val is None:
            fns.append(key)
            mismatch_logs.append(f"❌ [FN] {key}: Expected <{expected_val}> | Got <null>")
            continue

        # False Positives (Hallucinated)
        if expected_val is None and actual_val is not None:
            fps.append(key)
            mismatch_logs.append(f"❌ [FP] {key}: Expected <null> | Got <{actual_val}>")
            continue

        # Match Resolution
        if key in semantic_fields:
            judge_tasks[key] = asyncio.create_task(
                semantically_equivalent(client, expected_val, actual_val, key, judge_semaphore)
            )
        else:
            # Type-safe exact match to handle 1000.0 vs 1000 numeric overlaps safely
            str_expected = str(expected_val).lower().strip()
            str_actual = str(actual_val).lower().strip()

            is_match = False
            try:
                exp_float = float(expected_val)
                act_float = float(actual_val)
                if exp_float == act_float:
                    is_match = True
                else:
                    # 10% proportional tolerance math (min 3)
                    tolerance = max(3.0, 0.10 * abs(exp_float))
                    if abs(exp_float - act_float) <= tolerance:
                        is_match = True
            except (ValueError, TypeError):
                pass

            if is_match or str_expected == str_actual:
                tps.append(key)
            else:
                fps.append(key)  # Got it wrong
                fns.append(key)  # Missed the right answer
                mismatch_logs.append(f"❌ [MM] {key}: Expected <{expected_val}> | Got <{actual_val}>")

    judge_cost = 0.0
    # Await Judges
    for key, task in judge_tasks.items():
        is_match, task_cost = await task
        judge_cost += task_cost
        if is_match:
            tps.append(key)
        else:
            fps.append(key)
            fns.append(key)
            expected_val = expected[key]
            actual_val = actual.get(key)
            mismatch_logs.append(f"❌ [MM-SEMANTIC] {key}: Expected <{expected_val}> | Got <{actual_val}>")

    return {"tp": len(tps), "fp": len(fps), "fn": len(fns)}, mismatch_logs, cost, judge_cost, None


async def run_evaluation(model_name: str, thinking_budget: str | None, verbose: bool):
    client = genai.Client()

    fixture_path = Path(__file__).parent.parent / "fixtures" / "silver_entity_extraction_benchmark.json"
    with open(fixture_path, "r") as f:
        fixtures = json.load(f)

    print(f"Loaded {len(fixtures)} synthetic evaluation cases.")
    print(f"--- Entity Extraction Phase 2 Eval: {model_name} (Thinking: {thinking_budget}) ---")

    judge_semaphore = asyncio.Semaphore(5)
    tasks = [evaluate_extraction(client, fix, model_name, thinking_budget, judge_semaphore) for fix in fixtures]
    results = await tqdm.asyncio.tqdm.gather(*tasks, desc="Evaluating Extraction")

    total_tp = 0
    total_fp = 0
    total_fn = 0
    total_cost = 0.0
    total_judge_cost = 0.0
    all_errors = []

    for i, (metrics, logs, cost, j_cost, err) in enumerate(results):
        total_tp += metrics.get("tp", 0)
        total_fp += metrics.get("fp", 0)
        total_fn += metrics.get("fn", 0)
        total_cost += cost
        total_judge_cost += j_cost

        if err:
            all_errors.append(err)

    if verbose and all_errors:
        print("\n--- Generation Errors ---")
        for err in all_errors:
            print(err)

    for i, (metrics, logs, cost, j_cost, err) in enumerate(results):
        if verbose and logs:
            fixture = fixtures[i]
            print(f"\n--- Fixture {fixture['id']}: {fixture['brand']} {fixture.get('productName', '')} ---")
            for log in logs:
                print(log)

    # Calculate Micro F1
    precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
    recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    print("\n=========================================")
    print(f" ABSA CORE METRICS ({sum([total_tp, total_fp, total_fn])} total dim. checks)")
    print("=========================================")
    print(f"F1-Score:    {f1 * 100:.1f}%")
    print(f"Precision:   {precision * 100:.1f}% (TP: {total_tp}, FP: {total_fp})")
    print(f"Recall:      {recall * 100:.1f}% (TP: {total_tp}, FN: {total_fn})")
    print(f"Inference Cost:   ${total_cost:.5f}")
    print(f"Judge Cost:       ${total_judge_cost:.5f}")
    print(f"Total API Cost:   ${total_cost + total_judge_cost:.5f}")
    print("=========================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entity Extraction Phase 2 Eval")
    parser.add_argument("-m", "--model", default="gemini-2.5-flash-lite", help="Candidate Model")
    parser.add_argument("-t", "--thinking", type=str, default=None, help="Thinking constraint")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print mismatch details")
    args = parser.parse_args()

    if not os.environ.get("GEMINI_API_KEY"):
        print("ERROR: GEMINI_API_KEY is missing.")
        sys.exit(1)

    asyncio.run(run_evaluation(args.model, args.thinking, args.verbose))
