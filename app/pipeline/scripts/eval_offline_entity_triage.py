import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import tqdm.asyncio

# Allow import of the pipeline package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.utils.llm import run_entity_triage
from pipeline.utils.metrics import calculate_eval_metrics


async def _triage_fixture(
    fixture: dict, model_name: str, thinking: str | None, semaphore: asyncio.Semaphore
) -> tuple[dict, float, str | None]:
    """Thin wrapper around the shared run_entity_triage for offline eval context."""
    try:
        result = await run_entity_triage(
            raw_mention=fixture["raw_mention"],
            text=fixture["text"],
            parent_text=fixture.get("parent_text", ""),
            model_name=model_name,
            thinking=thinking,
            semaphore=semaphore,
        )
        return (
            {
                "id": fixture["id"],
                "expected": fixture["expected_passes"],
                "actual": result.passes,
                "reasoning": result.reasoning,
                "cost": result.cost,
                "signal_note": fixture.get("signal_note", ""),
            },
            result.cost,
            None,
        )

    except Exception as e:
        return (
            {
                "id": fixture["id"],
                "expected": fixture["expected_passes"],
                "actual": None,
                "reasoning": "",
                "cost": 0.0,
                "signal_note": fixture.get("signal_note", ""),
            },
            0.0,
            f"Failed on fixture {fixture['id']}: {e}",
        )


async def run_evaluation(model_name: str, thinking_budget: str | None, verbose: bool):
    print(f"--- Entity Triage Eval: {model_name} (Thinking: {thinking_budget}) ---")

    fixture_path = Path(__file__).parent.parent / "fixtures" / "silver_entity_triage_benchmark.json"
    with open(fixture_path, "r") as f:
        fixtures = json.load(f)

    print(
        f"Loaded {len(fixtures)} fixtures ({sum(1 for f in fixtures if f['expected_passes'])} pass, "
        f"{sum(1 for f in fixtures if not f['expected_passes'])} fail)."
    )

    semaphore = asyncio.Semaphore(10)
    print("Firing inference layer...")

    start_time = time.time()

    tasks = [_triage_fixture(fix, model_name, thinking_budget, semaphore) for fix in fixtures]
    raw_results = await tqdm.asyncio.tqdm.gather(*tasks, desc="Triaging Fixtures")

    latency = time.time() - start_time

    results = []
    all_errors = []
    total_cost = 0.0

    for scored, cost, err in raw_results:
        results.append(scored)
        total_cost += cost
        if err:
            all_errors.append(err)

    print(f"Inference complete. Cost: ${total_cost:.5f}. Latency: {latency:.2f}s")

    # --- SCORE ---
    # Positive class = passes (true). FN is most dangerous: missed real signal is unrecoverable.
    tp = sum(1 for r in results if r["expected"] is True and r["actual"] is True)
    tn = sum(1 for r in results if r["expected"] is False and r["actual"] is False)
    fp = sum(1 for r in results if r["expected"] is False and r["actual"] is True)
    fn = sum(1 for r in results if r["expected"] is True and r["actual"] is False)
    errors = sum(1 for r in results if r["actual"] is None)

    total = len(results) - errors
    m = calculate_eval_metrics(tp=tp, fp=fp, fn=fn, tn=tn)

    if verbose:
        mismatches = [r for r in results if r["expected"] != r["actual"] and r["actual"] is not None]
        if mismatches:
            print(f"\n--- Mismatches ({len(mismatches)}) ---")
            for r in mismatches:
                verdict = "FP (let noise through)" if r["actual"] is True else "FN (missed signal)"
                print(f"\n[{verdict}] Fixture {r['id']}")
                print(f"  Signal note: {r['signal_note']}")
                print(f"  Model reasoning: {r['reasoning']}")

        if all_errors:
            print(f"\n--- Errors ({len(errors)}) ---")
            for err in all_errors:
                print(f"  {err}")

    print("\n--- RESULTS (Entity Triage) ---")
    print(f"F1-Score:   {m.f1 * 100:.1f}%")
    print(f"Recall:     {m.recall * 100:.1f}%  (TP: {m.tp}, FN: {m.fn})  ← missed signal, unrecoverable")
    print(f"Precision:  {m.precision * 100:.1f}%  (TP: {m.tp}, FP: {m.fp})  ← noise passed through")
    print(f"Accuracy:   {m.accuracy * 100:.1f}%  (TP: {m.tp}, TN: {m.tn}, FP: {m.fp}, FN: {m.fn})")
    if errors:
        print(f"Errors:     {errors} fixture(s) failed to run")
    print(f"Total API Cost: ${total_cost:.5f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entity Triage Offline Eval")
    parser.add_argument("-m", "--model", default="gemini-2.5-flash-lite", help="Candidate model")
    parser.add_argument("-t", "--thinking", type=str, default=None, help="Thinking constraint")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print mismatch details")
    args = parser.parse_args()

    if not os.environ.get("GEMINI_API_KEY"):
        print("ERROR: GEMINI_API_KEY is missing.")
        sys.exit(1)

    asyncio.run(run_evaluation(args.model, args.thinking, args.verbose))
