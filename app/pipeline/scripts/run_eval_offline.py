import json
import os
import sys

from pydantic import TypeAdapter

from pipeline.schemas.reddit_llm_payloads import SilverRedditLlmPayload
from pipeline.utils.ai import get_client, invoke_entity_discovery


def main():
    seed_file = os.path.join(os.path.dirname(__file__), "..", "evals", "datasets", "offline_seed.json")
    if not os.path.exists(seed_file):
        print("offline_seed.json not found! Run materialize_offline_seed.py first.")
        sys.exit(1)

    with open(seed_file, "r") as f:
        data = json.load(f)

    client = get_client()

    total_tests = 0
    passed_tests = 0

    print(f"Running Offline Eval on {len(data)} cases...")

    for case in data:
        payload = TypeAdapter(SilverRedditLlmPayload).validate_python(case["payload"])
        expected_entities = case.get("expected_entities", [])

        # Omit cases not labeled yet
        if not expected_entities:
            print(f"[SKIP] {payload.bundle_id} - No golden labels provided.")
            continue

        print(f"Testing {payload.bundle_id}...")
        try:
            result = invoke_entity_discovery(client, payload)
        except Exception as e:
            print(f"[FAIL] Exception invoking pipeline: {e}")
            total_tests += 1
            continue

        # We perform Bidirectional Substring Matching against 'verbatim_quote'
        # The test passes if ALL expected entities are found in the result items.
        case_passed = True

        for expected in expected_entities:
            expected_quote = expected["verbatim_quote"].lower()

            # Look for it in the extracted items
            found = False
            for item in result.items:
                extracted = item.verbatim_quote.lower()
                if expected_quote in extracted or extracted in expected_quote:
                    found = True
                    break

            if not found:
                print(f"  [FAIL] Missed entity: '{expected['verbatim_quote']}'")
                case_passed = False

        if case_passed:
            print(f"  [PASS] Successfully extracted all expected entities.")
            passed_tests += 1

        total_tests += 1

    if total_tests == 0:
        print("\nNo labeled cases found. Please add labels to offline_seed.json.")
        return

    recall = passed_tests / total_tests
    print(f"\n--- OFFLINE EVAL RESULTS ---")
    print(f"Total Graded: {total_tests}")
    print(f"Recall (Golden Targets Hit): {recall * 100:.2f}%")


if __name__ == "__main__":
    main()
