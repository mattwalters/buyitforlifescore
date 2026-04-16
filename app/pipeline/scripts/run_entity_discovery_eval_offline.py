import json
import os
import string
import sys

from pydantic import TypeAdapter

from pipeline.schemas.reddit_llm_payloads import SilverRedditLlmPayload
from pipeline.utils.ai import get_client, invoke_entity_discovery


def main():
    seed_file = os.path.join(os.path.dirname(__file__), "..", "evals", "datasets", "entity_discovery_seed.json")
    if not os.path.exists(seed_file):
        print("entity_discovery_seed.json not found! Run materialize_entity_discovery_seed.py first.")
        sys.exit(1)

    with open(seed_file, "r") as f:
        data = json.load(f)

    client = get_client()

    total_tests = 0
    total_tp = 0
    total_fp = 0
    total_fn = 0
    total_cost_usd = 0.0

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

        # Helper function for liberal matching
        def is_match(expected_str: str, extracted_str: str) -> bool:
            # Lowercase and remove punctuation
            ex_clean = expected_str.lower().translate(str.maketrans('', '', string.punctuation))
            ext_clean = extracted_str.lower().translate(str.maketrans('', '', string.punctuation))
            
            # Substring exact
            if ex_clean in ext_clean or ext_clean in ex_clean:
                return True
                
            ex_tokens = set(ex_clean.split())
            ext_tokens = set(ext_clean.split())
            
            # Remove stop words
            stopwords = {"the", "a", "an", "and", "or", "to", "for", "in", "of", "on", "is", "it", "with"}
            ex_tokens = ex_tokens - stopwords
            ext_tokens = ext_tokens - stopwords
            
            if len(ex_tokens.intersection(ext_tokens)) > 0:
                return True
                
            return False

        # We perform Bidirectional Substring Matching against 'verbatim_quote'
        case_passed = True
        matched_extractions = set()

        for expected in expected_entities:
            expected_quote = expected["verbatim_quote"]

            # Look for it in the extracted items
            found = False
            for i, item in enumerate(result.items):
                extracted = item.verbatim_quote
                if is_match(expected_quote, extracted):
                    found = True
                    matched_extractions.add(i)
                    break

            if not found:
                print(f"  [FAIL] Missed entity (FN): '{expected['verbatim_quote']}'")
                case_passed = False
                total_fn += 1
            else:
                total_tp += 1

        # Check for False Positives (extracted things that don't match any golden entity)
        for i, item in enumerate(result.items):
            if i not in matched_extractions:
                print(f"  [WARN] Over-extracted entity (FP): '{item.verbatim_quote}'")
                case_passed = False
                total_fp += 1

        if case_passed:
            print(f"  [PASS] Successfully extracted all expected entities with no extras.")

        total_cost_usd += result.cost_usd or 0.0
        total_tests += 1

    if total_tests == 0:
        print("\nNo labeled cases found. Please add labels to entity_discovery_seed.json.")
        return

    recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0.0
    precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0.0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    print(f"\n--- OFFLINE EVAL RESULTS ---")
    print(f"Total Graded Cases: {total_tests}")
    print(f"True Positives (TP): {total_tp}")
    print(f"False Positives (FP): {total_fp}")
    print(f"False Negatives (FN): {total_fn}")
    print(f"----------------------------")
    print(f"Recall:    {recall * 100:.2f}%")
    print(f"Precision: {precision * 100:.2f}%")
    print(f"F1 Score:  {f1 * 100:.2f}%")
    print(f"----------------------------")
    print(f"Total LLM Cost: ${total_cost_usd:.4f}")


if __name__ == "__main__":
    main()
