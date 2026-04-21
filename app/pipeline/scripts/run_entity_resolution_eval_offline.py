import argparse
import json
import os
import string
import sys

# Allow imports from the src directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline.utils.ai import get_client, invoke_entity_resolution  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Run offline evaluation for entity resolution against golden labels.")
    parser.add_argument("-n", "--num-cases", type=int, default=None, help="Limit to first N labeled cases.")
    args = parser.parse_args()

    seed_file = os.path.join(os.path.dirname(__file__), "..", "evals", "datasets", "entity_resolution_seed.json")
    if not os.path.exists(seed_file):
        print("entity_resolution_seed.json not found! Run materialize_entity_resolution_seed.py first.")
        sys.exit(1)

    with open(seed_file, "r") as f:
        data = json.load(f)

    client = get_client()

    total_tests = 0
    total_brand_matches = 0
    total_brand_checks = 0
    total_specificity_matches = 0
    total_specificity_checks = 0
    total_null_brand_correct = 0
    total_null_brand_checks = 0
    total_cost_usd = 0.0

    # Filter to labeled cases only
    labeled_cases = [c for c in data if c.get("expected_resolutions")]
    if not labeled_cases:
        print("\nNo labeled cases found. Please add golden labels to entity_resolution_seed.json.")
        return

    if args.num_cases is not None:
        labeled_cases = labeled_cases[: args.num_cases]

    print(f"Running Offline Eval on {len(labeled_cases)} labeled cases...\n")

    for case in labeled_cases:
        node_id = case["node_id"]
        node_text = case.get("node_text", "")
        verbatim_quotes = case.get("verbatim_quotes", [])
        expected = case.get("expected_resolutions", [])

        print(f"Testing {node_id} ({len(verbatim_quotes)} quotes)...")

        try:
            result = invoke_entity_resolution(
                client,
                node_id=node_id,
                submission_id=case.get("submission_id", ""),
                node_text=node_text,
                verbatim_quotes=verbatim_quotes,
            )
        except Exception as e:
            print(f"  [FAIL] Exception invoking pipeline: {e}")
            total_tests += 1
            continue

        total_cost_usd += result.cost_usd or 0.0

        # Build a lookup of LLM results by verbatim_quote for matching
        result_by_quote: dict[str, dict] = {}
        for item in result.items:
            result_by_quote[item.verbatim_quote.lower().strip()] = {
                "brand": item.brand,
                "product_line": item.product_line,
                "product_model": item.product_model,
                "specificity_level": item.specificity_level,
            }

        # Liberal matching helper
        def find_match(expected_quote: str) -> dict | None:
            eq_clean = expected_quote.lower().translate(str.maketrans("", "", string.punctuation)).strip()
            for rq, rdata in result_by_quote.items():
                rq_clean = rq.translate(str.maketrans("", "", string.punctuation))
                if eq_clean in rq_clean or rq_clean in eq_clean:
                    return rdata
                eq_tokens = set(eq_clean.split()) - {"the", "a", "an", "and", "or", "to", "for", "in", "of"}
                rq_tokens = set(rq_clean.split()) - {"the", "a", "an", "and", "or", "to", "for", "in", "of"}
                if len(eq_tokens.intersection(rq_tokens)) > 0:
                    return rdata
            return None

        case_passed = True
        for exp in expected:
            exp_quote = exp["verbatim_quote"]
            matched = find_match(exp_quote)

            if not matched:
                print(f"  [MISS] No LLM output matched quote: '{exp_quote}'")
                case_passed = False
                continue

            # Check brand
            exp_brand = exp.get("brand")
            llm_brand = matched.get("brand")
            if exp_brand is None and llm_brand is None:
                total_null_brand_correct += 1
                total_null_brand_checks += 1
            elif exp_brand is None and llm_brand is not None:
                print(f"  [FAIL] Expected brand=null for '{exp_quote}', got '{llm_brand}'")
                total_null_brand_checks += 1
                case_passed = False
            elif exp_brand is not None and llm_brand is None:
                print(f"  [FAIL] Expected brand='{exp_brand}' for '{exp_quote}', got null")
                total_brand_checks += 1
                case_passed = False
            else:
                total_brand_checks += 1
                if exp_brand.lower().strip() == llm_brand.lower().strip():
                    total_brand_matches += 1
                else:
                    print(f"  [FAIL] Brand mismatch for '{exp_quote}': expected '{exp_brand}', got '{llm_brand}'")
                    case_passed = False

            # Check specificity
            exp_spec = exp.get("specificity_level")
            llm_spec = matched.get("specificity_level")
            if exp_spec:
                total_specificity_checks += 1
                if exp_spec == llm_spec:
                    total_specificity_matches += 1
                else:
                    print(f"  [FAIL] Specificity mismatch for '{exp_quote}': expected '{exp_spec}', got '{llm_spec}'")
                    case_passed = False

        if case_passed:
            print("  [PASS] All fields matched.")

        total_tests += 1

    if total_tests == 0:
        print("\nNo cases evaluated.")
        return

    brand_rate = total_brand_matches / total_brand_checks * 100 if total_brand_checks > 0 else 0.0
    spec_rate = total_specificity_matches / total_specificity_checks * 100 if total_specificity_checks > 0 else 0.0
    null_rate = total_null_brand_correct / total_null_brand_checks * 100 if total_null_brand_checks > 0 else 0.0

    print("\n--- OFFLINE EVAL RESULTS ---")
    print(f"Total Graded Cases:       {total_tests}")
    print(f"Brand Match Rate:         {brand_rate:.1f}% ({total_brand_matches}/{total_brand_checks})")
    print(f"Specificity Match Rate:   {spec_rate:.1f}% ({total_specificity_matches}/{total_specificity_checks})")
    print(f"Null-Brand Accuracy:      {null_rate:.1f}% ({total_null_brand_correct}/{total_null_brand_checks})")
    print("----------------------------")
    print(f"Total LLM Cost:           ${total_cost_usd:.4f}")


if __name__ == "__main__":
    main()
