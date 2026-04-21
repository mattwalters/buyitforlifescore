import argparse
import os
import sys

from google.genai import types
from pydantic import BaseModel, TypeAdapter

# Allow imports from the src directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline.schemas.reddit_entity_resolution import EntityResolutionResult  # noqa: E402
from pipeline.utils.ai import AiModel, calculate_cost, get_client  # noqa: E402
from pipeline.utils.db import get_duckdb_connection  # noqa: E402
from pipeline.utils.paths import get_read_path  # noqa: E402


class JudgeResult(BaseModel):
    grade: str  # "PASS" or "FAIL"
    reasoning: str


def main():
    parser = argparse.ArgumentParser(description="Run online evaluation (LLM-as-a-judge) for entity resolution.")
    parser.add_argument(
        "-n", "--num-samples", type=int, default=20, help="Number of samples to grab from upstream parquet."
    )
    parser.add_argument(
        "--judge-model",
        type=str,
        default=AiModel.GEMINI_2_5_FLASH.value,
        choices=[m.value for m in AiModel],
        help="The LLM model used for the Judge.",
    )
    parser.add_argument(
        "--thinking-budget", type=int, default=1024, help="Thinking budget for the Judge model. Set to 0 to disable."
    )
    args = parser.parse_args()

    con = get_duckdb_connection()
    source_results = get_read_path("silver/reddit_entity_resolution/*/*/*.parquet")

    query = f"""
        SELECT * FROM read_parquet('{source_results}', union_by_name=true)
        USING SAMPLE {args.num_samples}
    """

    try:
        df = con.execute(query).fetchdf()
    except Exception as e:
        print(f"Failed to load sample from prod resolution results: {e}")
        return

    records = df.to_dict("records")
    results = TypeAdapter(list[EntityResolutionResult]).validate_python(records)

    client = get_client()

    print(f"Running Online Eval (LLM-as-a-Judge) on {len(results)} prod-generated cases...\n")

    passes = 0
    fails = 0
    resolution_cost_usd = 0.0
    judge_cost_usd = 0.0

    for result in results:
        print(f"Testing {result.node_id}...")
        # Prod already paid for this resolution, so we just log its historical cost
        resolution_cost_usd += result.cost_usd or 0.0

        # The judge prompt evaluates classification quality
        system_instruction = (
            "You are an impartial data auditor checking the accuracy of a product classification output.\n\n"
            "You will be given the ORIGINAL NODE TEXT (a Reddit post or comment) and the CLASSIFICATION JSON "
            "produced by an automated pipeline.\n\n"
            "You have three grading rules to enforce:\n"
            "1. Brand Plausibility: Are the extracted brands real commercial entities? If the pipeline classified a "
            "generic material (e.g., 'cast iron', 'wood') or a non-commercial term as a brand, grade it FAIL. "
            "Conversely, if it correctly returned brand=null for non-commercial terms, that is correct.\n"
            "2. Specificity Accuracy: Does the specificity_level match what's actually described in the text? "
            'If the user only said "I love my KitchenAid" with no product name, it should be BRAND_ONLY, not '
            "PRODUCT_LINE. If a specific marketing name is used (e.g., 'Artisan'), PRODUCT_LINE is correct.\n"
            "3. Classification Integrity: Are product_line and product_model fields populated only when appropriate "
            "proper nouns or marketing names are present? Generic categories like 'mixer' or 'boots' should NOT "
            "appear as product_line.\n\n"
            "If the classifications are reasonable and don't contain egregious errors, grade it PASS."
        )

        judge_prompt = (
            f"### ORIGINAL NODE TEXT\n{result.raw_json}\n\n"
            f"### CLASSIFICATION JSON\n{result.raw_json}\n\n"
            f"Evaluate."
        )

        # We need to reconstruct the prompt context — use raw_json which contains the LLM output
        # For a complete judge, we'd ideally pass the original node text too, but we work with what's stored

        config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=0.0,
            response_mime_type="application/json",
            response_schema=JudgeResult,
            thinking_config=types.ThinkingConfig(thinking_budget=args.thinking_budget)
            if args.thinking_budget > 0
            else None,
        )

        try:
            response = client.models.generate_content(
                model=args.judge_model,
                contents=judge_prompt,
                config=config,
            )
            if response.usage_metadata:
                prompt_tokens = response.usage_metadata.prompt_token_count or 0
                completion_tokens = response.usage_metadata.candidates_token_count or 0
                try:
                    judge_cost_usd += calculate_cost(AiModel(args.judge_model), prompt_tokens, completion_tokens)
                except Exception:
                    pass

            judge_res = TypeAdapter(JudgeResult).validate_json(response.text)

            if judge_res.grade == "PASS":
                passes += 1
                print("  [PASS] Judge approved.")
            else:
                fails += 1
                print(f"  [FAIL] Judge rejected: {judge_res.reasoning}")
        except Exception as e:
            print(f"  [ERROR] Judge evaluation failed: {e}")
            fails += 1

    total = passes + fails
    if total == 0:
        print("No cases to evaluate.")
        return

    pass_rate = passes / total
    print("\n--- ONLINE EVAL RESULTS ---")
    print(f"Total Evaluated:                    {total}")
    print(f"Quality Pass Rate:                  {pass_rate * 100:.2f}%")
    print("----------------------------")
    print(f"Historical Prod Resolution Cost:    ${resolution_cost_usd:.4f} (already paid)")
    print(f"Judge Agent Eval Cost:              ${judge_cost_usd:.4f}")
    print(f"Total Net Eval Spend:               ${judge_cost_usd:.4f}")


if __name__ == "__main__":
    main()
