import argparse
from google.genai import types
from pydantic import BaseModel, TypeAdapter

from pipeline.schemas.reddit_entity_discovery import DiscoveryResult
from pipeline.utils.ai import AiModel, calculate_cost, get_client
from pipeline.utils.db import get_duckdb_connection
from pipeline.utils.paths import get_read_path


class JudgeResult(BaseModel):
    grade: str  # "PASS" or "FAIL"
    reasoning: str


def main():
    parser = argparse.ArgumentParser(description="Run online evaluation (LLM-as-a-judge).")
    parser.add_argument("-n", "--num-samples", type=int, default=20, help="Number of samples to grab from upstream parquet.")
    parser.add_argument("--judge-model", type=str, default=AiModel.GEMINI_2_5_FLASH.value, choices=[m.value for m in AiModel], help="The LLM model used for the Judge.")
    parser.add_argument("--thinking-budget", type=int, default=1024, help="Thinking budget for the Judge model. Set to 0 to disable.")
    args = parser.parse_args()

    con = get_duckdb_connection()
    source_results = get_read_path("silver/reddit_entity_discovery/*/*/*.parquet")

    query = f"""
        SELECT * FROM read_parquet('{source_results}', union_by_name=true)
        USING SAMPLE {args.num_samples}
    """

    try:
        df = con.execute(query).fetchdf()
    except Exception as e:
        print(f"Failed to load sample from prod discovery results: {e}")
        return

    records = df.to_dict("records")
    results = TypeAdapter(list[DiscoveryResult]).validate_python(records)

    client = get_client()

    print(f"Running Online Eval (LLM-as-a-Judge) on {len(results)} prod-generated cases...")

    passes = 0
    fails = 0
    extraction_cost_usd = 0.0
    judge_cost_usd = 0.0

    for result in results:
        print(f"Testing {result.bundle_id}...")
        # Prod already paid for this extraction, so we just log its historical cost
        extraction_cost_usd += result.cost_usd or 0.0

        # The judge prompt evaluates hallucination and scoping violations objectively
        system_instruction = (
            "You are an impartial data auditor checking the accuracy of an extraction file against a source text.\n\n"
            "You will be given the SOURCE XML TEXT and the EXTRACTED JSON.\n\n"
            "You have three grading rules to enforce:\n"
            "1. Boundary Verification: The SOURCE XML contains <analysis_block> and <context_block> tags. The extracted entities must securely map to the content of the specified <analysis_block> indexes. If an extraction is pulled entirely from a <context_block>, grade it FAIL.\n"
            "2. Hallucination Verification: The extracted JSON is expected to normalize and aggregate brand names. An extraction string passes as long as it is a highly accurate semantic match, abbreviation, capitalization fix, or minor URL derivative of a string actually present in the specified blocks (e.g. 'L.L. Bean' matches 'LL Bean', 'Vitamix' matches 'Vitamic'). It does NOT have to be character-for-character verbatim. However, if the JSON extracts a completely hallucinated entity, or extracts non-commercial entities (like a person's name or a subreddit acronym), grade it FAIL.\n"
            "3. Recall Verification: Do not penalize missed Retail stores (e.g. Costco, Target, Amazon). However, if the EXTRACTED JSON is empty `[]`, but the SOURCE XML clearly discusses a Proprietary Manufacturer or Specific Product Model inside an <analysis_block>, grade it FAIL for missing an obvious extraction.\n\n"
            "If the extractions successfully map to actual entities in the text without egregious hallucinations, grade it PASS."
        )

        judge_prompt = f"### SOURCE XML TEXT\n{result.prompt_text}\n\n### EXTRACTED JSON\n{result.raw_json}\n\nEvaluate."

        config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=0.0,
            response_mime_type="application/json",
            response_schema=JudgeResult,
            thinking_config=types.ThinkingConfig(thinking_budget=args.thinking_budget) if args.thinking_budget > 0 else None,
        )

        try:
            # Use a smarter model for judging
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
                print(f"  [PASS] Judge approved.")
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
    print(f"\n--- ONLINE EVAL RESULTS ---")
    print(f"Total Evaluated: {total}")
    print(f"Quality Pass Rate: {pass_rate * 100:.2f}%")
    print(f"----------------------------")
    print(f"Historical Prod Extraction Cost: ${extraction_cost_usd:.4f} (already paid)")
    print(f"Judge Agent Eval Cost:           ${judge_cost_usd:.4f}")
    print(f"Total Net Eval Spend:            ${judge_cost_usd:.4f}")


if __name__ == "__main__":
    main()
