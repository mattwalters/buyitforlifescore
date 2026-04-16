import duckdb
from google.genai import types
from pydantic import BaseModel, TypeAdapter

from pipeline.schemas.reddit_llm_payloads import SilverRedditLlmPayload
from pipeline.utils.ai import AiModel, get_client, invoke_entity_discovery
from pipeline.utils.paths import get_read_path


class JudgeResult(BaseModel):
    grade: str  # "PASS" or "FAIL"
    reasoning: str


def main():
    con = duckdb.connect()
    source_payloads = get_read_path("silver/reddit_llm_payloads/*/*/*.parquet")

    query = f"""
        SELECT * FROM read_parquet('{source_payloads}')
        USING SAMPLE 20
    """

    try:
        df = con.execute(query).fetchdf()
    except Exception as e:
        print(f"Failed to load sample: {e}")
        return

    records = df.to_dict("records")
    payloads = TypeAdapter(list[SilverRedditLlmPayload]).validate_python(records)

    client = get_client()

    print(f"Running Online Eval (LLM-as-a-Judge) on {len(payloads)} cases...")

    passes = 0
    fails = 0

    for payload in payloads:
        print(f"Testing {payload.bundle_id}...")
        try:
            result = invoke_entity_discovery(client, payload)
        except Exception as e:
            print(f"[ERROR] Exception invoking pipeline: {e}")
            fails += 1
            continue

        if not result.items:
            # If nothing extracted, nothing to judge objectively against hallucinations
            print("  [SKIP] No entities extracted to judge.")
            continue

        # The judge prompt evaluates hallucination and scoping violations
        system_instruction = (
            "You are an expert, impartial evaluator for an information extraction pipeline.\n"
            "You will be given the EXACT PROMPT provided to the extraction LLM, and the EXACT JSON OUTPUT it produced.\n\n"
            "Your job is to catch two fatal errors: \n"
            "1. Boundary Leakage: Did the JSON output contain products that were ONLY mentioned inside <context_block> tags? (They must only come from <analysis_block>).\n"
            "2. Hallucination: Did the JSON output invent a string that isn't verbatim in the text?\n\n"
            "If it made either error, grade it FAIL. If it correctly adhered to the rules, grade it PASS."
        )

        judge_prompt = f"### ORIGINAL PROMPT\n{result.prompt_text}\n\n### EXTRACTION JSON\n{result.raw_json}\n\nEvaluate."

        config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=0.0,
            response_mime_type="application/json",
            response_schema=JudgeResult,
        )

        try:
            # Use a smarter model for judging
            response = client.models.generate_content(
                model=AiModel.GEMINI_2_5_FLASH.value,
                contents=judge_prompt,
                config=config,
            )
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


if __name__ == "__main__":
    main()
