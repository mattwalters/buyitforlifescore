import json
import asyncio
from typing import Optional
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

from pipeline.utils.pricing import calculate_gemini_cost

# --- Alignment Judge Schemas (Offline Eval) ---

class AlignmentMapping(BaseModel):
    expected_index: int = Field(description="The index from the expected_benchmark list.")
    extracted_index: int = Field(description="The index from the extracted entities list.")

class AlignmentResult(BaseModel):
    mappings: list[AlignmentMapping] = Field(description="List of alignments mapping candidates to ground truth.")

async def run_judge_alignment(
    expected_mentions: list[dict], 
    extracted_mentions: list[dict], 
    semaphore: asyncio.Semaphore,
    judge_model_name: str,
    judge_thinking_level: str
) -> tuple[list[AlignmentMapping], float]:
    """Uses LLM Judge to semantically map Candidate -> Benchmark."""
    if not expected_mentions and not extracted_mentions:
        return [], 0.0

    client = genai.Client()
    
    # Strip unnecessary noise
    expected_for_judge = [{"index": i, "author_id": e.get("author_id"), "brand": e.get("brand"), "productName": e.get("productName")} for i, e in enumerate(expected_mentions)]
    extracted_for_judge = [{"index": i, "author_id": e.get("author_id"), "brand": e.get("brand"), "productName": e.get("productName")} for i, e in enumerate(extracted_mentions)]

    prompt = f"""You are an expert taxonomy aligner. Match extracted product mentions to the expected benchmark list. 
The items might have spelling errors or differing specificity, but you must map them if they refer to the exact same product from the underlying text.
CRITICAL RULE: The extracted item and the expected item MUST have the exact same 'author_id' to be considered a match. Do not map mismatched authors.

Expected Benchmark:
{json.dumps(expected_for_judge, indent=2)}

Extracted Entities:
{json.dumps(extracted_for_judge, indent=2)}

Return a list of mappings bridging the matching indices."""

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=AlignmentResult,
        temperature=0.0,
        thinking_config=types.ThinkingConfig(thinking_level=judge_thinking_level)
    )

    try:
        async with semaphore:
            response = await client.aio.models.generate_content(
                model=judge_model_name,
                contents=prompt,
                config=gen_config,
            )
        
        cost = 0.0
        if response.usage_metadata:
            cost = calculate_gemini_cost(judge_model_name, response.usage_metadata)
            
        if response.text:
            result_dict = json.loads(response.text)
            return [AlignmentMapping(**m) for m in result_dict.get("mappings", [])], cost
    except Exception as e:
        print(f"Skipping judge alignment due to API Error: {e}")
        
    return [], 0.0


# --- Blind Judge Schemas (Online Canary Check) ---

class CanaryValidation(BaseModel):
    is_valid_durable_good: bool = Field(description="True if the extraction is a valid physical, durable BIFL brand/product. False if it is a retailer, hallucination, raw material, metaphor, or generic concept.")
    reasoning: str = Field(description="A brief 1-sentence reasoning for the decision.")

class CanaryResult(BaseModel):
    validations: list[CanaryValidation] = Field(description="List of validations matching the exact order of the input dataset.")

async def run_blind_canary_evaluation(
    extractions: list[dict],
    semaphore: asyncio.Semaphore,
    judge_model_name: str,
    judge_thinking_level: str
) -> tuple[list[CanaryValidation], float, int, int, str]:
    """Uses a reference-free Judge to evaluate live extracted entities for statistical canary checking."""
    if not extractions:
        return [], 0.0, 0, 0, "[]"

    client = genai.Client()
    
    # Strip unnecessary noise, keep relevant text mapping
    batch_for_judge = [{"index": i, "brand": e.get("brand"), "productName": e.get("productName"), "context_snippet": e.get("body", "")} for i, e in enumerate(extractions)]

    prompt = f"""You are an expert Data Quality Judge analyzing a pipeline that extracts "Buy It For Life" (BIFL) product recommendations from Reddit.
Your job is to blindly evaluate a sample batch of extractions to determine if they are legitimate physical, durable goods.

CRITICAL RULES FOR INVALID EXTRACTIONS (Return False):
1. RETAILERS: "Costco", "Amazon", "Target", "Home Depot" are invalid. (Unless it explicitly says the in-house brand like Kirkland).
2. RAW MATERIALS: "Leather", "Wood", "Plastic", "Steel" are invalid brands.
3. METAPHORS: "Bulletproof", "Tank", "Workhorse" are invalid.
4. UNKNOWN/GENERIC: "unknown", "BRAND_ONLY", "N/A" are invalid.

Batch to Evaluate:
{json.dumps(batch_for_judge, indent=2)}

Output the validation result matching the exact index order of the batch."""

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=CanaryResult,
        temperature=0.0,
        thinking_config=types.ThinkingConfig(thinking_level=judge_thinking_level)
    )

    try:
        async with semaphore:
            response = await client.aio.models.generate_content(
                model=judge_model_name,
                contents=prompt,
                config=gen_config,
            )
        
        cost = 0.0
        input_tokens = 0
        output_tokens = 0
        if response.usage_metadata:
            cost = calculate_gemini_cost(judge_model_name, response.usage_metadata)
            input_tokens = response.usage_metadata.prompt_token_count or 0
            output_tokens = response.usage_metadata.candidates_token_count or 0
            
        if response.text:
            result_dict = json.loads(response.text)
            return [CanaryValidation(**m) for m in result_dict.get("validations", [])], cost, input_tokens, output_tokens, response.text
    except Exception as e:
        print(f"Skipping blind judge evaluation due to API Error: {e}")
        
    return [], 0.0, 0, 0, "[]"
