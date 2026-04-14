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
        pass # Allow calling process to handle the empty extraction response gracefully.
        
    return [], 0.0


from pipeline.prompts.entity_discovery import JudgeValidation, get_discovery_judge_prompt

async def run_blind_evaluation(
    extraction: dict,
    semaphore: asyncio.Semaphore,
    judge_model_name: str,
    judge_thinking_level: str
) -> tuple[Optional[JudgeValidation], float, int, int, str]:
    """Uses a reference-free Judge to evaluate a live extracted entity."""
    if not extraction:
        return None, 0.0, 0, 0, "{}"

    client = genai.Client()
    
    # Strip unnecessary noise, keep relevant text mapping
    cleaned_extraction = {
        "brand": extraction.get("brand"), 
        "productName": extraction.get("productName"), 
        "context_snippet": extraction.get("body", ""),
        "llm_generation_prompt": extraction["llm_generation_prompt"]
    }

    prompt = get_discovery_judge_prompt(cleaned_extraction)

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=JudgeValidation,
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
            return JudgeValidation(**result_dict), cost, input_tokens, output_tokens, response.text
    except Exception as e:
        pass # Allow calling process to handle the empty response gracefully.
        
    return None, 0.0, 0, 0, "{}"

from pipeline.prompts.entity_extraction import ExtractionCanaryValidation, ExtractionCanaryResult, get_extraction_canary_prompt

async def run_extraction_blind_canary_evaluation(
    extractions: list[dict],
    semaphore: asyncio.Semaphore,
    judge_model_name: str,
    judge_thinking_level: str
) -> tuple[list[ExtractionCanaryValidation], float, int, int, str]:
    if not extractions:
        return [], 0.0, 0, 0, "[]"

    client = genai.Client()
    
    batch_for_judge = [{"index": i, "quote": e.get("quote"), "sentiment": e.get("sentiment"), "ownershipDurationMonths": e.get("ownershipDurationMonths")} for i, e in enumerate(extractions)]

    prompt = get_extraction_canary_prompt(batch_for_judge)

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=ExtractionCanaryResult,
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
            return [ExtractionCanaryValidation(**m) for m in result_dict.get("validations", [])], cost, input_tokens, output_tokens, response.text
    except Exception as e:
        pass # Allow calling process to handle the empty extraction canary response gracefully.
        
    return [], 0.0, 0, 0, "[]"
