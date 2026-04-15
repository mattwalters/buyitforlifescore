from pydantic import BaseModel, Field


class JudgeDiscoveryResult(BaseModel):
    missed_brands: list[str] = Field(
        description="A list of obvious physical commercial product brands or manufacturers that were mentioned in the text but NOT extracted by the pipeline."
    )
    hallucinations: list[str] = Field(
        description="A list of items that the pipeline mistakenly extracted that are NOT physical commercial brands (e.g. generic nouns like 'shoe', 'wood', or 'car' with no brand attached)."
    )
    reasoning: str = Field(description="A brief explanation justifying the misses and hallucinations.")


def get_blind_judge_discovery_prompt(thread_text: str, extractions: list[dict]) -> str:
    import json

    extractions_str = json.dumps(extractions, indent=2) if extractions else "[]"

    return f"""You are the final Quality Assurance Auditor for a consumer goods data pipeline.
Your job is to read a user discussion thread, and then review the list of extracted physical product brands exactly as the pipeline outputted them.

You do not know the exact instructions the pipeline followed. You are simply judging its final output based on this high-level Human QA Rubric:

HUMAN QA RUBRIC:
1. Goal: The pipeline should have extracted every physical commercial brand, manufacturer, or named product line mentioned in the text.
2. Misses (False Negatives): Did the pipeline completely miss an obvious product brand? (e.g. they discussed 'Sony', but 'Sony' is not in the extraction list). Note: If the pipeline extracted 'Sony Playstation' instead of 'Sony', that is a valid catch, do NOT mark it as a miss.
3. Hallucinations (False Positives): Did the pipeline extract generic item classifications that DO NOT have a brand attached? (e.g. it extracted 'boots', 'teak wood', 'a car', or 'the company').
4. Mapping Hallucinations (False Positives): Did the pipeline extract a valid brand, but hallucinate the `author_id` or `source_block_ids`? YOU MUST VERIFY THE MAPPING. If the block IDs listed in the extraction do not contain the product, or were written by a different author, log the entire extraction as a hallucination due to invalid mapping!

ALLOWED EXTRACTIONS (DO NOT PENALIZE):
- Retail stores (like 'Amazon', 'Costco', 'Target') are perfectly acceptable extractions.
- Raw model numbers (like 'XJ-900') are acceptable.
- Exact repetitions or slight casing variations are fine.

Identify any missed brands and any hallucinations based strictly on the text provided.

--- Thread Text ---
{thread_text}

--- Pipeline Extractions ---
[{extractions_str}]
"""
