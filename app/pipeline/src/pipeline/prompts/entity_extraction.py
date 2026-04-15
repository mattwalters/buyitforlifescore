import json
from typing import Literal, Optional

from pydantic import BaseModel, Field

# --- Extraction Schema ---


class EntityExtraction(BaseModel):
    # Core Identification
    quote: str = Field(
        description="Extract exactly 1 phrase or continuous sentence from the text where the product opinion was explicitly named or heavily contextualized. Keep it short."
    )
    specificityLevel: Literal["EXACT_MODEL", "PRODUCT_LINE", "BRAND_ONLY"]
    acquiredPrice: Optional[float] = Field(
        description="Extract the explicitly stated purchase price OR estimated retail price mentioned (e.g., 'heard they are like $200' -> 200). Convert to a float."
    )

    # Longevity & Survival
    status: Optional[Literal["ACTIVE_USE", "RETIRED", "BROKEN", "SOLD_OR_GIFTED"]] = Field(
        description="The current explicitly stated status of the item."
    )
    ownershipDurationMonths: Optional[int] = Field(
        description="Calculate the TOTAL age/lifespan of the individual unit, even if passed down. If relative time is used (e.g. 'for a decade'), estimate the months (10 years = 120)."
    )

    # Usage Profile
    usageFrequency: Optional[Literal["DAILY", "WEEKLY", "MONTHLY", "SEASONAL", "RARELY"]] = Field(
        description="Frequency of use. DAILY (most days), WEEKLY (a few times/week), MONTHLY (a few times/month), SEASONAL (used for a season), RARELY (once/twice a year). If usage varies over time, extract the CURRENT or MOST RECENT frequency. If a range is given, err towards the baseline (e.g., choose WEEKLY over DAILY)."
    )

    # Failures & Maintenance
    primaryFlawOrFailure: Optional[str] = Field(
        description="The primary functional or physical failure that compromises the utility of the item. DO include material compromises (rust, chips, delamination) and accessory failures (laces, batteries, earpads) even if still used. Do NOT extract purely cosmetic wear (scuffs, fading)."
    )
    diyRepairability: Optional[Literal["EASY", "SPECIAL_TOOLS_REQUIRED", "IMPOSSIBLE_SEALED"]] = Field(
        description="Extract ONLY if the user explicitly describes the difficulty of a DIY (Do-It-Yourself) repair. Routine maintenance (oiling, cleaning) or professional repairs (taking to a cobbler, mechanic) do NOT count as DIY repair and MUST be null."
    )
    warrantyExperience: Optional[Literal["SEAMLESS_REPLACEMENT", "HONORED_WITH_FRICTION", "REJECTED", "IGNORED"]] = (
        Field(
            description="The warranty experience. If the user explicitly praises the warranty policy or considers it a massive positive (e.g., '100-year warranty'), extract as SEAMLESS_REPLACEMENT, even if they haven't personally used it."
        )
    )

    # Sentiment
    sentiment: Literal["POSITIVE", "NEUTRAL", "NEGATIVE"] = Field(description="Overall sentiment about the product.")


def get_extraction_prompt(brand: str, productName: str, target_authored_at: str, text: str, parent_text: str) -> str:
    return f"""You are a product analyst studying "Buy It For Life" patterns on Reddit.
You are evaluating a specific product mentioned by a user.
Target Product: {brand} {productName}

Context: The specific text you are analyzing was written on {target_authored_at}. Use this exact date as the absolute "present day" for all relative time calculations. Any mention of "bought 10 years ago" or "bought in 1980" should be calculated strictly relative to {target_authored_at}. Do not calculate relative to your system's current year.

Extract the exact nuanced opinions the user had regarding this specific product.
If a field is not explicitly mentioned or cannot be confidently inferred, map it to null. Do not hallucinate data.

CRITICAL EDGE CASES:
1. ISOLATION: You are evaluating ONLY the Target Product. Do NOT attribute the failure modes, ownership duration, or sentiment of other products mentioned in the text to the Target Product.
2. REVIEWS vs SARCASM: Bypass sarcasm. If the text says it broke immediately but is ironically called 'the best true BIFL', the sentiment is NEGATIVE.
3. CONTRADICTIONS: For contradictory reviews (e.g. 'indestructible but incredibly uncomfortable'), grade the sentiment as NEUTRAL unless the user explicitly praises it overall.
4. SENTIMENT NOBLE DEATHS vs ABANDONMENT: If a product 'finally died' after a long well-used life, sentiment is POSITIVE because it served its purpose well. If they abandoned the product for a competitor due to a flaw, the sentiment is NEGATIVE.
5. SENTIMENT REGRET & DECENT: If the user says a product is 'decent' or explicitly wishes they bought a competitor (regret) despite good durability, the sentiment is NEUTRAL at best, or NEGATIVE if critical flaws are present.
6. DECADE MATH: If a decade is mentioned (e.g., 'made in the 50s' or 'from the 70s'), use the start of the decade (e.g., 1950, 1970) to calculate the lifespan relative to {target_authored_at}.

Text to analyze:
{text}

Parent Context (for reference):
{parent_text}
"""


# --- Blind Canary Evaluation Schema ---


class ExtractionCanaryValidation(BaseModel):
    is_sentiment_logical: bool = Field(
        description="True if the extracted sentiment makes logical sense given the extracted quote. False if there is a severe mismatch (e.g. quote says it's garbage but sentiment is POSITIVE)."
    )
    is_lifespan_logical: bool = Field(
        description="True if the ownershipDurationMonths is physically possible (e.g., <= 1200 months/100 years). False if it is an obvious hallucination or extreme exaggeration. If null, return True."
    )
    reasoning: str = Field(description="A brief 1-sentence reasoning for the decisions.")


class ExtractionCanaryResult(BaseModel):
    validations: list[ExtractionCanaryValidation] = Field(
        description="List of validations matching the exact order of the input dataset."
    )


def get_extraction_canary_prompt(batch_for_judge: list) -> str:
    return f"""You are an expert Data Quality Judge analyzing a pipeline that extracts structured metadata from "Buy It For Life" (BIFL) product reviews.
Your job is to blindly evaluate a sample batch of extractions to determine if the LLM hallucinated, contradicted itself, or emitted physically impossible metadata.

CRITICAL RULES FOR INVALID EXTRACTIONS:
1. SENTIMENT MISMATCH (is_sentiment_logical = False): The extracted `sentiment` directly contradicts the literal text in the `quote`.
2. BIOLOGICAL IMPOSSIBILITY (is_lifespan_logical = False): The extracted `ownershipDurationMonths` exceeds 1200 months (100 years), representing an obvious hallucination or generic hyperbole rather than actual human ownership.

Note: If a field is null, its corresponding logic check is True by default.

Batch to Evaluate:
{json.dumps(batch_for_judge, indent=2)}

Output the validation result matching the exact index order of the batch."""
