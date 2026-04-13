import os
import sys
import json
import asyncio
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from google import genai
from google.genai import types

class ExpectedAttributes(BaseModel):
    status: Optional[str] = None
    ownershipDurationMonths: Optional[int] = None
    usageFrequency: Optional[str] = None
    usageIntensity: Optional[str] = None
    maintenanceDiscipline: Optional[str] = None
    primaryFlawOrFailure: Optional[str] = None
    diyRepairability: Optional[str] = None
    warrantyExperience: Optional[str] = None
    sentiment: str
    alternativeBrandMentioned: Optional[str] = None
    acquiredPrice: Optional[float] = None
    retailerMentioned: Optional[str] = None

class SyntheticFixture(BaseModel):
    id: str
    brand: str
    productName: str
    text: str
    parent_text: str
    expected: ExpectedAttributes

class FixtureList(BaseModel):
    fixtures: List[SyntheticFixture]

async def generate_fixtures():
    client = genai.Client()
    
    prompt = """
    You are an expert data annotator creating benchmarks for an Aspect-Based Sentiment Analysis (ABSA) model that extracts data from Reddit reviews of "Buy It For Life" products.
    
    Currently we have 20 benchmarks. I need you to generate 30 MORE highly realistic, chaotic Reddit scenarios.
    
    Make them brutally difficult. Include:
    - Sarcasm
    - Misspellings
    - Contradictory statements (e.g. ugly but durable)
    - Strange generational timeframes (e.g. "my grandfather gave it to my dad in 1980")
    - Mentions of multiple brands where the user hates one but loves the Target Product
    - Hearsay (e.g. "I heard they suck")
    - Explicit prices and retailers
    
    Ensure the `expected` object exactly perfectly aligns with what a strict human grader would say is the correct ABSA extraction for the SPECIFIC Target Product defined in `brand` and `productName`.
    
    For IDs, start from "21" and go to "50".
    """
    
    print("Generating 30 fixtures... this might take 30 seconds.")
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=FixtureList,
            temperature=0.7
        )
    )
    
    new_data = json.loads(response.text)
    
    file_path = Path(__file__).parent / "fixtures" / "silver_entity_attributes_benchmark.json"
    with open(file_path, "r") as f:
        existing = json.load(f)
        
    existing.extend(new_data["fixtures"])
    
    with open(file_path, "w") as f:
        json.dump(existing, f, indent=2)
        
    print(f"Successfully generated and appended {len(new_data['fixtures'])} new fixtures to reach {len(existing)} total.")

if __name__ == "__main__":
    if not os.environ.get("GEMINI_API_KEY"):
        print("ERROR: GEMINI_API_KEY missing.")
        sys.exit(1)
        
    asyncio.run(generate_fixtures())
