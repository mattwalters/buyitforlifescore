import json
from pydantic import BaseModel, Field

# --- Extraction Schema ---

class MentionItem(BaseModel):
    author_id: str = Field(description="The unique author identifier from the ContentBlock.")
    brand: str = Field(description="The stated brand name. Normalize to canonical proper spelling.")
    productName: str = Field(description="The specific marketed product line or model name. Leave as empty string if no specific model is mentioned.")
    source_block_ids: list[int] = Field(description="The list of block_ids where this author explicitly mentioned this product.")

def get_discovery_prompt(thread_text: str) -> str:
    return f"""You are an Entity Discovery agent studying text blocks.
Your task is to identify every durable product mentioned.

CRITICAL INSTRUCTIONS:
- Aggregate your extractions by Author. Output exactly ONE extraction per unique 'author_id' and product combination. List all 'block_id's where they discussed it.
- Your goal is to identify explicit OPINIONS or REVIEWS of brands and products. 
- Do NOT extract a product if the user is simply stating that they bought it, are considering buying it, or are asking a question about it. There MUST be a qualitative opinion, endorsement, or explicit statement of experience attached.
- You can extract general brand mentions (e.g., "Georgia has dropped in quality") if an opinion is attached. Do not limit yourself strictly to "physical" models if the brand quality itself is being reviewed.
- If a commenter refers to a specific model name but omits the brand (e.g., "The SL-1200 is a tank"), you MUST use the preceding conversation blocks to infer the correct brand name ("Technics").
- CRITICAL BOUNDARY: You MUST be able to tie an opinion to a specific BRAND. If a user states an experience about a generic component (e.g. "side zippers fail") or a generic product (e.g. "I love my boots") but the BRAND is unknown and cannot be inferred from context, you MUST NOT extract it.
- Validation Gate 1 (Metaphors): Check if the statement is a rhetorical analogy (e.g., "asking for a Cadillac at a Chevy price"). If it is a metaphor, ABORT the extraction.
- Validation Gate 2 (Retailers): Check if the brand is actually a generic retailer (e.g., Costco, Home Depot, Amazon). Retailers are not product brands unless explicitly an in-house brand (e.g. Kirkland). If it is just a retailer, ABORT the extraction.
- Validation Gate 3 (Raw Materials): Check if the brand is actually a raw material or generic noun (e.g., teak, wooden, plastic, memory foam, goretex). If it is a material, ABORT the extraction. A brand must represent a named manufacturer.
- Validation Gate 4 (Unknown Identity): Check if the identity of the brand is vague or unnamed (e.g. "these showerheads"). If you cannot confidently identify the exact capitalized proper noun of the brand, YOU MUST ABORT the extraction. The brand field must always contain a specific, capitalized proper noun.
- Do NOT extract generic product nouns (e.g., "mixer", "backpack", "pan").

Thread to analyze (JSON ContentBlocks):
{thread_text}"""

# --- Eval Verification Schema ---

class PayloadItemValidation(BaseModel):
    brand: str = Field(description="The brand that was extracted.")
    is_valid_durable_good: bool = Field(description="True if this specific extraction is a valid physical, durable BIFL brand/product. False if it is a retailer, hallucination, raw material, metaphor, or generic concept.")
    reasoning: str = Field(description="A brief 1-sentence reasoning for this specific decision.")

class JudgePayloadValidation(BaseModel):
    item_validations: list[PayloadItemValidation] = Field(description="List of validations matching the exact order of the items in the extracted JSON array.")
    missed_entities: list[str] = Field(description="List of obvious BIFL products mentioned in the text that the agent neglected to extract (False Negatives).")
    hallucinated_entities: list[str] = Field(description="List of products the agent completely fabricated that were NOT in the text.")

def get_payload_judge_prompt(content_blocks_json: str, raw_json_output: str) -> str:
    original_prompt = get_discovery_prompt(content_blocks_json)
    return f"""You are an expert Eval-as-a-Judge acting as a safeguard for an Entity Discovery pipeline.

Below is the EXACT prompt and instructions that the pipeline agent was given:
--- ORIGINAL PIPELINE AGENT PROMPT START ---
{original_prompt}
--- ORIGINAL PIPELINE AGENT PROMPT END ---

Your task is to judge whether the pipeline's output strictly adhered to the rules and instructions provided to it, given the same text context. 

Specifically, you need to:
1. Grade EVERY item present in the Pipeline Output array (is it a valid extraction based on the rules?).
2. Identify any obvious physical BIFL products that were mentioned in the text but NOT extracted by the pipeline.
3. Identify any complete fabrications (the agent extracted a brand/product that simply doesn't exist anywhere in the text).

Pipeline Output (JSON Array):
{raw_json_output}"""
