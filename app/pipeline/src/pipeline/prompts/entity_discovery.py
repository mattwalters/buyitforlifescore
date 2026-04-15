from pydantic import BaseModel, Field

# --- Extraction Schema ---


class MentionItem(BaseModel):
    author_id: str = Field(description="The unique author identifier from the ContentBlock.")
    raw_mention: str = Field(
        description="The exact continuous text string highlighting the brand and product. (e.g. 'Sony Playstation 5' or 'darn tough socks' or 'lodge cast iron')"
    )
    source_block_ids: list[int] = Field(description="The list of block_ids where this author mentioned this product.")


def get_entity_discovery_prompt(thread_text: str) -> str:
    return f"""You are an Entity Discovery agent scanning text blocks.
Your single objective is to find and extract every physical product or commercial brand mentioned in the text.

CRITICAL INSTRUCTIONS:
- Principle 1: Greedy Recall. Your job is to act as a wide-net spotlight for physical commercial entities. When in doubt, extract it. False positives are okay, false negatives are fatal.
- Principle 2: Verbatim Extraction. Extract the text exactly as the user wrote it. Do not correct their casing or spelling. If they wrote "darn tough", extract "darn tough". Do not add corporate suffixes.
- Principle 3: Irrelevant Context. Extract them regardless of context: if the user bought it, is asking a question about it, hates it, or is just lacking context, YOU MUST EXTRACT IT. Do not judge sentiment or intent.
- Exclusion: Do not extract generic item classifications without a brand attached (e.g., extract "Subaru Outback", but do NOT extract "car", "sedan", or "boots").
- Aggregate your extractions by Author. Output exactly ONE extraction per unique 'author_id' and product combination. List all 'block_id's where they discussed it.

Thread to analyze (JSON ContentBlocks):
{thread_text}"""
