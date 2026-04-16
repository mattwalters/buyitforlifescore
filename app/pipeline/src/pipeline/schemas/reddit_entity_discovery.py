from typing import List

from pydantic import BaseModel, ConfigDict, Field


class LlmDiscoveredEntity(BaseModel):
    """The schema the LLM is strictly constrained to output."""
    verbatim_quote: str = Field(description="The exact textual string verbatim from the text as it was written.")
    block_indexes: List[int] = Field(description="The integer index mapping to the XML analysis blocks.")


class DiscoveredEntity(BaseModel):
    """The final re-hydrated entity mapped to system identifiers."""
    verbatim_quote: str
    node_ids: List[str]


class DiscoveryResult(BaseModel):
    """The canonical return type for a single entity discovery LLM call."""
    model_config = ConfigDict(extra="ignore")

    bundle_id: str
    submission_id: str
    items: List[DiscoveredEntity]
    raw_json: str
    cost_usd: float
    prompt_tokens: int
    completion_tokens: int
    prompt_text: str
