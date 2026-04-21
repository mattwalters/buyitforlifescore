from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class LlmResolvedEntity(BaseModel):
    """The constrained JSON schema the LLM must output — one per verbatim quote."""

    verbatim_quote: str = Field(description="Echo back the exact verbatim quote this classification is for.")
    brand: Optional[str] = Field(
        default=None, description="The brand or manufacturer name, or null if not a real commercial brand."
    )
    product_line: Optional[str] = Field(
        default=None, description="The marketed product family or series name (e.g., 'Baggies', 'Artisan', 'D5')."
    )
    product_model: Optional[str] = Field(
        default=None,
        description="A specific identifiable unit or model (e.g., 'Iron Ranger 8111', 'iPad 3 64GB').",
    )
    specificity_level: str = Field(
        description="One of: BRAND_ONLY, PRODUCT_LINE, EXACT_MODEL.",
    )


class EntityResolutionResult(BaseModel):
    """One row per node_id — mirrors DiscoveryResult's nested pattern."""

    model_config = ConfigDict(extra="ignore")

    node_id: str = Field(description="The reddit node (submission or comment) ID that was analyzed.")
    submission_id: str = Field(description="The parent submission ID.")
    items: List[LlmResolvedEntity] = Field(description="Nested list of resolved entities for this node.")
    raw_json: str = Field(description="The raw JSON string returned by the LLM.")
    cost_usd: float = Field(description="Total cost of the LLM call for this node.")
    prompt_tokens: int = Field(description="Prompt tokens consumed.")
    completion_tokens: int = Field(description="Completion tokens generated.")
