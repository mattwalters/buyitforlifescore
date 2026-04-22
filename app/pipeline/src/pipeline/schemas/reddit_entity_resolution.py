from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class LlmResolvedEntity(BaseModel):
    """The constrained JSON schema the LLM must output — one per commercial verbatim quote."""

    verbatim_quote: str = Field(description="Echo back the exact verbatim quote this classification is for.")
    brand: str = Field(
        description="The brand or manufacturer name. Only real commercial brands should be returned."
    )
    product_line: str | None = Field(
        default=None, description="The marketed product family or series name (e.g., 'Baggies', 'Artisan', 'D5')."
    )
    product_model: str | None = Field(
        default=None,
        description="A specific identifiable unit or model (e.g., 'Iron Ranger 8111', 'iPad 3 64GB').",
    )
    specificity_level: Literal["BRAND_ONLY", "PRODUCT_LINE", "EXACT_MODEL"] = Field(
        description="The specificity level of the product mention.",
    )


class EntityResolutionResult(BaseModel):
    """One row per node_id — stores the raw LLM output and cost metadata."""

    model_config = ConfigDict(extra="ignore")

    node_id: str = Field(description="The reddit node (submission or comment) ID that was analyzed.")
    submission_id: str = Field(description="The parent submission ID.")
    node_text: str = Field(description="The original Reddit post or comment text that was classified.")
    raw_json: str = Field(description="The raw JSON string returned by the LLM.")
    resolved_count: int = Field(description="Number of entities resolved from this node.")
    cost_usd: float = Field(description="Total cost of the LLM call for this node.")
    prompt_tokens: int = Field(description="Prompt tokens consumed.")
    completion_tokens: int = Field(description="Completion tokens generated.")
