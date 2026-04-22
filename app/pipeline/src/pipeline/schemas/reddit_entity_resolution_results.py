from typing import Literal

from pydantic import BaseModel, Field


class RedditEntityResolutionResult(BaseModel):
    """Flattened resolution row — one per resolved entity per node."""

    node_id: str = Field(description="The reddit node (submission or comment) ID.")
    submission_id: str = Field(description="The parent submission ID.")
    verbatim_quote: str = Field(description="The original verbatim quote that was classified.")
    brand: str = Field(description="The brand or manufacturer name.")
    product_line: str | None = Field(default=None, description="The marketed product family or series name.")
    product_model: str | None = Field(default=None, description="A specific identifiable unit or model.")
    specificity_level: Literal["BRAND_ONLY", "PRODUCT_LINE", "EXACT_MODEL"] = Field(
        description="The specificity level of the product mention.",
    )
