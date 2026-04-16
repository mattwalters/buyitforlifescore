from pydantic import BaseModel, Field

class OpsAiCostSummary(BaseModel):
    service_name: str = Field(description="Name of the AI service or layer, e.g., 'node_summarization' or 'TOTAL'")
    total_cost_usd: float = Field(description="Total accumulated cost in USD")
    total_prompt_tokens: int = Field(description="Total prompt tokens consumed")
    total_completion_tokens: int = Field(description="Total completion tokens consumed")
    total_nodes_processed: int = Field(description="Total nodes or entities that underwent generation")
