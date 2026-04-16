from pydantic import BaseModel, ConfigDict, Field


class SilverChainBundle(BaseModel):
    model_config = ConfigDict(extra="ignore")

    bundle_id: str
    submission_id: str
    chain_id: str
    reddit_node_id: str
    sequence_order: int = Field(gt=0)
    is_canonical: bool
    needs_summarization: bool
