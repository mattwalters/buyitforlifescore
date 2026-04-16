from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class RedditLlmPayloadNode(BaseModel):
    chain_id: str
    sequence_order: int
    reddit_node_id: str
    author: str = Field(description="The Reddit user who authored this specific node.")
    created_utc: int = Field(description="The epoch timestamp when this node was created.")
    link_flair_text: Optional[str] = Field(default=None, description="The categorization flair (only populated on submissions).")
    is_canonical: bool = Field(description="True if this node is the analytical target; False if it is ancestor context.")
    needs_summarization: bool = Field(description="Whether the pipeline deemed this node too large and requested a summary.")
    text: str = Field(description="The complete, raw textual content of the submission or comment.")
    summary: Optional[str] = Field(default=None, description="The AI-generated summary, if needs_summarization is True.")


class SilverRedditLlmPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    bundle_id: str = Field(description="The LLM-sized chunk identifier mapping back to the bundles asset.")
    submission_id: str = Field(description="The parent submission ID spanning this bundle.")
    nodes: List[RedditLlmPayloadNode] = Field(description="The ordered, structured sequence of all nodes in this bundle.")
