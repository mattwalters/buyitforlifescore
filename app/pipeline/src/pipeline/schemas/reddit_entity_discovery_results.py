from pydantic import BaseModel, Field


class RedditEntityDiscoveryResult(BaseModel):
    bundle_id: str = Field(description="The bundle ID that generated the extraction")
    submission_id: str = Field(description="The submission ID of the bundle")
    verbatim_quote: str = Field(description="The extracted brand, manufacturer, or product model")
    node_id: str = Field(description="The specific reddit node (submission or comment) ID where it was discussed")
