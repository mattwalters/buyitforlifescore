from typing import Union

from pydantic import BaseModel, ConfigDict, Field


class BronzeSubmission(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    name: str  # e.g. t3_abcd
    subreddit: str
    created_utc: Union[int, str]


class BronzeComment(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    name: str  # e.g. t1_wxyz
    parent_id: str  # e.g. t3_abcd or "t3_abcd" (quote wrapped)
    subreddit: str
    created_utc: Union[int, str]


class SilverChain(BaseModel):
    model_config = ConfigDict(extra="ignore")

    chain_id: str
    submission_id: str
    reddit_node_id: str
    sequence_order: int = Field(gt=0)
