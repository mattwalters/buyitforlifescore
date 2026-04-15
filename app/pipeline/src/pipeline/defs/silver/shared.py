from typing import Optional

from dagster import Config, DailyPartitionsDefinition

PROMPT_VERSION = "v1.0.0"
SILVER_CODE_VERSION = "v1"

# We define a Daily Partition from Jan 2012.
bifl_daily_partitions = DailyPartitionsDefinition(start_date="2012-01-01")


class SilverLLMConfig(Config):
    limit: Optional[int] = None
    model: str = "gemini-2.5-flash-lite"
    thinking: Optional[str] = None
