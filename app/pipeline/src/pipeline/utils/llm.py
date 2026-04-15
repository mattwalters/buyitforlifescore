"""
Shared LLM inference functions for the BIFL pipeline.

These are the canonical implementations of each extraction phase's LLM call.
Both production Dagster assets and evaluation scripts import from here to
guarantee identical inference behavior.
"""

import asyncio
import json
from dataclasses import dataclass
from typing import Optional

from google import genai
from google.genai import types
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipeline.prompts.entity_discovery import MentionItem, get_entity_discovery_prompt
from pipeline.utils.pricing import calculate_gemini_cost

_client: Optional[genai.Client] = None


def _get_client() -> genai.Client:
    global _client
    if _client is None:
        _client = genai.Client()
    return _client


def apply_thinking_config(gen_config: types.GenerateContentConfig, thinking: Optional[str]) -> None:
    """Applies the appropriate ThinkingConfig to gen_config in-place.

    Accepts either a numeric string (thinking_budget) or a level string
    like "low", "medium", or "high" (thinking_level).
    """
    if not thinking:
        return
    if thinking.lstrip("-").isdigit():
        gen_config.thinking_config = types.ThinkingConfig(thinking_budget=int(thinking))
    else:
        gen_config.thinking_config = types.ThinkingConfig(thinking_level=thinking)


@dataclass
class DiscoveryResult:
    """The canonical return type for a single entity discovery LLM call."""

    items: list[dict]
    raw_json: str
    cost: float
    input_tokens: int
    output_tokens: int
    prompt_text: str


async def run_entity_discovery(
    content_blocks_json: str,
    model_name: str,
    thinking: Optional[str] = None,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> DiscoveryResult:
    """
    Runs Phase 1 (Entity Discovery) against the Gemini API.

    This is the single source of truth for the discovery LLM call. Both the
    production Dagster asset and all evaluation scripts call this function.

    Args:
        content_blocks_json: JSON-serialized list of ContentBlock dicts
            (with block_id, author_id, text).
        model_name: The Gemini model identifier (e.g. "gemini-2.5-flash-lite").
        thinking: Optional thinking config — either a numeric string for
            thinking_budget or a level string like "low"/"medium"/"high".
        semaphore: Optional asyncio.Semaphore for rate limiting.

    Returns:
        A DiscoveryResult containing parsed items, raw JSON, cost, and token counts.
    """
    client = _get_client()
    prompt = get_entity_discovery_prompt(content_blocks_json)

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=list[MentionItem],
        temperature=0.1,
    )
    apply_thinking_config(gen_config, thinking)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    async def call_api():
        return await client.aio.models.generate_content(
            model=model_name,
            contents=prompt,
            config=gen_config,
        )

    async def _execute():
        response = await call_api()

        cost = 0.0
        input_tokens = 0
        output_tokens = 0

        if response.usage_metadata:
            usage = response.usage_metadata
            cost = calculate_gemini_cost(model_name, usage)
            input_tokens = usage.prompt_token_count or 0
            output_tokens = usage.candidates_token_count or 0

        raw_json = response.text if response.text else "[]"
        # Sometimes Gemini wraps in markdown blocks
        if raw_json.startswith("```json"):
            raw_json = raw_json[7:-3]

        items = json.loads(raw_json)
        parsed_items = [item for item in items if isinstance(item, dict)]

        return DiscoveryResult(
            items=parsed_items,
            raw_json=raw_json,
            cost=cost,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            prompt_text=prompt,
        )

    if semaphore:
        async with semaphore:
            return await _execute()
    else:
        return await _execute()
