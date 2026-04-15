"""
Shared LLM inference functions for the BIFL pipeline.

These are the canonical implementations of each extraction phase's LLM call.
Both production Dagster assets and evaluation scripts import from here to
guarantee identical inference behavior.
"""

import asyncio
import json
from dataclasses import dataclass, field
from typing import Optional

from google import genai
from google.genai import types
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipeline.prompts.entity_discovery import MentionItem, get_entity_discovery_prompt
from pipeline.prompts.entity_extraction import EntityExtraction, get_extraction_prompt
from pipeline.prompts.entity_triage import TriageDecision, get_entity_triage_prompt
from pipeline.utils.pricing import calculate_gemini_cost
from pipeline.utils.tree import build_comment_tree, build_content_blocks, chunk_branches

_client: Optional[genai.Client] = None

# Default soft-max comments per chunk. Shared between production and eval.
DEFAULT_MAX_CHUNK_SIZE = 20


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


@dataclass
class ThreadDiscoveryResult:
    """Aggregate result of running entity discovery across all chunks of a single thread."""

    chunk_payloads: list[dict] = field(default_factory=list)
    all_items: list[dict] = field(default_factory=list)
    total_cost: float = 0.0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    errors: list[str] = field(default_factory=list)


async def process_thread_discovery(
    submission_id: str,
    title: str,
    body: str | None,
    comments: list[dict] | None,
    model_name: str,
    semaphore: asyncio.Semaphore,
    thinking: Optional[str] = None,
    created_utc: Optional[str] = None,
) -> ThreadDiscoveryResult:
    """
    The single source of truth for processing a Reddit thread through entity discovery.

    Builds the comment tree, chunks it by branch locality, runs parallel LLM
    inference for each chunk, and returns the aggregated results.

    Both the production Dagster asset and all evaluation scripts MUST call this
    function so that tree-building, chunking, and inference logic can never drift.

    Args:
        submission_id: The Reddit submission ID.
        title: The submission title.
        body: The submission body/selftext.
        comments: A flat list of comment dicts (with id, parent_id, body, author, created_utc).
        model_name: The Gemini model identifier.
        semaphore: asyncio.Semaphore for rate limiting.
        thinking: Optional thinking config string.
        created_utc: Timestamp for the OP block.

    Returns:
        A ThreadDiscoveryResult with chunk payloads, flattened items, cost totals, and errors.
    """
    if not comments:
        comments = []

    # Build the comment tree and chunk by complete conversational branches
    tree = build_comment_tree(comments, submission_id)
    chunks = chunk_branches(tree, max_chunk_size=DEFAULT_MAX_CHUNK_SIZE)

    # If there are no chunks (e.g., submission with no comments), still process the submission itself
    if not chunks:
        chunks = [[]]

    result = ThreadDiscoveryResult()

    async def _extract_chunk(chunk: list[dict], chunk_index: int):
        content_blocks = build_content_blocks(
            title=title,
            body=body,
            comments=chunk,
            created_utc=created_utc,
            include_op=(chunk_index == 0),
        )

        thread_text = json.dumps([{k: v for k, v in b.items() if k != "created_utc"} for b in content_blocks], indent=2)

        try:
            llm_result = await run_entity_discovery(
                content_blocks_json=thread_text,
                model_name=model_name,
                thinking=thinking,
                semaphore=semaphore,
            )

            chunk_id = f"{submission_id}_chunk_{chunk_index}"

            payload = {
                "chunk_id": chunk_id,
                "submission_id": submission_id,
                "chunk_index": chunk_index,
                "target_authored_at": created_utc,
                "model_used": model_name,
                "thinking_level": thinking,
                "input_tokens": llm_result.input_tokens,
                "output_tokens": llm_result.output_tokens,
                "cost_usd": llm_result.cost,
                "raw_json_output": llm_result.raw_json,
                "title": title,
                "body": body or "",
                "content_blocks_json": json.dumps(content_blocks),
                "full_prompt_text": llm_result.prompt_text,
            }

            result.chunk_payloads.append(payload)
            result.all_items.extend(llm_result.items)
            result.total_cost += llm_result.cost
            result.total_input_tokens += llm_result.input_tokens
            result.total_output_tokens += llm_result.output_tokens

        except Exception as e:
            result.errors.append(f"Skipping thread {submission_id} chunk {chunk_index} due to API Error: {e}")

    tasks = [_extract_chunk(chunk, chunk_index) for chunk_index, chunk in enumerate(chunks)]
    await asyncio.gather(*tasks)

    return result


@dataclass
class ExtractionResult:
    """The canonical return type for a single entity extraction LLM call."""

    payload: dict
    raw_json: str
    cost: float
    input_tokens: int
    output_tokens: int
    prompt_text: str


@dataclass
class TriageResult:
    """The canonical return type for a single entity triage LLM call."""

    passes: bool
    reasoning: str
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


async def run_entity_extraction(
    brand: str,
    product_name: str,
    target_authored_at: str,
    text: str,
    parent_text: str,
    model_name: str,
    thinking: Optional[str] = None,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> ExtractionResult:
    """
    Runs Phase 2 (Entity Extraction) against the Gemini API.

    This is the single source of truth for the extraction LLM call. Both the
    production Dagster asset and all evaluation scripts call this function.

    Args:
        brand: The brand name extracted in Phase 1.
        product_name: The product name extracted in Phase 1.
        target_authored_at: The date the source text was authored. Used as the
            reference date for all relative time calculations in the prompt.
        text: The source text block(s) to extract attributes from.
        parent_text: The parent submission text for additional context.
        model_name: The Gemini model identifier (e.g. "gemini-2.5-flash-lite").
        thinking: Optional thinking config — either a numeric string for
            thinking_budget or a level string like "low"/"medium"/"high".
        semaphore: Optional asyncio.Semaphore for rate limiting.

    Returns:
        An ExtractionResult containing the parsed payload, raw JSON, cost, and token counts.
    """
    client = _get_client()
    prompt = get_extraction_prompt(brand, product_name, target_authored_at, text, parent_text)

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=EntityExtraction,
        temperature=0.1,
    )
    apply_thinking_config(gen_config, thinking)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
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

        raw_json = response.text if response.text else "{}"
        if raw_json.startswith("```json"):
            raw_json = raw_json[7:-3]

        payload = json.loads(raw_json)

        return ExtractionResult(
            payload=payload,
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


async def run_entity_triage(
    raw_mention: str,
    text: str,
    parent_text: str,
    model_name: str,
    thinking: Optional[str] = None,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> TriageResult:
    """
    Runs Phase 1b (Entity Triage) against the Gemini API.

    This is the single source of truth for the triage LLM call. Both the
    production Dagster asset and all evaluation scripts call this function.

    Determines whether a discovered entity mention contains any meaningful
    signal — a fact, opinion, or experience about the product — that warrants
    running the expensive Phase 2 attribute extraction.

    Args:
        raw_mention: The verbatim entity string from Phase 1 discovery
            (e.g. "darn tough socks", "lodge cast iron skillet").
        text: The source text block(s) where the entity was discovered.
        parent_text: The parent submission text for additional context.
        model_name: The Gemini model identifier (e.g. "gemini-2.5-flash-lite").
        thinking: Optional thinking config — either a numeric string for
            thinking_budget or a level string like "low"/"medium"/"high".
        semaphore: Optional asyncio.Semaphore for rate limiting.

    Returns:
        A TriageResult with the pass/fail decision, reasoning, and token metadata.
    """
    client = _get_client()
    prompt = get_entity_triage_prompt(raw_mention, text, parent_text)

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=TriageDecision,
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

        raw_json = response.text if response.text else "{}"
        if raw_json.startswith("```json"):
            raw_json = raw_json[7:-3]

        decision = json.loads(raw_json)

        return TriageResult(
            passes=decision.get("passes", False),
            reasoning=decision.get("reasoning", ""),
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
