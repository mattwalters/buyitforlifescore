import json
import logging
from enum import Enum
from typing import Any

from google import genai
from google.genai import types
from pydantic import TypeAdapter, ValidationError
from tenacity import before_sleep_log, retry, retry_if_not_exception_type, stop_after_attempt, wait_exponential

from pipeline.schemas.reddit_entity_discovery import DiscoveredEntity, DiscoveryResult, LlmDiscoveredEntity
from pipeline.schemas.reddit_llm_payloads import SilverRedditLlmPayload

logger = logging.getLogger(__name__)


class AiModel(str, Enum):
    GEMINI_3_FLASH = "gemini-3-flash-preview"
    GEMINI_3_PRO = "gemini-3.1-pro-preview"
    GEMINI_3_PRO_IMAGE = "gemini-3.1-pro-image-preview"
    GEMINI_EMBEDDING_2_PREVIEW = "gemini-embedding-2-preview"
    GEMINI_3_1_FLASH_LITE = "gemini-3.1-flash-lite-preview"
    GEMINI_2_5_FLASH = "gemini-2.5-flash"
    GEMINI_2_5_FLASH_LITE = "gemini-2.5-flash-lite"


def get_client() -> genai.Client:
    """Initialize a thread-safe genai client with local credentials."""
    return genai.Client()


def get_model_pricing(model: AiModel) -> dict[str, float]:
    """Retrieve per-million token pricing for the specified model."""
    if model == AiModel.GEMINI_2_5_FLASH_LITE:
        return {
            "input": 0.10 / 1_000_000,
            "output": 0.40 / 1_000_000,
        }
    if model == AiModel.GEMINI_2_5_FLASH:
        return {
            "input": 0.30 / 1_000_000,
            "output": 2.50 / 1_000_000,
        }
    raise ValueError(f"Pricing for {model} is not configured in python yet.")


def calculate_cost(model: AiModel, prompt_tokens: int, completion_tokens: int) -> float:
    """Extremely simplified cost calculation based on raw token counts."""
    pricing = get_model_pricing(model)
    return (prompt_tokens * pricing["input"]) + (completion_tokens * pricing["output"])


def invoke_summarize_node(client: genai.Client, text: str, model: AiModel = AiModel.GEMINI_2_5_FLASH_LITE) -> dict[str, Any]:
    """Summarizes text into under 500 chars context."""
    system_instruction = (
        "You are an expert summarizer for internet discussion threads. Summarize the provided text to under 500 "
        "characters. Capture the core intent, context, and any specific entities (products, places), sentiment or "
        "judgments. Keep the output clinical and maximally compressed. Do not include introductions like 'This comment "
        "says'. Just return the summary."
    )

    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=0.0,  # Zero temperature for factual consistency
        # Zero thinking budget for flash-lite
        thinking_config=types.ThinkingConfig(thinking_budget=0) if model == AiModel.GEMINI_2_5_FLASH_LITE else None,
    )

    accumulated_prompt_tokens = 0
    accumulated_completion_tokens = 0
    accumulated_cost_usd = 0.0

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_not_exception_type(ValueError),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _attempt_call() -> str:
        nonlocal accumulated_prompt_tokens, accumulated_completion_tokens, accumulated_cost_usd

        response = client.models.generate_content(
            model=model.value,
            contents=text,
            config=config,
        )

        prompt_tokens = 0
        completion_tokens = 0
        if response.usage_metadata:
            prompt_tokens = response.usage_metadata.prompt_token_count or 0
            completion_tokens = response.usage_metadata.candidates_token_count or 0

        cost_usd = calculate_cost(model, prompt_tokens, completion_tokens)

        # Accumulate costs *before* assessing .text, in case it throws ValueError
        accumulated_prompt_tokens += prompt_tokens
        accumulated_completion_tokens += completion_tokens
        accumulated_cost_usd += cost_usd

        out_text = response.text
        if not out_text:
            out_text = text[:500] + "..."

        return out_text

    try:
        final_text = _attempt_call()
    except Exception as e:
        logger.error(f"Failed to summarize node after retries: {e}")
        final_text = text[:500] + "... [SUMMARIZATION_FAILED]"

    return {
        "summary": final_text,
        "prompt_tokens": accumulated_prompt_tokens,
        "completion_tokens": accumulated_completion_tokens,
        "cost_usd": accumulated_cost_usd,
    }


def invoke_entity_discovery(client: genai.Client, payload: SilverRedditLlmPayload, model: AiModel = AiModel.GEMINI_2_5_FLASH_LITE) -> DiscoveryResult:
    """Invokes LLM entity discovery using mapped indexes and XML scoping."""
    index_to_node_id = {}
    xml_blocks = []

    for idx, node in enumerate(payload.nodes):
        index_to_node_id[idx] = node.reddit_node_id
        tag_name = "analysis_block" if node.is_canonical else "context_block"
        content = node.summary if node.summary else node.text
        xml_blocks.append(f'<{tag_name} index="{idx}">\n<content>{content}</content>\n</{tag_name}>')

    prompt_text = "\n\n".join(xml_blocks)

    system_instruction = (
        "Your single objective is to find and extract a short verbatim quote referencing every Proprietary Commercial Brand, Manufacturer, or Specific Product Model mentioned in the text.\n\n"
        "CRITICAL INSTRUCTIONS:\n"
        "- Principle 1: Greedy Recall. Act as a wide-net spotlight for commercial brands. When in doubt, extract it. False positives are okay; false negatives are fatal.\n"
        "- Principle 2: Verbatim Anchor. Do not interpret or label the product genericly. Extract the exact short textual string or quote verbatim from the text as it was written (e.g., if they wrote 'darn tough', extract 'darn tough'). Do not add corporate suffixes.\n"
        "- Principle 3: Irrelevant Context. Extract regardless of sentiment (hates it, bought it, asking about it).\n"
        "- Principle 4: STRICT XML SCOPING. You must ONLY extract entities mentioned inside <analysis_block> tags! Submissions/comments nested within <context_block> tags are strictly OFF LIMITS.\n"
        "- Exclusion: Do NOT extract generic physical objects, raw materials, accessories, or categories. For example, explicitly skip 'cast iron', 'cedar shoe horns', 'giant wok', 'copper', 'adapter', 'propane tank', 'sedan'.\n"
        "- Aggregation: Output exactly ONE extraction per unique brand/product. List all block_index integers where this exact product was discussed.\n"
        "- TRUNCATION LIMIT: Do NOT extract more than 100 entities total. If there are massive lists of brands, only extract the 100 most prominent ones and ignore the rest to prevent JSON token overflow."
    )

    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=0.0,
        response_mime_type="application/json",
        response_schema=list[LlmDiscoveredEntity],
        thinking_config=types.ThinkingConfig(thinking_budget=512) if model == AiModel.GEMINI_2_5_FLASH_LITE else None,
    )

    accumulated_prompt_tokens = 0
    accumulated_completion_tokens = 0
    accumulated_cost_usd = 0.0

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_not_exception_type(ValueError),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _attempt_call() -> str:
        nonlocal accumulated_prompt_tokens, accumulated_completion_tokens, accumulated_cost_usd

        logger.info(f"[LLM] ▶ Calling {model.value} for bundle {payload.bundle_id} ({len(prompt_text)} chars)")

        response = client.models.generate_content(
            model=model.value,
            contents=prompt_text,
            config=config,
        )

        prompt_tokens = 0
        completion_tokens = 0
        if response.usage_metadata:
            prompt_tokens = response.usage_metadata.prompt_token_count or 0
            completion_tokens = response.usage_metadata.candidates_token_count or 0

        cost_usd = calculate_cost(model, prompt_tokens, completion_tokens)

        accumulated_prompt_tokens += prompt_tokens
        accumulated_completion_tokens += completion_tokens
        accumulated_cost_usd += cost_usd

        logger.info(f"[LLM] ◀ Response for {payload.bundle_id}: {prompt_tokens}p/{completion_tokens}c tokens, ${cost_usd:.6f}")

        out_text = response.text
        if not out_text:
            raise ValueError("Empty response text from LLM")

        return out_text

    try:
        raw_json = _attempt_call()
        llm_entities = TypeAdapter(list[LlmDiscoveredEntity]).validate_json(raw_json)
        
        # Re-hydrate the integers back into system node IDs
        hydrated_items = []
        for entity in llm_entities:
            mapped_node_ids = []
            for idx in entity.block_indexes:
                if idx in index_to_node_id:
                    mapped_node_ids.append(index_to_node_id[idx])
            
            # Deduplicate just in case
            mapped_node_ids = list(set(mapped_node_ids))
            
            if mapped_node_ids:
                hydrated_items.append(
                    DiscoveredEntity(
                        verbatim_quote=entity.verbatim_quote,
                        node_ids=mapped_node_ids,
                    )
                )

        logger.info(f"[LLM] ✅ {payload.bundle_id}: extracted {len(hydrated_items)} entities")

    except ValidationError as e:
        safe_err = str(e).splitlines()[0] if str(e) else "Invalid JSON EOF"
        preview_len = 250
        preview = f"{raw_json[:preview_len]}...\n\n...[SNIP ({len(raw_json)} chars total)]...\n\n...{raw_json[-preview_len:]}" if len(raw_json) > preview_len * 2 else raw_json
        logger.warning(f"[TRUNCATED] Payload {payload.bundle_id} exceeded JSON limits. Returning []. Err: {safe_err}\n--- JSON PREVIEW ---\n{preview}\n--------------------")
        raw_json = "[]"
        hydrated_items = []
    except Exception as e:
        logger.error(f"[LLM] ❌ {payload.bundle_id}: failed after retries: {e}")
        raw_json = "[]"
        hydrated_items = []

    return DiscoveryResult(
        bundle_id=payload.bundle_id,
        submission_id=payload.submission_id,
        items=hydrated_items,
        raw_json=raw_json,
        cost_usd=accumulated_cost_usd,
        prompt_tokens=accumulated_prompt_tokens,
        completion_tokens=accumulated_completion_tokens,
        prompt_text=prompt_text,
    )
