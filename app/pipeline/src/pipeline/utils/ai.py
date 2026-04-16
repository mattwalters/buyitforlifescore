import logging
import os
from enum import Enum
from typing import Any

from google import genai
from google.genai import types
from tenacity import before_sleep_log, retry, retry_if_not_exception_type, stop_after_attempt, wait_exponential

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
