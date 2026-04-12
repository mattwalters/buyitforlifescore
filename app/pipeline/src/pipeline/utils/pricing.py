from enum import Enum
from typing import Dict, Optional, Any

class AiModel(str, Enum):
    GEMINI_3_FLASH = "gemini-3-flash-preview"
    GEMINI_3_PRO = "gemini-3.1-pro-preview"
    GEMINI_3_PRO_IMAGE = "gemini-3.1-pro-image-preview"
    GEMINI_EMBEDDING_2_PREVIEW = "gemini-embedding-2-preview"
    GEMINI_3_1_FLASH_LITE = "gemini-3.1-flash-lite-preview"
    GEMINI_2_5_FLASH = "gemini-2.5-flash"
    GEMINI_2_5_FLASH_LITE = "gemini-2.5-flash-lite"

# Map pricing per 1 million tokens, then we divide
PRICING: Dict[AiModel, Dict[str, float]] = {
    AiModel.GEMINI_3_FLASH: {
        "input": 0.50 / 1_000_000,
        "output": 3.00 / 1_000_000,
        "inputCached": 0.05 / 1_000_000,
        "inputAudio": 1.00 / 1_000_000,
        "inputAudioCached": 0.10 / 1_000_000,
    },
    AiModel.GEMINI_3_PRO: {
        "input": 2.0 / 1_000_000,
        "output": 12.0 / 1_000_000,
        "inputCached": 0.2 / 1_000_000,
        "inputLong": 4.0 / 1_000_000,
        "outputLong": 18.0 / 1_000_000,
        "inputCachedLong": 0.4 / 1_000_000,
        "outputImage": 120.0 / 1_000_000,
    },
    AiModel.GEMINI_3_PRO_IMAGE: {
        "input": 2.0 / 1_000_000,
        "output": 12.0 / 1_000_000,
        "inputCached": 0.2 / 1_000_000,
        "inputLong": 4.0 / 1_000_000,
        "outputLong": 18.0 / 1_000_000,
        "inputCachedLong": 0.4 / 1_000_000,
        "outputImage": 120.0 / 1_000_000,
    },
    AiModel.GEMINI_EMBEDDING_2_PREVIEW: {
        "input": 0.20 / 1_000_000,
        "output": 0.0,
    },
    AiModel.GEMINI_3_1_FLASH_LITE: {
        "input": 0.25 / 1_000_000,
        "output": 1.50 / 1_000_000,
    },
    AiModel.GEMINI_2_5_FLASH: {
        "input": 0.30 / 1_000_000,
        "output": 2.50 / 1_000_000,
    },
    AiModel.GEMINI_2_5_FLASH_LITE: {
        "input": 0.10 / 1_000_000,
        "output": 0.40 / 1_000_000,
    },
}

def calculate_gemini_cost(model: str, usage_metadata: Any) -> float:
    """
    Calculate cost based on the Google GenAI usage metadata object.
    Supports Python SDK objects or raw dictionary representations.
    """
    # Defensive lookup
    try:
        model_enum = AiModel(model)
        pricing = PRICING[model_enum]
    except ValueError:
        return 0.0

    context_threshold = 200_000

    # Safely extract from either a Python object or dict
    def get_attr(obj: Any, key: str) -> int:
        if isinstance(obj, dict):
            return obj.get(key, 0)
        return getattr(obj, key, 0) or 0

    prompt_tokens = get_attr(usage_metadata, 'prompt_token_count') or get_attr(usage_metadata, 'promptTokenCount')
    cached_tokens = get_attr(usage_metadata, 'cached_content_token_count') or get_attr(usage_metadata, 'cachedContentTokenCount')
    candidates_tokens = get_attr(usage_metadata, 'candidates_token_count') or get_attr(usage_metadata, 'candidatesTokenCount')
    thoughts_tokens = get_attr(usage_metadata, 'thoughts_token_count') or get_attr(usage_metadata, 'thoughtsTokenCount')

    # The user was fundamentally correct: candidatesTokenCount is EXCLUSIVE of thoughtsTokenCount in the API response.
    response_tokens = candidates_tokens + thoughts_tokens
    is_long_context = prompt_tokens > context_threshold

    # Calculate Input Cost
    standard_input_tokens = max(0, prompt_tokens - cached_tokens)
    
    standard_input_rate = pricing.get("inputLong", pricing["input"]) if is_long_context else pricing["input"]
    cached_input_rate = pricing.get("inputCachedLong", pricing.get("inputCached", 0)) if is_long_context else pricing.get("inputCached", 0)

    input_cost = (standard_input_tokens * standard_input_rate) + (cached_tokens * cached_input_rate)

    # Calculate Output Cost
    output_rate = pricing.get("outputLong", pricing["output"]) if is_long_context else pricing["output"]
    output_cost = response_tokens * output_rate

    return max(0.0, input_cost + output_cost)
