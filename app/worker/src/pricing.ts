

// Enum for supported AI Models
export enum AiModel {
  GEMINI_3_FLASH = "gemini-3-flash-preview",
  GEMINI_3_PRO = "gemini-3.1-pro-preview",
  GEMINI_3_PRO_IMAGE = "gemini-3.1-pro-image-preview",
  GEMINI_EMBEDDING_2_PREVIEW = "gemini-embedding-2-preview",
  GEMINI_3_1_FLASH_LITE = "gemini-3.1-flash-lite-preview",
  GEMINI_2_5_FLASH = "gemini-2.5-flash",
  GEMINI_2_5_FLASH_LITE = "gemini-2.5-flash-lite",
}

export type ThinkingLevel = "low" | "medium" | "high" | "off";

export function getThinkingConfig(model: AiModel, level: ThinkingLevel): { thinkingLevel?: string; thinkingBudget?: number } | undefined {
  if (level === "off") {
     if (model.startsWith("gemini-3")) return undefined; 
     return { thinkingBudget: 0 };
  }

  const isGemini3 = model.startsWith("gemini-3");
  if (isGemini3) {
      return { thinkingLevel: level };
  } else {
      let budget = 1024;
      if (level === "low") budget = 1024;
      if (level === "medium") budget = 4096;
      if (level === "high") budget = -1; 
      
      return { thinkingBudget: budget };
  }
}

// Pricing rates per token (raw values, not per million)
export interface ModelPricing {
  input: number;
  output: number;
  inputLong?: number; // Rate for long context inputs
  outputLong?: number; // Rate for long context outputs
  inputCached?: number; // Rate for cached input tokens
  inputCachedLong?: number; // Rate for cached input tokens in long context
  inputAudio?: number; // Rate for audio input tokens
  inputAudioCached?: number; // Rate for cached audio input tokens
  outputImage?: number; // Per token for image output
}

const GEMINI_3_PRO_PRICING: ModelPricing = {
  // Estimated placeholders until official numbers are parsed
  input: 2.0 / 1_000_000,
  output: 12.0 / 1_000_000,
  inputCached: 0.2 / 1_000_000,
  inputLong: 4.0 / 1_000_000,
  outputLong: 18.0 / 1_000_000,
  inputCachedLong: 0.4 / 1_000_000,
  outputImage: 120.0 / 1_000_000,
};

export const PRICING: Record<AiModel, ModelPricing> = {
  [AiModel.GEMINI_3_FLASH]: {
    // Exact rates derived from Gemini 3 Flash Preview pricing image
    input: 0.50 / 1_000_000,
    output: 3.00 / 1_000_000,
    inputCached: 0.05 / 1_000_000,
    inputAudio: 1.00 / 1_000_000,
    inputAudioCached: 0.10 / 1_000_000,
  },
  [AiModel.GEMINI_3_PRO]: GEMINI_3_PRO_PRICING,
  [AiModel.GEMINI_3_PRO_IMAGE]: GEMINI_3_PRO_PRICING,
  [AiModel.GEMINI_EMBEDDING_2_PREVIEW]: {
    // Pricing based on Gemini Embedding 2 Preview Docs
    input: 0.20 / 1_000_000,
    output: 0.0 / 1_000_000,
  },
  [AiModel.GEMINI_3_1_FLASH_LITE]: {
    input: 0.25 / 1_000_000,
    output: 1.50 / 1_000_000,
  },
  [AiModel.GEMINI_2_5_FLASH]: {
    input: 0.30 / 1_000_000,
    output: 2.50 / 1_000_000,
  },
  [AiModel.GEMINI_2_5_FLASH_LITE]: {
    input: 0.10 / 1_000_000,
    output: 0.40 / 1_000_000,
  },
} as const;

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
export function calculateCost(model: AiModel, usage: any): number {
  const pricing = PRICING[model];

  // Fallback if model not found in PRICING (should be caught by type system mostly)
  if (!pricing) return 0;

  const contextThreshold = 200_000;

  const promptTokens = usage.promptTokenCount || 0;
  const cachedTokens = usage.cachedContentTokenCount || 0;
  
  // The Gemini/Vertex APIs separate these dynamically. We must sum them!
  const responseTokens = (usage.candidatesTokenCount || 0) + (usage.thoughtsTokenCount || 0);

  // Context Tier Check
  // Note: Flash uses flat pricing, logic will naturally fallback to base rates if Long rates undefined
  const isLongContext = promptTokens > contextThreshold;

  // 1. Calculate Input Cost
  let inputCost = 0;

  // Helper to calculate cost for a set of token details
  const calculateInputBatchCost = (
    details: typeof usage.promptTokensDetails,
    isCached: boolean,
  ) => {
    let cost = 0;
    if (!details || details.length === 0) return 0;

    for (const part of details) {
      const count = part.tokenCount || 0;
      if (part.modality === "AUDIO") {
        const rate = isCached
          ? (pricing.inputAudioCached ?? pricing.inputCached ?? 0)
          : (pricing.inputAudio ?? pricing.input);
        cost += count * rate;
      } else {
        // Text/Image/Video -> Standard Input Rate
        let rate: number;
        if (isCached) {
          rate = isLongContext
            ? (pricing.inputCachedLong ?? pricing.inputCached ?? 0)
            : (pricing.inputCached ?? 0);
        } else {
          rate = isLongContext ? (pricing.inputLong ?? pricing.input) : pricing.input;
        }
        cost += count * rate;
      }
    }
    return cost;
  };

  // If we have details, use them for precise calculation (especially for Audio)
  if (usage.promptTokensDetails && usage.promptTokensDetails.length > 0) {
    const grossCost = calculateInputBatchCost(usage.promptTokensDetails, false);

    let cachedDeduction = 0;
    let cachedAddition = 0;

    if (usage.cacheTokensDetails && usage.cacheTokensDetails.length > 0) {
      cachedDeduction = calculateInputBatchCost(usage.cacheTokensDetails, false); // Remove standard cost
      cachedAddition = calculateInputBatchCost(usage.cacheTokensDetails, true); // Add cached cost
    } else if (cachedTokens > 0) {
      // Fallback: Assume all cached tokens are Text/Standard if no detail
      const stdRate = isLongContext ? (pricing.inputLong ?? pricing.input) : pricing.input;

      const cRate = isLongContext
        ? (pricing.inputCachedLong ?? pricing.inputCached ?? 0)
        : (pricing.inputCached ?? 0);

      cachedDeduction = cachedTokens * stdRate;
      cachedAddition = cachedTokens * cRate;
    }

    inputCost = grossCost - cachedDeduction + cachedAddition;
  } else {
    // Fallback: No details, assume all Text/Standard
    const standardInputTokens = Math.max(0, promptTokens - cachedTokens);

    const standardInputRate = isLongContext ? (pricing.inputLong ?? pricing.input) : pricing.input;

    const cachedInputRate = isLongContext
      ? (pricing.inputCachedLong ?? pricing.inputCached ?? 0)
      : (pricing.inputCached ?? 0);

    inputCost = standardInputTokens * standardInputRate + cachedTokens * cachedInputRate;
  }

  // 2. Calculate Output Cost
  let outputCost = 0;

  if (model === AiModel.GEMINI_3_PRO || model === AiModel.GEMINI_3_PRO_IMAGE) {
    // Modality-aware pricing for Pro
    // Map candidatesTokensDetails (raw API) to responseTokensDetails (SDK type) if needed
    const responseDetails =
      usage.candidatesTokensDetails ||
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (usage as any).responseTokensDetails;

    if (responseDetails && responseDetails.length > 0) {
      for (const part of responseDetails) {
        const count = part.tokenCount || 0;
        if (part.modality === "IMAGE") {
          outputCost += count * (pricing.outputImage ?? pricing.output);
        } else {
          const textRate = isLongContext ? (pricing.outputLong ?? pricing.output) : pricing.output;
          outputCost += count * textRate;
        }
      }
    } else {
      const textRate = isLongContext ? (pricing.outputLong ?? pricing.output) : pricing.output;
      outputCost = responseTokens * textRate;
    }
  } else {
    // Standard/Flash Output Logic (Flat or Simple)
    const outputRate = isLongContext ? (pricing.outputLong ?? pricing.output) : pricing.output;
    outputCost = responseTokens * outputRate;
  }

  return Math.max(0, inputCost + outputCost);
}
