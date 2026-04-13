import sys
import json
import asyncio
import time
import argparse
from pathlib import Path
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

# Allow import of the pipeline package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from pipeline.utils.pricing import AiModel, calculate_gemini_cost

# --- Candidate Schema (Matches Canonical Model) ---
class MentionItem(BaseModel):
    author_id: str = Field(description="The unique author identifier from the ContentBlock.")
    brand: str = Field(description="The stated brand name. Normalize to canonical proper spelling.")
    productName: str = Field(description="The specific marketed product line or model name. Empty string if BRAND_ONLY.")
    source_block_ids: list[int] = Field(description="The list of block_ids where this author explicitly mentioned this product.")

# --- Judge Pydantic Schemas ---
class AlignmentMapping(BaseModel):
    expected_index: int
    extracted_index: int

class AlignmentResult(BaseModel):
    mappings: list[AlignmentMapping]

async def _extract_candidate(doc_id: str, content_blocks: list[dict], model_name: str, thinking: str | None, semaphore: asyncio.Semaphore) -> tuple[list[dict], float]:
    """Runs Phase 1 LLM extraction on flat Canonical Data."""
    client = genai.Client()
    
    thread_text = json.dumps(content_blocks, indent=2)

    prompt = f"""You are an Entity Discovery agent studying text blocks.
Your task is to identify every durable product mentioned.

CRITICAL INSTRUCTIONS:
- Aggregate your extractions by Author. Output exactly ONE extraction per unique 'author_id' and product combination. List all 'block_id's where they discussed it.
- Your goal is to identify explicit OPINIONS or REVIEWS of brands and products. 
- Do NOT extract a product if the user is simply stating that they bought it, are considering buying it, or are asking a question about it. There MUST be a qualitative opinion, endorsement, or explicit statement of experience attached.
- You can extract general brand mentions (e.g., "Georgia has dropped in quality") if an opinion is attached. Do not limit yourself strictly to "physical" models if the brand quality itself is being reviewed.
- If a commenter refers to a specific model name but omits the brand (e.g., "The SL-1200 is a tank"), you MUST use the preceding conversation blocks to infer the correct brand name ("Technics").
- CRITICAL BOUNDARY: You MUST be able to tie an opinion to a specific BRAND. If a user states an experience about a generic component (e.g. "side zippers fail") or a generic product (e.g. "I love my boots") but the BRAND is unknown and cannot be inferred from context, you MUST NOT extract it.
- DO NOT extract rhetorical metaphors or analogies (e.g., "You are asking for a Cadillac at a Chevy price"). You must only extract products that are explicitly part of the literal discussion.
- DO NOT extract generic nouns, raw materials, or components as brands (e.g., "Teak", "memory foam", "Goretex", "leather", "wooden"). A brand MUST represent a specific named manufacturer/store.
- DO NOT extract a comment if the brand is unknown. NEVER output literal string values like 'Unknown', 'N/A', or 'BRAND_ONLY'. If you cannot find or confidently infer a specific brand name, you must skip the extraction entirely.
- Do NOT extract generic product nouns (e.g., "mixer", "backpack", "pan").

Thread to analyze (JSON ContentBlocks):
{thread_text}"""
        
    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=list[MentionItem],
        temperature=0.1
    )
    
    if thinking:
        if thinking.lstrip('-').isdigit():
            gen_config.thinking_config = types.ThinkingConfig(thinking_budget=int(thinking))
        else:
            gen_config.thinking_config = types.ThinkingConfig(thinking_level=thinking)
            
    cost = 0.0
    results = []
    
    async with semaphore:
        try:
            # We skip the tenacity retry for this lightweight eval script
            response = await client.aio.models.generate_content(
                model=model_name,
                contents=prompt,
                config=gen_config,
            )
            
            if response.usage_metadata:
                cost = calculate_gemini_cost(model_name, response.usage_metadata)
            
            if response.text:
                items = json.loads(response.text)
                for item in items:
                    item['document_id'] = doc_id
                    results.append(item)
                    
        except Exception as e:
            print(f"Skipping doc {doc_id} due to API Error: {e}")
            
    return results, cost


async def run_judge_alignment(expected_mentions: list[dict], extracted_mentions: list[dict]) -> tuple[list[AlignmentMapping], float]:
    """Uses Gemini 3.1 Pro (High Thinking) as a Judge to semantically map Candidate -> Benchmark."""
    if not expected_mentions and not extracted_mentions:
        return [], 0.0

    client = genai.Client()
    
    # Strip unnecessary noise
    expected_for_judge = [{"index": i, "author_id": e.get("author_id"), "brand": e.get("brand"), "productName": e.get("productName")} for i, e in enumerate(expected_mentions)]
    extracted_for_judge = [{"index": i, "author_id": e.get("author_id"), "brand": e.get("brand"), "productName": e.get("productName")} for i, e in enumerate(extracted_mentions)]

    prompt = f"""You are an expert taxonomy aligner. Match extracted product mentions to the expected benchmark list. 
The items might have spelling errors or differing specificity, but you must map them if they refer to the exact same product from the underlying text.
CRITICAL RULE: The extracted item and the expected item MUST have the exact same 'author_id' to be considered a match. Do not map mismatched authors.

Expected Benchmark:
{json.dumps(expected_for_judge, indent=2)}

Extracted Entities:
{json.dumps(extracted_for_judge, indent=2)}

Return a list of mappings bridging the matching indices."""

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=AlignmentResult,
        temperature=0.0,
        thinking_config=types.ThinkingConfig(thinking_level="high")
    )

    try:
        response = await client.aio.models.generate_content(
            model=AiModel.GEMINI_3_FLASH.value,
            contents=prompt,
            config=gen_config,
        )
        
        cost = 0.0
        if response.usage_metadata:
            cost = calculate_gemini_cost(AiModel.GEMINI_3_FLASH.value, response.usage_metadata)
            
        if response.text:
            result_dict = json.loads(response.text)
            return [AlignmentMapping(**m) for m in result_dict.get("mappings", [])], cost
    except Exception as e:
        print(f"Judge failed on mapping: {e}")
        
    return [], 0.0


async def run_evaluation(model_name: str, thinking_budget: str | None, verbose: bool):
    print(f"--- Entity Discovery Phase 1 Eval: {model_name} (Thinking: {thinking_budget}) ---")
    
    fixture_path = Path(__file__).parent.parent / "fixtures" / "silver_entity_discovery_benchmark.json"
    with open(fixture_path, 'r') as f:
        fixtures = json.load(f)
        
    print(f"Loaded {len(fixtures)} synthetic benchmarks.")
    
    semaphore = asyncio.Semaphore(10)
    print("Firing inference layer...")
    
    start_time = time.time()
    
    # 1. GENERATE EXTRACTIONS
    extraction_tasks = []
    golden_map = {}
    
    for item in fixtures:
        doc_id = item["document"]["document_id"]
        blocks = item["document"]["content_blocks"]
        golden_map[doc_id] = item["expected_benchmark"]
        extraction_tasks.append(_extract_candidate(doc_id, blocks, model_name, thinking_budget, semaphore))
        
    extraction_results = await asyncio.gather(*extraction_tasks)
    
    # Aggregate Extraction Results
    all_extracted_items = []
    total_candidate_cost = 0.0
    for items, cost in extraction_results:
        all_extracted_items.extend(items)
        total_candidate_cost += cost

    latency = time.time() - start_time
    print(f"Inference complete. Cost: ${total_candidate_cost:.5f}. Latency: {latency:.2f}s")
    
    print("\nFiring Gemini 3.1 Pro Judge Alignment...")
    # 2. RUN JUDGE ALIGNMENT
    tp, fp, fn = 0, 0, 0
    total_judge_cost = 0.0
    
    for doc_id, expected_benchmark in golden_map.items():
        doc_extractions = [e for e in all_extracted_items if e.get('document_id') == doc_id]
        
        mappings, judge_cost = await run_judge_alignment(expected_benchmark, doc_extractions)
        total_judge_cost += judge_cost
        
        matched_expected_indices = set([m.expected_index for m in mappings])
        matched_extracted_indices = set([m.extracted_index for m in mappings])
        
        thread_tp = len(mappings)
        thread_fp = len(doc_extractions) - len(matched_extracted_indices)
        thread_fn = len(expected_benchmark) - len(matched_expected_indices)
        
        tp += thread_tp
        fp += thread_fp
        fn += thread_fn
        
        if verbose:
            if thread_fp > 0 or thread_fn > 0:
                print(f"\n--- Document {doc_id} Mismatches ---")
            for ex_idx, ex in enumerate(doc_extractions):
                if ex_idx not in matched_extracted_indices:
                    print(f"[HALLUCINATION (FP)]: {ex.get('author_id')} -> {ex.get('brand')} {ex.get('productName')}")
            for g_idx, g in enumerate(expected_benchmark):
                if g_idx not in matched_expected_indices:
                    print(f"[MISS (FN)]: {g.get('author_id')} -> {g.get('brand')} {g.get('productName')}")

    # 3. METRICS
    print("\n--- RESULTS PHASE 1 (Entity Discovery) ---")
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    print(f"Entity F1-Score:    {f1*100:.1f}%")
    print(f"  Precision:        {precision*100:.1f}% (TP: {tp}, FP: {fp})")
    print(f"  Recall:           {recall*100:.1f}% (TP: {tp}, FN: {fn})")
    print(f"Judge Overhead:     ${total_judge_cost:.5f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entity Discovery Phase 1 Eval")
    parser.add_argument("-m", "--model", default="gemini-2.5-flash-lite", help="Candidate Model")
    parser.add_argument("-t", "--thinking", type=str, default=None, help="Thinking constraint")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print mismatch details")
    args = parser.parse_args()
    
    import os
    if not os.environ.get("GEMINI_API_KEY"):
        print("ERROR: GEMINI_API_KEY is missing.")
        sys.exit(1)
        
    asyncio.run(run_evaluation(args.model, args.thinking, args.verbose))
