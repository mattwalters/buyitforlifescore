# BIFL Data Pipeline Architecture

This document accurately details the end-to-end data ingestion, extraction, canonicalization, and taxonomy pipeline. The system solves the "Brand Gravity" problem inherent in standard semantic embeddings by breaking the extraction process into three distinct layers (Bronze, Silver, Gold), heavily orchestrated by **Gemini 2.5 Flash Lite**, and executed via a unified bash orchestrator.

## Orchestration Overview
The entire pipeline is driven by `scripts/run-pipeline.sh`, which runs inside the `@mono/admin` workspace. We intentionally do **not** use background workers (BullMQ) for pipeline processing. Instead, synchronous `tsx` scripts sequentially advance the state of the data to ensure referential integrity.

---

## 1. The Bronze Layer (Raw Ingestion)
**Goal:** High-volume, lossless data retention.

The Bronze layer ingests bulk unstructured data from Reddit (historically ZST archives converted to Parquet). 
- **Models:** `BronzeRedditSubmission`, `BronzeRedditComment`
- **What happens here:** Data is shoveled directly into the Postgres database. No LLM processing occurs at this stage, preventing API bottlenecks. Parquet seeding is performed by robust batched ingestion scripts (`generate-silvers` directly pulls `Bronze` records).

---

## 2. The Silver Layer (Extraction Primitives)
**Goal:** Extract rigid concepts out of unstructured text while isolating extraction from canonicalization.

- **Process:** `scripts/generate-silvers.ts`
- **What happens here:**
  1. The script continuously scans un-processed Bronze records and uses **Gemini 2.5 Flash Lite** (via the Structured Outputs schema).
  2. It generates a separate `SilverProductMention` for each item found.
  3. **Specificity Tagging:** It identifies the mention's depth:
     - `BRAND_ONLY` (e.g., *"Patagonia is great"*)
     - `PRODUCT_LINE` (e.g., *"I love Patagonia Baggies"*)
     - `EXACT_MODEL` (e.g., *"Patagonia Baggies 5-inch inseam"*)
  4. **Vector Generation:** A secondary script (`embed-silvers.ts`) evaluates the output and natively generates a 1024-D embedding using the local CPU-bound ONNX engine (`mixedbread-ai/mxbai-embed-large-v1`).

---

## 3. The Gold Layer (Canonical Rollup & Titling)
**Goal:** Build a flawless programmatic SEO (pSEO) hierarchy to capture broad traffic (head terms) and high-intent conversions (exact models).

Standard semantic vectors suffer from **Brand Gravity** (e.g., placing "Ford Wrench" next to "Ford Taurus"). We transition to a **Hybrid Pipeline** using local ONNX vectors solely as a pre-filter hook, and then relying on Gemini 2.5 Flash Lite as the final disambiguation judge.

We operate a strict **3-Tier Ontology**:
1. `GoldBrand` (Ultimate Parent)
2. `GoldProductLine` (Child of Brand)
3. `GoldProduct` (Child of Line)

### Rollup Phase
1. `roll-up-brands.ts`: Groups `BRAND_ONLY` mentions using global lexical matches and LLM typo-correction.
2. `roll-up-product-lines.ts`: Targets `PRODUCT_LINE` mentions. Uses vector proximity to grab candidate product lines, then queries Gemini to match or reject them to prevent false aggregation.
3. `roll-up-exact-models.ts`: Operates identically to the above, strictly merging `EXACT_MODEL` instances representing extreme long-tail affiliate targets.

### Titling Phase
To ensure canonical cleanliness (preventing random sizes or SKUs from polluting canonical records), the **Titling Phase** executes:
- `titling-phase.ts`: Actively strips meaningless tracking metrics (e.g., "12-inch", "SKU 1845") while strictly preserving core defining features and versions (e.g., "Leap V2").

### Hierarchy Linking Phase
- `link-hierarchy.ts`: Links orphaned exact models up to their logical parent Product Lines, ensuring the visual breadcrumb UI flows accurately.

---

## 4. The Taxonomy Layer (Organic Categorization)
**Goal:** Dynamically generate an SEO-optimized site architecture and slot products into it.

Instead of hard-coding thousands of product categories, we utilize a 4-step organic taxonomy pipeline driven by the underlying dataset.

### Step 1: `seed-departments.ts`
Hardcodes the 19 absolute Top-Down macro departments (e.g., "Kitchen & Dining", "Apparel & Accessories").

### Step 2: `discover-categories.ts`
For every Gold Product Line, the script retrieves its foundational Reddit context (`BronzeRedditSubmission` title) and asks Gemini to brainstorm exactly 3 SEO-friendly category terms (e.g., "Manual Coffee Grinder"). These are saved as `SilverCategoryIdea`.

### Step 3: `consolidate-categories.ts`
Batches of Silver category ideas are fed to Gemini to aggressively merge duplicates. **CRITICAL:** We enforce strict negative constraints and Good vs. Bad examples here to prevent the AI from "over-consolidating" distinct tools with separate use-cases (e.g., it merges "Mens Belts" and "Casual Belts", but will actively refuse to merge "Work Boots" and "Casual Boots"). The output becomes canonical `GoldCategory` records.

### Step 4: `route-taxonomy.ts`
The final routing phase. The script selects a `GoldProductLine`, calculates its vector centroid, and fetches the top 60 `GoldCategory` candidates. 
- **Context Injection:** To prevent "Context Starvation", the LLM is fed the parent Reddit thread title and the pre-generated `SilverCategoryIdea`. 
- **Candidate Pinning:** The exact matching `GoldCategory` records are manually pinned into the top 60 candidate list.
The LLM evaluates the context and formally assigns the product to one and only one Department, and an array of Categories.

---

## Why This Pipeline Wins
* **No Artificial Merging:** The pipeline respects user specificity. If a user was vague, it attributes the social proof to the vague Brand bucket. If they were specific, it applies to the exact model. 
* **Self-Healing and Idempotent:** Processing operates using boolean tags (`isTitled`, `isProcessed`). We can iteratively refine prompts and quickly target subsets of failures without destroying the entire catalog representation.
