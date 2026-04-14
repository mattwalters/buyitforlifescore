# BIFL Pipeline Architecture & Philosophy

This document serves as the guiding light for the data engineering and LLM extraction philosophy powering the BuyItForLife (BIFL) scoring pipeline. 

By splitting the pipeline into atomic, specialized stages, we radically reduce LLM contextual overload, improve observability, and create a system that elegantly scales from unstructured text to highly verified analytical matrices.

## The Silver Layer
The Silver Layer is fully concerned with **Event-Level Extractions**. At this layer, we operate on isolated text blocks (threads/comments), parsing them for entities, validating intent, and extracting deep sentiment characteristics. This layer represents normalized, row-level product mention events.

To combat LLM rule fatigue, the extraction process is strictly decoupled into three distinct sequential phases:

### Phase 1: `entity_discovery` (The Broad Net)
*   **Concept:** Named Entity Recognition (NER) / Candidate Generation
*   **The Philosophy:** A fast, "dumb, but exhaustive" sponge. This step relies on three core principles:
    1.  **The Principle of Greedy Recall:** False positives are acceptable, but false negatives are fatal. If we miss an entity here, it's gone forever. The model's instruction is simple: "When in doubt, extract it."
    2.  **The Principle of Verbatim Extraction:** The LLM is strictly forbidden from "fixing" or semantically normalizing the text. If a user writes `"darn tough socks"`, we extract it verbatim. We do not let the LLM generate variations like `"Darn Tough LLC"` because generative modifications exponentially increase hallucination risk boundary. 
    3.  **The Principle of Irrelevant Context:** This phase does not care *why* a brand was mentioned. It is blind to intent (e.g. troubleshooting questions), blind to sentiment (e.g. "I hate this"), and blind to grammar (e.g. lower-case or missing punctuation). It scans text like an un-opinionated string-matching engine looking for commercial items.
*   **Outcome:** A highly-recalled list of verbatim, unstructured candidate entity strings attached to specific text blocks.

### Phase 2: `entity_triage` (The Filter)
*   **Concept:** Intent Validation & Heuristic Classification
*   **The Philosophy:** A specialized gateway model. It receives the localized text around an extracted candidate and cross-examines it against a rigorous set of validation rules. Is the user asking a troubleshooting question? Is the brand actually a generic retailer (Costco)? Is this a subreddit acronym (BIFL)? If the candidate fails the intent gates, the row is discarded.
*   **Outcome:** A refined, sanitized list of brand/product names where the user explicitly holds a valid opinion or statement of ownership.

### Phase 3: `entity_attribute_extraction` (The Deep Dive)
*   **Concept:** Attribute Enrichment
*   **The Philosophy:** The heavy lifter. Now that we are mathematically guaranteed to have a valid product mention with a valid opinion, we use complex chain-of-thought routing to extract the deep qualitative metadata. This phase pulls the exact user quote, determines the sentiment (Positive/Negative/Neutral), and calculates the ownership duration.
*   **Outcome:** A perfect, deeply enriched Silver record ready for analytical aggregation.

---

## The Gold Layer
*(Future Implementation)*

The Gold Layer will be concerned with **Aggregations and Rollups**. At this layer, we will group the thousands of individual Silver events by Brand and Product to calculate the final proprietary BIFL scores, market-share consensus, and aggregated sentiment indices.
