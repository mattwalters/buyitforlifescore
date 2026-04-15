# LLM Asset Development Workflow

This document describes the standard workflow for developing any Dagster asset whose primary job is an LLM transformation. Not every asset in this pipeline follows these rules — Bronze ingestion assets, pure SQL transformations, and aggregation assets do not. But for any asset where the work is "call an LLM and store the result," this workflow is mandatory.

---

## The Core Principle: One Function, Three Callers

Every LLM-wrapped asset must have a single shared Python function in `utils/llm.py` that is the **only** place the actual LLM call lives. That function is called by exactly three things:

1. The Dagster asset (production)
2. The offline eval script (`scripts/eval_offline_*.py`)
3. The online eval script (`scripts/eval_online_*.py`)

If the asset and the evals ever diverge — different prompts, different chunking logic, different parsing — then your eval scores are measuring a different system than the one running in production. The shared function is the invariant that prevents this.

**Existing examples:**
- `process_thread_discovery()` — called by `silver_entity_discovery_payloads`, `eval_offline_entity_discovery.py`, and `eval_online_entity_discovery.py`
- `run_entity_triage()` — called by the triage asset and `eval_offline_entity_triage.py`
- `run_entity_extraction()` — called by `silver_entity_extraction_payloads`

---

## The Dagster Asset Is a Thin Wrapper

An LLM asset should do exactly four things and nothing more:

1. Query its source data (from upstream parquet or Bronze)
2. Construct typed inputs for the shared function
3. Call the shared function
4. Write the output parquet and report metadata

It should not contain any prompt text, chunking logic, output parsing, or API call code. If you find yourself writing any of that inside the asset, it belongs in `utils/llm.py` instead.

---

## Development Order

Follow these steps in sequence. The order matters — writing the offline eval before the Dagster asset forces you to design the prompt and data shapes correctly, without the distraction of pipeline infrastructure.

### Step 1: Define Pydantic Shapes

Before writing any code, define three Pydantic models in the relevant `prompts/` file:

**Input shape** — what each item looks like going *into* the shared function:
```python
class ExtractionInput(BaseModel):
    brand: str
    product_name: str
    text: str
    parent_text: str
    target_authored_at: str
```

**Output shape** — what the LLM returns, used as the Gemini `response_schema`:
```python
class EntityExtraction(BaseModel):
    quote: str
    sentiment: Literal["POSITIVE", "NEUTRAL", "NEGATIVE"]
    ownershipDurationMonths: Optional[int]
    # ...
```

**Result wrapper** — the return type of the shared function, carrying the output plus cost/token telemetry:
```python
@dataclass
class ExtractionResult:
    payload: dict
    raw_json: str
    cost: float
    input_tokens: int
    output_tokens: int
    prompt_text: str
```

Defining these before anything else gives you a contract that the asset, the shared function, and both evals all code against. The input shape also directly determines the fixture format for the offline eval.

### Step 2: Write the Shared Function

Add the function to `utils/llm.py`. It should accept an instance of your input shape (or its fields directly, matching the dataclass approach already used), handle chunking if needed, call the Gemini API with your output schema, and return the result wrapper.

```python
async def run_entity_extraction(
    brand: str,
    product_name: str,
    # ...
    model_name: str,
    thinking: Optional[str] = None,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> ExtractionResult:
```

The function must be model-agnostic — `model_name` is always a parameter, never hardcoded.

### Step 3: Write the Offline Eval

Write `scripts/eval_offline_{phase_name}.py` before the Dagster asset. This is the right default — it forces you to validate your prompt design against labeled examples before wiring anything into the production DAG. The exception is exploratory work: if the output schema isn't clear yet, it's fine to prototype the asset first to see what the LLM actually produces. But the offline eval must exist and pass before the asset is treated as production-ready.

**Fixtures** live in `fixtures/silver_{phase_name}_benchmark.json`. 10 examples is the minimum to catch obvious prompt failures — enough to ship, not enough to trust long-term. Fixture design matters more than count: 10 cases that cover edge cases and known failure modes beats 50 happy-path examples. The fixture set should grow over time as production surfaces new patterns. Each fixture should directly exercise an input shape instance and label the expected output.

**Fixture format** should mirror exactly what the shared function receives as input. For a phase that processes individual items, a fixture looks like:

```json
{
  "id": "triage_01",
  "raw_mention": "Darn Tough socks",
  "text": "My Vitamix died after 12 years, how do I fix it?",
  "parent_text": "Title: Best blenders\nBody: ",
  "expected_passes": true,
  "signal_note": "Failure embedded in question — should PASS per rules"
}
```

For a phase that processes thread-level data, fixtures store the raw thread structure that the shared function receives, exactly as `load_bronze_threads_with_comments` would return it:

```json
{
  "document": {
    "document_id": "synth_01",
    "title": "My Darn Tough socks after 8 years",
    "body": "",
    "comments": []
  },
  "expected_benchmark": [
    { "author_id": "OP", "raw_mention": "Darn Tough socks", "source_block_ids": [0] }
  ]
}
```

**Scoring:** The right approach depends on output complexity:

| Output type | Scoring approach |
|---|---|
| Binary classification (pass/fail) | Deterministic: compare `actual` vs `expected`, compute accuracy/F1/recall |
| Named entity extraction | Deterministic: substring match + author_id + source_block_ids, compute precision/recall/F1 via `calculate_eval_metrics()` |
| Rich structured output (5+ optional fields) | LLM judge — hand-labeling complex multi-field JSON is error-prone and brittle |

For the rich structured case, "correct" is often genuinely ambiguous from the text alone, and a judge evaluating coherence catches the failures that matter in practice. The `silver_entity_extraction_eval` Dagster asset is the existing example of this. If you use an LLM judge in the offline eval, call it out explicitly in the script with a comment explaining why deterministic scoring wasn't viable.

The offline eval CLI must support:
```
-m / --model       Candidate model name
-t / --thinking    Thinking constraint (numeric budget or level string)
-v / --verbose     Print mismatch details
```

Use `calculate_eval_metrics()` from `utils/metrics.py` for all score computation.

### Step 4: Write the Dagster Asset

Now write the asset. It will be short. A typical LLM asset has this shape:

```python
@asset(group_name="silver", partitions_def=bifl_daily_partitions, ...)
def silver_entity_triage_payloads(context: AssetExecutionContext, config: SilverLLMConfig) -> MaterializeResult:
    # 1. Load source data
    rows = load_source_data(partition_date_str, limit=config.limit)
    if not rows:
        return MaterializeResult(metadata={"status": "skipped"})

    # 2. Run shared function for each item
    semaphore = asyncio.Semaphore(10)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results, total_cost, ... = loop.run_until_complete(
        _process_batch(rows, config.model, semaphore, config.thinking)
    )

    # 3. Write parquet
    out_df = pd.DataFrame(results)
    with get_duckdb_connection() as con:
        con.execute(f"COPY ... TO '{target_parquet}' (FORMAT PARQUET)")

    # 4. Return metadata
    return MaterializeResult(metadata={...})
```

The asset config always inherits from `SilverLLMConfig` (or an appropriate subclass) so that `model`, `thinking`, and `limit` are configurable from the Dagster UI.

Pair each payload asset with a lightweight unnesting asset that parses `raw_json_output` into flat rows. This two-asset pattern keeps the LLM output immutable and separates the expensive operation (the API call) from the cheap one (JSON parsing).

### Step 5: Write the Online Eval

Write `scripts/eval_online_{phase_name}.py` once the asset has run in production and generated enough data to sample meaningfully — a handful of partitions is usually sufficient. This step can be deferred: the offline eval is the primary quality gate while the asset is new. Do not block shipping on the online eval.

The online eval samples real production inputs, runs the shared function with a candidate model configuration, then uses an LLM blind judge to score the outputs (since there are no labels). The purpose is model comparison and regression detection against real data distribution, not ground-truth accuracy.

**Sampling:** Query the parquet files produced by the *upstream* asset using `load_bronze_threads_with_comments` or a direct DuckDB query. For example, the online entity discovery eval samples Bronze parquet directly; an online triage eval would sample the discovery output parquet.

**Blind judge:** The judge sees the input context and the candidate's output, but not the ground-truth label (there isn't one). It evaluates coherence, completeness, and correctness. The judge prompt and schema should live in `prompts/judge_{phase_name}.py`.

The online eval CLI must support:
```
-n  / --count             Number of samples
-m1 / --extractor-model   Candidate model for the phase under test
-t1 / --extractor-think   Thinking tokens for the extractor
-m2 / --judge-model       LLM judge model
-t2 / --judge-think       Thinking tokens for the judge
-v  / --verbose           Print per-item judge reasoning
```

---

## File Layout

When you add a new LLM phase, you should create exactly these files:

```
src/pipeline/prompts/{phase_name}.py        # Input schema, output schema, prompt function
utils/llm.py                                # +  run_{phase_name}() shared function
src/pipeline/defs/silver/{phase_name}.py    # Thin Dagster asset(s)
fixtures/silver_{phase_name}_benchmark.json # Labeled offline fixtures
scripts/eval_offline_{phase_name}.py        # Offline eval — required before production-ready
scripts/eval_online_{phase_name}.py         # Online eval — deferred until production data exists
tests/defs/silver/test_{phase_name}.py      # Unit tests for the Dagster asset
```

---

## Pressure Testing These Rules

These guidelines are not universally applicable. Here is where they break down or need qualification:

**"10 synthetic fixtures is enough"**
It's enough to ship, not enough to trust. 10 is the minimum to catch obvious prompt failures. The fixture set should grow over time as you encounter real-world edge cases in production. When the online eval consistently flags a class of failures, that's a signal to add fixtures covering that pattern.

**"Offline eval should be deterministic"**
This holds for binary classification (triage) and recall-oriented extraction (discovery). It breaks down for the attribute extraction phase, where the output is a rich multi-field struct with many optional fields. Hand-labeling 10 `EntityExtraction` objects with correct `ownershipDurationMonths`, `primaryFlawOrFailure`, and `diyRepairability` values is feasible but fragile — small wording differences in the source text make "correct" ambiguous. An LLM judge is the right call there, and the `silver_entity_extraction_eval` Dagster asset is the existing example of this.

**"One shared function per phase"**
Correct, but the shared function can internally call sub-functions. `process_thread_discovery()` calls `build_comment_tree()`, `chunk_branches()`, `build_content_blocks()`, and then `run_entity_discovery()`. The rule is that the *entry point* the eval calls must be identical to the one the asset calls — not that all underlying logic must be a single function.

**"Write the offline eval before the asset"**
This is the right default. The main exception is exploratory work: if you're not sure what the output schema should look like, it's fine to prototype the asset and run it manually to see what the LLM produces. But before treating the asset as production-ready, the offline eval with labeled fixtures must exist and pass.

**"Online eval requires a previous production run"**
True. If the upstream parquet doesn't exist yet, the online eval can't sample from it. During initial development, you can substitute by generating a small set of synthetic production-format inputs (matching what the upstream asset would write) and pointing the online eval at those instead. This is a temporary scaffold, not a permanent solution.

**"Every LLM asset needs both evals"**
Every LLM asset needs an offline eval. The online eval can be deferred until the asset is running in production and generating enough data to sample meaningfully — a handful of partitions is usually sufficient. For a new phase with no production history yet, the offline eval is the primary quality gate.
