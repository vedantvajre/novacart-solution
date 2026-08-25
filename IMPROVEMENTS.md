# NovaCart Pipeline — Issues & Improvements

> **How to use this file:**
> Every bug, code quality issue, and recommended improvement discovered during review is tracked here.
> Items are grouped by type and severity. Each entry references the exact file and line.
> This is a living document — add new findings as they are discovered.

---

## Table of Contents

1. [High Severity Issues](#1-high-severity-issues)
2. [Medium Severity Issues](#2-medium-severity-issues)
3. [Low Severity / Style Issues](#3-low-severity--style-issues)
4. [Recommended Improvements](#4-recommended-improvements)
5. [Data Model Redesign](#5-data-model-redesign)
6. [Novel & Innovative Additions](#6-novel--innovative-additions)
7. [Production Migration Plan](#7-production-migration-plan)
8. [Prioritised Backlog](#8-prioritised-backlog)

---

## 1. High Severity Issues

---

### H-1 · Watermark advances before downstream stages succeed

**File:** `src/ingest/products.py` · `src/utils/state.py:22–27`

**Problem:**
`set_watermark()` is called inside `ingest_products()` immediately after the Bronze write, *before* Silver or Gold stages run. If Silver or Gold then fails, the run is recorded as `FAIL` but the watermark has already moved forward. The next run skips the failed window permanently — **data is silently and irrecoverably lost.**

This directly explains the client problem: *"Orders sometimes go missing."*

**Fix:**
Move watermark advancement to after `run_one_date()` completes with `status == "SUCCESS"`. The `StateManager` already records run outcomes — gate the watermark update on that result. This is the standard two-phase commit pattern for ELT pipelines.

---

### H-2 · Products Bronze overwritten on every run — no partition history

**File:** `src/ingest/products.py:62`

**Problem:**
Every run writes to the same single file `bronze/products/data.parquet`, overwriting whatever was there. If the watermark advances but the pipeline fails mid-run, the Bronze file contains a stale delta and the gap is irrecoverable. Unlike orders (date-partitioned), products have no partition strategy.

**Fix:**
Partition products Bronze by ingestion timestamp: `bronze/products/ingested_at=<ts>/data.parquet`. Preserves a replay-capable history of each incremental delta.

---

### H-3 · Dead variable `updated_rows` in SCD2 logic

**File:** `src/transform/gold.py:82,108`

**Problem:**
`updated_rows = []` is declared and conditionally appended to (`if updated_rows: frames.append(...)`), but **nothing ever appends to it**. The SCD2 logic only appends to `new_rows`. The conditional is permanently dead code and is misleading to anyone reading the SCD2 logic.

**Fix:**
Remove the `updated_rows` variable and its conditional entirely.

---

### H-4 · MD5 used for SCD2 row hashing — collision risk

**File:** `src/transform/gold.py:17`

```python
return hashlib.md5(val.encode()).hexdigest()
```

**Problem:**
At scale across millions of customer records, MD5 hash collisions are a realistic risk. A collision means a changed customer record is **silently not SCD2-expired** — stale address data is kept as current, corrupting geographic analysis.

**Fix:**
Replace `hashlib.md5` with `hashlib.sha256`. One-line change, no other impact.

---

### H-5 · Row-by-row `iterrows()` loop for Pydantic validation — will not scale

**File:** `src/transform/silver.py:25`

**Problem:**
`df.iterrows()` is the slowest way to process a DataFrame. Combined with constructing a full Pydantic model per row, this is an O(n) Python loop that becomes a latency bottleneck at production order volumes. It also calls `row.to_dict()` twice per bad row.

**Fix:**
Use Pydantic's `TypeAdapter` for batch validation or a list comprehension over `model.model_validate()`. Per-record fallback only for rejected batches. See also I-6.

---

### H-6 · SCD2 `dim_customer` loop is O(n²)

**File:** `src/transform/gold.py:84`

**Problem:**
For every incoming customer, the loop filters the entire `current` DataFrame (`current[current["customer_id"] == cid]`). This is O(n × m) — quadratic as both datasets grow. Unusable at production customer volumes.

**Fix:**
Replace with a pandas merge/join: merge `incoming` against `current` on `customer_id`, compute hashes vectorially, then split changed/new/unchanged rows via boolean masks. Single merge pass — O(n log n). See also Iceberg `MERGE INTO` in the migration plan.

---

### H-7 · Race condition on `watermarks.json` — concurrent runs corrupt state

**File:** `src/utils/state.py:22–27`

**Problem:**
The read-modify-write on `watermarks.json` has no file locking. Two concurrent pipeline processes (e.g. a scheduled run and a manual backfill) can interleave reads and writes, causing one to silently clobber the other's watermark.

**Fix:**
Use `filelock` around the read-write cycle, or move state to PostgreSQL (resolved completely in the migration plan — Phase 2).

---

## 2. Medium Severity Issues

---

### M-1 · Pydantic schema inconsistency — two sources of truth per entity

**File:** `src/ingest/orders.py:13–16`, `src/ingest/customers.py:14–17`, `src/ingest/products.py:15–17`, `src/transform/schema_check.py`, `src/utils/schemas.py`

**Problem:**
Two completely different schema enforcement mechanisms exist side by side:

| Mechanism | Layer | What it checks |
|---|---|---|
| `EXPECTED_COLUMNS` list + `check_schema()` | Bronze | Column *names* only |
| Pydantic models (`OrderRow` etc.) | Silver | Types, values, business rules |
| Nothing | Gold | No enforcement at all |

`EXPECTED_COLUMNS` and the Pydantic model for the same entity declare the same field names in two places and can silently drift apart. `check_schema()` is also strictly weaker than Pydantic — it cannot detect type mismatches. The column name check is redundant because Pydantic would raise `ValidationError` for a missing field anyway.

**Fix:**
Replace `EXPECTED_COLUMNS` + `check_schema()` with **layer-scoped Pydantic models** per entity:

```
BronzeOrderRow   → raw types (str), structural check only — replaces EXPECTED_COLUMNS
SilverOrderRow   → typed + validated (current OrderRow)
GoldFactOrder    → derived fields validated (total_amount, joined keys)
```

Bronze models use `model_config = ConfigDict(strict=False)` to tolerate raw CSV strings. Single source of truth per layer. Eliminates `schema_check.py` and all `EXPECTED_COLUMNS` lists.

---

### M-2 · Email validation is trivially weak

**File:** `src/utils/schemas.py:53–57`

**Problem:**
Only checks for presence of `@`. Strings like `"@"`, `"a@"`, `"@b"` all pass. Invalid emails reach Silver and Gold.

**Fix:**
Use Pydantic's built-in `EmailStr` type (`pip install pydantic[email]`) for RFC-5322 compliant validation.

---

### M-3 · `CustomerRow.tier` accepts any string — unconstrained

**File:** `src/utils/schemas.py:50`

**Problem:**
Any arbitrary string is accepted. A CRM change introducing `"platinum"` or `"VIP"` silently corrupts tier-based reporting in Gold.

**Fix:**
```python
from typing import Literal
tier: Literal["standard", "silver", "gold"] = "standard"
```

---

### M-4 · `ProductRow.updated_at` typed as `str` instead of `datetime`

**File:** `src/utils/schemas.py:66`

**Problem:**
A malformed timestamp like `"not-a-date"` passes Pydantic validation and propagates through to Silver/Gold, silently breaking watermark comparisons on the next run.

**Fix:**
```python
from datetime import datetime
updated_at: datetime
```
Pydantic natively parses ISO 8601 strings — SQLite's string output is still accepted.

---

### M-5 · Overly broad `except (ValidationError, Exception)` swallows bugs

**File:** `src/transform/silver.py:29`

**Problem:**
Catching bare `Exception` means programming errors (`KeyError`, `AttributeError`) caused by code bugs are silently quarantined as "bad data" instead of surfacing as failures.

**Fix:**
```python
except ValidationError as exc:
```
Let all non-validation exceptions propagate.

---

### M-6 · `build_dim_product` reads the same Parquet file twice

**File:** `src/transform/gold.py:32,36`

**Problem:**
Line 32 reads the file to check emptiness; line 36 reads it again to use the data. Doubles I/O unnecessarily.

**Fix:**
```python
df = pd.read_parquet(src)
if df.empty:
    ...
```

---

### M-7 · `datetime.utcnow()` is deprecated in Python 3.12+

**File:** `src/pipeline.py:34,38,80`

**Problem:**
`datetime.utcnow()` is deprecated since Python 3.12 and scheduled for removal. Already used correctly elsewhere in the codebase.

**Fix:**
```python
datetime.now(timezone.utc)
```

---

### M-8 · Logger handler guard is not test-safe — duplicate handlers accumulate

**File:** `src/utils/logging_setup.py:13`

**Problem:**
`logging.getLogger(name)` returns the same global instance across all test runs. After the first test adds handlers, subsequent tests reuse stale file handlers pointing at the previous test's temp directory. Log output accumulates with duplicate entries across test runs.

**Fix:**
Clear existing handlers explicitly in the test fixture teardown, or clear handlers when the log directory changes.

---

### M-9 · Schema drift warning bypasses structured logging

**File:** `src/transform/schema_check.py:26`

**Problem:**
Every other log call uses `log_event()` producing structured JSON. This one calls `logger.warning()` directly, writing an unstructured plain-text line that breaks any log parser consuming `logs/pipeline.jsonl`.

**Fix:**
```python
log_event(logger, "WARNING", "schema_drift_additive",
          source=source_name, added_columns=sorted(added))
```

---

## 3. Low Severity / Style Issues

---

### L-1 · `src/load/` module is empty — misleading structure

**File:** `src/load/__init__.py`

Either move the Gold write logic here to complete the separation of concerns, or delete `src/load/` and update `README.md`. Note: this module is the natural home for the DuckDB loader in the migration plan (N-2).

---

### L-2 · `silver_cfg` and `gold_cfg` return untyped `dict` — no type safety

**File:** `src/utils/config.py:41–43`

Inline `.get("scd2_track_fields", [...])` calls in `pipeline.py` fail silently on key typos. Replace with typed dataclasses per config section.

---

### L-3 · `== True` comparison instead of truthy check

**File:** `src/transform/gold.py:80`

```python
# Before
current = existing[existing["_current"] == True].copy()
# After
current = existing[existing["_current"]].copy()
```

---

### L-4 · Dependency version pins use `>=` minimums — not reproducible

**File:** `requirements.txt`

All five dependencies use lower-bound `>=` pins. Commit a `requirements.lock` generated by `pip-compile` with exact pinned versions for reproducible installs.

---

### L-5 · `import csv` / `import main` inside test function bodies

**File:** `tests/test_pipeline_scenarios.py:118,132,227,244`

Move all imports to the top of the file.

---

## 4. Recommended Improvements

---

### I-1 · Two-phase commit: decouple watermark from downstream success

**Root problem:** Orders going missing after partial pipeline failures.

Advance the products watermark only after `run_one_date()` completes with `status == "SUCCESS"`. Failed runs are automatically retried on the next execution because the watermark never moved. See H-1.

---

### I-2 · Quarantine threshold: fail the pipeline on mass row rejection

**Root problem:** 12,000 rows silently dropped for three weeks; $400K revenue gap.

Add `quarantine_threshold_pct` to `pipeline.yaml` (e.g. `5.0`). If more than N% of rows for any source are quarantined in a single run, escalate to `FAIL` with a `quarantine_threshold_exceeded` log event.

---

### I-3 · Per-run data quality summary

**Root problem:** No observability; nothing to debug at 3am.

At the end of `run_one_date()`, write a structured `state/<date>/run_summary.json` containing rows ingested / quarantined / passed per source, rows written to Silver and Gold, SCD2 new / expired / unchanged counts, and wall-clock duration per stage. This feeds the Streamlit dashboard (N-1) and AI agent (N-3).

---

### I-4 · Referential integrity checks before writing `fact_orders`

**Root problem:** Reports don't match; orders go missing from dashboards.

Before writing `fact_orders`, left-join against `dim_customer` and `dim_product`. Rows that cannot be matched to a valid dimension record should be quarantined with a `WARNING` log. Orphan orders currently land silently in the fact table and cause dashboard mismatches.

---

### I-5 · SCD2 surrogate key `customer_sk` on `dim_customer`

**Root problem:** Geographic point-in-time analysis is impossible.

Add a UUID surrogate key `customer_sk` to each SCD2 row. Resolve the correct `customer_sk` when writing `fact_orders` by joining on `customer_id AND order_date BETWEEN _eff_start AND _eff_end`. See also D-2.

---

### I-6 · Vectorised Silver validation

**Root problem:** Performance at scale. See H-5.

Replace the `iterrows()` loop in `_validate_df` with Pydantic's `TypeAdapter` or a list comprehension over `model.model_validate()`. Per-record fallback only for rejected batches.

---

### I-7 · Schema version registry with evolution log

**Root problem:** Schema changes break things silently.

Add `schema_version` per source in `pipeline.yaml`. When additive drift is detected, log new columns and inferred types to `state/schema_evolution.jsonl`. Creates an auditable change record without halting the pipeline. The column lineage manifest (N-6) is the structural complement to this.

---

### I-8 · Checkpoint / skip-on-success for re-runs

**Root problem:** Re-runs cause duplicates; safe replay is not possible.

Extend `StateManager` to record which `(date, stage)` pairs have already succeeded. On re-run, skip completed stages unless `--force` is passed. Resolved more elegantly by Iceberg `MERGE INTO` in the migration plan.

---

### I-9 · Revenue anomaly detection

**Root problem:** The $400K gap went unnoticed for three weeks.

After each Gold run, compare daily revenue against a configurable rolling average stored in `state/`. If deviation exceeds a threshold (e.g. ±30%), emit a `WARNING`-level `revenue_anomaly_detected` log event with expected vs actual figures.

---

## 5. Data Model Redesign

> These are structural issues with the Gold data model itself — not code quality issues.
> They determine whether the analytics team's dashboards are actually trustworthy.
> Fixing the items in sections 1–4 does not fix these.

---

### D-1 · Orders have no lifecycle tracking — only a status snapshot

**Root problem:** Confirmed revenue vs pending revenue is indistinguishable. *"Reports don't match."*

`fact_orders` stores a `status` snapshot from whichever CSV arrived that day. An order `pending` on November 7th and `delivered` on November 10th produces two disconnected rows in two partitions with no link between them. NovaCart cannot answer: how long do orders take to fulfil? What is the cancellation rate? What is revenue from confirmed-delivered orders only?

**Fix:**
Introduce `fact_order_status_history` keyed on `(order_id, status, effective_date)`. The main `fact_orders` retains the latest known status for convenience, but the history table is the source of truth for lifecycle analysis.

---

### D-2 · `fact_orders` joins on natural key — all historical geographic analysis is silently wrong

**Root problem:** *"Customer history is lost. Geographic trend analysis is impossible."*

`fact_orders` stores `customer_id` (natural key). Any join to `dim_customer` returns the *current* address, not the address active at order time. An order placed by Alice in New York is now attributed to London (her current city) in every report — silently, with no error.

**Fix:**
1. Add `customer_sk` surrogate key to each SCD2 row in `dim_customer`
2. Resolve the correct `customer_sk` at `fact_orders` write time by joining on `customer_id AND order_date BETWEEN _eff_start AND _eff_end`
3. Store `customer_sk` as the foreign key in `fact_orders`

---

### D-3 · Cost data is never joined to revenue — margin analysis is impossible

**Root problem:** The pipeline has all data needed for profitability analysis but never computes it.

`fact_orders` has `unit_price`. `dim_product` has `unit_cost`. These are never joined. Gold can tell you revenue but not margin — arguably more important for a retailer.

**Fix:**
When writing `fact_orders`, join against `dim_product` and compute:
```
gross_margin = (unit_price - unit_cost) * quantity
margin_pct   = (unit_price - unit_cost) / unit_price
```

---

### D-4 · One product per order assumption — multi-item orders may be silently collapsed

**Root problem:** *"Orders sometimes go missing"* — a structural explanation.

`fact_orders` has a single `product_id` per row, and Silver deduplicates on `order_id` (keep last). If the source CSV is one row per order *line item* (common in order management systems), Silver collapses a three-item order into one row, silently dropping two line items and understating revenue with no error or quarantine event.

**Fix:**
1. Confirm with the source system team: is the CSV one row per order or per line item?
2. If per line item: change the primary key to `(order_id, product_id)` in `OrderRow` and Silver's dedup call
3. Rename to `fact_order_lines` to make granularity explicit

**This must be answered before any revenue numbers can be trusted.**

---

### D-5 · No pipeline lineage on Gold rows — cannot scope reprocessing after an incident

**Root problem:** *"The pipeline silently dropped 12,000 rows. Nobody noticed for three weeks."*

Gold rows have no metadata about when or how they were produced. When an incident is discovered, the only remediation option is "reprocess everything" — there is no way to identify which partitions were produced under a broken schema.

**Fix:**
Add to every Gold table at write time:
- `_pipeline_run_id` — UUID generated per `run_one_date()` call, also recorded in `state/`
- `_processed_at` — UTC timestamp of Gold write

Gives a direct handle from any Gold row back to the pipeline run that produced it.

---

### Data Model Summary

| # | Issue | Tables affected | Business consequence |
|---|---|---|---|
| D-1 | No order lifecycle / status history | `fact_orders` | Cannot measure fulfilment time, confirmed revenue, or cancellation rate |
| D-2 | Natural key join — SCD2 surrogate missing | `fact_orders`, `dim_customer` | Every historical geographic report is silently wrong |
| D-3 | Cost never joined to revenue | `fact_orders`, `dim_product` | Margin analysis impossible despite data existing |
| D-4 | One-product-per-order assumption | `fact_orders` | Multi-item orders may be silently collapsed, understating revenue |
| D-5 | No pipeline lineage on Gold rows | All Gold tables | Cannot scope reprocessing after a data quality incident |

---

## 6. Novel & Innovative Additions

> Additions that go beyond fixing existing problems. Each is mapped to the specific client problems it addresses.

The six client problems for reference:
- **P1** — Three source systems, three formats, not talking to each other
- **P2** — Malformed rows silently disappear or pollute reports
- **P3** — Re-runs cause duplicates
- **P4** — Schema changes break things silently ($400K gap, 3 weeks unnoticed)
- **P5** — Customer history lost, geographic trend analysis impossible
- **P6** — No observability, nothing to debug at 3am

---

### N-1 · Streamlit Pipeline Health Dashboard

**Client problems:** P2, P4, P6

Lowest effort of all ideas here. The pipeline already writes structured JSONL to `logs/pipeline.jsonl` and run metadata to `state/run_history.jsonl`. A Streamlit dashboard is a single Python file reading those outputs — no new infrastructure, deployable to Streamlit Cloud for free.

**What it shows:**
- Run history: date, status, duration, rows per stage
- Quarantine rate per source over time — spike = schema or data quality event
- Stage duration over time — spike = performance regression
- Revenue total per run vs rolling average — spike/drop = anomaly
- Schema drift event log

**Implementation:** `pip install streamlit` → `streamlit run dashboard.py`

---

### N-2 · DuckDB Analytical Layer

**Client problems:** P1, P5

DuckDB queries Parquet files in place — `SELECT * FROM 'data/gold/fact_orders/date=*/data.parquet'`. Zero loading step, zero infrastructure. The empty `src/load/` module is the natural home for a `DuckDBLoader` that registers Gold tables as views. Analytics team goes from "we can't query this" to "full SQL over everything" in an afternoon.

**Additional benefit:** DuckDB handles the referential integrity checks in I-4 significantly faster than pandas merges, and handles `MERGE INTO` for SCD2 in the migration path.

**Implementation:** `pip install duckdb` → `src/load/duckdb_loader.py`

---

### N-3 · AI Agent — Pipeline Consistency & Observability

**Client problems:** P2, P4, P6

**Fundamental objective:** NovaCart's core problems are lack of consistency and lack of observability. The agent is not a passive reporter — it is an active participant in the pipeline's operational loop, with the explicit goal of understanding and optimising both. It has full read access to all pipeline artefacts and limited operational access to the pipeline's control plane.

---

**Agent access model:**

| Access type | Resources | Purpose |
|---|---|---|
| Read | `state/<date>/run_summary.json` | Run outcome, row counts, durations |
| Read | `data/quarantine/**` | Bad row samples, quarantine reasons |
| Read | `state/schema_evolution.jsonl` | Schema drift history |
| Read | `lineage/*.yaml` | Column-to-metric blast radius |
| Read | `metrics/definitions.yaml` | Business metric definitions |
| Read | `state/run_history.jsonl` | Last N run outcomes for trend analysis |
| Read | `contracts/*.yaml` | Data contract definitions |
| Operational | Pipeline control plane | Requeue a failed date, flag drift for human review |
| Write | `state/<date>/agent_report.md` | Persist the agent's diagnosis |
| Notify | Slack / Teams webhook | Post digest or alert to on-call channel |

The agent does **not** have write access to Bronze, Silver, or Gold data — it cannot modify the pipeline's data outputs.

---

**Two operational modes:**

**Success mode** — after every successful run, posts a natural-language digest:
> *"November 7th: 847 orders processed, 3 quarantined (0.4%), 12 new customers, 2 address changes tracked via SCD2, gross revenue £41,200 — up 8% on 7-day average. All data contracts passed. No schema drift detected."*

**Failure / anomaly mode** — cross-references quarantine data, schema drift log, lineage manifest, and run history to produce a reasoned diagnosis:
> *"Stage silver_orders failed. 847 of 850 rows quarantined. Quarantine reason: missing field `unit_price`. Schema drift log shows `unit_price` was present in yesterday's run. Lineage manifest: `unit_price` feeds `fact_orders.total_amount` → metrics: `revenue`, `gross_margin`. Likely cause: upstream CSV schema change. Recommended action: inspect `orders_2025-11-08.csv` headers. I have flagged this date for requeue once the schema issue is resolved."*

---

**No knowledge graph or vector store needed.** All context fits in a single LLM prompt: run summary JSON, quarantine sample (first 10 rows), schema drift events, lineage manifest for affected sources, and last 7 run summaries for trend context. Total context is typically under 8K tokens.

---

**Model stack:**

| Environment | Model | Deployment |
|---|---|---|
| Local (demo) | `granite3.2:8b` or `llama3.2:3b` via Ollama | **Containerised open-weight model** — separate Docker container, no API key, no data leaves local environment. Pipeline calls `http://ollama:11434` via Docker Compose network. |
| Cloud (production) | `ibm/granite-3-3-8b-instruct` via watsonx.ai | Hosted API — lean pipeline container, no model weights bundled. Gracefully skipped if `WATSONX_API_KEY` not present. |

The local model container is separate from the pipeline container — model weights (~5 GB) live in a named Docker volume, not baked into any image. The pipeline image stays ~500 MB.

```
docker-compose.yml
  pipeline   (~500 MB image)  ──HTTP──►  ollama  (sidecar, model in volume)
  minio      (local S3)
  postgres   (state)
  grafana    (dashboards)
```

---

### N-4 · Data Contract Testing on Gold Outputs

**Client problems:** P2, P4

Pydantic validates individual row *values* at the Silver boundary. Data contracts validate the *Gold output as a whole* — column presence, types, row counts, quarantine rates. They are complementary guards at different levels.

```yaml
# contracts/fact_orders.yaml
table: fact_orders
required_columns:
  order_id: str
  customer_id: str
  total_amount: float
  status: str
min_rows_per_partition: 1
max_quarantine_rate_pct: 5.0
```

If `fact_orders` is written with 0 rows, or a column is dropped by an upstream schema change, the contract fires before any dashboard consumer sees it.

**Implementation:** Lightweight custom YAML checker (no external dependency) or `soda-core` / `great-expectations` for production reporting.

---

### N-5 · YAML Metrics Definition (Lightweight Semantic Layer)

**Client problems:** P2, P4 — specifically *"reports don't match each other"*

This is fundamentally a *semantic* problem, not a data quality or pipeline problem. Two dashboards show different revenue figures because they define revenue differently — one includes pending orders, one doesn't. Pydantic enforces that `total_amount` is a valid float. It says nothing about which rows should be included when reporting revenue. A semantic layer answers: *"what does revenue actually mean?"*

A full framework (dbt metrics, LookML, Cube.js) is premature. A version-controlled `metrics/definitions.yaml` is sufficient:

```yaml
metrics:
  revenue:
    description: Total value of shipped and delivered orders
    source: fact_orders
    calculation: SUM(quantity * unit_price)
    filters:
      - status IN ('shipped', 'delivered')

  confirmed_revenue:
    description: Revenue from delivered orders only
    source: fact_orders
    calculation: SUM(quantity * unit_price)
    filters:
      - status = 'delivered'

  gross_margin:
    description: Revenue minus supplier cost
    source: fact_orders JOIN dim_product ON product_id
    calculation: SUM((unit_price - unit_cost) * quantity)
    filters:
      - status = 'delivered'
```

When the analytics team argues about why two reports disagree, this file is the referee. When a new analyst joins, this file is the glossary. Graduate to dbt metrics when multiple BI tools need programmatic access.

---

### N-6 · Column-Level Lineage Manifest

**Client problems:** P1, P4

Distinct from the semantic layer — N-5 answers *"what does this metric mean?"*, N-6 answers *"where did this column come from?"*:

| | Semantic layer (N-5) | Lineage manifest (N-6) |
|---|---|---|
| **Answers** | What does this metric *mean*? | Where did this column *come from*? |
| **Purpose** | Business definition consistency | Blast radius / impact analysis |
| **Audience** | Analysts, dashboards | Engineers, pipeline operators |
| **Triggered by** | Report disagreements | Schema change events |

```yaml
# lineage/orders.yaml
fields:
  unit_price:
    source: data/landing/orders/orders_<date>.csv
    bronze: data/bronze/orders/date=<date>/data.parquet
    silver: data/silver/orders/date=<date>/data.parquet
    gold:
      - fact_orders.unit_price
      - fact_orders.total_amount
      - fact_orders.gross_margin
    metrics:
      - revenue
      - confirmed_revenue
      - gross_margin
```

When `check_schema()` detects drift, it reads the manifest and logs the downstream blast radius. The AI agent (N-3) uses the manifest as context when diagnosing failures. Graduate to OpenLineage/Marquez at scale.

---

### Innovation Priority Matrix

| # | Idea | Client problems | Effort | Priority |
|---|---|---|---|---|
| N-1 | Streamlit pipeline health dashboard | P2, P4, P6 | Small | **Do first** |
| N-2 | DuckDB analytical layer | P1, P5 | Small | **Do second** |
| N-3 | AI agent (summarise / diagnose) | P2, P4, P6 | Medium | **Do third** |
| N-4 | Data contract testing | P2, P4 | Small–Medium | **Alongside N-3** |
| N-5 | YAML metrics definition | P2, P4 | Small | **Alongside N-3** |
| N-6 | Column lineage manifest | P1, P4 | Small | **Alongside N-3** |
| — | Grafana + Loki | P6 | Medium | When N-1 is outgrown |
| — | Apache Superset | P1 | Medium | When analytics team needs self-serve BI |
| — | Postgres/Snowflake loaders | P1 | Medium | After data model is fixed |

---

## 7. Production Migration Plan

> **What we are building:** a local stack that is topologically identical to the cloud-production stack,
> suitable for demo and development. The local stack uses open-source self-hosted equivalents of every
> cloud-managed service. Migrating to cloud is a provider swap, not a code rewrite.
>
> **Sequencing:** fix the existing codebase first (Phase 0), add novel features on the fixed codebase
> (Phase 1), then build the full local lakehouse stack (Phase 2). Cloud deployment is Phase 3.
> Each phase leaves the pipeline in a working, tested state.

---

### Architecture overview

```
Lakehouse Architecture

  Object storage  =  the data lake  (MinIO locally / S3 cloud)
                     stores raw Parquet files
                     cheap, durable, scalable

  Apache Iceberg  =  table layer on top of the data lake
                     ACID transactions, schema enforcement,
                     time travel, MERGE INTO, partition evolution
                     data never leaves object storage

  dbt-DuckDB      =  transformation engine (local)
  dbt-Athena      =  transformation engine (cloud)
                     Bronze -> Silver -> Gold SQL models
                     same dbt models, different execution engine
```

**Why Iceberg over Delta Lake:**
Iceberg is a true open Apache standard with no single vendor ownership. It has first-class support in AWS Glue + Athena (cloud stack), works natively with DuckDB, Spark, Trino, Flink, and Snowflake, and is consistent across both local and cloud stacks. Delta Lake is tightly coupled to Databricks; Iceberg is engine-agnostic.

---

### Stack comparison: local vs cloud

Every local service has a direct cloud equivalent. The code is identical — only the provider configuration changes.

| Layer | Local (demo) | Cloud (production) | What changes |
|---|---|---|---|
| Object storage | MinIO | AWS S3 | Endpoint URL in config |
| Table format | Apache Iceberg + PyIceberg | Iceberg + AWS Glue catalog | Catalog config |
| Transformations | dbt-duckdb | dbt-Athena | dbt profile target |
| Orchestration | Astro CLI (local Airflow) | Airflow on EKS / AWS MWAA | Deployment target |
| Containers | Docker Compose | EKS (Helm charts) | Compose vs K8s manifests |
| CI/CD | GitHub Actions | GitHub Actions | Deploy step target only |
| Pipeline dashboard | Streamlit | Streamlit / Grafana Cloud | Optional upgrade |
| Metrics + logs | Prometheus + Loki + Grafana | Grafana Cloud | Managed vs self-hosted |
| Infra monitoring | — | CloudWatch | AWS-native only |
| IaC | Terraform (MinIO provider) | Terraform (AWS provider) | Provider + backend |
| Secrets | Docker secrets / `.env` | AWS Secrets Manager | Secret source |
| AI agent | Ollama sidecar container | watsonx.ai hosted API | Model endpoint |
| Query layer | DuckDB (embedded) | AWS Athena + MotherDuck (optional) | Query client |
| Scale (if needed) | — | PySpark on AWS EMR | Phase 4 only |

---

### Docker Compose topology (local stack)

All local services are wired together in a single `docker-compose.yml`. The pipeline container image is identical to the production image — only environment variables differ.

```
docker-compose.yml services:

  pipeline       (~500 MB, ubi9-minimal)
    calls  -> minio      (object storage)
    calls  -> postgres   (state + Airflow metadata)
    calls  -> ollama     (AI agent inference)
    writes -> state/, logs/, quarantine/ (via MinIO)

  minio          (S3-compatible object storage)
    hosts  -> bronze/, silver/, gold/, quarantine/ Iceberg tables

  postgres       (state management + Airflow metadata DB)
    tables -> pipeline_runs, stage_runs, watermarks, schema_drift_events

  airflow        (via Astro CLI)
    DAGs   -> wraps run_one_date() pipeline logic
    DB     -> postgres (shared instance)

  ollama         (AI agent sidecar — no API key required)
    model  -> granite3.2:8b (IBM-aligned) or llama3.2:3b (lighter)
    weights-> named Docker volume (~5 GB, not baked into image)
    exposes-> http://ollama:11434

  grafana        (dashboards + alerting)
    sources-> prometheus (metrics), loki (logs)

  prometheus     (metrics scraping)
    scrapes-> pipeline OpenTelemetry metrics

  loki           (log aggregation)
    ingests-> logs/pipeline.jsonl structured logs
```

---

### Phase 0 — Correctness fixes (existing stack, no infrastructure change)

Fix all H, M, L, I, D items. Technology unchanged — pure Python, local Parquet files, flat JSON state.

**Prerequisite:** D-4 (order line granularity) must be confirmed with the source system team before finalising the `fact_orders` schema. This determines the primary key of the most important Gold table and cannot be retrofitted after Phase 2.

**Deliverables:**
- All 14 existing tests green throughout
- Watermark two-phase commit (H-1 / I-1)
- Dead SCD2 variable removed (H-3)
- SHA-256 hashing (H-4)
- Vectorised Silver validation (H-5 / I-6)
- Vectorised SCD2 merge (H-6)
- File locking on watermarks (H-7)
- Layer-scoped Pydantic models — `EXPECTED_COLUMNS` + `schema_check.py` removed (M-1)
- All remaining M, L fixes
- Per-run summary JSON written to `state/<date>/run_summary.json` (I-3)
- Quarantine threshold enforcement (I-2)
- Revenue anomaly detection (I-9)
- Data model updated: `customer_sk`, margin columns, `fact_order_status_history`, lineage columns (D-1 through D-5)

---

### Phase 1 — Novel additions (fixed codebase, no infrastructure change)

Build on the corrected pipeline. All additions read existing `state/`, `logs/`, and Gold Parquet outputs — no new infrastructure required.

**Deliverables:**
- `metrics/definitions.yaml` — canonical business metric definitions (N-5)
- `lineage/orders.yaml`, `lineage/customers.yaml`, `lineage/products.yaml` — column lineage manifests (N-6)
- `contracts/fact_orders.yaml`, `contracts/dim_customer.yaml`, `contracts/dim_product.yaml` — Gold output contracts (N-4)
- `dashboard.py` — Streamlit pipeline health dashboard, reads `state/` + `logs/` (N-1)
- `src/load/duckdb_loader.py` — DuckDB view registrations over Gold Parquet (N-2)

---

### Phase 2 — Local lakehouse stack (the demo build)

Replace the file-based pipeline with the full local lakehouse stack. This is the primary build target.

**2a — Containerise the pipeline**

Package the pipeline as a lean Docker image. This image is identical to what runs in production — only environment variables change between local and cloud.

```dockerfile
FROM registry.redhat.io/ubi9/python-311-minimal:latest
RUN useradd -m -u 1001 appuser
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=appuser:appuser . .
USER 1001
CMD ["python", "-m", "src.pipeline"]
```

**2b — MinIO (local S3)**

Deploy via Docker Compose. Configure pipeline to write Iceberg tables to `s3://novacart-local/bronze/`, `silver/`, `gold/`. Use `fsspec` + `s3fs` for path abstraction — switching to AWS S3 is a single config line change:

```yaml
# config/pipeline.yaml
storage:
  endpoint: http://minio:9000         # local — change to blank for AWS S3
  bucket: novacart-data
  access_key: ${MINIO_ACCESS_KEY}
  secret_key: ${MINIO_SECRET_KEY}
```

**2c — Apache Iceberg + PyIceberg**

Replace direct `df.to_parquet()` writes with PyIceberg table writes:
- Iceberg catalog: REST catalog locally (via PyIceberg) → AWS Glue catalog in cloud (same PyIceberg API, catalog config only)
- Resolves H-1 and H-7 at the storage layer — ACID transactions replace file locking and watermark race conditions
- Enables `MERGE INTO` for SCD2, replacing the O(n²) Python loop (H-6)
- Enables time travel for incident scoping and reprocessing (D-5)

**2d — dbt-DuckDB transformations**

Migrate Bronze → Silver → Gold transform logic to dbt SQL models. DuckDB is the local execution engine — embedded, no cluster, queries Iceberg tables directly. Same dbt models run against Athena in cloud (dbt profile swap only):

```
dbt/models/
  bronze/
    orders.sql          replaces ingest/orders.py transform logic
    customers.sql
    products.sql
  silver/
    orders.sql          replaces transform/silver.py
    customers.sql
    products.sql
  gold/
    dim_product.sql     SCD1 via dbt snapshot
    dim_customer.sql    SCD2 via dbt snapshot + MERGE INTO
    fact_orders.sql     idempotent partition replace
```

**2e — PostgreSQL state management**

Replaces `watermarks.json` + `run_history.jsonl`. Shared with Airflow metadata database (one Postgres instance, separate schemas). Eliminates H-7 completely.

```sql
CREATE TABLE pipeline_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_date        DATE NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('SUCCESS','FAIL','RUNNING')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    error           TEXT
);

CREATE TABLE stage_runs (
    run_id          UUID REFERENCES pipeline_runs,
    stage_name      TEXT NOT NULL,
    status          TEXT NOT NULL,
    rows_in         INT,
    rows_out        INT,
    rows_quarantined INT,
    duration_sec    FLOAT,
    PRIMARY KEY (run_id, stage_name)
);

CREATE TABLE watermarks (
    source          TEXT PRIMARY KEY,
    value           TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE schema_drift_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    detected_at     TIMESTAMPTZ DEFAULT NOW(),
    source          TEXT NOT NULL,
    drift_type      TEXT NOT NULL CHECK (drift_type IN ('additive','subtractive')),
    affected_columns TEXT[],
    run_id          UUID REFERENCES pipeline_runs
);
```

**2f — Astro CLI (local Airflow)**

`run_one_date()` becomes an Airflow DAG; each pipeline stage becomes a task. Astro CLI runs a full Airflow instance locally via Docker (scheduler, webserver, triggerer). DAGs developed locally deploy to cloud Airflow unchanged. Airflow metadata DB reuses the PostgreSQL instance from 2e.

**2g — Observability: Prometheus + Loki + Grafana**

- **OpenTelemetry:** instrument pipeline stages — stage duration, rows processed, quarantine rate, run status
- **Prometheus:** scrapes OTel metrics, stores time-series
- **Loki:** ingests `logs/pipeline.jsonl`, queryable via LogQL from Grafana
- **Grafana:** dashboards over Prometheus + Loki with alerting on quarantine threshold and revenue anomaly
- Same Grafana dashboard JSON works against Grafana Cloud in production — only the data source endpoint changes

**2h — Terraform (infrastructure as code)**

```
terraform/
  modules/
    storage/        MinIO bucket config (local) / S3 bucket (cloud)
    database/       Postgres schema (local) / RDS (cloud)
    compute/        Docker Compose (local) / EKS (cloud)
  environments/
    local/          uses MinIO + Docker providers
    cloud/          uses AWS provider — same modules, different vars
```

**2i — AI agent: Ollama sidecar**

`ollama` container added to Docker Compose with model weights in a named volume. Pipeline calls `http://ollama:11434/api/generate` — no API key, no data leaves the local environment. `src/utils/agent.py` reads `OLLAMA_HOST` env var; falls back gracefully if not set. In cloud, the same `agent.py` reads `WATSONX_API_KEY` instead.

**Phase 2 deliverables:**
- `docker-compose.yml` — full local stack (pipeline, MinIO, Postgres, Airflow, Ollama, Grafana, Prometheus, Loki)
- `Dockerfile` — production-equivalent pipeline image (~500 MB, non-root, ubi9-minimal)
- `dbt/` — Bronze/Silver/Gold models replacing pandas transform logic
- `terraform/environments/local/` — IaC for local stack provisioning
- `terraform/environments/cloud/` — IaC skeleton for cloud deployment
- Grafana dashboard JSON — pipeline health, quarantine rates, schema drift, revenue anomaly
- GitHub Actions workflows — lint/test on push, build/push image on merge to main

---

### Phase 3 — Cloud deployment

Swap local service providers for managed AWS equivalents. Pipeline code, dbt models, and Grafana dashboards are unchanged.

| Step | Action | What changes |
|---|---|---|
| 3a | Provision S3 bucket via Terraform | `storage.endpoint` in config |
| 3b | Register Iceberg tables in AWS Glue catalog | Catalog config in PyIceberg |
| 3c | Switch dbt profile target to `dbt-athena` | `profiles.yml` target |
| 3d | Deploy Airflow to EKS via Helm (or AWS MWAA) | Deployment target |
| 3e | Migrate Postgres to RDS | Connection string in config |
| 3f | Point Grafana dashboards at Grafana Cloud data sources | Data source endpoint config |
| 3g | Switch AI agent from Ollama to watsonx.ai | `OLLAMA_HOST` replaced by `WATSONX_API_KEY` |
| 3h | Move secrets from `.env` to AWS Secrets Manager | Secret source in config |
| 3i | Apply `terraform/environments/cloud/` | Full cloud infra provisioned |
| 3j | Configure CloudWatch for AWS infrastructure alerts | Complements Grafana for infra layer |

**Phase 3 deliverables:**
- `terraform/environments/cloud/` applied and verified
- dbt running against Athena with identical outputs to local DuckDB run
- Airflow DAGs live on EKS, triggered on schedule
- Grafana Cloud dashboards live with production data
- GitHub Actions deploy step targeting EKS on release tag

---

### Phase 4 — Scale (when DuckDB becomes the bottleneck)

**Trigger:** daily row volume consistently exceeds ~50–100M rows.

**PySpark on AWS EMR**
- Replaces dbt-Athena/DuckDB for transformation at scale
- Same Apache Iceberg tables — zero storage migration, same Parquet files underneath
- dbt models graduate to dbt-Spark or are rewritten as PySpark jobs
- Pydantic validation becomes a Spark UDF or is replaced by Iceberg schema enforcement at write time
- dbt metrics layer replaces `metrics/definitions.yaml`
- OpenLineage / Marquez replaces `lineage/*.yaml`

The Iceberg table format and Airflow orchestration from Phase 3 mean this is a processing layer swap, not a rebuild.

---

## 8. Prioritised Backlog

### Phase 0 — Correctness fixes (do now, existing stack)

| # | Item | Type | Effort | Impact |
|---|---|---|---|---|
| 1 | H-1 / I-1 · Two-phase commit — watermark after pipeline success | Bug fix | Small | Critical |
| 2 | I-2 · Quarantine threshold — fail on mass rejection | Feature | Small | Critical |
| 3 | H-3 · Dead `updated_rows` variable in SCD2 | Bug fix | Trivial | High |
| 4 | H-4 · MD5 → SHA-256 for SCD2 row hash | Fix | Trivial | High |
| 5 | I-4 · Referential integrity checks in `fact_orders` | Feature | Medium | High |
| 6 | I-3 · Per-run data quality summary | Feature | Small | High |
| 7 | D-2 / I-5 · SCD2 surrogate key `customer_sk` | Feature | Medium | High |
| 8 | M-1 · Layer-scoped Pydantic models — replace `EXPECTED_COLUMNS` + `schema_check.py` | Refactor | Medium | High |
| 9 | M-5 · Overly broad `except Exception` in Silver | Fix | Trivial | Medium |
| 10 | H-5 / I-6 · Vectorised Silver validation — replace `iterrows` | Optimisation | Medium | Medium |
| 11 | H-6 · SCD2 O(n²) loop — replace with merge/join | Optimisation | Medium | Medium |
| 12 | D-3 · Join cost to revenue — add margin columns to `fact_orders` | Feature | Small | Medium |
| 13 | D-1 · Order lifecycle — add `fact_order_status_history` | Feature | Medium | Medium |
| 14 | D-4 · Confirm order line granularity with source team | Investigation | Trivial | Critical |
| 15 | D-5 / I-3 · Pipeline lineage columns on Gold rows | Feature | Small | Medium |
| 16 | M-9 · Structured logging for schema drift warning | Fix | Trivial | Medium |
| 17 | M-3 · `tier` field — constrain with `Literal` | Fix | Trivial | Medium |
| 18 | M-4 · `updated_at` typed as `datetime` not `str` | Fix | Trivial | Medium |
| 19 | I-8 · Checkpoint / skip-on-success for re-runs | Feature | Medium | Medium |
| 20 | I-7 · Schema version registry + evolution log | Feature | Small | Medium |
| 21 | I-9 · Revenue anomaly detection | Feature | Small | Medium |
| 22 | M-2 · Email validation — `pydantic[email]` / `EmailStr` | Fix | Trivial | Low–Medium |
| 23 | M-7 · `datetime.utcnow()` deprecation | Fix | Trivial | Low |
| 24 | H-7 · File locking on `watermarks.json` | Fix | Small | Low |
| 25 | H-2 · Products Bronze partitioning strategy | Fix | Small | Low |
| 26 | M-8 · Logger handler accumulation in tests | Fix | Trivial | Low |
| 27 | M-6 · Double Parquet read in `build_dim_product` | Fix | Trivial | Low |
| 28 | L-2 · Typed config dataclasses for `silver_cfg` / `gold_cfg` | Refactor | Small | Low |
| 29 | L-4 · Lock file for reproducible dependency installs | DevOps | Trivial | Low |
| 30 | L-1 · Remove or fill empty `src/load/` module | Cleanup | Trivial | Low |
| 31 | L-3 · `== True` comparison in SCD2 filter | Style | Trivial | Low |
| 32 | L-5 · Module-level imports in test functions | Style | Trivial | Low |

### Phase 1 — Novel additions (no infrastructure change)

| # | Item | Effort | Priority |
|---|---|---|---|
| N-1 | Streamlit pipeline health dashboard | Small | Do first |
| N-2 | DuckDB analytical layer (`src/load/duckdb_loader.py`) | Small | Do second |
| N-5 | `metrics/definitions.yaml` — semantic layer | Small | Alongside N-2 |
| N-6 | `lineage/*.yaml` — column lineage manifest | Small | Alongside N-2 |
| N-4 | Data contract testing (`contracts/*.yaml`) | Small–Medium | Alongside N-2 |
| N-3 | AI agent (Ollama local / watsonx.ai cloud) | Medium | After I-3 lands |

### Phase 2–4 — Migration (see Section 7)

| Phase | Focus | Key technologies |
|---|---|---|
| 2 | Local lakehouse stack — the demo build | Docker Compose, MinIO, Apache Iceberg + PyIceberg, dbt-duckdb, PostgreSQL, Astro CLI (Airflow), Prometheus + Loki + Grafana, Ollama sidecar, Terraform |
| 3 | Cloud deployment — provider swap only | AWS S3, Glue catalog, dbt-Athena, Airflow on EKS / MWAA, RDS, Grafana Cloud, watsonx.ai, AWS Secrets Manager, Terraform (AWS provider) |
| 4 | Scale (when DuckDB bottlenecks) | PySpark on AWS EMR, dbt-Spark, OpenLineage / Marquez |
