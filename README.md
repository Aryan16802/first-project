# Mutual Fund RAG Chatbot (Groww + Groq)

Production-ready, phase-wise architecture for a Retrieval-Augmented Generation (RAG) chatbot that answers factual mutual-fund scheme queries using data from [Groww](https://groww.in).

## Core Principles

- Groq is the LLM provider.
- Answers are retrieval-grounded only.
- No standalone model guessing.
- Personal-information and out-of-scope queries are refused.
- Prioritize freshness, structured retrieval, low latency, and measurable accuracy.

## Phase 0: Scope, Compliance, and Canonical Data Contract

### Scope

The chatbot answers factual scheme-level mutual-fund queries such as:

- Expense Ratio
- Exit Load
- Minimum SIP
- Lock-in Period (ELSS)
- Riskometer
- Benchmark
- NAV
- Portfolio Holdings
- Fund Size (AUM)
- Fund Manager(s)

Out of scope:

- Personal information queries
- Personal finance profiling
- Generic world knowledge unrelated to indexed mutual-fund data

### Compliance and Source Policy

- Use approved Groww data endpoints/pages with policy-compliant access.
- Track source URL and capture timestamp for each record.
- Maintain immutable versioned snapshots for auditability.

### Canonical Schema (Single Source of Truth)

```text
scheme_id, scheme_name, amc_name, category, subcategory,
expense_ratio, exit_load, min_sip, lock_in_period_days,
riskometer, benchmark, nav_value, nav_date,
aum_value, aum_date, fund_managers[],
portfolio_holdings[] (security_name, sector, weight, as_of_date),
source_url, source_timestamp, ingestion_run_id, version
```

## Phase 1: Data Ingestion from Groww

### Objectives

- Fetch latest mutual-fund records and related attributes from Groww.
- Persist raw snapshots for traceability.

### Components

- Scheme master fetcher (list of schemes and IDs)
- Scheme detail fetcher (expense ratio, exit load, benchmark, managers, riskometer)
- NAV/AUM updater
- Holdings fetcher
- Raw landing store (JSON/HTML snapshots)
- Ingestion metadata tracker (run ID, start/end, status, checksum, retries)

### Output

- Versioned raw dataset per run
- Complete ingestion logs

## Phase 2: Scheduler and Orchestration (Freshness Engine)

### Objectives

- Keep data continuously up to date.
- Trigger downstream phases automatically on fresh data.

### Cadence (Example)

- NAV: every 15-60 minutes (market-aware window)
- Scheme facts (expense ratio, exit load, benchmark, managers): daily
- AUM and holdings: daily or on source change

### Workflow DAG

`Ingest -> Validate -> Normalize -> Upsert Structured Store -> Rebuild/Update Embeddings -> Cache Warmup -> Smoke Evaluation`

### Trigger Model

- Scheduler triggers ingestion run.
- On successful completion, publish `dataset_updated` event.
- Event triggers Phase 3 onward.
- Failed run keeps last good snapshot active with freshness warning.

## Phase 3: Preprocessing, Normalization, and Data Quality

### Objectives

- Transform raw source data into clean, query-safe records.
- Enforce schema consistency and attribute completeness.

### Processing Steps

- Normalize units and formatting:
  - Percentages (`expense_ratio`, `exit_load`)
  - Currency (`min_sip`, `aum`)
  - Dates (`nav_date`, `aum_date`, holdings date)
- Canonicalize riskometer labels.
- ELSS logic: enforce lock-in period for ELSS schemes.
- Validate critical fields and value ranges.
- Resolve conflicts using timestamp recency and source-priority rules.
- Add data-quality flags (`missing_field`, `stale_field`, `anomaly_flag`).

### Output

- Curated, validated, structured mutual-fund dataset

## Phase 4: Storage Layer (Hybrid: Structured + Vector)

### 4A) Structured Store (Truth Layer)

Use PostgreSQL (or equivalent) for deterministic factual retrieval.

Suggested tables:

- `schemes`
- `scheme_metrics_history` (NAV, AUM, as-of dates)
- `scheme_holdings`
- `scheme_managers`
- `ingestion_versions`
- `quality_flags`

Why this matters:

- Exact numerical fields are fetched directly (high precision, low hallucination risk).

### 4B) Vector Store (Semantic Context Layer)

- Store embeddings for scheme summaries, explanatory text, policy notes, and derived docs.
- Attach metadata: `scheme_id`, `category`, `as_of_date`, `version`, `source_url`.

### 4C) Cache Layer

- Redis for hot queries and popular comparisons.
- Cache key includes data version to avoid stale answers.

## Phase 5: Retrieval Pipeline (Strict Grounding)

### Objective

Return only verified context needed to answer user queries.

### Pipeline

1. Query understanding (intent + entity extraction)
2. Scope filter:
   - Reject non-mutual-fund and personal-information requests
3. Structured retrieval first:
   - Pull factual fields from SQL truth layer
4. Embedding retrieval second:
   - Fetch semantic support/context from vector store
5. Rerank + confidence gating
6. Build final grounded context packet with citations and timestamps

### Hard Constraints

- If relevant context is below threshold, do not answer from model priors.
- Return safe fallback:
  - "I do not have enough verified data in the indexed knowledge base to answer this."
- No retrieval -> no factual answer.

## Phase 6: Response Generation (Groq LLM)

### Objective

Generate concise, factual answers grounded only in retrieved context.

### Groq Integration

- Use Groq-hosted LLM for generation.
- Externalize model settings:
  - `GROQ_API_KEY`
  - `GROQ_MODEL`
  - `temperature` (low for factual mode)
  - `max_tokens`

### Prompt and Policy Controls

System instruction must enforce:

- Use only retrieved context.
- Never infer or fabricate values.
- Always include source attribution and as-of date when returning facts.
- Refuse personal-information and out-of-scope requests.

### Post-Generation Answer Verifier

- Validate every numeric/factual claim against retrieved context IDs.
- If any claim is ungrounded, block and regenerate or return fallback.

## Phase 7: Frontend + Backend Chat Application

### Frontend

- Chat UI with streaming responses
- Scheme autocomplete and compare mode
- Data freshness badge (`as_of` timestamp)
- Citations panel (source URL + retrieval timestamp)
- Clear refusal messaging for out-of-scope/PII requests

### Backend Services

API gateway:

- `POST /chat`
- `GET /scheme/{id}`
- `POST /compare`

Orchestrator and safety:

- Orchestrator service for intent, guardrails, retrieval, generation
- Safety middleware:
  - PII detector
  - Domain scope classifier
  - policy-enforced refusal templates

Observability:

- request trace IDs
- retrieval latency
- groundedness check status
- refusal reason analytics

## Phase 8: Security, Guardrails, and Policy Enforcement

### PII and Out-of-Scope Handling

Block and refuse requests for:

- Personal contacts, IDs, account details, addresses, or individual-specific data
- Questions unrelated to mutual-fund scheme facts in indexed corpus

Refusal template:

- "I cannot help with personal information or out-of-scope queries. I can answer mutual-fund scheme questions from the approved indexed data."

### Data Protection

- Store minimal conversation telemetry.
- Avoid logging sensitive user payloads in full.
- Mask high-risk patterns in logs.

## Phase 9: Evaluation and Monitoring

### Freshness and Reliability

- Ingestion success rate
- Data lag by attribute (NAV lag, AUM lag, holdings lag)
- Pipeline recovery time after failures

### Retrieval Quality

- Recall@k and MRR
- Structured hit rate for mandatory fields
- Low-confidence retrieval rate

### Response Quality

- Field-level exact match for key attributes
- Grounded answer rate (all claims traceable to retrieved context)
- Ungrounded response rate (target near zero)
- Citation correctness
- Completeness score (requested fields returned)

### Safety Metrics

- PII refusal precision/recall
- Out-of-scope rejection accuracy
- False refusal rate for valid mutual-fund queries

## End-to-End Request Flow

1. User asks a scheme question.
2. Scope/PII filter checks policy compliance.
3. Retriever fetches facts from structured DB + support from embeddings.
4. Confidence gate checks retrieval sufficiency.
5. Groq LLM generates answer from provided context only.
6. Verifier confirms all claims are grounded.
7. Response is returned with citations and as-of date.

## Non-Negotiable Guardrails (Implementation Checklist)

- Groq is the configured LLM provider.
- Chatbot is retrieval-grounded only; no standalone model answering.
- Personal-information requests are always refused.
- Out-of-scope queries are refused.
- Missing/low-confidence data returns a safe fallback.
- All factual responses include source and freshness metadata.

## Quick Run (Current App)

### Setup

```powershell
cd C:\first-genAI-project\app
& "C:\Users\Lenovo\AppData\Local\Programs\Python\Python312\python.exe" -m venv .venv
.\.venv\Scripts\python.exe -m pip install -U pip
.\.venv\Scripts\python.exe -m pip install -e .
```

### Configure Groq

Create/update:

- `C:\first-genAI-project\.env\groq.env`

With:

- `GROQ_API_KEY=...`
- `GROQ_MODEL=...`

### Start

```powershell
.\.venv\Scripts\python.exe -m uvicorn backend.main:app --reload --port 8000
```

Open:

- `http://127.0.0.1:8000`

## Run on Streamlit

Use Streamlit directly (no separate frontend server needed):

```powershell
cd C:\First Project
python -m pip install -e .
streamlit run streamlit_app.py
```

Open:

- `http://localhost:8501`

Notes:

- Uses the same RAG pipeline and guardrails.
- Loads `GROQ_API_KEY` and `GROQ_MODEL` from environment (or local `.env` file).
- Keeps the same dark chat layout style with selected-funds panel and source/freshness metadata.

### Streamlit Cloud Deployment Note

If Streamlit Cloud attempts Poetry install and reports:
"No file/folder found for package mf-rag-chatbot",
this repo is configured to avoid root-package installation in Poetry (`package-mode = false`)
and also includes a `requirements.txt` fallback for dependency installation.

## Live Groww Scraping Notes (Important)

Groww may block automated/headless scraping (returns a 404 page to programmatic requests).  
If you see schemes ingested with missing NAV/expense/AUM, run ingestion using your **real Chrome session** via CDP.

### Run Chrome with CDP enabled (Windows)

Close all Chrome windows, then run:

```powershell
& "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="$env:TEMP\\chrome-cdp-mf-rag"
```

Then set:

```powershell
$env:MF_CDP_ENDPOINT="http://127.0.0.1:9222"
$env:MF_USE_LIVE_GROWW="true"
python -c "from pathlib import Path; from mf_rag.phase1_runner import run_phase1; print(run_phase1(Path('data'), use_live=True))"
```

This lets Playwright reuse your real browser session (cookies/normal rendering) for best chance of full scheme facts.

## Suggested Next Build Steps

1. Initialize repository structure (`backend`, `ingestion`, `retrieval`, `ui`, `infra`).
2. Implement Phase 1 ingestion with raw snapshot versioning.
3. Add Phase 3 normalization + quality flags.
4. Stand up PostgreSQL schema and seed first canonical dataset.
5. Add minimal `/chat` pipeline with strict retrieval gating and Groq generation.
6. Add grounding verifier and refusal policy middleware before enabling UI polish.
