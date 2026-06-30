# Traceset v2: E2E Langfuse Ingestion + Validation — Design Spec

**Date**: 2026-06-30
**Status**: Approved (brainstorming complete)
**Branch**: `main` (new feature branch TBD)

---

## 1. Overview

### 1.1 Purpose

Rework the existing `traceset/` package from a static wire-event catalog into a full end-to-end pipeline that:

1. **Generates** valid, sophisticated multi-span traces for various Dify app modes.
2. **Packs** traces into valid Langfuse `/api/public/ingestion` batch payloads.
3. **Ingests** traces into a real Langfuse cluster via raw HTTP POST.
4. **Validates** that traces are correctly stored and indexed in Langfuse with deep field-level assertions.

### 1.2 Relationship to Existing Code

**Replace** the existing 14 single-turn scenarios entirely. **Reuse** the helpers/schema infrastructure. **Add** new layers: ingestion, validation, pipeline orchestration.

| Component | Action |
|---|---|
| `helpers.py` (102 lines) | **Reuse** — UUID v5, timestamps, camelCase, body builders, `wrap_event` |
| `schema.py` (76 lines) | **Reuse** — two-tier validator (local + langfuse SDK) |
| `scenarios/` (14 modules) | **Replace** — 13 new multi-span scenarios (10+ spans each) |
| `generate_traceset.py` | **Update** — generate new scenario set |
| `ingest.py` | **New** — pack events into batch payload + raw HTTP POST |
| `validate.py` | **New** — query Langfuse API + deep field-level assertions |
| `pipeline.py` | **New** — orchestrate generate → pack → ingest → validate |
| `tests/` | **Update + New** — update scenario tests, add e2e tests |
| Docker setup | **Reuse existing** — `../difyapp3` Langfuse stack |

### 1.3 Non-Goals

- Mocking HTTP for ingestion/validation tests (e2e only, no mocks).
- Creating a new Docker setup (use existing `../difyapp3` stack).
- Capturing real traces from a live Dify instance (synthetic generation).
- Latency scenario replay (structural, not temporal).
- OTLP / OpenTelemetry-format traces (REST wire events only).
- Internal trace types (`PromptGenerationTraceInfo`, `DraftNodeExecutionTrace`).

---

## 2. Architecture

### 2.1 4-Stage Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    traceset/pipeline.py                         │
│                                                                 │
│  1. GENERATE          2. PACK           3. INGEST       4. VALIDATE │
│  ───────────          ───────           ────────         ────────── │
│  scenarios/           ingest.py         ingest.py        validate.py│
│  build_events()       pack_batch()      post_batch()     query_api()│
│  → list[dict]         → {batch:[...]}   → POST /api/...  → assert  │
│                                                                 │
│  helpers.py ◄── reused by all stages                            │
│  schema.py   ◄── reused by all stages                           │
└─────────────────────────────────────────────────────────────────┘

         ┌─────────────────────────────────────┐
         │    ../difyapp3 (existing Docker)     │
         │  ┌──────────┐  ┌────────────┐       │
         │  │ Langfuse │──│ PostgreSQL │       │
         │  │  :3000   │  │   :5432    │       │
         │  └────┬─────┘  └────────────┘       │
         │       │                              │
         │  ┌────┴─────┐  ┌────────────┐       │
         │  │ClickHouse│  │   Redis    │       │
         │  │   :8123  │  │   :6379    │       │
         │  └──────────┘  └────────────┘       │
         └─────────────────────────────────────┘
```

### 2.2 Dependency Posture

| Layer | Runtime deps | Notes |
|---|---|---|
| `helpers.py` | stdlib only (`uuid`, `datetime`) | UUID v5, ISO 8601, camelCase |
| `schema.py` (local tier) | stdlib only | Envelope + required-field + snake_case guard |
| `schema.py` (SDK tier) | `langfuse>=4.2.0,<5.0.0` (optional) | Pydantic `model_validate` |
| `generate_traceset.py` | stdlib only (`json`, `os`, `sys`) | Build + validate + write + assert |
| `ingest.py` | stdlib only (`urllib.request`, `json`, `base64`) | Raw HTTP POST, no `requests` |
| `validate.py` | stdlib only (`urllib.request`, `json`, `base64`) | Query Langfuse API |
| `pipeline.py` | stdlib only (`subprocess`, `os`, `time`) | Orchestration + Docker launch |
| `scenarios/sNN_*.py` | stdlib only (via `helpers`) | Pure data + construction |
| `tests/` | `pytest>=7.0` (dev) | E2e tests auto-start Langfuse |

**Runtime**: zero dependencies. **Dev-only**: `langfuse` (schema validation), `pytest` (tests). The entire pipeline runs on bare CPython.

### 2.3 Contract Enforced at Every Layer

Same 6 invariants as the existing traceset, plus 1 new:

1. **Wire envelope** `{id, timestamp, type, body}` — constructed by `helpers.wrap_event`, checked by `schema._local_validate`, re-checked by generator self-checks + tests.
2. **camelCase body keys** — produced by `helpers.to_camel_case`, rejected by `schema` (any `_` in body keys), re-checked by generator + tests.
3. **Event count** — `scenario.EXPECTED_EVENT_COUNT` is the single source of truth; asserted by generator, tests.
4. **Span count** (new) — `scenario.EXPECTED_SPAN_COUNT` (observation count = span-create + generation-create, excluding trace-create); asserted by generator, tests, and e2e validation.
5. **Monotonic timestamps** — produced by `helpers.make_timestamp(_BASE, increasing_offset)`, re-checked by generator + tests.
6. **meta/events dual invariant** — `build_meta().events_in_order` mirrors `build_events()`; asserted by generator + tests.
7. **Catalog coverage** — all 7 Dify trace types + all 5 app modes represented; enforced in `main()`.

---

## 3. Scenario Catalog

### 3.1 Design Principles

- **Replace** all 14 existing single-turn scenarios with 13 new multi-span scenarios.
- Each scenario is a single user request that traverses a sophisticated pipeline producing **10+ observation events** (span-create + generation-create).
- Varied span patterns: linear chains, parallel branches, ReAct loops, conditional branches, nested workflows, error-recovery, error-propagation, multi-model, feature-combinations, multi-hop retrieval, streaming, sequential chains.

### 3.2 Scenario Table

| # | Scenario | Pattern | App mode | Spans | Events | Description |
|---|---|---|---|---|---|---|
| 01 | `linear-llm-chain` | Linear chain | workflow | 13 | 14 | 10 sequential LLM nodes (Start → LLM×10 → End). Straight-line, no branching. |
| 02 | `parallel-branches` | Parallel fork-join | workflow | 11 | 12 | Forks into 3 parallel branches (2 LLM each), then joins. |
| 03 | `agent-react-loop` | ReAct loop | agent-chat | 11 | 12 | 5 ReAct iterations (think→act→observe). Each = 1 LLM + 1 tool. + final synthesis. |
| 04 | `multi-tool-chain` | Sequential tool chain | agent-chat | 10 | 12 | 8 sequential tool calls, then synthesis + GenerateName. |
| 05 | `rag-multi-hop` | Multi-hop retrieval | chat | 10 | 12 | 6 sequential knowledge retrievals, then synthesis + SuggestedQuestions + GenerateName. |
| 06 | `moderation-rag-tool-combo` | Feature combination | chat | 10 | 11 | Moderation + RAG + 4 tool calls + synthesis + SuggestedQuestions + GenerateName. |
| 07 | `workflow-conditional` | Conditional branching | workflow | 11 | 12 | 2 IF/ELSE nodes, each branching to 3 LLM nodes. |
| 08 | `error-recovery-agent` | Error → retry → success | agent-chat | 10 | 12 | 5 tool attempts (4 errors + 1 success) + RAG + synthesis + GenerateName. |
| 09 | `nested-workflow` | Nested sub-workflow | advanced-chat | 12 | 14 | Chatflow with sub-workflow node. Outer: Start → LLM → SubWorkflow → LLM → End. Inner: Start → LLM×2 → End. + Message + GenerateName. |
| 10 | `workflow-error-propagation` | Mid-pipeline failure | workflow | 10 | 11 | 7 LLM nodes, node 5 fails (level=ERROR), error handler, End. |
| 11 | `streaming-chatflow` | Streaming + multi-stage | advanced-chat | 11 | 13 | Chatflow with 5 LLM nodes + streaming Message generation + GenerateName. Streaming metadata (TTFT/TTG). |
| 12 | `multi-model-pipeline` | Multi-provider | workflow | 11 | 12 | 8 LLM nodes alternating across 3 models (gpt-4o, claude-3-5-sonnet, deepseek-chat). |
| 13 | `completion-multi-feature` | Feature combination | completion | 10 | 12 | Moderation + 5-hop RAG + 2 tool calls + synthesis + SuggestedQuestions + GenerateName. No `sessionId` (completion has no conversation). |

**Totals**: 13 scenarios, ~159 events, ~140 spans. Every scenario has 10+ spans.

### 3.3 Coverage

- **App modes**: workflow (5), agent-chat (3), chat (2), advanced-chat (2), completion (1). All 5 Dify app modes represented.
- **Trace types**: all 7 Dify trace types exercised (Message, Workflow, Moderation, DatasetRetrieval, Tool, GenerateName, SuggestedQuestion).
- **Patterns**: linear, parallel, loop, conditional, nested, error-recovery, error-propagation, multi-model, feature-combo, multi-hop, streaming, sequential-chain.
- **Models**: gpt-4o-mini, gpt-4o, claude-3-5-sonnet-20241022, deepseek-chat.
- **Error levels**: ERROR (scenarios 08, 10), WARNING (scenario 06 moderation).

### 3.4 Scenario Module Contract

Same as existing, extended with 2 new constants:

| Constant | Example | Purpose |
|---|---|---|
| `SCENARIO_ID` | `"01-linear-llm-chain"` | Directory name + catalog key |
| `SCENARIO_DESCRIPTION` | `"10 sequential LLM nodes in a straight line"` | Human description |
| `APP_TYPE` | `"workflow"` | High-level app category |
| `DIFY_APP_MODE` | `"workflow"` | One of Dify's 5 app modes |
| `EDGE_CASE` | `None` or `"error-recovery"` / `"streaming"` / ... | `None` for baseline; slug for edge cases |
| `TRACE_TYPES_EMITTED` | `["WorkflowTraceInfo"]` | Dify `*TraceInfo` types exercised |
| `EXPECTED_EVENT_COUNT` | `14` | Total wire events (trace-create + span-create + generation-create) |
| `EXPECTED_SPAN_COUNT` (new) | `13` | Observation count (span-create + generation-create, excluding trace-create) |
| `SPAN_PATTERN` (new) | `"linear"` | Pattern slug (linear, parallel-fork-join, react-loop, conditional-branch, nested, etc.) |

**Functions** (same as existing):
- `build_events() -> list[dict]` — ordered wire-event list via `helpers.make_*` + `wrap_event`.
- `build_meta() -> dict` — `meta.json` payload with `events_in_order` manifest + provenance pins + notes.

**Authoring conventions** (same as existing):
- Event IDs: `make_event_id("sNN-eNN")` — deterministic, greppable.
- Single `_BASE` timestamp per scenario; increasing offsets.
- Module docstring = event manifest.
- Edge-case scenarios carry `VERIFY:` notes in docstrings and `meta.json`.

---

## 4. Ingestion Layer (`ingest.py`)

### 4.1 Functions

```python
pack_batch(events: list[dict]) -> dict
    Input:  list of event dicts (each is {id, timestamp, type, body})
    Output: {"batch": [event1, event2, ...]} — the Langfuse ingestion POST body

post_batch(batch: dict, endpoint: str, public_key: str, secret_key: str) -> dict
    POSTs to {endpoint}/api/public/ingestion
    Auth: Authorization: Basic base64(public_key:secret_key)
    Returns: parsed JSON response ({"success": [...], "errors": [...]})

ingest_all(scenarios: list, endpoint: str, public_key: str, secret_key: str, 
           batch_mode: str = "per-scenario") -> dict
    Iterates all scenarios, packs + POSTs each, returns ingestion_report dict
```

### 4.2 Batch Strategy

Configurable via `batch_mode` parameter:
- `"per-scenario"` (default): all events for one scenario in one POST. Efficient.
- `"per-event"`: one event per POST. Mirrors Dify's actual behavior. Useful for latency testing.

### 4.3 HTTP Client

`urllib.request` (stdlib) — keeps the entire pipeline zero-runtime-dep. The ingestion is a simple JSON POST with headers. No `requests` dependency.

### 4.4 Error Handling

- **HTTP 207** (multi-status): Langfuse returns `{"success": [...], "errors": [...]}`. Log errors, continue.
- **HTTP 4xx**: auth failure or malformed payload — raise with response body.
- **HTTP 5xx**: Langfuse server error — retry up to 3 times with 1s backoff.
- **Network error**: retry up to 3 times with 1s backoff.

### 4.5 Output

`ingestion_report.json` with per-scenario ingestion status (HTTP status, success count, error count, latency). Consumed by the validation layer.

---

## 5. Docker Setup (Existing — `../difyapp3`)

### 5.1 No New Docker Setup

The project at `../difyapp3` already runs a full Dify + Langfuse stack. The `dify-deepdive` repo references it.

### 5.2 Existing Langfuse Services

From `../difyapp3/docker-compose.override.yml`:
- `langfuse-web` (image `langfuse/langfuse:3`) — port 3000
- `langfuse-worker` — background job processing
- `langfuse-db` (Postgres 15) — metadata
- `langfuse-redis` (Redis 6) — job queue
- `langfuse-clickhouse` — trace data
- `langfuse-minio` — S3-compatible event storage

### 5.3 Existing Credentials

From `../difyapp3/.env`:
```
LANGFUSE_HOST=http://localhost:3000
LANGFUSE_PUBLIC_KEY=pk-lf-8722237f-6999-4675-9753-6be8f404144e
LANGFUSE_SECRET_KEY=sk-lf-2c9165db-aab3-424c-8d24-8d69677a6d0d
```

### 5.4 Pipeline Connection

1. **Config loading**: `pipeline.py` reads `../difyapp3/.env` to extract `LANGFUSE_HOST`, `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`. No hardcoded credentials.
2. **Health check**: Poll `GET {LANGFUSE_HOST}/api/public/health` until 200 OK (60s timeout).
3. **Auto-start**: If health check fails, `pipeline.py` runs `docker compose -f ../difyapp3/docker-compose.yaml -f ../difyapp3/docker-compose.override.yml up -d` and re-checks health (120s timeout). If Docker is not installed, fail with a clear message.
4. **No new docker-compose.yml** in the `dify-deepdive` repo.

### 5.5 Makefile

```makefile
e2e:      python3 -m traceset.pipeline
generate: python3 -m traceset.generate_traceset
ingest:   python3 -m traceset.ingest
validate: python3 -m traceset.validate
health:   curl -s http://localhost:3000/api/public/health
```

---

## 6. Validation Layer (`validate.py`)

### 6.1 Langfuse API Endpoints

```
GET /api/public/traces?limit=100         — list traces
GET /api/public/traces/{traceId}         — get specific trace
GET /api/public/observations?traceId=X   — list observations under a trace
GET /api/public/observations/{obsId}     — get specific observation
```

Auth: `Authorization: Basic base64(public_key:secret_key)`.

### 6.2 Wait for Indexing

After ingestion, traces aren't immediately queryable — Langfuse processes them through its ingestion pipeline (queue → ClickHouse write). The validator:
1. Polls `GET /api/public/traces/{traceId}` until the trace appears (up to 30s).
2. Polls `GET /api/public/observations?traceId=X` until the observation count matches `EXPECTED_SPAN_COUNT` (up to 30s).

### 6.3 Validation Assertions (~44 per scenario, ~570 total)

**Trace-level (5 assertions):**
1. Trace exists with expected `id`.
2. `name` matches.
3. `userId` matches.
4. `input` matches (deep dict equality).
5. `metadata` matches (deep dict equality).

**Per-observation (3 assertions × ~12 observations = 36):**
6. Observation exists with expected `id` and correct `type` (SPAN/GENERATION).
7. `input` and `output` match (deep equality).
8. For generations: `model` + `usageDetails` (tokens + costs) match. For spans: `parentObservationId` matches expected parent.

**Cross-cutting (3 assertions):**
9. Observation count == `EXPECTED_SPAN_COUNT`.
10. Parent-child chain is consistent (every `parentObservationId` points to an existing observation, no orphans).
11. Timestamps are monotonic within the trace.

### 6.4 Output

`validation_report.json` with per-scenario, per-assertion results (pass/fail + diff on failure). Printed summary table:

```
Scenario                    Assertions   Pass   Fail
─────────────────────────── ─────────── ────── ──────
01-linear-llm-chain             44        44      0
02-parallel-branches            41        41      0
...
─────────────────────────── ─────────── ────── ──────
Total                          570       570      0  ✓ ALL PASS
```

### 6.5 Failure Handling

All assertions run (a failed assertion doesn't stop the rest). The report shows the full diff for each failure. The pipeline exits with code 1 if any assertion fails.

---

## 7. Pipeline Orchestration (`pipeline.py`)

### 7.1 Entry Points

```bash
python3 -m traceset.pipeline                    # full e2e: generate → pack → ingest → validate
python3 -m traceset.pipeline --stage generate   # just generate
python3 -m traceset.pipeline --stage ingest     # just ingest
python3 -m traceset.pipeline --stage validate   # just validate
python3 -m traceset.pipeline --clean            # delete all traces, then e2e
```

### 7.2 Pipeline Flow

```
1. Load config
   └── read ../difyapp3/.env → extract LANGFUSE_HOST, LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY

2. Health check + auto-start
   └── poll GET {LANGFUSE_HOST}/api/public/health until 200 OK (60s)
   └── if not healthy: docker compose up -d in ../difyapp3, re-check (120s)
   └── if Docker not installed: fail with clear message

3. Generate (if --stage generate or e2e)
   └── for each scenario: build_events() + validate via schema + write events.jsonl
   └── print "Generated N scenarios, M events"

4. Ingest (if --stage ingest or e2e)
   └── for each scenario: pack_batch(events) → post_batch → record result
   └── write ingestion_report.json
   └── print "Ingested N scenarios: X successes, Y errors"

5. Wait for indexing (if e2e or --stage validate)
   └── for each scenario: poll traces/{traceId} until trace exists (30s)
   └── poll observations until count == EXPECTED_SPAN_COUNT (30s)

6. Validate (if --stage validate or e2e)
   └── for each scenario: query trace + observations → run ~44 assertions
   └── write validation_report.json
   └── print summary table

7. Exit
   └── exit 0 if all assertions pass, exit 1 if any fail
```

### 7.3 Error Handling

- **Per-scenario isolation**: One scenario failing ingestion or validation doesn't stop the rest.
- **Stage-level hard stop**: If Langfuse is unreachable (health check + auto-start both fail), stop immediately. If all scenarios fail ingestion (e.g., auth error), stop before validation.
- **`--clean` flag**: Calls `DELETE /api/public/traces/{traceId}` for each trace before ingesting. For reproducible runs.

### 7.4 Idempotency

Langfuse trace-create is idempotent by trace ID (upsert semantics). Re-running without `--clean` upserts existing traces and re-validates them. Safe to re-run.

### 7.5 Reporting

`pipeline_report.json` combines `ingestion_report.json` + `validation_report.json` with timestamps, config, and per-scenario results.

---

## 8. Testing Strategy

### 8.1 E2E Only, No Mocks

No mocked HTTP for ingestion/validation. E2e tests run against the real Langfuse stack at `../difyapp3`. If Langfuse isn't running, the test fixture **auto-starts it via Docker**. Tests are NOT complete until real Langfuse validation passes.

### 8.2 Test Files

| File | Type | Coverage | Tests |
|---|---|---|---|
| `test_helpers.py` | Unit (unchanged) | All 7 helpers: determinism, camelCase, body shapes | 12 |
| `test_schema.py` | Unit (unchanged) | Validator happy/rejection paths | 8 |
| `test_scenarios.py` | Unit (updated) | Per-scenario contract for 13 new scenarios (incl. `EXPECTED_SPAN_COUNT`) | 13 |
| `test_generate_traceset.py` | Unit (updated) | Pipeline & artifact tests for new scenarios | 9 |
| `test_e2e.py` | **E2E (new, mandatory)** | Auto-starts Langfuse. Full pipeline + per-scenario ingestion+validation against real Langfuse. | 14 |

**Total**: ~55 tests (41 unit + 14 e2e).

### 8.3 E2E Test Fixture

```python
@pytest.fixture(scope="session", autouse=True)
def ensure_langfuse_running():
    # 1. Check GET /api/public/health
    # 2. If not healthy:
    #    - Run: docker compose -f ../difyapp3/docker-compose.yaml \
    #           -f ../difyapp3/docker-compose.override.yml up -d
    #    - Poll health endpoint for 120s
    # 3. If still not healthy: FAIL (not skip)
    # 4. If Docker not installed: FAIL with clear message
```

### 8.4 E2E Test Functions

```python
def test_e2e_full_pipeline():
    """Run the complete pipeline: generate → ingest → validate."""
    # 1. Run generate_traceset.main()
    # 2. Run ingest.ingest_all() against real Langfuse
    # 3. Wait for indexing
    # 4. Run validate.validate_all() → assert 0 failures

@pytest.mark.parametrize("scenario", SCENARIOS)
def test_e2e_scenario(scenario, ensure_langfuse_running):
    """Per-scenario e2e: ingest one scenario's events → validate."""
    # 1. build_events() for this scenario
    # 2. pack_batch + post to real Langfuse
    # 3. Wait for trace + observations to appear
    # 4. Run ~44 assertions → all must pass
```

### 8.5 Test Commands

- `python3 -m pytest traceset/ -v -m "not e2e"` — unit tests only (41 tests, no Langfuse)
- `python3 -m pytest traceset/ -v` — **all tests including e2e** (55 tests). Auto-starts Langfuse if needed. NOT complete until e2e passes.

### 8.6 Self-Checks (6 per scenario, same as existing + 1 new)

Run by `generate_scenario()` before writing any file:

1. Event count matches `EXPECTED_EVENT_COUNT`.
2. All event `type` values are valid.
3. No body key contains `_` (camelCase enforced).
4. Timestamps are monotonically non-decreasing.
5. `meta.events_in_order` length matches event count.
6. `meta.events_in_order` per-index type matches actual event type.
7. **(New)** Span count matches `EXPECTED_SPAN_COUNT`.

### 8.7 Catalog-Wide Coverage Assertions (2, same as existing)

Run by `main()`:
1. All 7 Dify trace types represented.
2. All 5 Dify app modes represented.

---

## 9. File Structure

```
traceset/
├── helpers.py              # REUSED — event construction (102 lines)
├── schema.py               # REUSED — wire schema validator (76 lines)
├── generate_traceset.py    # UPDATED — generates 13 new scenarios
├── ingest.py               # NEW — pack_batch + post_batch + ingest_all
├── validate.py             # NEW — query Langfuse API + ~44 assertions/scenario
├── pipeline.py             # NEW — orchestrate generate → ingest → validate
├── conftest.py             # UPDATED — sys.path + e2e fixture
├── pyproject.toml          # UPDATED — add pytest markers (e2e)
├── scenarios/
│   ├── __init__.py         # UPDATED — registry of 13 new scenarios
│   ├── s01_linear_llm_chain.py
│   ├── s02_parallel_branches.py
│   ├── s03_agent_react_loop.py
│   ├── s04_multi_tool_chain.py
│   ├── s05_rag_multi_hop.py
│   ├── s06_moderation_rag_tool_combo.py
│   ├── s07_workflow_conditional.py
│   ├── s08_error_recovery_agent.py
│   ├── s09_nested_workflow.py
│   ├── s10_workflow_error_propagation.py
│   ├── s11_streaming_chatflow.py
│   ├── s12_multi_model_pipeline.py
│   └── s13_completion_multi_feature.py
├── tests/
│   ├── __init__.py
│   ├── test_helpers.py             # REUSED (12 tests)
│   ├── test_schema.py              # REUSED (8 tests)
│   ├── test_scenarios.py           # UPDATED (12 tests)
│   ├── test_generate_traceset.py   # UPDATED (9 tests)
│   └── test_e2e.py                 # NEW (14 e2e tests)
├── catalog.json            # GENERATED — 13 entries
├── README.md               # GENERATED — catalog overview
├── schema.md               # GENERATED — wire event field reference
├── ingestion_report.json   # GENERATED — per-scenario ingestion status
├── validation_report.json  # GENERATED — per-scenario validation results
├── pipeline_report.json    # GENERATED — combined e2e report
├── 01-linear-llm-chain/
│   ├── events.jsonl        # GENERATED — wire events
│   └── meta.json           # GENERATED — scenario metadata
├── 02-parallel-branches/
│   ├── events.jsonl
│   └── meta.json
├── ... (13 scenario dirs total)
Makefile                    # NEW — convenience shortcuts
```

---

## 10. References

- Existing traceset design: [`docs/04-design-doc.md`](../../04-design-doc.md)
- Original catalog spec: [`docs/superpowers/specs/2026-06-26-dify-trace-catalog-design.md`](./2026-06-26-dify-trace-catalog-design.md)
- Implementation plan (v1): [`docs/superpowers/plans/2026-06-26-dify-trace-catalog.md`](./../plans/2026-06-26-dify-trace-catalog.md)
- Langfuse ingestion API: `POST /api/public/ingestion` with `{"batch": [events]}`
- Langfuse trace API: `GET /api/public/traces/{traceId}`
- Langfuse observation API: `GET /api/public/observations?traceId=X`
- Existing Docker setup: `../difyapp3/docker-compose.override.yml`
- Existing credentials: `../difyapp3/.env`
