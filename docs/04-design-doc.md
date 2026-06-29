# Dify App Trace Reference Catalog — Design Document

**Project**: `dify-deepdive` · **Package**: `traceset/` (`dify-trace-catalog`)
**Branch**: `main` (merged from `feat/dify-trace-catalog`, 17 commits)
**Dify source pin**: `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`
**Langfuse SDK pin**: `>=4.2.0,<5.0.0` (v4 ingestion wire format)
**Date**: 2026-06-29

---

## 1. Executive Summary

The `dify-deepdive` project investigates why Dify's native Langfuse integration is structurally ~10× slower than achievable (~11s floor vs. ~1.5s), then pivots from diagnosis to a concrete deliverable: a **synthetic-but-realistic reference catalog of Dify app traces** captured as Langfuse wire events.

The catalog lives in `traceset/` and contains:

- **14 scenarios** spanning all 5 Dify app modes (`chat`, `completion`, `agent-chat`, `workflow`, `advanced-chat`) and all 7 Dify trace types (`Message`, `Workflow`, `Moderation`, `DatasetRetrieval`, `Tool`, `GenerateName`, `SuggestedQuestion`).
- **88 Langfuse wire events** (`trace-create` / `span-create` / `generation-create`) — the exact JSON bodies Dify POSTs one-at-a-time to `/api/public/ingestion`.
- **6 edge cases**: high-N workflow (17 events), moderation-blocked, empty-RAG, tool-failure, suggested-questions-error, streaming.
- **43 tests** passing, **6 self-checks per scenario**, **2 catalog-wide coverage assertions**.
- A deterministic generation script (`generate_traceset.py`) that rebuilds the entire catalog byte-identically from source.

The package runs on **zero runtime dependencies** (bare CPython); the `langfuse` SDK is a dev-only extra for authoritative schema validation.

---

## 2. Background & Motivation

### 2.1 The Latency Problem

Research documented in [`docs/01-research-report.md`](./01-research-report.md), [`docs/02-dify-trace-flow.md`](./02-dify-trace-flow.md), and [`docs/03-langfuse-staging-tables.md`](./03-langfuse-staging-tables.md) established that Dify's native Langfuse integration cannot be made low-latency by configuration alone, for three stacking structural reasons:

1. **REST ingestion (5s server queue)** — Dify uses Langfuse's REST ingestion endpoint, which carries a hardcoded 5-second server-side queue delay (`getDelay()` in `processEventBatch.ts:66-91`). The OTLP path gets 0s. Dify cannot use OTLP because it doesn't use the SDK's OpenTelemetry span path.

2. **Dify's 3-stage decoupling (Timer → Celery → sync HTTP)** — Dify decouples trace emission through: (a) an in-process `threading.Timer` with a 5-second default (`TRACE_QUEUE_MANAGER_INTERVAL`); (b) a Celery task queue (`ops_trace` queue, 5s retry backoff, 60 max retries); (c) synchronous per-event HTTP POSTs inside the Celery worker. Each `add_trace`/`add_span`/`add_generation` call is **one HTTP POST** (`api.ingestion.batch(batch=[event])`).

3. **Missing v4 direct-write signal (up to 10 min UI latency)** — Dify never sends `x-langfuse-ingestion-version: 4` and cannot via REST. Without the v4 signal, Langfuse routes traces through the `observations_batch_staging` ClickHouse table + a periodic `handleEventPropagationJob` (BullMQ cron) before they reach the final `events` table — adding up to 10 minutes of UI latency on top of transfer latency.

**Critical failure mode**: Langfuse HTTP failures raise `ValueError` (not `RetryableTraceDispatchError`) in Dify's `LangFuseDataTrace` provider, so they are **never retried** — silent permanent data loss, with only a Redis counter incrementing.

**Debunked knobs**: `LANGFUSE_FLUSH_AT` / `LANGFUSE_FLUSH_INTERVAL`, `OTEL_BSP_*`, and `langfuse.flush()` are all no-ops on Dify's code path — Dify never touches the SDK's `BatchSpanProcessor`. The widely-held belief that tuning these helps Dify is a misconception.

### 2.2 Why a Reference Catalog?

The latency research required a concrete, inspectable artifact representing **what Dify actually emits** — not what the Langfuse UI shows (post-staging), and not what Dify's internal `TraceInfo` Pydantic models hold (pre-serialization). The catalog captures the **wire events**: the JSON bodies that hit `/api/public/ingestion` before any server-side processing.

This intermediate format is the load-bearing layer for any latency analysis, replay tooling, or alternative ingestion path — yet it is invisible in both Dify's source (which only constructs it) and Langfuse's UI (which only displays post-storage). The catalog makes it first-class.

---

## 3. Goals & Non-Goals

### Goals

1. **Cover all 7 Dify trace types** (`MessageTraceInfo`, `WorkflowTraceInfo`, `ModerationTraceInfo`, `DatasetRetrievalTraceInfo`, `ToolTraceInfo`, `GenerateNameTraceInfo`, `SuggestedQuestionTraceInfo`).
2. **Cover all 5 Dify app modes** (`chat`, `completion`, `agent-chat`, `workflow`, `advanced-chat`).
3. **Exercise 6 edge-case variants**: high-N workflow, moderation-blocked, empty-RAG, tool-failure, suggested-questions-error, streaming.
4. **Every field Dify populates** represented with realistic values (real model IDs, realistic token counts/costs, valid UUIDs, ISO 8601 timestamps, realistic text content).
5. **Reproducible generation** — deterministic UUIDs (v5) and fixed base timestamps so re-running the generator produces byte-identical output.
6. **Schema-validated** — every event validated against the real langfuse SDK Pydantic models (with a local fallback validator).

### Non-Goals

- Capturing real traces from a live Dify instance (the catalog is synthetic).
- Dify-internal `TraceInfo` JSON format (pre-serialization).
- Langfuse post-ingestion storage format (post-staging ClickHouse rows).
- Latency scenario replay (the catalog is structural, not temporal).
- Internal trace types (`PromptGenerationTraceInfo`, `DraftNodeExecutionTrace`) — these are not user-facing.
- OTLP / OpenTelemetry-format traces — the catalog is REST wire events only.

---

## 4. Architecture Overview

### 4.1 Layering

```
generate_traceset.py        orchestrator (build → validate → self-check → write → coverage-assert)
        │
        ├── scenarios/__init__.py    explicit ordered registry (SCENARIOS list)
        │       │
        │       └── scenarios/sNN_*.py    14 flat modules: constants + build_events/build_meta
        │               │
        │               └── helpers.py    make_event_id/timestamp/*_create/wrap_event (pure)
        │
        └── schema.py    validate_event (local + optional langfuse SDK)
```

**Top to bottom**: the orchestrator imports the registry, which imports the 14 scenario modules, which import the helpers. The schema validator is called by the orchestrator on every event. Tests sit alongside and exercise every layer independently.

### 4.2 Dependency Posture

| Layer | Runtime deps | Notes |
|---|---|---|
| `helpers.py` | stdlib only (`uuid`, `datetime`) | UUID v5, ISO 8601 arithmetic, camelCase conversion |
| `schema.py` (local tier) | stdlib only | Envelope + required-field + snake_case guard |
| `schema.py` (SDK tier) | `langfuse>=4.2.0,<5.0.0` (optional) | Pydantic `model_validate` against `IngestionEvent_*Create` |
| `generate_traceset.py` | stdlib only (`json`, `os`, `sys`) | Build + validate + write + assert |
| `scenarios/sNN_*.py` | stdlib only (via `helpers`) | Pure data + construction logic |
| `tests/` | `pytest>=7.0` (dev) | `tmp_path` isolation, no network |

**Runtime**: zero dependencies. **Dev-only**: `langfuse` (authoritative validation) + `pytest`. The `<5.0.0` upper bound is load-bearing — it pins to the v4 ingestion wire format that the project's latency research depends on.

### 4.3 The Contract Enforced at Every Layer

Six invariants are checked redundantly at the helper, schema, scenario, generator, and test layers:

1. **Wire envelope** `{id, timestamp, type, body}` — `helpers.wrap_event` constructs; `schema._local_validate` checks; generator self-check + tests re-check.
2. **camelCase body keys** — `helpers.to_camel_case` produces; `schema` rejects any `_` in body keys; generator self-check + tests re-check.
3. **Event count** — `scenario.EXPECTED_EVENT_COUNT` is the single source of truth; generator self-check, scenario tests, and pipeline tests all assert.
4. **Monotonic timestamps** — `helpers.make_timestamp(_BASE, increasing_offset)` produces; generator self-check + tests re-check.
5. **meta/events dual invariant** — `build_meta().events_in_order` must mirror `build_events()` in length, order, and types; generator self-checks + tests re-check.
6. **Catalog coverage** — all 7 Dify trace types + all 5 app modes represented; enforced in `main()`.

This redundancy means a regression is caught at the most specific layer first with the clearest failure message.

---

## 5. Component Design

### 5.1 `helpers.py` — Event Construction (102 lines)

Pure-function factory layer. Zero state, zero I/O, zero dependencies beyond stdlib.

| Function | Responsibility |
|---|---|
| `make_event_id(seed) -> str` | Deterministic UUID **v5** (NAMESPACE_URL) from a seed string. Same seed → same ID. Enables reproducible traces. |
| `make_timestamp(base, offset_seconds=0.0) -> str` | Parses ISO-8601 base, adds float offset, returns microsecond-precision ISO-8601. |
| `to_camel_case(snake) -> str` | Splits on `_`, title-cases all parts except the first. (`user_id` → `userId`, `name` → `name`.) |
| `make_trace_create(trace_id, name, user_id=None, **kwargs) -> dict` | Builds `trace-create` body. `id`/`name` always set; `userId` only if not None; kwargs camelCased. |
| `make_span_create(span_id, trace_id, name, start_time, end_time, **kwargs) -> dict` | Builds `span-create` body with 5 required camelCase fields + camelCased kwargs. |
| `make_generation_create(gen_id, trace_id, name, model, start_time, end_time, usage, **kwargs) -> dict` | Builds `generation-create` body. Maps `usage` → `usageDetails` (the one non-trivial wire name). |
| `wrap_event(event_id, timestamp, event_type, body) -> dict` | Wraps body into the 4-field envelope `{id, timestamp, type, body}`. |

**Key design pattern**: Callers author fields in idiomatic Python snake_case (`parent_observation_id`, `completion_start_time`, `model_parameters`) and the helpers transparently serialize to Langfuse's wire camelCase. This removes the entire class of "did I spell the field name right?" bugs.

### 5.2 `schema.py` — Wire Schema Validator (76 lines)

Two-tier validator with graceful degradation.

**Tier 1 — Local validator (`_local_validate`)**: Always runs. Checks:
- Envelope has all 4 required fields: `{id, timestamp, type, body}`.
- `type` ∈ `{trace-create, span-create, generation-create}`.
- `body` is a dict.
- Type-specific required body fields present:
  - `trace-create`: `id`, `name`
  - `span-create`: `id`, `traceId`, `name`, `startTime`, `endTime`
  - `generation-create`: `id`, `traceId`, `name`, `startTime`, `endTime`, `model`, `usageDetails`
- **No snake_case leakage**: any body key containing `_` raises `ValueError` — the canonical guard enforcing camelCase end-to-end.

**Tier 2 — SDK validator (`_validate_with_sdk`)**: Runs only if `langfuse` is importable. Imports `IngestionEvent_TraceCreate` / `IngestionEvent_SpanCreate` / `IngestionEvent_GenerationCreate` (Pydantic models from `langfuse.api`) and calls `.model_validate(event)`. If `langfuse` is absent, the `ImportError` is silently swallowed.

**Fail-fast with descriptive messages**: Every branch raises `ValueError` with a message naming the exact field/type violated (e.g. `"span-create body missing required field: traceId"`, `"snake_case key in generation-create body: status_message"`). Tests match on these substrings via `pytest.raises(ValueError, match=...)`.

### 5.3 `scenarios/` — Scenario Modules & Registry

#### Registry (`scenarios/__init__.py`, 34 lines)

An **explicit, ordered, hand-maintained registry** — not decorator-based auto-registration:

```python
from . import s01_chat_basic, s02_chat_rag, ... s14_message_streaming

SCENARIOS = [
    s01_chat_basic, s02_chat_rag, ... s14_message_streaming,
]
```

**Why explicit over auto-discovery**: Fail-closed (a missing import means the scenario isn't in the catalog — no silent skip from a glob that matched nothing). Deterministic ordering without relying on filesystem sort. Adding a scenario is a two-edit operation: create `sNN_slug.py`, then add one import line and one list entry. The tests `test_scenarios_registry_has_14` and `test_all_scenario_ids_unique` lock cardinality and uniqueness.

#### Scenario module contract

Each scenario is a flat Python module (no class) exporting **module-level constants** and **two functions**:

| Constant | Example | Purpose |
|---|---|---|
| `SCENARIO_ID` | `"01-chat-basic"` | Directory name + catalog key |
| `SCENARIO_DESCRIPTION` | `"Basic chatbot, single-turn Q&A"` | Human description |
| `APP_TYPE` | `"chatbot"` | High-level app category |
| `DIFY_APP_MODE` | `"chat"` | One of Dify's 5 app modes (coverage assertion target) |
| `EDGE_CASE` | `None` or `"high-n"` / `"moderation-blocked"` / ... | `None` for baseline; slug for edge cases |
| `TRACE_TYPES_EMITTED` | `["MessageTraceInfo", "GenerateNameTraceInfo"]` | Which of the 7 Dify `*TraceInfo` types (coverage assertion target) |
| `EXPECTED_EVENT_COUNT` | `4` | Single source of truth for event count |

| Function | Returns | Responsibility |
|---|---|---|
| `build_events()` | `list[dict]` | Ordered list of wire-event dicts via `helpers.make_*` + `wrap_event`. Timestamps from a single `_BASE` + increasing offsets. |
| `build_meta()` | `dict` | `meta.json` payload: all constants + `events_in_order` (1-indexed manifest mapping each event to its `source_trace_type` and `dify_handler`) + provenance pins (`dify_commit`, `langfuse_sdk_version`) + `notes`. |

**Authoring conventions** (consistent across all 14 scenarios):
- Event IDs follow `sNN-eNN` seed convention (`make_event_id("s01-e01")`, ...) — greppable back to scenario.
- Single `_BASE` timestamp per scenario; different scenarios use different wall-clock windows.
- Module docstring = event manifest (enumerated "Events (N):" list naming each event's type + source `*TraceInfo`).
- `s01_chat_basic.py` is the declared **reference template** (all field values shown explicitly; other scenarios are variations).
- Edge-case scenarios carry `VERIFY:` notes in docstrings and `meta.json` notes — encoding research uncertainty directly in the catalog.

### 5.4 `generate_traceset.py` — Generation Pipeline (335 lines)

Orchestrates the full catalog build.

| Function | Responsibility |
|---|---|
| `generate_scenario(scenario, base_dir) -> dict` | Builds one scenario's `NN-slug/` directory: calls `build_events()`, validates every event via `schema.validate_event`, runs 6 self-checks, writes `events.jsonl` + `meta.json`, returns catalog entry dict. |
| `generate_catalog(scenarios, base_dir)` | Writes `catalog.json` — JSON array of per-scenario summary entries. |
| `generate_readme(scenarios, base_dir)` | Writes `README.md` — human-facing catalog index with scenarios table and provenance. |
| `generate_schema_doc(base_dir)` | Writes `schema.md` — static field-reference doc for the wire envelope and body types + Dify TraceInfo → wire-event mapping. |
| `main()` | Entry point: iterates `SCENARIOS`, calls `generate_scenario` for each, then the three root generators, prints progress, runs 2 coverage assertions. |

**The 6 per-scenario self-checks** (run before any file is written — a failing scenario aborts the build):

1. Event count: `len(events) == scenario.EXPECTED_EVENT_COUNT`.
2. Valid types: every event `type` ∈ the 3 valid wire types.
3. No snake_case body keys.
4. Monotonic timestamps: `timestamps == sorted(timestamps)`.
5. `meta.events_in_order` length matches event count.
6. `meta.events_in_order` per-index: 1-based sequential, each entry's `type` matches the corresponding event's `type`.

**The 2 catalog-wide coverage assertions** (in `main()`):
1. All 7 Dify trace types represented across the catalog.
2. All 5 Dify app modes represented across the catalog.

**Idempotent / reproducible**: Deterministic event IDs (UUID v5) + deterministic JSON serialization (`ensure_ascii=False`, `indent=2`) → byte-identical output on re-runs. Generated artifacts are committed (`.gitignore` excludes only `__pycache__`, `*.egg-info`, `.pytest_cache`), so the catalog is simultaneously a build *output* and a versioned *deliverable*.

### 5.5 `tests/` — Test Suite (435 lines across 4 files)

| File | Lines | Coverage |
|---|---|---|
| `test_helpers.py` | 113 | All 7 helpers: determinism, camelCase, body shapes, envelope. 12 tests. |
| `test_schema.py` | 96 | Validator happy/rejection paths with pinned error message substrings. 8 tests. |
| `test_scenarios.py` | 106 | `_check_scenario(module)` applied to all 14 modules — re-implements the 6 self-checks at test time. 14 tests. |
| `test_generate_traceset.py` | 120 | Pipeline & artifact tests with `tmp_path` isolation + end-to-end `main()` smoke test. 9 tests. |

**Conventions**: No network, no SDK required for the core suite (only `test_schema` benefits from the SDK, silently skipping the SDK tier if absent). `tmp_path` isolation — no test writes to the real catalog. The `gt._BASE_DIR` monkeypatch is the single escape hatch for the end-to-end test. Redundancy is intentional — the same invariants are checked at multiple layers so regressions are caught at the most specific layer first.

---

## 6. Wire Event Format

### 6.1 Envelope

Every event is a JSON object with 4 fields:

```json
{
  "id": "<uuid>",
  "timestamp": "<ISO 8601 UTC>",
  "metadata": null,
  "type": "trace-create | span-create | generation-create",
  "body": { ... }
}
```

### 6.2 Body Types

| Type | Required body fields | Optional fields |
|---|---|---|
| `trace-create` | `id`, `name` | `userId`, `input`, `output`, `sessionId`, `metadata`, `version`, `release`, `tags`, `public` |
| `span-create` | `id`, `traceId`, `name`, `startTime`, `endTime` | `input`, `output`, `metadata`, `level`, `statusMessage`, `parentObservationId`, `version` |
| `generation-create` | `id`, `traceId`, `name`, `startTime`, `endTime`, `model`, `usageDetails` | `modelParameters`, `input`, `output`, `metadata`, `level`, `statusMessage`, `parentObservationId`, `version`, `completionStartTime` |

### 6.3 Serialization Rules

- **camelCase** body keys (the local schema validator rejects any key containing `_`).
- **`exclude_unset + exclude_none`**: only populated fields appear on the wire. Fields left at default `None` or explicitly set to `None` are stripped.
- **1 event per HTTP POST**: Dify sends single-element `batch=[event]` to `/api/public/ingestion` — one POST per `add_trace`/`add_span`/`add_generation` call.

### 6.4 Dify TraceInfo → Wire Event Mapping

| Dify TraceInfo | Wire events | Handler | Typical scenarios |
|---|---|---|---|
| `MessageTraceInfo` | 1 trace-create + 1 generation-create | `message_trace` | chat, completion, chatflow |
| `WorkflowTraceInfo` | 1 trace-create + 1 span-create + K node events | `workflow_trace` | workflow, chatflow |
| `ModerationTraceInfo` | 1 span-create | `moderation_trace` | moderation-enabled chat |
| `DatasetRetrievalTraceInfo` | 1 span-create | `dataset_retrieval_trace` | RAG-enabled chat |
| `ToolTraceInfo` | 1 span-create per tool call | `tool_trace` | agent apps |
| `GenerateNameTraceInfo` | 1 trace-create (upsert) + 1 span-create | `generate_name_trace` | all chat/completion/agent apps |
| `SuggestedQuestionTraceInfo` | 1 generation-create | `suggested_question_trace` | chat with suggested questions enabled |

**Emission order within a run** (when multiple trace types fire):
```
ModerationTraceInfo → MessageTraceInfo → DatasetRetrievalTraceInfo → ToolTraceInfo → SuggestedQuestionTraceInfo → GenerateNameTraceInfo
```

This order follows Dify's `add_trace_task()` call sequence: moderation fires first (input check), then the message pipeline completes (LLM response), then RAG/tool traces are emitted (they ran during the message), then suggested questions (post-response), then generate name (last).

---

## 7. Scenario Catalog

### 7.1 Coverage Matrix

| # | Scenario | App mode | Edge? | Events | trace-create | span-create | generation-create | Trace types |
|---|---|---|---|---|---|---|---|---|
| 01 | `01-chat-basic` | chat | — | 4 | 2 | 1 | 1 | Message, GenerateName |
| 02 | `02-chat-rag` | chat | — | 6 | 2 | 2 | 2 | Message, DatasetRetrieval, SuggestedQuestion, GenerateName |
| 03 | `03-completion-basic` | completion | — | 4 | 2 | 1 | 1 | Message, GenerateName |
| 04 | `04-agent-single-tool` | agent-chat | — | 5 | 2 | 2 | 1 | Message, Tool, GenerateName |
| 05 | `05-agent-multi-tool` | agent-chat | — | 7 | 2 | 4 | 1 | Message, Tool, GenerateName |
| 06 | `06-workflow-5node` | workflow | — | 7 | 1 | 4 | 2 | Workflow |
| 07 | `07-workflow-15node` | workflow | **high-n** | 17 | 1 | 12 | 4 | Workflow |
| 08 | `08-chatflow-basic` | advanced-chat | — | 11 | 3 | 5 | 3 | Workflow, Message, GenerateName |
| 09 | `09-moderation-blocked` | chat | **moderation-blocked** | 3 | 1 | 1 | 1 | Moderation, Message |
| 10 | `10-moderation-pass-through` | chat | — | 5 | 2 | 2 | 1 | Moderation, Message, GenerateName |
| 11 | `11-rag-empty-results` | chat | **empty-rag** | 5 | 2 | 2 | 1 | DatasetRetrieval, Message, GenerateName |
| 12 | `12-tool-failure` | agent-chat | **tool-error** | 5 | 2 | 2 | 1 | Tool, Message, GenerateName |
| 13 | `13-suggested-questions-error` | chat | **suggested-questions-error** | 5 | 2 | 1 | 2 | Message, GenerateName, SuggestedQuestion |
| 14 | `14-message-streaming` | chat | **streaming** | 4 | 2 | 1 | 1 | Message, GenerateName |
| | **Total** | | **6 edge / 8 normal** | **88** | **26** | **40** | **22** | |

### 7.2 Scenario Details

#### 01 — `01-chat-basic` (normal, 4 events)
Simplest chat run. Single-turn Q&A ("Redis vs Memcached") on `gpt-4o-mini` (42 in / 156 out). The GenerateName `trace-create` (event 3) **reuses the same trace ID** as the Message `trace-create` (event 1) — Langfuse upsert semantics. Declared as the **reference template** for all other scenarios.

#### 02 — `02-chat-rag` (normal, 6 events)
RAG retrieval returns 3 documents (scores 0.95/0.89/0.82). Two distinct `generation-create` events: the message answer and the suggested follow-up questions (`output.questions` array of 3). Exercises the most trace types of any chatbot scenario (4).

#### 03 — `03-completion-basic` (normal, 4 events)
Structurally identical to 01 **except** the `trace-create` body has **no `sessionId`** — completion apps have no conversation. `input` is `{"prompt": ...}` (not `{"query": ...}`). Confirms that `sessionId` is conditionally emitted.

#### 04 — `04-agent-single-tool` (normal, 5 events)
`weather_api` tool span, then synthesis `generation-create` whose `input.messages` includes a `{"role":"tool", ...}` entry. The Tool `span-create` is emitted **between** the Message `trace-create` and `generation-create` — the tool runs and is traced before the LLM synthesis is recorded.

#### 05 — `05-agent-multi-tool` (normal, 7 events)
Three sequential tools (`web_search`, `data_fetch`, `calculator`), then synthesis on **`gpt-4o`** (not mini). `input.messages` carries three `role:tool` entries. Demonstrates N-tool scaling on the agent path.

#### 06 — `06-workflow-5node` (normal, 7 events)
Pipeline: `Start → KnowledgeRetrieval → LLM(Generate) → LLM(Refine) → End`. All child nodes carry `parentObservationId` pointing at the root "workflow" span. Two `generation-create` events (Generate + Refine), both `gpt-4o`. Pure `WorkflowTraceInfo` (no Message/GenerateName).

#### 07 — `07-workflow-15node` (edge: high-N, 17 events)
**The high-N stress case** — 17 sequential HTTP POSTs from a single `WorkflowTraceInfo` task. 4 LLM generations across **two providers** (3× `gpt-4o`, 1× `claude-3-5-sonnet-20241022`). Tool-named spans (`web_search`, `data_fetch`) and control-flow spans (`Check Confidence`, `validate_output`, `Quality Check`). Largest scenario (~19% of all catalog events). Only scenario with a non-OpenAI model.

#### 08 — `08-chatflow-basic` (normal, 11 events)
The only `advanced-chat` scenario — **composition case**: chatflow = workflow + message. First 7 events are the workflow (mirrors scenario 06's shape); then a second `trace-create` (Message) and `generation-create` record the conversation message; then GenerateName. All three `trace-create` events **share the same trace ID** (upsert). Highest `trace-create` count of any scenario (3).

#### 09 — `09-moderation-blocked` (edge: moderation-blocked, 3 events)
Shortest scenario. Moderation span has `level: "WARNING"`, `statusMessage: "Input blocked by moderation: flagged categories [violence, hate]"`. The `generation-create` emits a **preset response** (`metadata.preset_response: true`, `temperature: 0.0`). **No GenerateName** — the blocked path skips conversation-name generation. The `meta.json` note flags a VERIFY — the wire **confirms the generation-create fires**.

#### 10 — `10-moderation-pass-through` (normal, 5 events)
The foil to 09 — same Moderation+Message+GenerateName skeleton but the happy path. Moderation span `output.flagged: false`, `output.action: "pass"`, **no `level` field** (implicitly DEFAULT, omitted under `exclude_unset`). Confirms that a passing moderation emits no `level`/`statusMessage`.

#### 11 — `11-rag-empty-results` (edge: empty-rag, 5 events)
`dataset_retrieval` span `output: {"documents": [], "result_count": 0}`. The LLM answers without knowledge ("I don't have specific information…"). The DatasetRetrieval span is emitted **before** the Message events (retrieval runs first, then the message trace is opened). Empty-results edge — the span still fires with a 0-count output; the LLM still answers (graceful degradation).

#### 12 — `12-tool-failure` (edge: tool-error, 5 events)
`stock_api` tool span has `level: "ERROR"`, `statusMessage: "ConnectionError: ... timeout after 5000ms"`. The agent still completes — the synthesis `generation-create`'s `input.messages` includes `{"role":"tool", "content":"ConnectionError: ..."}` and the LLM apologizes. Failure is recorded, not fatal. Note: the Tool span leads (idx 1), unlike the normal agent path (04/05) where Message `trace-create` leads.

#### 13 — `13-suggested-questions-error` (edge: suggested-questions-error, 5 events)
The failing SuggestedQuestion `generation-create` (event 5) has `level: "ERROR"`, `statusMessage: "RateLimitError: ... 429"`, and **`usageDetails.output: 0`** (input 85 tokens consumed, 0 output produced). The failing generation **consumed input tokens but produced zero output** — a partial-failure billing signature. Emitted **last** (after GenerateName), unlike scenario 02 where SuggestedQuestion precedes GenerateName.

#### 14 — `14-message-streaming` (edge: streaming, 4 events)
Structurally identical to 01 (same 4-event shape) — the **only** difference is the streaming latency metadata on the `generation-create`: `metadata: {"streaming": true, "gen_ai_server_time_to_first_token": 320, "llm_streaming_time_to_generate": 4150}` plus `completionStartTime`. The `meta.json` note flagged a VERIFY of the exact metadata key names — the wire **confirms** they are `gen_ai_server_time_to_first_token` (320ms TTFT) and `llm_streaming_time_to_generate` (4150ms TTG). This is the latency-relevant scenario for the real-time-tracing thesis.

### 7.3 Cross-Cutting Patterns

1. **Trace-ID upsert semantics**: In every scenario with a GenerateName (and triply so in chatflow 08), multiple `trace-create` events reuse the *same* `id`. Langfuse treats a repeated trace ID as an upsert/merge, not a conflict.

2. **Event ordering is emission order, not logical order**: Wire timestamps are when Dify's `LangFuseDataTrace.*` handler *fires*, not when the underlying work happened. Events cluster at completion, not at occurrence — directly relevant to the latency thesis.

3. **`sessionId` is conditional**: Present for chat/agent-chat/advanced-chat (conversational); absent for completion (03) and pure workflows (06, 07).

4. **Failure paths still emit full event sequences**: 09 (preset response), 12 (apology synthesis), 13 (rate-limited suggested questions) all complete their Message/GenerateName traces — failures are recorded as `level=ERROR`/`WARNING` + `statusMessage` on the offending span/generation, not as missing events. The exception is 09, where the blocked path drops GenerateName entirely.

---

## 8. Determinism & Provenance

### Determinism

The catalog is **byte-reproducible** from source:

- **UUID v5** (not v4) for event IDs — `make_event_id("sNN-eNN")` produces the same ID for the same seed, every time.
- **Fixed base timestamps** per scenario (`_BASE` ISO-8601 constants) — `make_timestamp(_BASE, offset)` produces deterministic ISO-8601 strings.
- **Deterministic JSON serialization** — `ensure_ascii=False`, `indent=2` in all `json.dump` calls.
- **Idempotent file operations** — `os.makedirs(..., exist_ok=True)`, overwrite-on-write.

Re-running `python3 -m traceset.generate_traceset` produces output byte-identical to the committed artifacts. This is verified by the clean working tree after generation.

### Provenance

Every `meta.json` pins two upstream revisions:

- `dify_commit`: `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6` — the Dify source revision against which the wire event field assignments were verified.
- `langfuse_sdk_version`: `>=4.2.0,<5.0.0` — the Langfuse Python SDK version range whose Pydantic models define the authoritative wire schema.

These pins tie the synthetic traces to specific upstream revisions, making the catalog a verifiable artifact rather than a free-floating dataset.

---

## 9. Validation Strategy

### 9.1 Two-Tier Validator

`schema.validate_event(event)` runs both tiers in sequence:

1. **Local validator** (always runs): Envelope structure, type validity, required body fields per type, no-snake-case guard. Stdlib-only.
2. **SDK validator** (runs if `langfuse` importable): Pydantic `model_validate` against `IngestionEvent_TraceCreate` / `IngestionEvent_SpanCreate` / `IngestionEvent_GenerationCreate` — the same models the Langfuse server uses. Silently skipped if SDK absent.

This split enables **zero-runtime-dep generation** with **optional authoritative validation** in CI/dev.

### 9.2 Per-Scenario Self-Checks (6)

Run by `generate_scenario()` before writing any file — a failing scenario aborts the build:

1. Event count matches `EXPECTED_EVENT_COUNT`.
2. All event `type` values are valid.
3. No body key contains `_` (camelCase enforced).
4. Timestamps are monotonically non-decreasing.
5. `meta.events_in_order` length matches event count.
6. `meta.events_in_order` per-index type matches the actual event type.

### 9.3 Catalog-Wide Coverage Assertions (2)

Run by `main()` after all scenarios are generated:

1. **All 7 Dify trace types represented** across the catalog's `TRACE_TYPES_EMITTED` fields.
2. **All 5 Dify app modes represented** across the catalog's `DIFY_APP_MODE` fields.

These guarantee the catalog is a *complete* coverage matrix, not just a handful of examples.

---

## 10. Open Questions

Two items are flagged in scenario `meta.json` `notes` fields and the design spec ([`docs/superpowers/specs/2026-06-26-dify-trace-catalog-design.md`](./superpowers/specs/2026-06-26-dify-trace-catalog-design.md)):

1. **Scenario 09 (moderation-blocked)** — Does `MessageTraceInfo` still emit `generation-create` when moderation blocks the input (no real LLM call)? The catalog **assumes yes** (3 events, with a preset-response `generation-create`). The wire confirms this assumption — the `generation-create` fires with `metadata.preset_response: true` and small token usage (15 in / 40 out). **Status**: Assumption verified by the catalog's own wire output; remains to be confirmed against Dify source (`message_service.py` / `input_moderation.py`) for production fidelity.

2. **Scenario 14 (streaming)** — Exact metadata key names for `gen_ai_server_time_to_first_token` (TTFT) and `llm_streaming_time_to_generate` (TTG). The catalog uses these key names in the `generation-create` body's `metadata`. **Status**: The wire output uses these names consistently; remains to be verified against `LangFuseDataTrace.message_trace` source for production fidelity.

3. **Agent emission ordering (softer item)** — For agent scenarios (04, 05, 12), the exact relative emission order of `ToolTraceInfo` vs. `MessageTraceInfo` depends on callback fire timing in Dify's agent execution loop. The catalog assumes tool spans are emitted before the message `generation-create`. This is consistent with Dify's `add_trace_task()` call sequence but has not been verified against a live agent run.

---

## 11. Implementation Status

**Complete.** All 15 tasks from the implementation plan ([`docs/superpowers/plans/2026-06-26-dify-trace-catalog.md`](./superpowers/plans/2026-06-26-dify-trace-catalog.md)) are done, merged to `main` via `--no-ff` merge commit `3b83cc4`.

| Metric | Value |
|---|---|
| Scenarios | 14 |
| Wire events | 88 (26 trace-create + 40 span-create + 22 generation-create) |
| Tests | 43 passing (8 schema + 12 helpers + 14 scenarios + 9 generation) |
| Self-checks per scenario | 6 |
| Catalog-wide assertions | 2 |
| Edge cases | 6 (high-N, moderation-blocked, empty-RAG, tool-error, suggested-questions-error, streaming) |
| Dify trace types covered | 7 / 7 |
| Dify app modes covered | 5 / 5 |
| Runtime dependencies | 0 |
| Dev dependencies | `langfuse>=4.2.0,<5.0.0`, `pytest>=7.0` |
| Python version | >=3.10 |
| Total source lines | ~3,480 (27 Python/TOML files) |

**Reproduce**: `python3 -m traceset.generate_traceset` from repo root. **Test**: `python3 -m pytest traceset/`.

---

## 12. References

### Project docs

- [`docs/01-research-report.md`](./01-research-report.md) — Latency research synthesis (5-layer breakdown, latency budget by approach, debunked knobs, 7 options, critical gotchas).
- [`docs/02-dify-trace-flow.md`](./02-dify-trace-flow.md) — Dify trace emission deep dive (Steps 0-5 with code quotes, 12 latency scenarios A-L).
- [`docs/03-langfuse-staging-tables.md`](./03-langfuse-staging-tables.md) — Langfuse server write-path internals (staging table, v4 direct-write path, server-side knobs).
- [`docs/superpowers/specs/2026-06-26-dify-trace-catalog-design.md`](./superpowers/specs/2026-06-26-dify-trace-catalog-design.md) — Catalog design spec (10 sections: overview, scenario catalog, directory structure, wire format, meta schema, realism conventions, generation method, scope boundaries, references).
- [`docs/superpowers/plans/2026-06-26-dify-trace-catalog.md`](./superpowers/plans/2026-06-26-dify-trace-catalog.md) — 15-task TDD implementation plan with complete code.

### Dify source (pinned at `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`)

- `api/core/ops/ops_trace_manager.py:1485-1568` — `OpsTraceManager.add_trace_task()`, `run()`, `collect_tasks()`, `send_to_celery()`.
- `api/core/ops/trace_entity.py` — `TraceTask` dataclass, `trace_info_info_map` (10-entry dispatch table).
- `api/providers/trace/trace-langfuse/src/dify_trace_langfuse/langfuse_trace.py` — `LangFuseDataTrace` provider with `message_trace`, `workflow_trace`, `moderation_trace`, `dataset_retrieval_trace`, `tool_trace`, `generate_name_trace`, `suggested_question_trace` handlers.
- `api/core/app/easy_ui_based_generate_task_pipeline.py:419` — `MessageTraceInfo` call site.
- `api/core/app/workflow/layers/persistence.py:439` — `WorkflowTraceInfo` call site.
- `api/core/moderation/input_moderation.py:52` — `ModerationTraceInfo` call site.
- `api/core/rag/retrieval/dataset_retrieval.py:998` — `DatasetRetrievalTraceInfo` call site.
- `api/core/callback_handler/agent_tool_callback_handler.py:74` — `ToolTraceInfo` call site.
- `api/core/llm_generator/llm_generator.py:140` — `GenerateNameTraceInfo` + `SuggestedQuestionTraceInfo` call sites.

### Langfuse source (pinned at `216d422635f5634bfaa8a295041f92c81a1c2aed`)

- `src/server/ingestion.ts` — REST ingestion endpoint.
- `src/server/otel/v1/traces/index.ts` — OTLP ingestion endpoint.
- `src/server/services/EventService.ts:66-91` — `getDelay()` (REST 5s, OTLP 0s).
- `src/server/queues/otelIngestionQueue.ts:358-459` — v4 direct-write routing decision.
- `src/server/env.ts:97-108` — `LANGFUSE_INGESTION_QUEUE_DELAY_MS` and related env vars.

### Langfuse Python SDK (pinned at `>=4.2.0,<5.0.0`)

- `langfuse/api.py` — `IngestionEvent_TraceCreate`, `IngestionEvent_SpanCreate`, `IngestionEvent_GenerationCreate` Pydantic models (used by `schema.py` SDK tier).
- `langfuse/span_processor.py` — `BatchSpanProcessor` (the OTel path Dify does not use).
