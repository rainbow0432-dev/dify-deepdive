# Dify App Trace Reference Catalog — Design

**Date**: 2026-06-26
**Status**: Design (awaiting user review)
**Dify commit**: `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`
**Langfuse SDK**: `>=4.2.0,<5.0.0` (Dify pin)

## 1. Overview

This spec designs a **comprehensive traceset of Dify app traces** for use as a reference catalog. The traceset contains the Langfuse wire events that Dify POSTs to `/api/public/ingestion` — the trace-create / span-create / generation-create events that Langfuse receives and stores. The catalog is organized as one full run per Dify app type, plus edge-case variants, with realistic field values matching what Dify actually emits.

**Goals**:
- Cover all 7 Dify trace types (`MessageTraceInfo`, `WorkflowTraceInfo`, `ModerationTraceInfo`, `DatasetRetrievalTraceInfo`, `ToolTraceInfo`, `GenerateNameTraceInfo`, `SuggestedQuestionTraceInfo`).
- Cover all Dify app types (chatbot, completion, agent, workflow, chatflow) plus moderation and RAG configurations.
- Include edge-case variants (high-N workflow, moderation-blocked, empty RAG, tool failure, suggested-questions error, streaming).
- Every event has all fields Dify populates for that trace type, with realistic values — no placeholders, no stubbed fields.

**Non-goals**:
- Capturing real traces from a live Dify instance (the traceset is synthetic but realistic).
- Covering the Dify-internal `TraceInfo` Pydantic format (the catalog is wire events only).
- Covering Langfuse's post-ingestion storage format (the catalog is pre-storage wire events).
- Latency scenario replay (the existing `docs/02-dify-trace-flow.md` scenarios A–L are not reproduced as traces; the catalog is a reference, not a latency test).

## 2. Scenario catalog

14 scenarios, ~88 wire events total. Event counts per trace type follow the research in `docs/02-dify-trace-flow.md`:

| Trace type | Events per occurrence |
|---|---|
| `MessageTraceInfo` | 2 (1 trace-create + 1 generation-create) |
| `WorkflowTraceInfo` | 2 + K (1 trace-create + 1 span-create for the workflow + K node spans/generations) |
| `ModerationTraceInfo` | 1 (1 span-create) |
| `DatasetRetrievalTraceInfo` | 1 (1 span-create) |
| `ToolTraceInfo` | 1 (1 span-create per tool call) |
| `GenerateNameTraceInfo` | 2 (1 trace-create + 1 span-create) |
| `SuggestedQuestionTraceInfo` | 1 (1 generation-create) |

| # | Directory | App type | Trace types emitted | Events | Edge? |
|---|---|---|---|---|---|
| 01 | `01-chat-basic` | Chatbot | Message + GenerateName | 4 | |
| 02 | `02-chat-rag` | Chatbot+RAG | Message + DatasetRetrieval + GenerateName + SuggestedQuestion | 6 | |
| 03 | `03-completion-basic` | Completion | Message + GenerateName | 4 | |
| 04 | `04-agent-single-tool` | Agent | Message + Tool×1 + GenerateName | 5 | |
| 05 | `05-agent-multi-tool` | Agent | Message + Tool×3 + GenerateName | 7 | |
| 06 | `06-workflow-5node` | Workflow | WorkflowTraceInfo (5 nodes, 2 LLM) | 7 | |
| 07 | `07-workflow-15node` | Workflow | WorkflowTraceInfo (15 nodes, 3 tool, 4 LLM) | 17 | edge: high-N |
| 08 | `08-chatflow-basic` | Chatflow | Workflow + Message + GenerateName | 11 | |
| 09 | `09-moderation-blocked` | Chatbot+Moderation | Moderation + Message (preset response) | 3 | edge: blocked |
| 10 | `10-moderation-pass-through` | Chatbot+Moderation | Moderation + Message + GenerateName | 5 | |
| 11 | `11-rag-empty-results` | Chatbot+RAG | DatasetRetrieval (empty) + Message + GenerateName | 5 | edge: empty |
| 12 | `12-tool-failure` | Agent | Tool (error) + Message + GenerateName | 5 | edge: error |
| 13 | `13-suggested-questions-error` | Chatbot | Message + GenerateName + SuggestedQuestion (error) | 5 | edge: error |
| 14 | `14-message-streaming` | Chatbot | Message (streaming, TTFT/TTG) + GenerateName | 4 | edge: streaming |

**Total**: 88 wire events across 14 scenarios.

**Coverage check**: all 7 trace types appear. 6 edge-case variants included. All 5 Dify app modes represented (`chat`, `completion`, `agent-chat`, `workflow`, `advanced-chat`/chatflow).

## 3. Directory structure

```
traceset/
├── catalog.json                 # index of all runs (machine-readable)
├── README.md                    # explains the catalog, schema summary, how to read
├── schema.md                    # Langfuse wire event field reference (all 3 event types)
├── generate_traceset.py         # reproducible generation script
└── <NN-slug>/                   # one per scenario (14 directories)
    ├── events.jsonl             # wire events, one per line, timestamp-ascending
    └── meta.json                # scenario metadata
```

Each `events.jsonl` line is **one Langfuse ingestion event** — the inner event object `{id, timestamp, type, body}`. This is what Langfuse stores post-ingestion. The transport wrapper (`{"batch":[<event>],"metadata":null}`) is identical for every POST (Dify sends 1 event per `api.ingestion.batch(batch=[event])` call) and is documented in `schema.md` rather than repeated per line.

## 4. Wire event format

### 4.1 Event envelope

Every line in `events.jsonl`:

```json
{
  "id": "<uuid v4>",
  "timestamp": "<ISO 8601 UTC, e.g. 2025-01-15T10:30:00.123456+00:00>",
  "metadata": null,
  "type": "trace-create" | "span-create" | "generation-create",
  "body": { ... type-specific ... }
}
```

- `id`: event ID, generated by Dify's `_make_event_id()` → `str(uuid.uuid4())`.
- `timestamp`: event creation time, generated by Dify's `_now_iso()` → `datetime.now(timezone.utc).isoformat()`.
- `metadata`: event-level metadata. Dify does NOT set this; it defaults to `null` and is excluded from the wire (the langfuse-python SDK uses `exclude_unset=True, exclude_none=True`). Shown here for schema completeness; fixtures will omit it (matching the wire).
- `type`: one of `"trace-create"`, `"span-create"`, `"generation-create"`.

### 4.2 `trace-create` body (`TraceBody`)

```json
{
  "id": "<trace_id>",
  "name": "<string>",
  "userId": "<string>",
  "input": "<any>",
  "output": "<any>",
  "sessionId": "<string>",
  "version": "<string>",
  "release": "<string>",
  "metadata": "<any>",
  "tags": ["<string>"],
  "public": <bool>
}
```

All fields optional; only populated fields appear on the wire. Dify does NOT set `timestamp` or `environment` on trace bodies (they're unset → excluded). **Note**: the schemas in sections 4.2–4.4 show ALL possible fields per the Langfuse wire schema. Which fields Dify actually populates depends on the trace type and the `LangFuseDataTrace` provider code. The implementation plan will enumerate the per-trace-type populated field set.

### 4.3 `span-create` body (`CreateSpanBody`)

```json
{
  "id": "<span_id>",
  "traceId": "<trace_id>",
  "name": "<string>",
  "startTime": "<ISO 8601>",
  "endTime": "<ISO 8601>",
  "metadata": "<any>",
  "input": "<any>",
  "output": "<any>",
  "level": "DEBUG" | "DEFAULT" | "WARNING" | "ERROR",
  "statusMessage": "<string>",
  "parentObservationId": "<string>",
  "version": "<string>",
  "environment": "<string>"
}
```

### 4.4 `generation-create` body (`CreateGenerationBody`)

Extends `CreateSpanBody` (section 4.3) with:

```json
{
  "completionStartTime": "<ISO 8601>",
  "model": "<string>",
  "modelParameters": { "<key>": "<value>" },
  "usageDetails": {
    "input": <int>,
    "output": <int>,
    "total": <int>,
    "unit": "CHARACTERS" | "TOKENS",
    "inputCost": <float>,
    "outputCost": <float>,
    "totalCost": <float>
  },
  "costDetails": { "<key>": "<float>" },
  "promptName": "<string>",
  "promptVersion": <int>
}
```

### 4.5 Wire serialization rules

- **camelCase aliases**: all field names are camelCase on the wire (`userId`, `sessionId`, `traceId`, `startTime`, `endTime`, `statusMessage`, `parentObservationId`, `completionStartTime`, `modelParameters`, `usageDetails`, `inputCost`, `outputCost`, `totalCost`). The langfuse-python SDK serializes via Pydantic aliases.
- **exclude_unset + exclude_none**: only fields that Dify explicitly populates with a non-None value appear. Unset and None fields are excluded. Dify also calls `filter_none_values()` on the trace data before passing to the SDK, providing a second layer of None removal.
- **1 event per POST**: Dify calls `api.ingestion.batch(batch=[event])` with a single-element list. Each HTTP POST to `/api/public/ingestion` carries exactly one event in its `batch` array.

## 5. Per-scenario event composition

Events within each scenario are in **emission order** — the order Dify's Celery worker would POST them (sequential for-loop in `LangFuseDataTrace.trace()`). The trace ID is consistent within a run (Dify uses `message_id` for chat apps, `workflow_run_id` for workflows).

### Example: `01-chat-basic` (4 events)

1. **trace-create** — trace ID = `message_id`, name = conversation name, userId, input = user query. (From `MessageTraceInfo`.)
2. **generation-create** — traceId = `message_id`, model = `"gpt-4o-mini"`, usageDetails = `{input: 42, output: 156, total: 198, unit: "TOKENS", inputCost: 0.000063, outputCost: 0.000234, totalCost: 0.000297}`, startTime, endTime, completionStartTime, input = messages array, output = assistant response. (From `MessageTraceInfo`.)
3. **trace-create** — trace ID = same (upsert), name = `"Generate Name"`, userId. (From `GenerateNameTraceInfo`.)
4. **span-create** — traceId = same, name = `"Generate Name"`, startTime, endTime, input = conversation messages, output = generated conversation name. (From `GenerateNameTraceInfo`.)

### Per-scenario trace type → event mapping

| Scenario | Events in order |
|---|---|
| `01-chat-basic` | trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name) |
| `02-chat-rag` | trace-create(msg) → generation-create(msg) → span-create(rag) → generation-create(sugg-q) → trace-create(name) → span-create(name) |
| `03-completion-basic` | trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name) |
| `04-agent-single-tool` | trace-create(msg) → span-create(tool) → generation-create(msg) → trace-create(name) → span-create(name) |
| `05-agent-multi-tool` | trace-create(msg) → span-create(tool1) → span-create(tool2) → span-create(tool3) → generation-create(msg) → trace-create(name) → span-create(name) |
| `06-workflow-5node` | trace-create(wf) → span-create(wf-span) → span-create(node1) → span-create(node2) → generation-create(node3-llm) → generation-create(node4-llm) → span-create(node5) |
| `07-workflow-15node` | trace-create(wf) → span-create(wf-span) → [15 node events: mix of span-create and generation-create; enumerated in implementation plan] |
| `08-chatflow-basic` | trace-create(wf) → span-create(wf-span) → [5 node events; enumerated in implementation plan] → trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name) |
| `09-moderation-blocked` | span-create(mod) → trace-create(msg) → generation-create(msg, preset response) |
| `10-moderation-pass-through` | span-create(mod) → trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name) |
| `11-rag-empty-results` | span-create(rag, empty docs) → trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name) |
| `12-tool-failure` | span-create(tool, level=ERROR, statusMessage=error) → trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name) |
| `13-suggested-questions-error` | trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name) → generation-create(sugg-q, level=ERROR) |
| `14-message-streaming` | trace-create(msg) → generation-create(msg, streaming fields) → trace-create(name) → span-create(name) |

**Note**: the exact ordering of traces within a run depends on Dify's `add_trace_task()` call order, which is: message pipeline → moderation → dataset retrieval → agent tool → suggested questions → generate name. The table above follows this order. This is **emission order** (when `add_trace_task` is called), not **execution order** (when the work actually happens). For agent scenarios (04–05), the tool call executes *during* the LLM generation, but the `ToolTraceInfo` is emitted as a separate trace task — its emission order relative to the `MessageTraceInfo` depends on when Dify's agent callback fires vs. when the message pipeline completes. The implementation plan will verify exact ordering against the Dify source.

**Note on scenario 09 (moderation-blocked)**: when moderation blocks an input, Dify returns a preset response without calling the LLM. It is unclear from the existing research whether `MessageTraceInfo` still emits a `generation-create` event (there's no real LLM call) or only a `trace-create`. The table assumes a generation-create is still emitted (matching Dify's uniform message-trace flow), but the implementation plan must verify this against the `message_service.py` / `input_moderation.py` source. If no generation is emitted, scenario 09 has 2 events, not 3.

## 6. `meta.json` schema

```json
{
  "scenario_id": "01-chat-basic",
  "scenario_description": "Basic chatbot, single-turn Q&A, no extras",
  "app_type": "chatbot",
  "dify_app_mode": "chat",
  "edge_case": null,
  "trace_types_emitted": ["MessageTraceInfo", "GenerateNameTraceInfo"],
  "expected_event_count": 4,
  "events_in_order": [
    {
      "index": 1,
      "type": "trace-create",
      "source_trace_type": "MessageTraceInfo",
      "dify_handler": "LangFuseDataTrace.message_trace"
    },
    {
      "index": 2,
      "type": "generation-create",
      "source_trace_type": "MessageTraceInfo",
      "dify_handler": "LangFuseDataTrace.message_trace"
    },
    {
      "index": 3,
      "type": "trace-create",
      "source_trace_type": "GenerateNameTraceInfo",
      "dify_handler": "LangFuseDataTrace.generate_name_trace"
    },
    {
      "index": 4,
      "type": "span-create",
      "source_trace_type": "GenerateNameTraceInfo",
      "dify_handler": "LangFuseDataTrace.generate_name_trace"
    }
  ],
  "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
  "langfuse_sdk_version": ">=4.2.0,<5.0.0",
  "notes": "Simplest chat run. GenerateName upserts the same trace ID."
}
```

**Fields**:
- `scenario_id`: matches the directory name.
- `scenario_description`: one-line human-readable description.
- `app_type`: high-level app type (`chatbot`, `completion`, `agent`, `workflow`, `chatflow`).
- `dify_app_mode`: Dify's `app.mode` enum value (`chat`, `completion`, `agent-chat`, `workflow`, `advanced-chat`).
- `edge_case`: `null` for normal scenarios; one of `"high-n"`, `"moderation-blocked"`, `"empty-rag"`, `"tool-error"`, `"suggested-questions-error"`, `"streaming"` for edge cases.
- `trace_types_emitted`: list of Dify `TraceInfo` subclass names present in this run.
- `expected_event_count`: integer; must match the number of lines in `events.jsonl`.
- `events_in_order`: per-event metadata (1-indexed), with `type` (wire event type), `source_trace_type` (Dify `TraceInfo` subclass), `dify_handler` (the `LangFuseDataTrace` method that built it).
- `dify_commit` / `langfuse_sdk_version`: provenance for reproducibility.
- `notes`: free-text notes about the scenario.

## 7. Realism conventions

All values are synthetic but realistic — matching what a real Dify instance would emit.

- **Model names**: real model IDs — `gpt-4o-mini`, `gpt-4o`, `claude-3-5-sonnet-20241022`, `deepseek-chat`, `doubao-pro-4k`, `qwen2.5-72b-instruct`. Model choice varies by scenario (chat uses `gpt-4o-mini`; workflow LLM nodes use `gpt-4o` or `claude-3-5-sonnet`).
- **Token counts**: realistic ranges — input 50–2000 tokens, output 100–3000 tokens, depending on scenario complexity. Costs computed from realistic per-token prices (e.g., `gpt-4o-mini`: $0.15/1M input, $0.60/1M output).
- **Timestamps**: ISO 8601 UTC with microseconds (`2025-01-15T10:30:00.123456+00:00`). Events within a run span realistic durations: chat generation 1–5s, workflow node 0.5–10s, tool call 0.5–3s, moderation 0.1–0.5s. Event timestamps are monotonically increasing within a run.
- **IDs**: valid UUID v4 for event IDs. Trace IDs use Dify's format: `message_id` (UUID v4), `workflow_run_id` (UUID v4), `conversation_id` (UUID v4). User IDs are realistic Dify user IDs (UUID v4 or `user-<hash>` format).
- **Inputs/outputs**: realistic text content — actual user queries (e.g., "What are the key differences between Redis and Memcached?"), LLM responses (paragraphs of real-sounding text), tool inputs/outputs (JSON with realistic parameters and results). No placeholder strings like "test" or "hello".
- **Metadata**: realistic Dify metadata structure — includes `user_id`, `conversation_id`, `parent_trace_context` (for nested workflows), app metadata, etc.
- **Usage details**: `unit: "TOKENS"` for LLM calls. Costs have 6 decimal places (e.g., `0.000297`). `inputCost`/`outputCost`/`totalCost` are consistent with `input`/`output` token counts and the model's pricing.
- **Level/status**: `DEFAULT` for success scenarios. `ERROR` for error edge cases (`12-tool-failure`, `13-suggested-questions-error`), with `statusMessage` containing a realistic error message.
- **Streaming fields** (scenario 14 only): `completionStartTime` set on the generation event. The Dify-internal `gen_ai_server_time_to_first_token` and `llm_streaming_time_to_generate` fields do NOT have direct Langfuse wire equivalents — they are carried in the generation body's `metadata` field (Dify passes them through `metadata` on the `CreateGenerationBody`). The implementation plan will verify the exact metadata key names against the `LangFuseDataTrace.message_trace` source.

## 8. Generation & validation method

A Python script (`generate_traceset.py`) constructs the fixtures reproducibly.

### 8.1 Construction

For each scenario:
1. Build a sequence of event dicts with camelCase keys and realistic values, in emission order.
2. Each event dict has the envelope `{id, timestamp, type, body}` with a type-specific `body`.
3. Use deterministic UUIDs (fixed seeds via `uuid.UUID("...")`) and fixed timestamps so the catalog is reproducible across runs.

### 8.2 Validation

Validate each event dict against the langfuse wire schema. **Option A (chosen)**: install `langfuse>=4.2.0,<5.0.0` as a dev dependency and validate each event by constructing the SDK's Pydantic models (`IngestionEvent_TraceCreate`, `IngestionEvent_SpanCreate`, `IngestionEvent_GenerationCreate`) from the event dict. Construction success = field correctness. If the SDK proves incompatible with the project's Python version or dependency tree, fall back to **Option B**: ship a local `schema.py` module mirroring the wire schema (field names, types, required/optional) and validate against it. The implementation plan will attempt Option A first and fall back to B only if needed.

### 8.3 Serialization

- Write `events.jsonl`: one `json.dumps(event, ensure_ascii=False)` per line, in emission order.
- Write `meta.json`: `json.dumps(meta, indent=2, ensure_ascii=False)`.

### 8.4 Self-check

The script asserts:
- Event count in `events.jsonl` matches `expected_event_count` in `meta.json`.
- Every trace type in `trace_types_emitted` is represented by at least one event.
- All event `type` values are one of `trace-create`, `span-create`, `generation-create`.
- All `body` fields are camelCase (no snake_case keys leak through).
- Timestamps are monotonically non-decreasing within each run.

### 8.5 Root files

- `catalog.json`: index of all 14 scenarios — `[{scenario_id, app_type, edge_case, event_count, trace_types}, ...]`.
- `README.md`: explains the catalog, how to read `events.jsonl` and `meta.json`, points to `schema.md` for field reference.
- `schema.md`: documents the wire event schema (envelope + 3 body types + serialization rules), with the field-by-field source mapping from Dify `TraceInfo` → Langfuse wire event.

## 9. Scope boundaries

**In scope**:
- 14 scenarios as listed in section 2.
- Langfuse wire events (trace-create, span-create, generation-create).
- Realistic synthetic values for all fields Dify populates.
- Generation script with schema validation.
- Root catalog index, README, and schema reference.

**Out of scope**:
- Dify-internal `TraceInfo` JSON format (the `TaskData` envelope). Not included in the catalog.
- Langfuse post-ingestion storage format (ClickHouse rows, observation model).
- Latency scenario replay (scenarios A–L from `docs/02-dify-trace-flow.md`).
- Capturing real traces from a live Dify instance.
- The `PromptGenerationTraceInfo`, `WorkflowNodeTraceInfo`, and `DraftNodeExecutionTrace` types (not in the user's requested 7 types; `WorkflowNodeTraceInfo` data is embedded inside `WorkflowTraceInfo`'s node events, not emitted as a separate trace).
- Multi-tenant or enterprise telemetry traces.
- OTLP/OTel-format traces (Dify uses REST ingestion only).

## 10. References

- `docs/01-research-report.md` — latency research, identifies the 7 trace types and the REST ingestion path.
- `docs/02-dify-trace-flow.md` — per-trace-type HTTP POST counts, emission order, call sites.
- Dify source: `api/core/ops/entities/trace_entity.py` — `TraceInfo` Pydantic schemas (10 types in `trace_info_info_map`).
- Dify source: `api/providers/trace/trace-langfuse/src/dify_trace_langfuse/langfuse_trace.py` — `LangFuseDataTrace` provider, builds wire events.
- Dify source: `api/core/ops/ops_trace_manager.py` — `TraceTask`, `TaskData`, serialization path.
- Langfuse Python SDK: `langfuse.api.ingestion` — `IngestionEvent_TraceCreate` / `IngestionEvent_SpanCreate` / `IngestionEvent_GenerationCreate`, `TraceBody`, `CreateSpanBody`, `CreateGenerationBody`.
- Langfuse server: `packages/shared/src/server/ingestion/types.ts` — Zod schema (source of truth for wire format).
