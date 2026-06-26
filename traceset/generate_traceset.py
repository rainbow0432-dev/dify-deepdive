#!/usr/bin/env python3
"""Generate the Dify trace reference catalog.

For each scenario:
  1. Build events via scenario.build_events()
  2. Validate each event via schema.validate_event()
  3. Run self-checks (event count, monotonic timestamps, no snake_case)
  4. Write events.jsonl and meta.json

Also generates root files: catalog.json, README.md, schema.md (Task 14).
"""
from __future__ import annotations

import json
import os
import sys

from traceset.schema import validate_event
from traceset.scenarios import SCENARIOS

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def generate_scenario(scenario, base_dir: str) -> dict:
    """Generate events.jsonl and meta.json for one scenario.

    Returns a catalog entry dict.
    """
    scenario_id = scenario.SCENARIO_ID
    scenario_dir = os.path.join(base_dir, scenario_id)
    os.makedirs(scenario_dir, exist_ok=True)

    events = scenario.build_events()
    meta = scenario.build_meta()

    # ── Validate each event against the wire schema ─────────────────
    for event in events:
        validate_event(event)

    # ── Self-checks ─────────────────────────────────────────────────
    # 1. Event count matches EXPECTED_EVENT_COUNT
    assert len(events) == scenario.EXPECTED_EVENT_COUNT, (
        f"{scenario_id}: event count {len(events)} != "
        f"{scenario.EXPECTED_EVENT_COUNT}"
    )

    # 2. All event types are valid
    valid_types = {"trace-create", "span-create", "generation-create"}
    for i, e in enumerate(events):
        assert e["type"] in valid_types, (
            f"{scenario_id}[{i}]: invalid type {e['type']}"
        )

    # 3. No snake_case body keys
    for i, e in enumerate(events):
        for key in e["body"]:
            assert "_" not in key, (
                f"{scenario_id}[{i}]: snake_case body key '{key}'"
            )

    # 4. Timestamps monotonically non-decreasing
    timestamps = [e["timestamp"] for e in events]
    assert timestamps == sorted(timestamps), (
        f"{scenario_id}: timestamps not monotonic"
    )

    # 5. meta events_in_order count matches event count
    assert len(meta["events_in_order"]) == len(events), (
        f"{scenario_id}: meta events_in_order count "
        f"{len(meta['events_in_order'])} != event count {len(events)}"
    )

    # 6. meta events_in_order types match actual events
    for i, (e, m) in enumerate(zip(events, meta["events_in_order"]), 1):
        assert m["index"] == i, f"{scenario_id}: meta index mismatch at {i}"
        assert m["type"] == e["type"], (
            f"{scenario_id}[{i}]: meta type {m['type']} != event type {e['type']}"
        )

    # ── Write events.jsonl ──────────────────────────────────────────
    events_path = os.path.join(scenario_dir, "events.jsonl")
    with open(events_path, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    # ── Write meta.json ─────────────────────────────────────────────
    meta_path = os.path.join(scenario_dir, "meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
        f.write("\n")

    return {
        "scenario_id": scenario_id,
        "app_type": scenario.APP_TYPE,
        "dify_app_mode": scenario.DIFY_APP_MODE,
        "edge_case": scenario.EDGE_CASE,
        "event_count": len(events),
        "trace_types": scenario.TRACE_TYPES_EMITTED,
    }


def generate_catalog(scenarios, base_dir: str) -> None:
    """Generate catalog.json — machine-readable index of all scenarios."""
    catalog = []
    for scenario in scenarios:
        events = scenario.build_events()
        catalog.append({
            "scenario_id": scenario.SCENARIO_ID,
            "app_type": scenario.APP_TYPE,
            "dify_app_mode": scenario.DIFY_APP_MODE,
            "edge_case": scenario.EDGE_CASE,
            "event_count": len(events),
            "trace_types": scenario.TRACE_TYPES_EMITTED,
        })

    catalog_path = os.path.join(base_dir, "catalog.json")
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)
        f.write("\n")


def generate_readme(scenarios, base_dir: str) -> None:
    """Generate README.md — catalog overview and how-to-read guide."""
    total_events = sum(len(s.build_events()) for s in scenarios)

    lines = [
        "# Dify App Trace Reference Catalog",
        "",
        "A collection of 14 reference Dify app traces, captured as Langfuse wire events.",
        "",
        "## Structure",
        "",
        "Each scenario directory (`NN-slug/`) contains:",
        "- `events.jsonl` — wire events, one per line, in emission order",
        "- `meta.json` — scenario metadata",
        "",
        "Root files:",
        "- `catalog.json` — machine-readable index of all scenarios",
        "- `schema.md` — wire event field reference",
        "",
        "## Wire event format",
        "",
        "Each line in `events.jsonl` is a JSON object:",
        "```json",
        '{"id": "<uuid>", "timestamp": "<ISO8601>", "type": "trace-create|span-create|generation-create", "body": {...}}',
        "```",
        "",
        "See `schema.md` for the full field reference.",
        "",
        "## Scenarios",
        "",
        "| # | Directory | App Type | Events | Edge? |",
        "|---|---|---|---|---|",
    ]

    for s in scenarios:
        events = s.build_events()
        edge = s.EDGE_CASE or ""
        num = s.SCENARIO_ID.split("-")[0]
        lines.append(
            f"| {num} | `{s.SCENARIO_ID}` | {s.APP_TYPE} | {len(events)} | {edge} |"
        )

    lines.extend([
        "",
        f"**Total**: {total_events} events across {len(scenarios)} scenarios.",
        "",
        "## Provenance",
        "",
        "- Dify commit: `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`",
        "- Langfuse SDK: `>=4.2.0,<5.0.0`",
    ])

    readme_path = os.path.join(base_dir, "README.md")
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def generate_schema_doc(base_dir: str) -> None:
    """Generate schema.md — wire event field reference."""
    content = """# Langfuse Wire Event Schema

This document describes the wire event format used in the Dify trace catalog.

## Event Envelope

Every event in `events.jsonl` has this structure:

```json
{
  "id": "<uuid v4>",
  "timestamp": "<ISO 8601 UTC, e.g. 2025-01-15T10:30:00.123456+00:00>",
  "type": "trace-create" | "span-create" | "generation-create",
  "body": { ... type-specific ... }
}
```

- `id`: event ID (UUID, generated by Dify's `_make_event_id()`)
- `timestamp`: event creation time (ISO 8601 UTC, from Dify's `_now_iso()`)
- `type`: one of `trace-create`, `span-create`, `generation-create`
- `body`: type-specific payload (see below)

## trace-create body

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
  "public": "<bool>"
}
```

All fields optional; only populated fields appear on the wire.

## span-create body

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

## generation-create body

Extends span-create with:

```json
{
  "completionStartTime": "<ISO 8601>",
  "model": "<string>",
  "modelParameters": { "<key>": "<value>" },
  "usageDetails": {
    "input": "<int>",
    "output": "<int>",
    "total": "<int>",
    "unit": "CHARACTERS" | "TOKENS",
    "inputCost": "<float>",
    "outputCost": "<float>",
    "totalCost": "<float>"
  },
  "costDetails": { "<key>": "<float>" },
  "promptName": "<string>",
  "promptVersion": "<int>"
}
```

## Serialization rules

- **camelCase**: all body field names are camelCase on the wire.
- **exclude_unset + exclude_none**: only fields with non-None values appear.
- **1 event per POST**: Dify sends one event per HTTP POST to `/api/public/ingestion`.

## Dify trace type to wire event mapping

| Dify TraceInfo type | Wire events | Handler |
|---|---|---|
| MessageTraceInfo | 1 trace-create + 1 generation-create | `LangFuseDataTrace.message_trace` |
| WorkflowTraceInfo | 1 trace-create + 1 span-create + K node events | `LangFuseDataTrace.workflow_trace` |
| ModerationTraceInfo | 1 span-create | `LangFuseDataTrace.moderation_trace` |
| DatasetRetrievalTraceInfo | 1 span-create | `LangFuseDataTrace.dataset_retrieval_trace` |
| ToolTraceInfo | 1 span-create per tool call | `LangFuseDataTrace.tool_trace` |
| GenerateNameTraceInfo | 1 trace-create + 1 span-create | `LangFuseDataTrace.generate_name_trace` |
| SuggestedQuestionTraceInfo | 1 generation-create | `LangFuseDataTrace.suggested_question_trace` |
"""
    schema_path = os.path.join(base_dir, "schema.md")
    with open(schema_path, "w", encoding="utf-8") as f:
        f.write(content)


def main():
    base_dir = _BASE_DIR

    print("Generating Dify trace reference catalog...")
    catalog_entries = []
    for scenario in SCENARIOS:
        entry = generate_scenario(scenario, base_dir)
        catalog_entries.append(entry)
        print(f"  {entry['scenario_id']}: {entry['event_count']} events")

    # Generate root files
    generate_catalog(SCENARIOS, base_dir)
    print("  catalog.json")
    generate_readme(SCENARIOS, base_dir)
    print("  README.md")
    generate_schema_doc(base_dir)
    print("  schema.md")

    total_events = sum(e["event_count"] for e in catalog_entries)
    print(f"\nTotal: {total_events} events across {len(SCENARIOS)} scenarios")

    # Verify all 7 trace types are represented
    all_types = set()
    for s in SCENARIOS:
        all_types.update(s.TRACE_TYPES_EMITTED)
    expected_types = {
        "MessageTraceInfo", "WorkflowTraceInfo", "ModerationTraceInfo",
        "DatasetRetrievalTraceInfo", "ToolTraceInfo", "GenerateNameTraceInfo",
        "SuggestedQuestionTraceInfo",
    }
    assert all_types == expected_types, (
        f"Missing trace types: {expected_types - all_types}"
    )

    # Verify all 5 Dify app modes are represented
    all_modes = {s.DIFY_APP_MODE for s in SCENARIOS}
    expected_modes = {"chat", "completion", "agent-chat", "workflow", "advanced-chat"}
    assert all_modes == expected_modes, (
        f"Missing app modes: {expected_modes - all_modes}"
    )

    print("All self-checks passed.")


if __name__ == "__main__":
    main()
