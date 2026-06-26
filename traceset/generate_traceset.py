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


def main():
    base_dir = _BASE_DIR

    print("Generating Dify trace reference catalog...")
    catalog_entries = []
    for scenario in SCENARIOS:
        entry = generate_scenario(scenario, base_dir)
        catalog_entries.append(entry)
        print(f"  {entry['scenario_id']}: {entry['event_count']} events")

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
