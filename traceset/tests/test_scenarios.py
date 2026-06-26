"""Tests for all scenario modules."""
import pytest

from traceset.helpers import wrap_event


def _check_scenario(module):
    """Assert a scenario module's events and meta are valid."""
    events = module.build_events()
    assert len(events) == module.EXPECTED_EVENT_COUNT, (
        f"{module.SCENARIO_ID}: expected {module.EXPECTED_EVENT_COUNT} events, "
        f"got {len(events)}"
    )
    valid_types = {"trace-create", "span-create", "generation-create"}
    for i, e in enumerate(events):
        assert e["type"] in valid_types, f"{module.SCENARIO_ID}[{i}]: bad type {e['type']}"
        assert "id" in e and "timestamp" in e and "body" in e, (
            f"{module.SCENARIO_ID}[{i}]: missing envelope field"
        )
        for key in e["body"]:
            assert "_" not in key, (
                f"{module.SCENARIO_ID}[{i}]: snake_case body key '{key}'"
            )
    timestamps = [e["timestamp"] for e in events]
    assert timestamps == sorted(timestamps), (
        f"{module.SCENARIO_ID}: timestamps not monotonic"
    )
    meta = module.build_meta()
    assert meta["scenario_id"] == module.SCENARIO_ID
    assert meta["expected_event_count"] == module.EXPECTED_EVENT_COUNT
    assert len(meta["events_in_order"]) == len(events)
    for i, (e, m) in enumerate(zip(events, meta["events_in_order"]), 1):
        assert m["index"] == i
        assert m["type"] == e["type"], (
            f"{module.SCENARIO_ID}[{i}]: meta type {m['type']} != event type {e['type']}"
        )


def test_s01_chat_basic():
    from traceset.scenarios import s01_chat_basic
    _check_scenario(s01_chat_basic)
