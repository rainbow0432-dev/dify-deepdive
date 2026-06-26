"""Tests for the generation script."""
import json
import os
import tempfile
import pytest

from traceset.scenarios import SCENARIOS
from traceset.generate_traceset import generate_scenario


def test_scenarios_registry_has_14():
    assert len(SCENARIOS) == 14, f"Expected 14 scenarios, got {len(SCENARIOS)}"


def test_all_scenario_ids_unique():
    ids = [s.SCENARIO_ID for s in SCENARIOS]
    assert len(ids) == len(set(ids)), f"Duplicate scenario IDs: {ids}"


def test_generate_scenario_writes_files(tmp_path):
    from traceset.scenarios import s01_chat_basic
    generate_scenario(s01_chat_basic, str(tmp_path))

    scenario_dir = tmp_path / "01-chat-basic"
    assert scenario_dir.exists()

    events_path = scenario_dir / "events.jsonl"
    assert events_path.exists()
    lines = events_path.read_text().strip().split("\n")
    assert len(lines) == 4

    for line in lines:
        event = json.loads(line)
        assert "id" in event
        assert "timestamp" in event
        assert "type" in event
        assert "body" in event

    meta_path = scenario_dir / "meta.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta["scenario_id"] == "01-chat-basic"
    assert meta["expected_event_count"] == 4


def test_generate_scenario_self_checks(tmp_path):
    """Verify the self-checks (event count, monotonic, no snake_case) pass."""
    from traceset.scenarios import s07_workflow_15node
    # This should not raise
    generate_scenario(s07_workflow_15node, str(tmp_path))
    assert (tmp_path / "07-workflow-15node" / "events.jsonl").exists()


def test_generate_all_scenarios(tmp_path):
    for scenario in SCENARIOS:
        generate_scenario(scenario, str(tmp_path))
        scenario_dir = tmp_path / scenario.SCENARIO_ID
        assert scenario_dir.exists(), f"Missing dir for {scenario.SCENARIO_ID}"
        events_path = scenario_dir / "events.jsonl"
        lines = events_path.read_text().strip().split("\n")
        assert len(lines) == scenario.EXPECTED_EVENT_COUNT, (
            f"{scenario.SCENARIO_ID}: {len(lines)} events != {scenario.EXPECTED_EVENT_COUNT}"
        )
