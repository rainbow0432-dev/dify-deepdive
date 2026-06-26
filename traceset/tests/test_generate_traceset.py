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


def test_generate_catalog(tmp_path):
    from traceset.generate_traceset import generate_catalog
    generate_catalog(SCENARIOS, str(tmp_path))
    catalog_path = tmp_path / "catalog.json"
    assert catalog_path.exists()
    catalog = json.loads(catalog_path.read_text())
    assert len(catalog) == 14
    entry = catalog[0]
    assert "scenario_id" in entry
    assert "app_type" in entry
    assert "event_count" in entry
    assert "trace_types" in entry


def test_generate_readme(tmp_path):
    from traceset.generate_traceset import generate_readme
    generate_readme(SCENARIOS, str(tmp_path))
    readme_path = tmp_path / "README.md"
    assert readme_path.exists()
    content = readme_path.read_text()
    assert "Dify App Trace Reference Catalog" in content
    assert "01-chat-basic" in content
    assert "14-message-streaming" in content


def test_generate_schema_doc(tmp_path):
    from traceset.generate_traceset import generate_schema_doc
    generate_schema_doc(str(tmp_path))
    schema_path = tmp_path / "schema.md"
    assert schema_path.exists()
    content = schema_path.read_text()
    assert "trace-create" in content
    assert "span-create" in content
    assert "generation-create" in content
    assert "usageDetails" in content
    assert "camelCase" in content


def test_main_generates_all_files(tmp_path):
    """Run main() in a temp dir and verify all outputs exist."""
    import importlib
    from traceset import generate_traceset as gt
    gt._BASE_DIR = str(tmp_path)
    gt.main()

    # Root files
    assert (tmp_path / "catalog.json").exists()
    assert (tmp_path / "README.md").exists()
    assert (tmp_path / "schema.md").exists()

    # All 14 scenario directories
    for s in SCENARIOS:
        scenario_dir = tmp_path / s.SCENARIO_ID
        assert (scenario_dir / "events.jsonl").exists(), f"Missing events.jsonl for {s.SCENARIO_ID}"
        assert (scenario_dir / "meta.json").exists(), f"Missing meta.json for {s.SCENARIO_ID}"
