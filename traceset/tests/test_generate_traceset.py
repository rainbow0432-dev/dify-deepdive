"""Tests for the generation script."""
import json
import os
import pytest

from traceset.scenarios import SCENARIOS
from traceset.generate_traceset import generate_scenario


def test_scenarios_registry_has_13():
    assert len(SCENARIOS) == 13, f"Expected 13 scenarios, got {len(SCENARIOS)}"


def test_all_scenario_ids_unique():
    ids = [s.SCENARIO_ID for s in SCENARIOS]
    assert len(ids) == len(set(ids)), f"Duplicate scenario IDs: {ids}"


def test_all_scenarios_have_span_count():
    for s in SCENARIOS:
        assert hasattr(s, "EXPECTED_SPAN_COUNT"), f"{s.SCENARIO_ID} missing EXPECTED_SPAN_COUNT"
        assert hasattr(s, "SPAN_PATTERN"), f"{s.SCENARIO_ID} missing SPAN_PATTERN"


def test_generate_scenario_writes_files(tmp_path):
    from traceset.scenarios import s01_linear_llm_chain
    generate_scenario(s01_linear_llm_chain, str(tmp_path))

    scenario_dir = tmp_path / "01-linear-llm-chain"
    assert scenario_dir.exists()

    events_path = scenario_dir / "events.jsonl"
    assert events_path.exists()
    lines = events_path.read_text().strip().split("\n")
    assert len(lines) == 14

    for line in lines:
        event = json.loads(line)
        assert "id" in event
        assert "timestamp" in event
        assert "type" in event
        assert "body" in event

    meta_path = scenario_dir / "meta.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta["scenario_id"] == "01-linear-llm-chain"
    assert meta["expected_event_count"] == 14
    assert meta["expected_span_count"] == 13
    assert meta["span_pattern"] == "linear"


def test_generate_scenario_self_checks(tmp_path):
    from traceset.scenarios import s09_nested_workflow
    generate_scenario(s09_nested_workflow, str(tmp_path))
    assert (tmp_path / "09-nested-workflow" / "events.jsonl").exists()


def test_generate_all_scenarios(tmp_path):
    for scenario in SCENARIOS:
        generate_scenario(scenario, str(tmp_path))
        scenario_dir = tmp_path / scenario.SCENARIO_ID
        assert scenario_dir.exists(), f"Missing dir for {scenario.SCENARIO_ID}"
        events_path = scenario_dir / "events.jsonl"
        lines = events_path.read_text().strip().split("\n")
        assert len(lines) == scenario.EXPECTED_EVENT_COUNT


def test_generate_catalog(tmp_path):
    from traceset.generate_traceset import generate_catalog
    generate_catalog(SCENARIOS, str(tmp_path))
    catalog_path = tmp_path / "catalog.json"
    assert catalog_path.exists()
    catalog = json.loads(catalog_path.read_text())
    assert len(catalog) == 13
    entry = catalog[0]
    assert "scenario_id" in entry
    assert "app_type" in entry
    assert "event_count" in entry
    assert "span_count" in entry
    assert "span_pattern" in entry
    assert "trace_types" in entry


def test_generate_readme(tmp_path):
    from traceset.generate_traceset import generate_readme
    generate_readme(SCENARIOS, str(tmp_path))
    readme_path = tmp_path / "README.md"
    assert readme_path.exists()
    content = readme_path.read_text()
    assert "Dify App Trace Reference Catalog" in content
    assert "01-linear-llm-chain" in content
    assert "13-completion-multi-feature" in content


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
    import importlib
    from traceset import generate_traceset as gt
    gt._BASE_DIR = str(tmp_path)
    gt.main()

    assert (tmp_path / "catalog.json").exists()
    assert (tmp_path / "README.md").exists()
    assert (tmp_path / "schema.md").exists()

    for s in SCENARIOS:
        scenario_dir = tmp_path / s.SCENARIO_ID
        assert (scenario_dir / "events.jsonl").exists(), f"Missing events.jsonl for {s.SCENARIO_ID}"
        assert (scenario_dir / "meta.json").exists(), f"Missing meta.json for {s.SCENARIO_ID}"
