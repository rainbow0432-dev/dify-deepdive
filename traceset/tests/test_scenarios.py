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
    span_count = sum(
        1 for e in events if e["type"] in ("span-create", "generation-create")
    )
    assert span_count == module.EXPECTED_SPAN_COUNT, (
        f"{module.SCENARIO_ID}: expected {module.EXPECTED_SPAN_COUNT} spans, "
        f"got {span_count}"
    )
    valid_types = {"trace-create", "span-create", "generation-create"}
    for i, e in enumerate(events):
        assert e["type"] in valid_types, (
            f"{module.SCENARIO_ID}[{i}]: bad type {e['type']}"
        )
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
    assert meta["expected_span_count"] == module.EXPECTED_SPAN_COUNT
    assert meta["span_pattern"] == module.SPAN_PATTERN
    assert len(meta["events_in_order"]) == len(events)
    for i, (e, m) in enumerate(zip(events, meta["events_in_order"]), 1):
        assert m["index"] == i
        assert m["type"] == e["type"], (
            f"{module.SCENARIO_ID}[{i}]: meta type {m['type']} != event type {e['type']}"
        )


def test_s01_linear_llm_chain():
    from traceset.scenarios import s01_linear_llm_chain
    _check_scenario(s01_linear_llm_chain)


def test_s02_parallel_branches():
    from traceset.scenarios import s02_parallel_branches
    _check_scenario(s02_parallel_branches)


def test_s03_agent_react_loop():
    from traceset.scenarios import s03_agent_react_loop
    _check_scenario(s03_agent_react_loop)


def test_s04_multi_tool_chain():
    from traceset.scenarios import s04_multi_tool_chain
    _check_scenario(s04_multi_tool_chain)


def test_s05_rag_multi_hop():
    from traceset.scenarios import s05_rag_multi_hop
    _check_scenario(s05_rag_multi_hop)


def test_s06_moderation_rag_tool_combo():
    from traceset.scenarios import s06_moderation_rag_tool_combo
    _check_scenario(s06_moderation_rag_tool_combo)


def test_s07_workflow_conditional():
    from traceset.scenarios import s07_workflow_conditional
    _check_scenario(s07_workflow_conditional)


def test_s08_error_recovery_agent():
    from traceset.scenarios import s08_error_recovery_agent
    _check_scenario(s08_error_recovery_agent)


def test_s09_nested_workflow():
    from traceset.scenarios import s09_nested_workflow
    _check_scenario(s09_nested_workflow)


def test_s10_workflow_error_propagation():
    from traceset.scenarios import s10_workflow_error_propagation
    _check_scenario(s10_workflow_error_propagation)


def test_s11_streaming_chatflow():
    from traceset.scenarios import s11_streaming_chatflow
    _check_scenario(s11_streaming_chatflow)


def test_s12_multi_model_pipeline():
    from traceset.scenarios import s12_multi_model_pipeline
    _check_scenario(s12_multi_model_pipeline)


def test_s13_completion_multi_feature():
    from traceset.scenarios import s13_completion_multi_feature
    _check_scenario(s13_completion_multi_feature)
