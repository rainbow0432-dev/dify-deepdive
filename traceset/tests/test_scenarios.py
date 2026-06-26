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


def test_s02_chat_rag():
    from traceset.scenarios import s02_chat_rag
    _check_scenario(s02_chat_rag)


def test_s03_completion_basic():
    from traceset.scenarios import s03_completion_basic
    _check_scenario(s03_completion_basic)


def test_s04_agent_single_tool():
    from traceset.scenarios import s04_agent_single_tool
    _check_scenario(s04_agent_single_tool)


def test_s05_agent_multi_tool():
    from traceset.scenarios import s05_agent_multi_tool
    _check_scenario(s05_agent_multi_tool)


def test_s06_workflow_5node():
    from traceset.scenarios import s06_workflow_5node
    _check_scenario(s06_workflow_5node)


def test_s07_workflow_15node():
    from traceset.scenarios import s07_workflow_15node
    _check_scenario(s07_workflow_15node)


def test_s08_chatflow_basic():
    from traceset.scenarios import s08_chatflow_basic
    _check_scenario(s08_chatflow_basic)


def test_s09_moderation_blocked():
    from traceset.scenarios import s09_moderation_blocked
    _check_scenario(s09_moderation_blocked)


def test_s10_moderation_pass_through():
    from traceset.scenarios import s10_moderation_pass_through
    _check_scenario(s10_moderation_pass_through)


def test_s11_rag_empty_results():
    from traceset.scenarios import s11_rag_empty_results
    _check_scenario(s11_rag_empty_results)


def test_s12_tool_failure():
    from traceset.scenarios import s12_tool_failure
    _check_scenario(s12_tool_failure)


def test_s13_suggested_questions_error():
    from traceset.scenarios import s13_suggested_questions_error
    _check_scenario(s13_suggested_questions_error)


def test_s14_message_streaming():
    from traceset.scenarios import s14_message_streaming
    _check_scenario(s14_message_streaming)
