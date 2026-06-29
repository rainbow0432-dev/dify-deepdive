"""Tests for event construction helpers."""
import uuid
import pytest
from traceset.helpers import (
    make_event_id,
    make_timestamp,
    make_trace_create,
    make_span_create,
    make_generation_create,
    wrap_event,
    to_camel_case,
)


def test_make_event_id_deterministic():
    assert make_event_id("seed-1") == make_event_id("seed-1")


def test_make_event_id_different_seeds():
    assert make_event_id("seed-1") != make_event_id("seed-2")


def test_make_event_id_valid_uuid():
    id_str = make_event_id("test")
    parsed = uuid.UUID(id_str)
    assert str(parsed) == id_str


def test_make_timestamp_no_offset():
    ts = make_timestamp("2025-01-15T10:30:00.000000+00:00", 0.0)
    assert ts == "2025-01-15T10:30:00.000000+00:00"


def test_make_timestamp_with_offset():
    ts = make_timestamp("2025-01-15T10:30:00.000000+00:00", 1.5)
    assert ts == "2025-01-15T10:30:01.500000+00:00"


def test_to_camel_case():
    assert to_camel_case("user_id") == "userId"
    assert to_camel_case("session_id") == "sessionId"
    assert to_camel_case("parent_observation_id") == "parentObservationId"
    assert to_camel_case("completion_start_time") == "completionStartTime"
    assert to_camel_case("name") == "name"
    assert to_camel_case("model_parameters") == "modelParameters"


def test_make_trace_create_basic():
    body = make_trace_create(trace_id="t1", name="test", user_id="u1")
    assert body == {"id": "t1", "name": "test", "userId": "u1"}


def test_make_trace_create_no_user_id():
    body = make_trace_create(trace_id="t1", name="test")
    assert "userId" not in body


def test_make_trace_create_with_kwargs():
    body = make_trace_create(
        trace_id="t1", name="test", user_id="u1",
        session_id="s1", input={"q": "hello"}, metadata={"k": "v"},
    )
    assert body["sessionId"] == "s1"
    assert body["input"] == {"q": "hello"}
    assert body["metadata"] == {"k": "v"}


def test_make_span_create():
    body = make_span_create(
        span_id="sp1", trace_id="t1", name="span",
        start_time="2025-01-15T10:30:00.000000+00:00",
        end_time="2025-01-15T10:30:01.000000+00:00",
        parent_observation_id="parent-1",
        input={"q": "in"}, output={"r": "out"},
    )
    assert body["id"] == "sp1"
    assert body["traceId"] == "t1"
    assert body["startTime"] == "2025-01-15T10:30:00.000000+00:00"
    assert body["endTime"] == "2025-01-15T10:30:01.000000+00:00"
    assert body["parentObservationId"] == "parent-1"
    assert body["input"] == {"q": "in"}
    assert body["output"] == {"r": "out"}


def test_make_generation_create():
    usage = {"input": 10, "output": 20, "total": 30, "unit": "TOKENS"}
    body = make_generation_create(
        gen_id="g1", trace_id="t1", name="gpt-4o-mini",
        model="gpt-4o-mini",
        start_time="2025-01-15T10:30:00.000000+00:00",
        end_time="2025-01-15T10:30:01.000000+00:00",
        usage=usage,
        completion_start_time="2025-01-15T10:30:00.100000+00:00",
        model_parameters={"temperature": 0.7},
        input={"messages": []}, output={"text": "hello"},
    )
    assert body["model"] == "gpt-4o-mini"
    assert body["usageDetails"] == usage
    assert body["completionStartTime"] == "2025-01-15T10:30:00.100000+00:00"
    assert body["modelParameters"] == {"temperature": 0.7}
    assert body["input"] == {"messages": []}
    assert body["output"] == {"text": "hello"}


def test_wrap_event():
    body = {"id": "t1", "name": "test"}
    event = wrap_event("e1", "2025-01-15T10:30:00.000000+00:00", "trace-create", body)
    assert event == {
        "id": "e1",
        "timestamp": "2025-01-15T10:30:00.000000+00:00",
        "type": "trace-create",
        "body": body,
    }
