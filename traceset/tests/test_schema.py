"""Tests for the wire schema validator."""
import pytest
from traceset.schema import validate_event


def _valid_trace_create():
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "timestamp": "2025-01-15T10:30:00.000000+00:00",
        "type": "trace-create",
        "body": {"id": "trace-1", "name": "test"},
    }


def _valid_span_create():
    return {
        "id": "00000000-0000-0000-0000-000000000002",
        "timestamp": "2025-01-15T10:30:00.000000+00:00",
        "type": "span-create",
        "body": {
            "id": "span-1",
            "traceId": "trace-1",
            "name": "test-span",
            "startTime": "2025-01-15T10:30:00.000000+00:00",
            "endTime": "2025-01-15T10:30:01.000000+00:00",
        },
    }


def _valid_generation_create():
    return {
        "id": "00000000-0000-0000-0000-000000000003",
        "timestamp": "2025-01-15T10:30:00.000000+00:00",
        "type": "generation-create",
        "body": {
            "id": "gen-1",
            "traceId": "trace-1",
            "name": "gpt-4o-mini",
            "startTime": "2025-01-15T10:30:00.000000+00:00",
            "endTime": "2025-01-15T10:30:01.000000+00:00",
            "model": "gpt-4o-mini",
            "usageDetails": {
                "input": 10,
                "output": 20,
                "total": 30,
                "unit": "TOKENS",
            },
        },
    }


def test_validate_trace_create():
    validate_event(_valid_trace_create())


def test_validate_span_create():
    validate_event(_valid_span_create())


def test_validate_generation_create():
    validate_event(_valid_generation_create())


def test_rejects_snake_case_body_key():
    event = _valid_span_create()
    event["body"]["status_message"] = "error"
    with pytest.raises(ValueError, match="snake_case"):
        validate_event(event)


def test_rejects_invalid_type():
    event = _valid_trace_create()
    event["type"] = "invalid-type"
    with pytest.raises(ValueError, match="invalid event type"):
        validate_event(event)


def test_rejects_missing_envelope_field():
    event = _valid_trace_create()
    del event["timestamp"]
    with pytest.raises(ValueError, match="missing envelope field"):
        validate_event(event)


def test_rejects_missing_required_body_field():
    event = _valid_trace_create()
    del event["body"]["name"]
    with pytest.raises(ValueError, match="missing required field"):
        validate_event(event)


def test_rejects_non_dict_body():
    event = _valid_trace_create()
    event["body"] = "not a dict"
    with pytest.raises(ValueError, match="body must be a dict"):
        validate_event(event)
