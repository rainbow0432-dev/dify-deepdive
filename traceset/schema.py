"""Wire schema validator for Langfuse ingestion events.

Tries the langfuse SDK's Pydantic models first (Option A).
Falls back to a local validator (Option B) if the SDK is unavailable.
"""
from __future__ import annotations

_ENVELOPE_REQUIRED = {"id", "timestamp", "type", "body"}
_VALID_TYPES = {"trace-create", "span-create", "generation-create"}

_TRACE_BODY_REQUIRED = {"id", "name"}
_SPAN_BODY_REQUIRED = {"id", "traceId", "name", "startTime", "endTime"}
_GEN_BODY_REQUIRED = {
    "id", "traceId", "name", "startTime", "endTime", "model", "usageDetails",
}

_REQ_MAP = {
    "trace-create": _TRACE_BODY_REQUIRED,
    "span-create": _SPAN_BODY_REQUIRED,
    "generation-create": _GEN_BODY_REQUIRED,
}


def validate_event(event: dict) -> None:
    """Validate an event dict against the Langfuse wire schema.

    Raises ValueError on any schema violation.
    """
    _local_validate(event)
    try:
        _validate_with_sdk(event)
    except ImportError:
        pass


def _validate_with_sdk(event: dict) -> None:
    """Validate using langfuse SDK Pydantic models. Raises ImportError if SDK absent."""
    from langfuse.api import (
        IngestionEvent_TraceCreate,
        IngestionEvent_SpanCreate,
        IngestionEvent_GenerationCreate,
    )

    etype = event.get("type")
    if etype == "trace-create":
        IngestionEvent_TraceCreate.model_validate(event)
    elif etype == "span-create":
        IngestionEvent_SpanCreate.model_validate(event)
    elif etype == "generation-create":
        IngestionEvent_GenerationCreate.model_validate(event)
    else:
        raise ValueError(f"invalid event type: {etype}")


def _local_validate(event: dict) -> None:
    """Fallback local validator: checks envelope, type, required fields, camelCase."""
    for field in _ENVELOPE_REQUIRED:
        if field not in event:
            raise ValueError(f"missing envelope field: {field}")

    etype = event["type"]
    if etype not in _VALID_TYPES:
        raise ValueError(f"invalid event type: {etype}")

    body = event["body"]
    if not isinstance(body, dict):
        raise ValueError("body must be a dict")

    required = _REQ_MAP[etype]
    for field in required:
        if field not in body:
            raise ValueError(f"{etype} body missing required field: {field}")

    for key in body:
        if "_" in key:
            raise ValueError(f"snake_case key in {etype} body: {key}")
