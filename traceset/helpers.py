"""Event construction helpers for Langfuse wire events.

All make_*_create functions return body dicts with camelCase keys.
wrap_event wraps a body dict into the full event envelope.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta


def make_event_id(seed: str) -> str:
    """Deterministic UUID (v5) from a seed string."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def make_timestamp(base: str, offset_seconds: float = 0.0) -> str:
    """ISO 8601 UTC timestamp from a base time + offset in seconds."""
    dt = datetime.fromisoformat(base) + timedelta(seconds=offset_seconds)
    return dt.isoformat(timespec="microseconds")


def to_camel_case(snake: str) -> str:
    """Convert snake_case to camelCase."""
    parts = snake.split("_")
    return parts[0] + "".join(p.title() for p in parts[1:])


def make_trace_create(
    trace_id: str,
    name: str,
    user_id: str | None = None,
    **kwargs,
) -> dict:
    """Build a trace-create body dict with camelCase keys."""
    body: dict = {"id": trace_id, "name": name}
    if user_id is not None:
        body["userId"] = user_id
    for k, v in kwargs.items():
        body[to_camel_case(k)] = v
    return body


def make_span_create(
    span_id: str,
    trace_id: str,
    name: str,
    start_time: str,
    end_time: str,
    **kwargs,
) -> dict:
    """Build a span-create body dict with camelCase keys."""
    body: dict = {
        "id": span_id,
        "traceId": trace_id,
        "name": name,
        "startTime": start_time,
        "endTime": end_time,
    }
    for k, v in kwargs.items():
        body[to_camel_case(k)] = v
    return body


def make_generation_create(
    gen_id: str,
    trace_id: str,
    name: str,
    model: str,
    start_time: str,
    end_time: str,
    usage: dict,
    **kwargs,
) -> dict:
    """Build a generation-create body dict with camelCase keys."""
    body: dict = {
        "id": gen_id,
        "traceId": trace_id,
        "name": name,
        "startTime": start_time,
        "endTime": end_time,
        "model": model,
        "usageDetails": usage,
    }
    for k, v in kwargs.items():
        body[to_camel_case(k)] = v
    return body


def wrap_event(
    event_id: str,
    timestamp: str,
    event_type: str,
    body: dict,
) -> dict:
    """Wrap a body dict into the full event envelope."""
    return {
        "id": event_id,
        "timestamp": timestamp,
        "type": event_type,
        "body": body,
    }
