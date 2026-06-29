"""Scenario 01: Basic chatbot, single-turn Q&A, no extras.

Events (4):
  1. trace-create       (MessageTraceInfo)
  2. generation-create  (MessageTraceInfo)
  3. trace-create       (GenerateNameTraceInfo, upsert)
  4. span-create        (GenerateNameTraceInfo)

This scenario is the COMPLETE REFERENCE TEMPLATE — all field values are
shown explicitly below. Other scenarios use the same helper functions
with different values.
"""
from traceset.helpers import (
    make_event_id,
    make_timestamp,
    make_trace_create,
    make_span_create,
    make_generation_create,
    wrap_event,
)

SCENARIO_ID = "01-chat-basic"
SCENARIO_DESCRIPTION = "Basic chatbot, single-turn Q&A, no extras"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 4

_BASE = "2025-01-15T10:30:00.000000+00:00"

_TRACE = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"

_USER = "u-7f3a2b8c4d"
_CONV = "conv-2b3c4d5e6f7a"

_GEN = "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"

_NAME_SPAN = "c3d4e5f6-a7b8-4c9d-0e1f-2a3b4c5d6e7f"

_USER_QUERY = "What are the key differences between Redis and Memcached?"

_LLM_RESPONSE = (
    "Redis and Memcached are both in-memory key-value stores, but they differ "
    "in several key areas. Redis supports diverse data structures (lists, sets, "
    "sorted sets, hashes, streams) while Memcached only supports strings. "
    "Redis offers built-in persistence via RDB snapshots and AOF logs; Memcached "
    "has no persistence. Redis is single-threaded (multiplexed I/O); Memcached is "
    "multi-threaded. Redis supports pub/sub, Lua scripting, and clustering; "
    "Memcached is simpler and excels at raw cache performance for simple key-value "
    "lookups."
)

_CONV_NAME = "Redis vs Memcached Comparison"

_MODEL = "gpt-4o-mini"
_MODEL_PARAMS = {"temperature": 0.7, "max_tokens": 500}
_USAGE = {
    "input": 42,
    "output": 156,
    "total": 198,
    "unit": "TOKENS",
    "inputCost": 0.000063,
    "outputCost": 0.000234,
    "totalCost": 0.000297,
}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s01-e01"),
        timestamp=make_timestamp(_BASE, 2.123),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE,
            name="Dify Chatbot",
            user_id=_USER,
            input={"query": _USER_QUERY},
            session_id=_CONV,
            metadata={
                "user_id": _USER,
                "conversation_id": _CONV,
            },
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s01-e02"),
        timestamp=make_timestamp(_BASE, 2.124),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_GEN,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 2.023),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.250),
            model_parameters=_MODEL_PARAMS,
            input={
                "messages": [
                    {"role": "user", "content": _USER_QUERY}
                ]
            },
            output={"text": _LLM_RESPONSE},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s01-e03"),
        timestamp=make_timestamp(_BASE, 3.500),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE,
            name="Generate Name",
            user_id=_USER,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s01-e04"),
        timestamp=make_timestamp(_BASE, 3.501),
        event_type="span-create",
        body=make_span_create(
            span_id=_NAME_SPAN,
            trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 3.000),
            end_time=make_timestamp(_BASE, 3.450),
            input={
                "messages": [
                    {"role": "user", "content": _USER_QUERY}
                ]
            },
            output={"text": _CONV_NAME},
        ),
    ))

    return events


def build_meta():
    return {
        "scenario_id": SCENARIO_ID,
        "scenario_description": SCENARIO_DESCRIPTION,
        "app_type": APP_TYPE,
        "dify_app_mode": DIFY_APP_MODE,
        "edge_case": EDGE_CASE,
        "trace_types_emitted": TRACE_TYPES_EMITTED,
        "expected_event_count": EXPECTED_EVENT_COUNT,
        "events_in_order": [
            {
                "index": 1,
                "type": "trace-create",
                "source_trace_type": "MessageTraceInfo",
                "dify_handler": "LangFuseDataTrace.message_trace",
            },
            {
                "index": 2,
                "type": "generation-create",
                "source_trace_type": "MessageTraceInfo",
                "dify_handler": "LangFuseDataTrace.message_trace",
            },
            {
                "index": 3,
                "type": "trace-create",
                "source_trace_type": "GenerateNameTraceInfo",
                "dify_handler": "LangFuseDataTrace.generate_name_trace",
            },
            {
                "index": 4,
                "type": "span-create",
                "source_trace_type": "GenerateNameTraceInfo",
                "dify_handler": "LangFuseDataTrace.generate_name_trace",
            },
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Simplest chat run. GenerateName upserts the same trace ID.",
    }
