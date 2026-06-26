"""Scenario 10: Moderation passes, normal chat proceeds.

Events (5):
  1. span-create        (ModerationTraceInfo — passed)
  2. trace-create       (MessageTraceInfo)
  3. generation-create  (MessageTraceInfo)
  4. trace-create       (GenerateNameTraceInfo, upsert)
  5. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "10-moderation-pass-through"
SCENARIO_DESCRIPTION = "Chatbot with moderation that passes, normal chat proceeds"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["ModerationTraceInfo", "MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 5

_BASE = "2025-01-15T15:00:00.000000+00:00"
_TRACE = "a4b5c6d7-e8f9-4a0b-1c2d-3e4f5a6b7c8d"
_USER = "u-8b9c0d1e2f"
_CONV = "conv-0d1e2f3a4b5c"
_MOD_SPAN = "b5c6d7e8-f9a0-4b1c-2d3e-4f5a6b7c8d9e"
_GEN = "c6d7e8f9-a0b1-4c2d-3e4f-5a6b7c8d9e0f"
_NAME_SPAN = "d7e8f9a0-b1c2-4d3e-4f5a-6b7c8d9e0f1a"

_QUERY = "What are the safety considerations for rock climbing?"
_LLM_RESPONSE = (
    "Key safety considerations for rock climbing: 1) Always wear a helmet "
    "and harness. 2) Check all gear before each climb. 3) Use proper "
    "communication signals between climber and belayer. 4) Inspect anchors "
    "and knots. 5) Know your limits and don't push beyond your skill level. "
    "6) Check weather conditions. 7) Have a first aid kit and emergency plan."
)
_CONV_NAME = "Rock Climbing Safety"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 400}
_USAGE = {"input": 42, "output": 130, "total": 172, "unit": "TOKENS",
          "inputCost": 0.000006, "outputCost": 0.000078, "totalCost": 0.000084}


def build_events():
    events = []

    # 1. span-create (Moderation — passed)
    events.append(wrap_event(
        make_event_id("s10-e01"), make_timestamp(_BASE, 2.800),
        "span-create",
        make_span_create(
            span_id=_MOD_SPAN, trace_id=_TRACE,
            name="input_moderation",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.200),
            input={"query": _QUERY},
            output={"flagged": False, "action": "pass"},
        ),
    ))

    # 2. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s10-e02"), make_timestamp(_BASE, 2.801),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV},
        ),
    ))

    # 3. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s10-e03"), make_timestamp(_BASE, 2.802),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.250),
            end_time=make_timestamp(_BASE, 2.700),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.400),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 4. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s10-e04"), make_timestamp(_BASE, 4.000),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 5. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s10-e05"), make_timestamp(_BASE, 4.001),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 3.600),
            end_time=make_timestamp(_BASE, 3.950),
            input={"messages": [{"role": "user", "content": _QUERY}]},
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
            {"index": 1, "type": "span-create", "source_trace_type": "ModerationTraceInfo", "dify_handler": "LangFuseDataTrace.moderation_trace"},
            {"index": 2, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 4, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Moderation passes (level=DEFAULT), normal chat proceeds with GenerateName.",
    }
