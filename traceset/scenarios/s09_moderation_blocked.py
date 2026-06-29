"""Scenario 09: Moderation blocks the user input (edge: blocked).

Events (3):
  1. span-create        (ModerationTraceInfo — blocked)
  2. trace-create       (MessageTraceInfo — preset response)
  3. generation-create  (MessageTraceInfo — preset response, no real LLM call)

VERIFY: does MessageTraceInfo emit generation-create when moderation blocks?
If not, this scenario has 2 events.
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "09-moderation-blocked"
SCENARIO_DESCRIPTION = "Chatbot with moderation that blocks the input, preset response returned"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = "moderation-blocked"
TRACE_TYPES_EMITTED = ["ModerationTraceInfo", "MessageTraceInfo"]
EXPECTED_EVENT_COUNT = 3

_BASE = "2025-01-15T14:30:00.000000+00:00"
_TRACE = "d1e2f3a4-b5c6-4d7e-8f9a-0b1c2d3e4f5a"
_USER = "u-7a8b9c0d1e"
_CONV = "conv-9c0d1e2f3a4b"
_MOD_SPAN = "e2f3a4b5-c6d7-4e8f-9a0b-1c2d3e4f5a6b"
_GEN = "f3a4b5c6-d7e8-4f9a-0b1c-2d3e4f5a6b7c"

_QUERY = "Write something inappropriate that triggers moderation."
_PRESET_RESPONSE = (
    "Sorry, your message has been flagged by our moderation system "
    "and cannot be processed. Please rephrase your message and try again."
)

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.0, "max_tokens": 100}
_USAGE = {"input": 15, "output": 40, "total": 55, "unit": "TOKENS",
          "inputCost": 0.000002, "outputCost": 0.000024, "totalCost": 0.000026}


def build_events():
    events = []

    # 1. span-create (Moderation — blocked)
    events.append(wrap_event(
        make_event_id("s09-e01"), make_timestamp(_BASE, 0.500),
        "span-create",
        make_span_create(
            span_id=_MOD_SPAN, trace_id=_TRACE,
            name="input_moderation",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.350),
            input={"query": _QUERY},
            output={"flagged": True, "action": "blocked",
                    "categories": ["violence", "hate"]},
            level="WARNING",
            status_message="Input blocked by moderation: flagged categories [violence, hate]",
        ),
    ))

    # 2. trace-create (Message — preset response)
    events.append(wrap_event(
        make_event_id("s09-e02"), make_timestamp(_BASE, 0.501),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV,
                      "moderation_blocked": True},
        ),
    ))

    # 3. generation-create (Message — preset response)
    events.append(wrap_event(
        make_event_id("s09-e03"), make_timestamp(_BASE, 0.502),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.350),
            end_time=make_timestamp(_BASE, 0.400),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.360),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _PRESET_RESPONSE},
            metadata={"preset_response": True, "moderation_blocked": True},
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
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: moderation-blocked. Preset response without real LLM call. VERIFY: does generation-create fire?",
    }
