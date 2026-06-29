"""Scenario 13: Suggested questions generation fails (edge: sugg-q-error).

Events (5):
  1. trace-create       (MessageTraceInfo)
  2. generation-create  (MessageTraceInfo)
  3. trace-create       (GenerateNameTraceInfo, upsert)
  4. span-create        (GenerateNameTraceInfo)
  5. generation-create  (SuggestedQuestionTraceInfo — level=ERROR)

The suggested-questions LLM call fails. The generation-create event for it
has level=ERROR and a statusMessage with the error.
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "13-suggested-questions-error"
SCENARIO_DESCRIPTION = "Chatbot where suggested questions generation fails with an error"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = "suggested-questions-error"
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "GenerateNameTraceInfo", "SuggestedQuestionTraceInfo"]
EXPECTED_EVENT_COUNT = 5

_BASE = "2025-01-15T16:30:00.000000+00:00"
_TRACE = "d7e8f9a0-b1c2-4d3e-4f5a-6b7c8d9e0f1a"
_USER = "u-1e2f3a4b5c"
_CONV = "conv-3a4b5c6d7e8f"
_GEN_MSG = "e8f9a0b1-c2d3-4e4f-5a6b-7c8d9e0f1a2b"
_NAME_SPAN = "f9a0b1c2-d3e4-4f5a-6b7c-8d9e0f1a2b3c"
_SUGG_GEN = "a0b1c2d3-e4f5-4a6b-7c8d-9e0f1a2b3c4d"

_QUERY = "What is the difference between TCP and UDP?"
_LLM_RESPONSE = (
    "TCP (Transmission Control Protocol) is connection-oriented, ensuring "
    "reliable, ordered delivery of data through handshakes, acknowledgments, "
    "and retransmission. UDP (User Datagram Protocol) is connectionless, "
    "prioritizing speed over reliability — it sends datagrams without "
    "guaranteeing delivery or order. TCP is used for web browsing, email, "
    "and file transfer; UDP for streaming, gaming, and DNS."
)
_CONV_NAME = "TCP vs UDP"

_SUGG_ERROR_MSG = "RateLimitError: Rate limit exceeded for model gpt-4o-mini (429 Too Many Requests)"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 400}
_MSG_USAGE = {"input": 42, "output": 130, "total": 172, "unit": "TOKENS",
              "inputCost": 0.000006, "outputCost": 0.000078, "totalCost": 0.000084}
# Suggested questions failed — minimal usage (input tokens consumed before error)
_SUGG_USAGE = {"input": 85, "output": 0, "total": 85, "unit": "TOKENS",
               "inputCost": 0.000013, "outputCost": 0.000000, "totalCost": 0.000013}


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s13-e01"), make_timestamp(_BASE, 2.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV},
        ),
    ))

    # 2. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s13-e02"), make_timestamp(_BASE, 2.801),
        "generation-create",
        make_generation_create(
            gen_id=_GEN_MSG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 2.700),
            usage=_MSG_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.250),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 3. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s13-e03"), make_timestamp(_BASE, 4.000),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 4. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s13-e04"), make_timestamp(_BASE, 4.001),
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

    # 5. generation-create (SuggestedQuestion — ERROR)
    events.append(wrap_event(
        make_event_id("s13-e05"), make_timestamp(_BASE, 5.200),
        "generation-create",
        make_generation_create(
            gen_id=_SUGG_GEN, trace_id=_TRACE,
            name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 4.100),
            end_time=make_timestamp(_BASE, 5.100),
            usage=_SUGG_USAGE,
            completion_start_time=make_timestamp(_BASE, 4.150),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "assistant", "content": _LLM_RESPONSE}]},
            output={"error": _SUGG_ERROR_MSG},
            level="ERROR",
            status_message=_SUGG_ERROR_MSG,
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
            {"index": 1, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 2, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 5, "type": "generation-create", "source_trace_type": "SuggestedQuestionTraceInfo", "dify_handler": "LangFuseDataTrace.suggested_question_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: suggested-questions-error. SuggestedQuestion generation-create has level=ERROR and statusMessage with rate limit error. The LLM call consumed input tokens before failing (output=0).",
    }
