"""Scenario 12: Agent tool call fails (edge: tool-error).

Events (5):
  1. span-create        (ToolTraceInfo — level=ERROR, statusMessage=error)
  2. trace-create       (MessageTraceInfo)
  3. generation-create  (MessageTraceInfo — LLM responds despite tool failure)
  4. trace-create       (GenerateNameTraceInfo, upsert)
  5. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "12-tool-failure"
SCENARIO_DESCRIPTION = "Agent app where the tool call fails, LLM responds without tool data"
APP_TYPE = "agent"
DIFY_APP_MODE = "agent-chat"
EDGE_CASE = "tool-error"
TRACE_TYPES_EMITTED = ["ToolTraceInfo", "MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 5

_BASE = "2025-01-15T16:00:00.000000+00:00"
_TRACE = "c6d7e8f9-a0b1-4c2d-3e4f-5a6b7c8d9e0f"
_USER = "u-0d1e2f3a4b"
_CONV = "conv-2f3a4b5c6d7e"
_TOOL_SPAN = "d7e8f9a0-b1c2-4d3e-4f5a-6b7c8d9e0f1a"
_GEN = "e8f9a0b1-c2d3-4e4f-5a6b-7c8d9e0f1a2b"
_NAME_SPAN = "f9a0b1c2-d3e4-4f5a-6b7c-8d9e0f1a2b3c"

_QUERY = "Look up the stock price for AAPL."
_TOOL_INPUT = {"symbol": "AAPL", "endpoint": "/api/v1/stocks"}
_TOOL_ERROR_MSG = "ConnectionError: Failed to connect to stock API (timeout after 5000ms)"

_LLM_RESPONSE = (
    "I attempted to look up the stock price for AAPL, but the stock data "
    "service is currently unavailable due to a connection timeout. Please "
    "try again in a moment, or check a financial website like Yahoo Finance "
    "or Google Finance for the latest AAPL stock price."
)
_CONV_NAME = "AAPL Stock Price Lookup Error"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 200}
_USAGE = {"input": 55, "output": 72, "total": 127, "unit": "TOKENS",
          "inputCost": 0.000008, "outputCost": 0.000043, "totalCost": 0.000051}


def build_events():
    events = []

    # 1. span-create (Tool — ERROR)
    events.append(wrap_event(
        make_event_id("s12-e01"), make_timestamp(_BASE, 5.200),
        "span-create",
        make_span_create(
            span_id=_TOOL_SPAN, trace_id=_TRACE,
            name="stock_api",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 5.000),
            input=_TOOL_INPUT,
            output={"error": _TOOL_ERROR_MSG},
            level="ERROR",
            status_message=_TOOL_ERROR_MSG,
        ),
    ))

    # 2. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s12-e02"), make_timestamp(_BASE, 5.201),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Agent", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV,
                      "tool_failed": True},
        ),
    ))

    # 3. generation-create (Message — LLM responds despite tool failure)
    events.append(wrap_event(
        make_event_id("s12-e03"), make_timestamp(_BASE, 5.202),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 5.050),
            end_time=make_timestamp(_BASE, 5.150),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 5.080),
            model_parameters=_PARAMS,
            input={
                "messages": [
                    {"role": "user", "content": _QUERY},
                    {"role": "tool", "content": _TOOL_ERROR_MSG},
                ],
            },
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 4. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s12-e04"), make_timestamp(_BASE, 6.400),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 5. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s12-e05"), make_timestamp(_BASE, 6.401),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 6.000),
            end_time=make_timestamp(_BASE, 6.350),
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
            {"index": 1, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 2, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 4, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: tool-error. Tool span has level=ERROR and statusMessage. LLM responds with an apology message despite the tool failure.",
    }
