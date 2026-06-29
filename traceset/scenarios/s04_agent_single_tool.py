"""Scenario 04: Agent with a single tool call.

Events (5):
  1. trace-create       (MessageTraceInfo)
  2. span-create        (ToolTraceInfo)
  3. generation-create  (MessageTraceInfo)
  4. trace-create       (GenerateNameTraceInfo, upsert)
  5. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "04-agent-single-tool"
SCENARIO_DESCRIPTION = "Agent app, single tool call before LLM response"
APP_TYPE = "agent"
DIFY_APP_MODE = "agent-chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "ToolTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 5

_BASE = "2025-01-15T12:00:00.000000+00:00"
_TRACE = "f2a3b4c5-d6e7-4f8a-9b0c-1d2e3f4a5b6c"
_USER = "u-2b3c4d5e6f"
_CONV = "conv-6f7a8b9c0d1e"
_GEN = "a3b4c5d6-e7f8-4a9b-0c1d-2e3f4a5b6c7d"
_TOOL_SPAN = "b4c5d6e7-f8a9-4b0c-1d2e-3f4a5b6c7d8e"
_NAME_SPAN = "c5d6e7f8-a9b0-4c1d-2e3f-4a5b6c7d8e9f"

_QUERY = "What's the current weather in San Francisco?"
_TOOL_INPUT = {"location": "San Francisco, CA", "unit": "fahrenheit"}
_TOOL_OUTPUT = {
    "location": "San Francisco, CA",
    "temperature": 62,
    "unit": "fahrenheit",
    "condition": "Partly Cloudy",
    "humidity": 65,
    "wind_speed": 12,
}
_LLM_RESPONSE = (
    "The current weather in San Francisco is partly cloudy with a "
    "temperature of 62°F. Humidity is at 65% and wind speed is 12 mph. "
    "It's a typical San Francisco day — cool and mild."
)
_CONV_NAME = "SF Weather Query"

_MODEL = "gpt-4o-mini"
_MODEL_PARAMS = {"temperature": 0.7, "max_tokens": 300}
_USAGE = {"input": 65, "output": 89, "total": 154, "unit": "TOKENS",
          "inputCost": 0.000010, "outputCost": 0.000053, "totalCost": 0.000063}


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s04-e01"), make_timestamp(_BASE, 3.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Agent", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV},
        ),
    ))

    # 2. span-create (Tool)
    events.append(wrap_event(
        make_event_id("s04-e02"), make_timestamp(_BASE, 3.501),
        "span-create",
        make_span_create(
            span_id=_TOOL_SPAN, trace_id=_TRACE,
            name="weather_api",
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 0.850),
            input=_TOOL_INPUT, output=_TOOL_OUTPUT,
        ),
    ))

    # 3. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s04-e03"), make_timestamp(_BASE, 3.502),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.900),
            end_time=make_timestamp(_BASE, 3.400),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 1.050),
            model_parameters=_MODEL_PARAMS,
            input={
                "messages": [
                    {"role": "user", "content": _QUERY},
                    {"role": "tool", "content": str(_TOOL_OUTPUT)},
                ],
            },
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 4. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s04-e04"), make_timestamp(_BASE, 4.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 5. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s04-e05"), make_timestamp(_BASE, 4.801),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 4.400),
            end_time=make_timestamp(_BASE, 4.750),
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
            {"index": 1, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 2, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 3, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 4, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Agent with 1 tool call. Tool span emitted between trace-create and generation-create.",
    }
