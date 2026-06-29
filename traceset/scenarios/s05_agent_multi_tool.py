"""Scenario 05: Agent with three sequential tool calls.

Events (7):
  1. trace-create       (MessageTraceInfo)
  2. span-create        (ToolTraceInfo — search)
  3. span-create        (ToolTraceInfo — fetch)
  4. span-create        (ToolTraceInfo — calculate)
  5. generation-create  (MessageTraceInfo)
  6. trace-create       (GenerateNameTraceInfo, upsert)
  7. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "05-agent-multi-tool"
SCENARIO_DESCRIPTION = "Agent app, three sequential tool calls before LLM response"
APP_TYPE = "agent"
DIFY_APP_MODE = "agent-chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "ToolTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 7

_BASE = "2025-01-15T12:30:00.000000+00:00"
_TRACE = "d6e7f8a9-b0c1-4d2e-3f4a-5b6c7d8e9f0a"
_USER = "u-3c4d5e6f7a"
_CONV = "conv-7a8b9c0d1e2f"
_GEN = "e7f8a9b0-c1d2-4e3f-4a5b-6c7d8e9f0a1b"
_TOOL1 = "f8a9b0c1-d2e3-4f4a-5b6c-7d8e9f0a1b2c"
_TOOL2 = "a9b0c1d2-e3f4-4a5b-6c7d-8e9f0a1b2c3d"
_TOOL3 = "b0c1d2e3-f4a5-4b6c-7d8e-9f0a1b2c3d4e"
_NAME_SPAN = "c1d2e3f4-a5b6-4c7d-8e9f-0a1b2c3d4e5f"

_QUERY = "Research the population of Tokyo, fetch GDP data, and calculate GDP per capita."
_TOOL1_INPUT = {"query": "Tokyo population 2024"}
_TOOL1_OUTPUT = {"population": 13960000, "year": 2024, "source": "World Bank"}
_TOOL2_INPUT = {"query": "Tokyo GDP 2024 USD"}
_TOOL2_OUTPUT = {"gdp_usd": 1100000000000, "year": 2024, "source": "IMF"}
_TOOL3_INPUT = {"gdp": 1100000000000, "population": 13960000}
_TOOL3_OUTPUT = {"gdp_per_capita_usd": 78796.56, "currency": "USD"}
_LLM_RESPONSE = (
    "Based on the research: Tokyo has a population of approximately "
    "13.96 million (2024, World Bank). The GDP is approximately $1.1 "
    "trillion USD (2024, IMF). The calculated GDP per capita is "
    "approximately $78,797 USD. Tokyo remains one of the most "
    "economically productive metropolitan areas in the world."
)
_CONV_NAME = "Tokyo GDP Per Capita Research"

_MODEL = "gpt-4o"
_MODEL_PARAMS = {"temperature": 0.3, "max_tokens": 600}
_USAGE = {"input": 180, "output": 145, "total": 325, "unit": "TOKENS",
          "inputCost": 0.000450, "outputCost": 0.001450, "totalCost": 0.001900}


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s05-e01"), make_timestamp(_BASE, 5.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Agent", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV},
        ),
    ))

    # 2. span-create (Tool 1: search)
    events.append(wrap_event(
        make_event_id("s05-e02"), make_timestamp(_BASE, 5.501),
        "span-create",
        make_span_create(
            span_id=_TOOL1, trace_id=_TRACE, name="web_search",
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 1.200),
            input=_TOOL1_INPUT, output=_TOOL1_OUTPUT,
        ),
    ))

    # 3. span-create (Tool 2: fetch)
    events.append(wrap_event(
        make_event_id("s05-e03"), make_timestamp(_BASE, 5.502),
        "span-create",
        make_span_create(
            span_id=_TOOL2, trace_id=_TRACE, name="data_fetch",
            start_time=make_timestamp(_BASE, 1.300),
            end_time=make_timestamp(_BASE, 2.500),
            input=_TOOL2_INPUT, output=_TOOL2_OUTPUT,
        ),
    ))

    # 4. span-create (Tool 3: calculate)
    events.append(wrap_event(
        make_event_id("s05-e04"), make_timestamp(_BASE, 5.503),
        "span-create",
        make_span_create(
            span_id=_TOOL3, trace_id=_TRACE, name="calculator",
            start_time=make_timestamp(_BASE, 2.600),
            end_time=make_timestamp(_BASE, 2.750),
            input=_TOOL3_INPUT, output=_TOOL3_OUTPUT,
        ),
    ))

    # 5. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s05-e05"), make_timestamp(_BASE, 5.504),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 2.800),
            end_time=make_timestamp(_BASE, 5.400),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 2.950),
            model_parameters=_MODEL_PARAMS,
            input={
                "messages": [
                    {"role": "user", "content": _QUERY},
                    {"role": "tool", "content": str(_TOOL1_OUTPUT)},
                    {"role": "tool", "content": str(_TOOL2_OUTPUT)},
                    {"role": "tool", "content": str(_TOOL3_OUTPUT)},
                ],
            },
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 6. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s05-e06"), make_timestamp(_BASE, 6.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 7. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s05-e07"), make_timestamp(_BASE, 6.801),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 6.400),
            end_time=make_timestamp(_BASE, 6.750),
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
            {"index": 3, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 5, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 6, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Agent with 3 sequential tool calls. Uses gpt-4o for the final synthesis.",
    }
