"""Scenario 14: Streaming chat with TTFT/TTG metadata (edge: streaming).

Events (4):
  1. trace-create       (MessageTraceInfo)
  2. generation-create  (MessageTraceInfo — streaming fields)
  3. trace-create       (GenerateNameTraceInfo, upsert)
  4. span-create        (GenerateNameTraceInfo)

The generation-create event includes:
  - completionStartTime (set for all generations, but especially relevant for streaming)
  - metadata with streaming latency metrics:
    - gen_ai_server_time_to_first_token (TTFT in milliseconds)
    - llm_streaming_time_to_generate (total generation time in milliseconds)

VERIFY: the exact metadata key names against LangFuseDataTrace.message_trace source.
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "14-message-streaming"
SCENARIO_DESCRIPTION = "Streaming chatbot response with TTFT/TTG latency metadata"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = "streaming"
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 4

_BASE = "2025-01-15T17:00:00.000000+00:00"
_TRACE = "e8f9a0b1-c2d3-4e4f-5a6b-7c8d9e0f1a2b"
_USER = "u-2f3a4b5c6d"
_CONV = "conv-4b5c6d7e8f9a"
_GEN = "f9a0b1c2-d3e4-4f5a-6b7c-8d9e0f1a2b3c"
_NAME_SPAN = "a0b1c2d3-e4f5-4a6b-7c8d-9e0f1a2b3c4d"

_QUERY = "Write a short essay about the importance of open source software."
_LLM_RESPONSE = (
    "Open source software is the backbone of modern technology infrastructure. "
    "From the Linux kernel that powers most of the internet's servers to the "
    "Apache web server, from the Python and JavaScript languages that drive "
    "application development to the Kubernetes platform that orchestrates "
    "cloud-native deployments, open source projects enable innovation at a "
    "scale that no single company could match. The collaborative model of "
    "open source development — where anyone can read, modify, and contribute "
    "code — produces software that is often more secure, more reliable, and "
    "more adaptable than proprietary alternatives. The transparency enables "
    "peer review, the license freedom prevents vendor lock-in, and the "
    "community-driven roadmap ensures that features serve users rather than "
    "shareholders. Open source is not just a licensing model; it is a "
    "philosophy of knowledge sharing that has transformed how we build software."
)
_CONV_NAME = "Open Source Software Essay"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.7, "max_tokens": 1000}
_USAGE = {"input": 48, "output": 285, "total": 333, "unit": "TOKENS",
          "inputCost": 0.000007, "outputCost": 0.000171, "totalCost": 0.000178}

# Streaming latency metrics (in milliseconds)
_TTFT_MS = 320       # Time to first token
_TTG_MS = 4150       # Total time to generate


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s14-e01"), make_timestamp(_BASE, 4.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV,
                      "streaming": True},
        ),
    ))

    # 2. generation-create (Message — streaming fields)
    #    completionStartTime = base + 0.320s (TTFT)
    #    metadata carries Dify-internal streaming latency metrics
    events.append(wrap_event(
        make_event_id("s14-e02"), make_timestamp(_BASE, 4.501),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 4.250),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.320),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _LLM_RESPONSE},
            metadata={
                "streaming": True,
                "gen_ai_server_time_to_first_token": _TTFT_MS,
                "llm_streaming_time_to_generate": _TTG_MS,
            },
        ),
    ))

    # 3. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s14-e03"), make_timestamp(_BASE, 5.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 4. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s14-e04"), make_timestamp(_BASE, 5.801),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 5.400),
            end_time=make_timestamp(_BASE, 5.750),
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
            {"index": 2, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: streaming. generation-create includes completionStartTime and metadata with gen_ai_server_time_to_first_token (320ms TTFT) and llm_streaming_time_to_generate (4150ms TTG). VERIFY: exact metadata key names against LangFuseDataTrace.message_trace source.",
    }
