"""Scenario 11: RAG retrieval returns no documents (edge: empty-rag).

Events (5):
  1. span-create        (DatasetRetrievalTraceInfo — empty results)
  2. trace-create       (MessageTraceInfo)
  3. generation-create  (MessageTraceInfo — LLM answers without context)
  4. trace-create       (GenerateNameTraceInfo, upsert)
  5. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "11-rag-empty-results"
SCENARIO_DESCRIPTION = "Chatbot with RAG that returns zero documents, LLM answers without context"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = "empty-rag"
TRACE_TYPES_EMITTED = ["DatasetRetrievalTraceInfo", "MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 5

_BASE = "2025-01-15T15:30:00.000000+00:00"
_TRACE = "b5c6d7e8-f9a0-4b1c-2d3e-4f5a6b7c8d9e"
_USER = "u-9c0d1e2f3a"
_CONV = "conv-1e2f3a4b5c6d"
_RAG_SPAN = "c6d7e8f9-a0b1-4c2d-3e4f-5a6b7c8d9e0f"
_GEN = "d7e8f9a0-b1c2-4d3e-4f5a-6b7c8d9e0f1a"
_NAME_SPAN = "e8f9a0b1-c2d3-4e4f-5a6b-7c8d9e0f1a2b"

_QUERY = "What is the internal policy for sabbatical leave?"
_LLM_RESPONSE = (
    "I don't have specific information about internal sabbatical leave policies "
    "in my knowledge base. I'd recommend checking your company's HR portal or "
    "contacting your HR representative directly for the most accurate and "
    "up-to-date information regarding sabbatical leave eligibility, duration, "
    "and application procedures."
)
_CONV_NAME = "Sabbatical Leave Policy Inquiry"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 300}
_USAGE = {"input": 38, "output": 95, "total": 133, "unit": "TOKENS",
          "inputCost": 0.000006, "outputCost": 0.000057, "totalCost": 0.000063}


def build_events():
    events = []

    # 1. span-create (DatasetRetrieval — empty results)
    events.append(wrap_event(
        make_event_id("s11-e01"), make_timestamp(_BASE, 2.500),
        "span-create",
        make_span_create(
            span_id=_RAG_SPAN, trace_id=_TRACE,
            name="dataset_retrieval",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.150),
            input={"query": _QUERY},
            output={"documents": [], "result_count": 0},
        ),
    ))

    # 2. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s11-e02"), make_timestamp(_BASE, 2.501),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV,
                      "rag_empty": True},
        ),
    ))

    # 3. generation-create (Message — LLM answers without context)
    events.append(wrap_event(
        make_event_id("s11-e03"), make_timestamp(_BASE, 2.502),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.200),
            end_time=make_timestamp(_BASE, 2.400),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.350),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 4. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s11-e04"), make_timestamp(_BASE, 2.700),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 5. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s11-e05"), make_timestamp(_BASE, 2.701),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 3.300),
            end_time=make_timestamp(_BASE, 3.650),
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
            {"index": 1, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 2, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 4, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: empty-rag. DatasetRetrieval returns 0 documents. LLM answers without context. RAG span emitted before message events.",
    }
