"""Scenario 02: Chatbot with RAG, suggested questions, and auto-name.

Events (6):
  1. trace-create       (MessageTraceInfo)
  2. generation-create  (MessageTraceInfo)
  3. span-create        (DatasetRetrievalTraceInfo)
  4. generation-create  (SuggestedQuestionTraceInfo)
  5. trace-create       (GenerateNameTraceInfo, upsert)
  6. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "02-chat-rag"
SCENARIO_DESCRIPTION = "Chatbot with knowledge base retrieval, suggested questions, auto-name"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = [
    "MessageTraceInfo", "DatasetRetrievalTraceInfo",
    "SuggestedQuestionTraceInfo", "GenerateNameTraceInfo",
]
EXPECTED_EVENT_COUNT = 6

_BASE = "2025-01-15T11:00:00.000000+00:00"
_TRACE = "d4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f8a"
_USER = "u-9a1b2c3d4e"
_CONV = "conv-5e6f7a8b9c0d"
_GEN = "e5f6a7b8-c9d0-4e1f-2a3b-4c5d6e7f8a9b"
_RAG_SPAN = "f6a7b8c9-d0e1-4f2a-3b4c-5d6e7f8a9b0c"
_SUGG_GEN = "a7b8c9d0-e1f2-4a3b-4c5d-6e7f8a9b0c1d"
_NAME_SPAN = "b8c9d0e1-f2a3-4b4c-5d6e-7f8a9b0c1d2e"

_QUERY = "How does Redis handle persistence compared to Memcached?"

_LLM_RESPONSE = (
    "Redis provides two main persistence mechanisms: RDB (Redis Database) "
    "snapshots that save point-in-time copies of the dataset at intervals, "
    "and AOF (Append-Only File) logs that record every write operation. "
    "You can use either or both. Memcached has no built-in persistence — "
    "data is lost on restart. This makes Redis suitable for scenarios "
    "where data durability matters, while Memcached is purely for "
    "transient caching."
)

_RAG_DOCS = [
    {"title": "Redis Persistence Guide", "content": "Redis supports RDB and AOF...", "score": 0.95},
    {"title": "Memcached vs Redis", "content": "Memcached has no persistence...", "score": 0.89},
    {"title": "In-Memory Databases Comparison", "content": "Redis and Memcached compared...", "score": 0.82},
]

_SUGG_QUESTIONS = [
    "What are the performance trade-offs of RDB vs AOF?",
    "How do I configure Redis persistence for my use case?",
    "Can Memcached be made persistent with external tools?",
]

_MODEL = "gpt-4o-mini"
_MODEL_PARAMS = {"temperature": 0.7, "max_tokens": 800}
_MSG_USAGE = {"input": 58, "output": 234, "total": 292, "unit": "TOKENS",
              "inputCost": 0.000009, "outputCost": 0.000140, "totalCost": 0.000149}
_SUGG_USAGE = {"input": 85, "output": 45, "total": 130, "unit": "TOKENS",
               "inputCost": 0.000013, "outputCost": 0.000027, "totalCost": 0.000040}
_CONV_NAME = "Redis Persistence vs Memcached"


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s02-e01"), make_timestamp(_BASE, 3.200),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV},
        ),
    ))

    # 2. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s02-e02"), make_timestamp(_BASE, 3.201),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.150),
            end_time=make_timestamp(_BASE, 3.100),
            usage=_MSG_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.300),
            model_parameters=_MODEL_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}],
                   "context": _RAG_DOCS},
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 3. span-create (DatasetRetrieval)
    events.append(wrap_event(
        make_event_id("s02-e03"), make_timestamp(_BASE, 3.202),
        "span-create",
        make_span_create(
            span_id=_RAG_SPAN, trace_id=_TRACE,
            name="dataset_retrieval",
            start_time=make_timestamp(_BASE, 0.050),
            end_time=make_timestamp(_BASE, 0.120),
            input={"query": _QUERY},
            output={"documents": _RAG_DOCS},
        ),
    ))

    # 4. generation-create (SuggestedQuestion)
    events.append(wrap_event(
        make_event_id("s02-e04"), make_timestamp(_BASE, 4.000),
        "generation-create",
        make_generation_create(
            gen_id=_SUGG_GEN, trace_id=_TRACE,
            name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 3.500),
            end_time=make_timestamp(_BASE, 3.950),
            usage=_SUGG_USAGE,
            completion_start_time=make_timestamp(_BASE, 3.600),
            model_parameters=_MODEL_PARAMS,
            input={"messages": [{"role": "assistant", "content": _LLM_RESPONSE}]},
            output={"questions": _SUGG_QUESTIONS},
        ),
    ))

    # 5. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s02-e05"), make_timestamp(_BASE, 4.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 6. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s02-e06"), make_timestamp(_BASE, 4.501),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 4.100),
            end_time=make_timestamp(_BASE, 4.450),
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
            {"index": 3, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 4, "type": "generation-create", "source_trace_type": "SuggestedQuestionTraceInfo", "dify_handler": "LangFuseDataTrace.suggested_question_trace"},
            {"index": 5, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 6, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Chat with knowledge base. RAG retrieval + suggested questions + auto-name.",
    }
