"""Scenario 08: Chatflow (advanced-chat), workflow + message + generate name.

Events (11):
  1.  trace-create       (WorkflowTraceInfo)
  2.  span-create        (workflow-level span)
  3.  span-create        (node 1: Start)
  4.  generation-create  (node 2: LLM — Classify Intent, gpt-4o-mini)
  5.  span-create        (node 3: Knowledge Retrieval)
  6.  generation-create  (node 4: LLM — Generate Response, gpt-4o-mini)
  7.  span-create        (node 5: End)
  8.  trace-create       (MessageTraceInfo)
  9.  generation-create  (MessageTraceInfo)
  10. trace-create       (GenerateNameTraceInfo, upsert)
  11. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "08-chatflow-basic"
SCENARIO_DESCRIPTION = "Chatflow (advanced-chat), workflow + message + generate name"
APP_TYPE = "chatflow"
DIFY_APP_MODE = "advanced-chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo", "MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 11

_BASE = "2025-01-15T14:00:00.000000+00:00"
_TRACE = "a0b1c2d3-e4f5-4a6b-7c8d-9e0f1a2b3c4d"
_USER = "u-6f7a8b9c0d"
_CONV = "conv-8b9c0d1e2f3a"
_WF_SPAN = "b1c2d3e4-f5a6-4b7c-8d9e-0f1a2b3c4d5e"
_GEN_MSG = "c2d3e4f5-a6b7-4c8d-9e0f-1a2b3c4d5e6f"
_N1 = "d3e4f5a6-b7c8-4d9e-0f1a-2b3c4d5e6f7a"
_N2 = "e4f5a6b7-c8d9-4e0f-1a2b-3c4d5e6f7a8b"
_N3 = "f5a6b7c8-d9e0-4f1a-2b3c-4d5e6f7a8b9c"
_N4 = "a6b7c8d9-e0f1-4a2b-3c4d-5e6f7a8b9c0d"
_N5 = "b7c8d9e0-f1a2-4b3c-4d5e-6f7a8b9c0d1e"
_NAME_SPAN = "c8d9e0f1-a2b3-4c4d-5e6f-7a8b9c0d1e2f"

_QUERY = "What are the best practices for API versioning?"
_RAG_DOCS = [{"title": "API Versioning Guide", "content": "Use semantic versioning...", "score": 0.88}]
_N2_RESPONSE = "Intent: technical_question. Category: api_design. Confidence: 0.92."
_N4_RESPONSE = (
    "Best practices for API versioning: 1) Use semantic versioning (v1, v2). "
    "2) Version in the URL path (/api/v1/resource) or header. "
    "3) Deprecate old versions with clear timelines. 4) Maintain backward compatibility. "
    "5) Document breaking changes thoroughly."
)
_CONV_NAME = "API Versioning Best Practices"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 400}
_N2_USAGE = {"input": 45, "output": 25, "total": 70, "unit": "TOKENS",
             "inputCost": 0.000007, "outputCost": 0.000015, "totalCost": 0.000022}
_N4_USAGE = {"input": 120, "output": 110, "total": 230, "unit": "TOKENS",
             "inputCost": 0.000018, "outputCost": 0.000066, "totalCost": 0.000084}
_MSG_USAGE = {"input": 130, "output": 110, "total": 240, "unit": "TOKENS",
              "inputCost": 0.000020, "outputCost": 0.000066, "totalCost": 0.000086}


def build_events():
    events = []

    # 1. trace-create (Workflow)
    events.append(wrap_event(
        make_event_id("s08-e01"), make_timestamp(_BASE, 5.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Chatflow: API Q&A",
            user_id=_USER,
            input={"query": _QUERY},
            metadata={"workflow_id": "wf-008", "workflow_run_id": _TRACE,
                      "conversation_id": _CONV},
        ),
    ))

    # 2. span-create (workflow-level span)
    events.append(wrap_event(
        make_event_id("s08-e02"), make_timestamp(_BASE, 5.501),
        "span-create",
        make_span_create(
            span_id=_WF_SPAN, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 5.400),
            input={"query": _QUERY}, output={"text": _N4_RESPONSE},
        ),
    ))

    # 3. span-create (node 1: Start)
    events.append(wrap_event(
        make_event_id("s08-e03"), make_timestamp(_BASE, 5.502),
        "span-create",
        make_span_create(
            span_id=_N1, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.010),
            parent_observation_id=_WF_SPAN,
            input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    # 4. generation-create (node 2: LLM — Classify Intent)
    events.append(wrap_event(
        make_event_id("s08-e04"), make_timestamp(_BASE, 5.503),
        "generation-create",
        make_generation_create(
            gen_id=_N2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.050),
            end_time=make_timestamp(_BASE, 1.200),
            usage=_N2_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.150),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _N2_RESPONSE},
        ),
    ))

    # 5. span-create (node 3: Knowledge Retrieval)
    events.append(wrap_event(
        make_event_id("s08-e05"), make_timestamp(_BASE, 5.504),
        "span-create",
        make_span_create(
            span_id=_N3, trace_id=_TRACE, name="Knowledge Retrieval",
            start_time=make_timestamp(_BASE, 1.250),
            end_time=make_timestamp(_BASE, 2.000),
            parent_observation_id=_WF_SPAN,
            input={"query": "API versioning best practices"},
            output={"documents": _RAG_DOCS},
        ),
    ))

    # 6. generation-create (node 4: LLM — Generate Response)
    events.append(wrap_event(
        make_event_id("s08-e06"), make_timestamp(_BASE, 5.505),
        "generation-create",
        make_generation_create(
            gen_id=_N4, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 2.050),
            end_time=make_timestamp(_BASE, 5.300),
            usage=_N4_USAGE,
            completion_start_time=make_timestamp(_BASE, 2.200),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}],
                   "context": _RAG_DOCS},
            output={"text": _N4_RESPONSE},
        ),
    ))

    # 7. span-create (node 5: End)
    events.append(wrap_event(
        make_event_id("s08-e07"), make_timestamp(_BASE, 5.506),
        "span-create",
        make_span_create(
            span_id=_N5, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 5.350),
            end_time=make_timestamp(_BASE, 5.400),
            parent_observation_id=_WF_SPAN,
            input={"text": _N4_RESPONSE}, output={"text": _N4_RESPONSE},
        ),
    ))

    # 8. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s08-e08"), make_timestamp(_BASE, 5.510),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatflow", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV,
                      "workflow_run_id": _TRACE},
        ),
    ))

    # 9. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s08-e09"), make_timestamp(_BASE, 5.511),
        "generation-create",
        make_generation_create(
            gen_id=_GEN_MSG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.050),
            end_time=make_timestamp(_BASE, 5.350),
            usage=_MSG_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.200),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _N4_RESPONSE},
        ),
    ))

    # 10. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s08-e10"), make_timestamp(_BASE, 6.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 11. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s08-e11"), make_timestamp(_BASE, 6.801),
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
            {"index": 1, "type": "trace-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 2, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 4, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 6, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 8, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 9, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 10, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 11, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Chatflow = workflow + message. The workflow produces the response; the message trace records it for the conversation.",
    }
