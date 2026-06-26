"""Scenario 06: Workflow with 5 nodes (2 LLM, 1 knowledge retrieval, start, end).

Events (7):
  1. trace-create       (WorkflowTraceInfo)
  2. span-create        (workflow-level span)
  3. span-create        (node 1: Start)
  4. span-create        (node 2: Knowledge Retrieval)
  5. generation-create  (node 3: LLM — Generate Answer)
  6. generation-create  (node 4: LLM — Refine Answer)
  7. span-create        (node 5: End)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "06-workflow-5node"
SCENARIO_DESCRIPTION = "Workflow app, 5 nodes, 2 LLM nodes, 1 knowledge retrieval"
APP_TYPE = "workflow"
DIFY_APP_MODE = "workflow"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo"]
EXPECTED_EVENT_COUNT = 7

_BASE = "2025-01-15T13:00:00.000000+00:00"
_TRACE = "e3f4a5b6-c7d8-4e9f-0a1b-2c3d4e5f6a7b"
_USER = "u-4d5e6f7a8b"
_WF_SPAN = "f4a5b6c7-d8e9-4f0a-1b2c-3d4e5f6a7b8c"
_N1 = "a5b6c7d8-e9f0-4a1b-2c3d-4e5f6a7b8c9d"
_N2 = "b6c7d8e9-f0a1-4b2c-3d4e-5f6a7b8c9d0e"
_N3 = "c7d8e9f0-a1b2-4c3d-4e5f-6a7b8c9d0e1f"
_N4 = "d8e9f0a1-b2c3-4d4e-5f6a-7b8c9d0e1f2a"
_N5 = "e9f0a1b2-c3d4-4e5f-6a7b-8c9d0e1f2a3b"

_QUERY = "Summarize the key points of microservices architecture."
_RAG_DOCS = [
    {"title": "Microservices Patterns", "content": "Key patterns include...", "score": 0.91},
    {"title": "Monolith vs Microservices", "content": "Trade-offs of...", "score": 0.85},
]
_N3_RESPONSE = (
    "Microservices architecture breaks applications into small, independent "
    "services that communicate via APIs. Key points include: service autonomy, "
    "decentralized data management, independent deployment, and technology diversity."
)
_N4_RESPONSE = (
    "Refined summary: Microservices architecture decomposes applications into "
    "autonomous services with independent data stores, deployable independently, "
    "using diverse technologies, communicating via lightweight APIs. Benefits "
    "include scalability and fault isolation; challenges include operational "
    "complexity and distributed data management."
)
_FINAL_OUTPUT = {"summary": _N4_RESPONSE}

_MODEL = "gpt-4o"
_MODEL_PARAMS = {"temperature": 0.5, "max_tokens": 800}
_N3_USAGE = {"input": 120, "output": 85, "total": 205, "unit": "TOKENS",
             "inputCost": 0.000300, "outputCost": 0.000850, "totalCost": 0.001150}
_N4_USAGE = {"input": 210, "output": 95, "total": 305, "unit": "TOKENS",
             "inputCost": 0.000525, "outputCost": 0.000950, "totalCost": 0.001475}


def build_events():
    events = []

    # 1. trace-create (Workflow)
    events.append(wrap_event(
        make_event_id("s06-e01"), make_timestamp(_BASE, 6.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Content Summarization Workflow",
            user_id=_USER,
            input={"query": _QUERY},
            metadata={"workflow_id": "wf-001", "workflow_run_id": _TRACE},
        ),
    ))

    # 2. span-create (workflow-level span)
    events.append(wrap_event(
        make_event_id("s06-e02"), make_timestamp(_BASE, 6.501),
        "span-create",
        make_span_create(
            span_id=_WF_SPAN, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 6.400),
            input={"query": _QUERY}, output=_FINAL_OUTPUT,
        ),
    ))

    # 3. span-create (node 1: Start)
    events.append(wrap_event(
        make_event_id("s06-e03"), make_timestamp(_BASE, 6.502),
        "span-create",
        make_span_create(
            span_id=_N1, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.010),
            parent_observation_id=_WF_SPAN,
            input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    # 4. span-create (node 2: Knowledge Retrieval)
    events.append(wrap_event(
        make_event_id("s06-e04"), make_timestamp(_BASE, 6.503),
        "span-create",
        make_span_create(
            span_id=_N2, trace_id=_TRACE, name="Knowledge Retrieval",
            start_time=make_timestamp(_BASE, 0.050),
            end_time=make_timestamp(_BASE, 0.800),
            parent_observation_id=_WF_SPAN,
            input={"query": _QUERY}, output={"documents": _RAG_DOCS},
        ),
    ))

    # 5. generation-create (node 3: LLM — Generate Answer)
    events.append(wrap_event(
        make_event_id("s06-e05"), make_timestamp(_BASE, 6.504),
        "generation-create",
        make_generation_create(
            gen_id=_N3, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.850),
            end_time=make_timestamp(_BASE, 3.200),
            usage=_N3_USAGE,
            completion_start_time=make_timestamp(_BASE, 1.000),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}],
                   "context": _RAG_DOCS},
            output={"text": _N3_RESPONSE},
        ),
    ))

    # 6. generation-create (node 4: LLM — Refine Answer)
    events.append(wrap_event(
        make_event_id("s06-e06"), make_timestamp(_BASE, 6.505),
        "generation-create",
        make_generation_create(
            gen_id=_N4, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 3.300),
            end_time=make_timestamp(_BASE, 6.300),
            usage=_N4_USAGE,
            completion_start_time=make_timestamp(_BASE, 3.450),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [
                {"role": "user", "content": _QUERY},
                {"role": "assistant", "content": _N3_RESPONSE},
            ]},
            output={"text": _N4_RESPONSE},
        ),
    ))

    # 7. span-create (node 5: End)
    events.append(wrap_event(
        make_event_id("s06-e07"), make_timestamp(_BASE, 6.506),
        "span-create",
        make_span_create(
            span_id=_N5, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 6.350),
            end_time=make_timestamp(_BASE, 6.400),
            parent_observation_id=_WF_SPAN,
            input={"summary": _N4_RESPONSE}, output=_FINAL_OUTPUT,
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
            {"index": 4, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 5, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 6, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "5-node workflow: Start -> KnowledgeRetrieval -> LLM(Generate) -> LLM(Refine) -> End.",
    }
