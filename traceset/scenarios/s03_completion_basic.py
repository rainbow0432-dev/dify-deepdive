"""Scenario 03: Basic completion app, single-turn, no extras.

Events (4):
  1. trace-create       (MessageTraceInfo)
  2. generation-create  (MessageTraceInfo)
  3. trace-create       (GenerateNameTraceInfo, upsert)
  4. span-create        (GenerateNameTraceInfo)

Same structure as s01 but app_mode = 'completion' (no conversation context).
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "03-completion-basic"
SCENARIO_DESCRIPTION = "Basic completion app, single-turn text generation, no extras"
APP_TYPE = "completion"
DIFY_APP_MODE = "completion"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 4

_BASE = "2025-01-15T11:30:00.000000+00:00"
_TRACE = "c9d0e1f2-a3b4-4c5d-6e7f-8a9b0c1d2e3f"
_USER = "u-1a2b3c4d5e"
_GEN = "d0e1f2a3-b4c5-4d6e-7f8a-9b0c1d2e3f4a"
_NAME_SPAN = "e1f2a3b4-c5d6-4e7f-8a9b-0c1d2e3f4a5b"

_PROMPT = "Explain the concept of ACID compliance in database transactions."

_LLM_RESPONSE = (
    "ACID compliance ensures reliable database transactions through four "
    "properties: Atomicity (all operations in a transaction succeed or "
    "none do), Consistency (transactions move the database from one valid "
    "state to another), Isolation (concurrent transactions don't interfere "
    "with each other), and Durability (committed transactions persist even "
    "after crashes). These guarantees make relational databases trustworthy "
    "for financial and mission-critical applications."
)

_TITLE = "ACID Compliance Explained"

_MODEL = "gpt-4o-mini"
_MODEL_PARAMS = {"temperature": 0.3, "max_tokens": 500}
_USAGE = {"input": 35, "output": 120, "total": 155, "unit": "TOKENS",
          "inputCost": 0.000005, "outputCost": 0.000072, "totalCost": 0.000077}


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s03-e01"), make_timestamp(_BASE, 2.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Completion",
            user_id=_USER,
            input={"prompt": _PROMPT},
        ),
    ))

    # 2. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s03-e02"), make_timestamp(_BASE, 2.501),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 2.400),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.200),
            model_parameters=_MODEL_PARAMS,
            input={"messages": [{"role": "user", "content": _PROMPT}]},
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 3. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s03-e03"), make_timestamp(_BASE, 3.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 4. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s03-e04"), make_timestamp(_BASE, 3.801),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 3.400),
            end_time=make_timestamp(_BASE, 3.750),
            input={"messages": [{"role": "user", "content": _PROMPT}]},
            output={"text": _TITLE},
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
        "notes": "Completion app (no conversation). Same trace structure as chat but app_mode=completion.",
    }
