"""Scenario 10: Workflow error propagation — 7 LLM nodes, node 5 fails.

Events (11):
  1.  trace-create       (WorkflowTraceInfo)
  2.  span-create        (workflow root)
  3.  span-create        (Start, parent=workflow)
  4.  generation-create  (LLM 1: Validate Input)
  5.  generation-create  (LLM 2: Extract Data)
  6.  generation-create  (LLM 3: Transform)
  7.  generation-create  (LLM 4: Enrich)
  8.  generation-create  (LLM 5: Aggregate, level=ERROR)
  9.  generation-create  (LLM 6: Error Recovery)
  10.  generation-create  (LLM 7: Finalize)
  11.  span-create        (End, parent=workflow)

VERIFY: LLM 5 has level=ERROR with statusMessage. Pipeline continues with recovery.
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "10-workflow-error-propagation"
SCENARIO_DESCRIPTION = "7 LLM nodes, node 5 fails (level=ERROR), error handler, End"
APP_TYPE = "workflow"
DIFY_APP_MODE = "workflow"
EDGE_CASE = "error-propagation"
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo"]
EXPECTED_EVENT_COUNT = 11
EXPECTED_SPAN_COUNT = 10
SPAN_PATTERN = "error-propagation"

_BASE = "2025-01-15T15:00:00.000000+00:00"
_TRACE = "a1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_WF = "a2a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_START = "a3a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N1 = "a4a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N2 = "a5a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N3 = "a6a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N4 = "a7a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N5 = "a8a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N6 = "a9a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N7 = "aaa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_END = "aba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-6c2d1e0f9a"
_QUERY = "Run the daily data aggregation pipeline for 2025-01-15"
_MODEL = "gpt-4o"
_PARAMS = {"temperature": 0.3, "max_tokens": 800}

_N1_OUT = "Input validated: date=2025-01-15, sources=5, expected_records=~50K. All source systems reachable."
_N2_OUT = "Data extraction: source_1=12K records, source_2=8K, source_3=15K, source_4=7K, source_5=9K. Total: 51K records."
_N3_OUT = "Transformation: normalized 51K records, deduplicated 847, resolved 23 data quality issues. Final: 50,153 records."
_N4_OUT = "Enrichment: added geocoding (50,153/50,153), company firmographics (48,221/50,153), industry classification (49,890/50,153)."
_N5_OUT = {"error": "Aggregation failed: KeyError 'revenue_category' in merge step. 0 records aggregated. Schema drift detected: field 'rev_cat' renamed to 'revenue_category' in source_3."}
_N6_OUT = "Schema mismatch detected. Applied fallback: mapped 'rev_cat' to 'revenue_category'. Re-ran aggregation: 50,153 records aggregated successfully."
_N7_OUT = "Pipeline complete: 50,153 records aggregated, enriched, and loaded to warehouse. Duration: 4m 32s. Data quality score: 96.2%. Next run: 2025-01-16 06:00 UTC."
_FINAL = {"status": "completed", "records": 50153, "duration": "4m 32s", "quality_score": 0.962}

_N1_U = {"input": 80, "output": 60, "total": 140, "unit": "TOKENS", "inputCost": 0.000200, "outputCost": 0.000600, "totalCost": 0.000800}
_N2_U = {"input": 100, "output": 80, "total": 180, "unit": "TOKENS", "inputCost": 0.000250, "outputCost": 0.000800, "totalCost": 0.001050}
_N3_U = {"input": 120, "output": 70, "total": 190, "unit": "TOKENS", "inputCost": 0.000300, "outputCost": 0.000700, "totalCost": 0.001000}
_N4_U = {"input": 150, "output": 90, "total": 240, "unit": "TOKENS", "inputCost": 0.000375, "outputCost": 0.000900, "totalCost": 0.001275}
_N5_U = {"input": 130, "output": 50, "total": 180, "unit": "TOKENS", "inputCost": 0.000325, "outputCost": 0.000500, "totalCost": 0.000825}
_N6_U = {"input": 180, "output": 120, "total": 300, "unit": "TOKENS", "inputCost": 0.000450, "outputCost": 0.001200, "totalCost": 0.001650}
_N7_U = {"input": 200, "output": 150, "total": 350, "unit": "TOKENS", "inputCost": 0.000500, "outputCost": 0.001500, "totalCost": 0.002000}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s10-e01"), timestamp=make_timestamp(_BASE, 7.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Data Aggregation Pipeline", user_id=_USER,
            input={"query": _QUERY},
            metadata={"workflow_id": "wf-dap-010", "workflow_run_id": _TRACE},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s10-e02"), timestamp=make_timestamp(_BASE, 7.002),
        event_type="span-create",
        body=make_span_create(
            span_id=_WF, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 7.000),
            input={"query": _QUERY}, output=_FINAL,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s10-e03"), timestamp=make_timestamp(_BASE, 7.003),
        event_type="span-create",
        body=make_span_create(
            span_id=_START, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 0.050),
            parent_observation_id=_WF, input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    nodes = [
        ("Validate Input", _N1, 0.100, 1.000, _N1_OUT, _N1_U, None, None),
        ("Extract Data", _N2, 1.050, 2.000, _N2_OUT, _N2_U, None, None),
        ("Transform", _N3, 2.050, 3.000, _N3_OUT, _N3_U, None, None),
        ("Enrich", _N4, 3.050, 4.000, _N4_OUT, _N4_U, None, None),
        ("Aggregate", _N5, 4.050, 4.500, _N5_OUT, _N5_U, "ERROR", "KeyError: 'revenue_category' in merge step. Schema drift in source_3."),
        ("Error Recovery", _N6, 4.550, 5.500, _N6_OUT, _N6_U, None, None),
        ("Finalize", _N7, 5.550, 6.500, _N7_OUT, _N7_U, None, None),
    ]
    for i, (name, nid, start, end, out, usage, level, msg) in enumerate(nodes):
        kw = {
            "gen_id": nid, "trace_id": _TRACE, "name": _MODEL, "model": _MODEL,
            "start_time": make_timestamp(_BASE, start), "end_time": make_timestamp(_BASE, end),
            "usage": usage,
            "completion_start_time": make_timestamp(_BASE, start + 0.150),
            "model_parameters": _PARAMS, "parent_observation_id": _WF,
            "input": {"messages": [{"role": "user", "content": f"Execute node: {name}"}]},
            "output": out if isinstance(out, dict) and "text" not in out else {"text": out} if isinstance(out, str) else out,
        }
        if level:
            kw["level"] = level
        if msg:
            kw["status_message"] = msg
        events.append(wrap_event(
            event_id=make_event_id(f"s10-e{i+4:02d}"),
            timestamp=make_timestamp(_BASE, 7.004 + i * 0.001),
            event_type="generation-create",
            body=make_generation_create(**kw),
        ))

    events.append(wrap_event(
        event_id=make_event_id("s10-e11"), timestamp=make_timestamp(_BASE, 7.011),
        event_type="span-create",
        body=make_span_create(
            span_id=_END, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 6.950), end_time=make_timestamp(_BASE, 7.000),
            parent_observation_id=_WF, input=_FINAL, output=_FINAL,
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
        "expected_span_count": EXPECTED_SPAN_COUNT,
        "span_pattern": SPAN_PATTERN,
        "events_in_order": [
            {"index": i, "type": t, "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"}
            for i, t in enumerate(
                ["trace-create", "span-create", "span-create",
                 "generation-create", "generation-create", "generation-create",
                 "generation-create", "generation-create", "generation-create",
                 "generation-create", "span-create"], 1)
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "VERIFY: LLM 5 (Aggregate) has level=ERROR with statusMessage describing schema drift. LLM 6 (Error Recovery) handles the error. Pipeline completes successfully.",
    }
