"""Scenario 07: High-N workflow with 15 nodes (4 LLM, 3 tool, 8 other).

Events (17):
  1.  trace-create       (WorkflowTraceInfo)
  2.  span-create        (workflow-level span)
  3.  span-create        (node 1: Start)
  4.  generation-create  (node 2: LLM — Analyze Query, gpt-4o)
  5.  span-create        (node 3: Knowledge Retrieval)
  6.  span-create        (node 4: Tool — Web Search)
  7.  generation-create  (node 5: LLM — Synthesize Answer, gpt-4o)
  8.  span-create        (node 6: IF/ELSE — Check Confidence)
  9.  span-create        (node 7: Tool — Fetch Additional Data)
  10. generation-create  (node 8: LLM — Refine Response, claude-3-5-sonnet)
  11. span-create        (node 9: Template Transform — Format Output)
  12. span-create        (node 10: Tool — Validate Output)
  13. span-create        (node 11: Variable Aggregator — Merge)
  14. generation-create  (node 12: LLM — Final Polish, gpt-4o)
  15. span-create        (node 13: IF/ELSE — Quality Check)
  16. span-create        (node 14: Code — Post-process)
  17. span-create        (node 15: End)

Edge case: high-N (many sequential HTTP POSTs from one Celery task).
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "07-workflow-15node"
SCENARIO_DESCRIPTION = "High-N workflow, 15 nodes, 4 LLM, 3 tool calls, edge case"
APP_TYPE = "workflow"
DIFY_APP_MODE = "workflow"
EDGE_CASE = "high-n"
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo"]
EXPECTED_EVENT_COUNT = 17

_BASE = "2025-01-15T13:30:00.000000+00:00"
_TRACE = "f5a6b7c8-d9e0-4f1a-2b3c-4d5e6f7a8b9c"
_USER = "u-5e6f7a8b9c"
_WF_SPAN = "a6b7c8d9-e0f1-4a2b-3c4d-5e6f7a8b9c0d"

_N1  = "b7c8d9e0-f1a2-4b3c-4d5e-6f7a8b9c0d1e"
_N2  = "c8d9e0f1-a2b3-4c4d-5e6f-7a8b9c0d1e2f"
_N3  = "d9e0f1a2-b3c4-4d5e-6f7a-8b9c0d1e2f3a"
_N4  = "e0f1a2b3-c4d5-4e6f-7a8b-9c0d1e2f3a4b"
_N5  = "f1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N6  = "a2b3c4d5-e6f7-4a8b-9c0d-1e2f3a4b5c6d"
_N7  = "b3c4d5e6-f7a8-4b9c-0d1e-2f3a4b5c6d7e"
_N8  = "c4d5e6f7-a8b9-4c0d-1e2f-3a4b5c6d7e8f"
_N9  = "d5e6f7a8-b9c0-4d1e-2f3a-4b5c6d7e8f9a"
_N10 = "e6f7a8b9-c0d1-4e2f-3a4b-5c6d7e8f9a0b"
_N11 = "f7a8b9c0-d1e2-4f3a-4b5c-6d7e8f9a0b1c"
_N12 = "a8b9c0d1-e2f3-4a4b-5c6d-7e8f9a0b1c2d"
_N13 = "b9c0d1e2-f3a4-4b5c-6d7e-8f9a0b1c2d3e"
_N14 = "c0d1e2f3-a4b5-4c6d-7e8f-9a0b1c2d3e4f"
_N15 = "d1e2f3a4-b5c6-4d7e-8f9a-0b1c2d3e4f5a"

_QUERY = "Produce a comprehensive research report on renewable energy adoption trends."
_GPT4O = "gpt-4o"
_CLAUDE = "claude-3-5-sonnet-20241022"
_PARAMS = {"temperature": 0.5, "max_tokens": 1000}
_N2_USAGE = {"input": 85, "output": 120, "total": 205, "unit": "TOKENS",
             "inputCost": 0.000213, "outputCost": 0.001200, "totalCost": 0.001413}
_N5_USAGE = {"input": 350, "output": 280, "total": 630, "unit": "TOKENS",
             "inputCost": 0.000875, "outputCost": 0.002800, "totalCost": 0.003675}
_N8_USAGE = {"input": 450, "output": 320, "total": 770, "unit": "TOKENS",
             "inputCost": 0.001350, "outputCost": 0.004800, "totalCost": 0.006150}
_N12_USAGE = {"input": 550, "output": 190, "total": 740, "unit": "TOKENS",
              "inputCost": 0.001375, "outputCost": 0.001900, "totalCost": 0.003275}


def build_events():
    events = []

    # 1. trace-create (Workflow)
    events.append(wrap_event(
        make_event_id("s07-e01"), make_timestamp(_BASE, 15.000),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Research Report Workflow",
            user_id=_USER,
            input={"query": _QUERY},
            metadata={"workflow_id": "wf-007", "workflow_run_id": _TRACE},
        ),
    ))

    # 2. span-create (workflow-level span)
    events.append(wrap_event(
        make_event_id("s07-e02"), make_timestamp(_BASE, 15.001),
        "span-create",
        make_span_create(
            span_id=_WF_SPAN, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 14.900),
            input={"query": _QUERY},
            output={"report": "Renewable energy adoption is accelerating globally..."},
        ),
    ))

    # 3. span-create (node 1: Start)
    events.append(wrap_event(
        make_event_id("s07-e03"), make_timestamp(_BASE, 15.002),
        "span-create",
        make_span_create(
            span_id=_N1, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.010),
            parent_observation_id=_WF_SPAN,
            input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    # 4. generation-create (node 2: LLM — Analyze Query)
    events.append(wrap_event(
        make_event_id("s07-e04"), make_timestamp(_BASE, 15.003),
        "generation-create",
        make_generation_create(
            gen_id=_N2, trace_id=_TRACE, name=_GPT4O, model=_GPT4O,
            start_time=make_timestamp(_BASE, 0.050),
            end_time=make_timestamp(_BASE, 1.500),
            usage=_N2_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.200),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": "Query analysis: The user wants a comprehensive report on renewable energy adoption trends, covering solar, wind, hydro, and policy dimensions."},
        ),
    ))

    # 5. span-create (node 3: Knowledge Retrieval)
    events.append(wrap_event(
        make_event_id("s07-e05"), make_timestamp(_BASE, 15.004),
        "span-create",
        make_span_create(
            span_id=_N3, trace_id=_TRACE, name="Knowledge Retrieval",
            start_time=make_timestamp(_BASE, 1.550),
            end_time=make_timestamp(_BASE, 2.300),
            parent_observation_id=_WF_SPAN,
            input={"query": "renewable energy adoption trends 2024"},
            output={"documents": [{"title": "IEA Renewables Report", "score": 0.93}]},
        ),
    ))

    # 6. span-create (node 4: Tool — Web Search)
    events.append(wrap_event(
        make_event_id("s07-e06"), make_timestamp(_BASE, 15.005),
        "span-create",
        make_span_create(
            span_id=_N4, trace_id=_TRACE, name="web_search",
            start_time=make_timestamp(_BASE, 2.350),
            end_time=make_timestamp(_BASE, 3.500),
            parent_observation_id=_WF_SPAN,
            input={"query": "renewable energy statistics 2024"},
            output={"results": [{"url": "https://example.com/iea", "snippet": "Global renewable capacity increased 50% in 2024"}]},
        ),
    ))

    # 7. generation-create (node 5: LLM — Synthesize Answer)
    events.append(wrap_event(
        make_event_id("s07-e07"), make_timestamp(_BASE, 15.006),
        "generation-create",
        make_generation_create(
            gen_id=_N5, trace_id=_TRACE, name=_GPT4O, model=_GPT4O,
            start_time=make_timestamp(_BASE, 3.550),
            end_time=make_timestamp(_BASE, 7.000),
            usage=_N5_USAGE,
            completion_start_time=make_timestamp(_BASE, 3.700),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": "Renewable energy adoption is accelerating. Solar leads with 50% growth, wind at 20%, and policy support is strong in EU and China."},
        ),
    ))

    # 8. span-create (node 6: IF/ELSE — Check Confidence)
    events.append(wrap_event(
        make_event_id("s07-e08"), make_timestamp(_BASE, 15.007),
        "span-create",
        make_span_create(
            span_id=_N6, trace_id=_TRACE, name="Check Confidence",
            start_time=make_timestamp(_BASE, 7.050),
            end_time=make_timestamp(_BASE, 7.060),
            parent_observation_id=_WF_SPAN,
            input={"confidence": 0.85}, output={"branch": "high_confidence"},
        ),
    ))

    # 9. span-create (node 7: Tool — Fetch Additional Data)
    events.append(wrap_event(
        make_event_id("s07-e09"), make_timestamp(_BASE, 15.008),
        "span-create",
        make_span_create(
            span_id=_N7, trace_id=_TRACE, name="data_fetch",
            start_time=make_timestamp(_BASE, 7.100),
            end_time=make_timestamp(_BASE, 8.200),
            parent_observation_id=_WF_SPAN,
            input={"endpoint": "/api/stats/renewable"},
            output={"data": {"solar_gw": 1400, "wind_gw": 900, "hydro_gw": 1300}},
        ),
    ))

    # 10. generation-create (node 8: LLM — Refine Response, claude-3-5-sonnet)
    events.append(wrap_event(
        make_event_id("s07-e10"), make_timestamp(_BASE, 15.009),
        "generation-create",
        make_generation_create(
            gen_id=_N8, trace_id=_TRACE, name=_CLAUDE, model=_CLAUDE,
            start_time=make_timestamp(_BASE, 8.250),
            end_time=make_timestamp(_BASE, 12.000),
            usage=_N8_USAGE,
            completion_start_time=make_timestamp(_BASE, 8.400),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": "Refined report: Global renewable capacity reached 3.6 TW in 2024. Solar 1.4 TW, wind 0.9 TW, hydro 1.3 TW. EU Green Deal and China's 14th Five-Year Plan drive growth."},
        ),
    ))

    # 11. span-create (node 9: Template Transform)
    events.append(wrap_event(
        make_event_id("s07-e11"), make_timestamp(_BASE, 15.010),
        "span-create",
        make_span_create(
            span_id=_N9, trace_id=_TRACE, name="Format Output",
            start_time=make_timestamp(_BASE, 12.050),
            end_time=make_timestamp(_BASE, 12.100),
            parent_observation_id=_WF_SPAN,
            input={"text": "Refined report..."},
            output={"formatted": "# Renewable Energy Adoption Report\n\n## Key Findings\n..."},
        ),
    ))

    # 12. span-create (node 10: Tool — Validate Output)
    events.append(wrap_event(
        make_event_id("s07-e12"), make_timestamp(_BASE, 15.011),
        "span-create",
        make_span_create(
            span_id=_N10, trace_id=_TRACE, name="validate_output",
            start_time=make_timestamp(_BASE, 12.150),
            end_time=make_timestamp(_BASE, 12.400),
            parent_observation_id=_WF_SPAN,
            input={"text": "# Renewable Energy Adoption Report..."},
            output={"valid": True, "issues": []},
        ),
    ))

    # 13. span-create (node 11: Variable Aggregator)
    events.append(wrap_event(
        make_event_id("s07-e13"), make_timestamp(_BASE, 15.012),
        "span-create",
        make_span_create(
            span_id=_N11, trace_id=_TRACE, name="Merge Variables",
            start_time=make_timestamp(_BASE, 12.450),
            end_time=make_timestamp(_BASE, 12.460),
            parent_observation_id=_WF_SPAN,
            input={"variables": ["report", "stats", "validation"]},
            output={"merged": {"report": "...", "stats": {}, "valid": True}},
        ),
    ))

    # 14. generation-create (node 12: LLM — Final Polish, gpt-4o)
    events.append(wrap_event(
        make_event_id("s07-e14"), make_timestamp(_BASE, 15.013),
        "generation-create",
        make_generation_create(
            gen_id=_N12, trace_id=_TRACE, name=_GPT4O, model=_GPT4O,
            start_time=make_timestamp(_BASE, 12.500),
            end_time=make_timestamp(_BASE, 14.500),
            usage=_N12_USAGE,
            completion_start_time=make_timestamp(_BASE, 12.650),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": "Polish the final report"}]},
            output={"text": "Final polished report: Renewable energy adoption surged in 2024 with global capacity reaching 3.6 TW..."},
        ),
    ))

    # 15. span-create (node 13: IF/ELSE — Quality Check)
    events.append(wrap_event(
        make_event_id("s07-e15"), make_timestamp(_BASE, 15.014),
        "span-create",
        make_span_create(
            span_id=_N13, trace_id=_TRACE, name="Quality Check",
            start_time=make_timestamp(_BASE, 14.550),
            end_time=make_timestamp(_BASE, 14.560),
            parent_observation_id=_WF_SPAN,
            input={"quality_score": 0.92}, output={"passed": True},
        ),
    ))

    # 16. span-create (node 14: Code — Post-process)
    events.append(wrap_event(
        make_event_id("s07-e16"), make_timestamp(_BASE, 15.015),
        "span-create",
        make_span_create(
            span_id=_N14, trace_id=_TRACE, name="Post-process",
            start_time=make_timestamp(_BASE, 14.600),
            end_time=make_timestamp(_BASE, 14.800),
            parent_observation_id=_WF_SPAN,
            input={"report": "Final polished report..."},
            output={"report": "## Renewable Energy Adoption Report\n\nFinal version with formatting..."},
        ),
    ))

    # 17. span-create (node 15: End)
    events.append(wrap_event(
        make_event_id("s07-e17"), make_timestamp(_BASE, 15.016),
        "span-create",
        make_span_create(
            span_id=_N15, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 14.850),
            end_time=make_timestamp(_BASE, 14.900),
            parent_observation_id=_WF_SPAN,
            input={"report": "## Renewable Energy Adoption Report..."},
            output={"report": "## Renewable Energy Adoption Report\n\nFinal version..."},
        ),
    ))

    return events


def build_meta():
    nodes = [
        (3, "span-create", "Start"),
        (4, "generation-create", "Analyze Query (gpt-4o)"),
        (5, "span-create", "Knowledge Retrieval"),
        (6, "span-create", "Web Search (tool)"),
        (7, "generation-create", "Synthesize Answer (gpt-4o)"),
        (8, "span-create", "Check Confidence (IF/ELSE)"),
        (9, "span-create", "Fetch Additional Data (tool)"),
        (10, "generation-create", "Refine Response (claude-3-5-sonnet)"),
        (11, "span-create", "Format Output (Template)"),
        (12, "span-create", "Validate Output (tool)"),
        (13, "span-create", "Merge Variables (Aggregator)"),
        (14, "generation-create", "Final Polish (gpt-4o)"),
        (15, "span-create", "Quality Check (IF/ELSE)"),
        (16, "span-create", "Post-process (Code)"),
        (17, "span-create", "End"),
    ]
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
        ] + [
            {"index": idx, "type": etype, "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"}
            for idx, etype, _name in nodes
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: high-N. 15-node workflow with 4 LLM nodes (gpt-4o x3, claude-3-5-sonnet x1), 3 tool calls, 8 other nodes. 17 sequential HTTP POSTs from one WorkflowTraceInfo task.",
    }
