"""Scenario 12: Multi-model pipeline — 8 LLM nodes alternating across 3 models.

Events (12):
  1.  trace-create       (WorkflowTraceInfo)
  2.  span-create        (workflow root)
  3.  span-create        (Start, parent=workflow)
  4.  generation-create  (LLM 1: gpt-4o, Detect Languages)
  5.  generation-create  (LLM 2: claude-3-5-sonnet, Translate FR)
  6.  generation-create  (LLM 3: deepseek-chat, Translate DE)
  7.  generation-create  (LLM 4: gpt-4o, Analyze Policy)
  8.  generation-create  (LLM 5: claude-3-5-sonnet, Cross-language Compare)
  9.  generation-create  (LLM 6: deepseek-chat, Summarize)
  10.  generation-create  (LLM 7: gpt-4o, Critique)
  11.  generation-create  (LLM 8: claude-3-5-sonnet, Final Report)
  12.  span-create        (End, parent=workflow)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "12-multi-model-pipeline"
SCENARIO_DESCRIPTION = "8 LLM nodes alternating across 3 models (gpt-4o, claude, deepseek)"
APP_TYPE = "workflow"
DIFY_APP_MODE = "workflow"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo"]
EXPECTED_EVENT_COUNT = 12
EXPECTED_SPAN_COUNT = 11
SPAN_PATTERN = "multi-model"

_BASE = "2025-01-15T16:00:00.000000+00:00"
_TRACE = "c1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_WF = "c2a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_START = "c3a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N1 = "c4a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N2 = "c5a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N3 = "c6a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N4 = "c7a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N5 = "c8a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N6 = "c9a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N7 = "caa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N8 = "cba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_END = "cca2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-8e4f3a2b1c"
_QUERY = "Translate, analyze, and summarize this multilingual document about climate policy"
_PARAMS = {"temperature": 0.5, "max_tokens": 1500}

_M1 = "gpt-4o"
_M2 = "claude-3-5-sonnet-20241022"
_M3 = "deepseek-chat"

_N1_OUT = "Language detection: English (60%), French (25%), German (15%). Document type: climate policy white paper. Total length: 12,400 words."
_N2_OUT = "French sections translated: 'La politique climatique necessite une action immediate' becomes 'Climate policy requires immediate action.' All 3,100 French words translated."
_N3_OUT = "German sections translated: 'Die Klimapolitik erfordert sofortige Massnahmen' becomes 'Climate policy requires immediate measures.' All 1,860 German words translated."
_N4_OUT = "Policy analysis: The document proposes 3-tier emission reduction targets: 40% by 2030, 65% by 2040, 90% by 2050 (vs 2019 baseline)."
_N5_OUT = "Cross-language comparison: French text emphasizes 'solidarite' (solidarity), German emphasizes 'Nachhaltigkeit' (sustainability), English emphasizes 'commitment'. Nuances preserved in translation."
_N6_OUT = "Summary: Multilateral climate policy framework with binding targets, differentiated responsibilities for developed/developing nations, and technology transfer provisions. 15-year implementation timeline."
_N7_OUT = "Critique: Strengths include binding targets and technology transfer mechanisms. Weaknesses: enforcement relies on self-reporting, equity concerns around baseline selection, no penalty mechanism."
_N8_OUT = "Final Report: A comprehensive multilingual climate policy analysis. The document presents ambitious but feasible targets across 3 languages. Key recommendation: strengthen enforcement and equity provisions."
_FINAL = {"report": _N8_OUT, "languages": ["en", "fr", "de"], "models_used": 3}

_N1_U = {"input": 100, "output": 120, "total": 220, "unit": "TOKENS", "inputCost": 0.000250, "outputCost": 0.001200, "totalCost": 0.001450}
_N2_U = {"input": 110, "output": 130, "total": 240, "unit": "TOKENS", "inputCost": 0.000330, "outputCost": 0.001950, "totalCost": 0.002280}
_N3_U = {"input": 120, "output": 140, "total": 260, "unit": "TOKENS", "inputCost": 0.000017, "outputCost": 0.000039, "totalCost": 0.000056}
_N4_U = {"input": 130, "output": 150, "total": 280, "unit": "TOKENS", "inputCost": 0.000325, "outputCost": 0.001500, "totalCost": 0.001825}
_N5_U = {"input": 140, "output": 160, "total": 300, "unit": "TOKENS", "inputCost": 0.000420, "outputCost": 0.002400, "totalCost": 0.002820}
_N6_U = {"input": 150, "output": 170, "total": 320, "unit": "TOKENS", "inputCost": 0.000021, "outputCost": 0.000048, "totalCost": 0.000069}
_N7_U = {"input": 160, "output": 180, "total": 340, "unit": "TOKENS", "inputCost": 0.000400, "outputCost": 0.001800, "totalCost": 0.002200}
_N8_U = {"input": 170, "output": 190, "total": 360, "unit": "TOKENS", "inputCost": 0.000510, "outputCost": 0.002850, "totalCost": 0.003360}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s12-e01"), timestamp=make_timestamp(_BASE, 8.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Multilingual Translation Pipeline", user_id=_USER,
            input={"query": _QUERY},
            metadata={"workflow_id": "wf-mtp-012", "workflow_run_id": _TRACE},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s12-e02"), timestamp=make_timestamp(_BASE, 8.002),
        event_type="span-create",
        body=make_span_create(
            span_id=_WF, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 8.000),
            input={"query": _QUERY}, output=_FINAL,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s12-e03"), timestamp=make_timestamp(_BASE, 8.003),
        event_type="span-create",
        body=make_span_create(
            span_id=_START, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 0.050),
            parent_observation_id=_WF, input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    nodes = [
        ("Detect Languages", _N1, _M1, 0.100, 1.000, _N1_OUT, _N1_U),
        ("Translate FR", _N2, _M2, 1.050, 2.000, _N2_OUT, _N2_U),
        ("Translate DE", _N3, _M3, 2.050, 3.000, _N3_OUT, _N3_U),
        ("Analyze Policy", _N4, _M1, 3.050, 4.000, _N4_OUT, _N4_U),
        ("Cross-language Compare", _N5, _M2, 4.050, 5.000, _N5_OUT, _N5_U),
        ("Summarize", _N6, _M3, 5.050, 6.000, _N6_OUT, _N6_U),
        ("Critique", _N7, _M1, 6.050, 7.000, _N7_OUT, _N7_U),
        ("Final Report", _N8, _M2, 7.050, 7.900, _N8_OUT, _N8_U),
    ]
    for i, (name, nid, model, start, end, out, usage) in enumerate(nodes):
        events.append(wrap_event(
            event_id=make_event_id(f"s12-e{i+4:02d}"),
            timestamp=make_timestamp(_BASE, 8.004 + i * 0.001),
            event_type="generation-create",
            body=make_generation_create(
                gen_id=nid, trace_id=_TRACE, name=model, model=model,
                start_time=make_timestamp(_BASE, start), end_time=make_timestamp(_BASE, end),
                usage=usage, completion_start_time=make_timestamp(_BASE, start + 0.200),
                model_parameters=_PARAMS, parent_observation_id=_WF,
                input={"messages": [{"role": "user", "content": f"Execute node: {name}"}]},
                output={"text": out},
            ),
        ))

    events.append(wrap_event(
        event_id=make_event_id("s12-e12"), timestamp=make_timestamp(_BASE, 8.012),
        event_type="span-create",
        body=make_span_create(
            span_id=_END, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 7.950), end_time=make_timestamp(_BASE, 8.000),
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
                 "generation-create", "generation-create", "span-create"], 1)
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "8 LLM nodes alternating: gpt-4o, claude-3-5-sonnet-20241022, deepseek-chat, gpt-4o, claude, deepseek, gpt-4o, claude. Each node uses a different model for diversity.",
    }
