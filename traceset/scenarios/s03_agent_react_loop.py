"""Scenario 03: Agent ReAct loop — 5 iterations of think-act-observe.

Events (12):
  1.  trace-create       (MessageTraceInfo)
  2.  generation-create  (Think 1: decide to query DB)
  3.  span-create        (Tool 1: sql_query)
  4.  generation-create  (Think 2: calculate growth)
  5.  span-create        (Tool 2: calculator)
  6.  generation-create  (Think 3: batch calculate)
  7.  span-create        (Tool 3: python_interpreter)
  8.  generation-create  (Think 4: search for context)
  9.  span-create        (Tool 4: web_search)
  10.  generation-create  (Think 5: lookup marketing data)
  11.  span-create        (Tool 5: data_lookup)
  12.  generation-create  (Final synthesis)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "03-agent-react-loop"
SCENARIO_DESCRIPTION = "5 ReAct iterations (think-act) + final synthesis"
APP_TYPE = "agent-chat"
DIFY_APP_MODE = "agent-chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "ToolTraceInfo"]
EXPECTED_EVENT_COUNT = 12
EXPECTED_SPAN_COUNT = 11
SPAN_PATTERN = "react-loop"

_BASE = "2025-01-15T11:30:00.000000+00:00"
_TRACE = "31a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T1 = "32a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T2 = "33a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T3 = "34a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T4 = "35a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T5 = "36a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T6 = "37a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T7 = "38a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T8 = "39a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T9 = "3aa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T10 = "3ba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T11 = "3ca2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-9b5c4d3e2f"
_CONV = "conv-3c4d5e6f7a8b"
_QUERY = "What is the year-over-year revenue growth for our top 5 products?"
_MODEL = "gpt-4o"
_PARAMS = {"temperature": 0.7, "max_tokens": 1000}

_THINK1_IN = "The user wants YoY revenue growth for top 5 products. I need to query the sales database."
_THINK1_OUT = "I need to query the sales database to get revenue data for our top 5 products in 2024 and 2023. I will use the sql_query tool."
_TOOL1_IN = {"query": "SELECT product_name, SUM(revenue) as total FROM sales WHERE year IN (2024, 2023) GROUP BY product_name, year ORDER BY total DESC LIMIT 10"}
_TOOL1_OUT = {"results": [{"product": "Analytics Pro", "2024": 1700000, "2023": 1200000}, {"product": "Enterprise", "2024": 4000000, "2023": 3000000}, {"product": "Pro Suite", "2024": 2400000, "2023": 2000000}, {"product": "Cloud Basic", "2024": 1300000, "2023": 1130000}, {"product": "Mobile Add-on", "2024": 910000, "2023": 960000}]}

_THINK2_IN = "I have the revenue data. Now I need to calculate the YoY growth percentage."
_THINK2_OUT = "I have the raw revenue data. Let me calculate the growth percentage for Analytics Pro first to verify the approach."
_TOOL2_IN = {"expression": "(1700000-1200000)/1200000*100"}
_TOOL2_OUT = {"result": "41.666666666666664"}

_THINK3_IN = "The calculation works. Now I need to do this for all 5 products."
_THINK3_OUT = "The calculation works. I will use the python_interpreter to calculate growth for all 5 products at once."
_TOOL3_IN = {"code": "products = {'Analytics Pro': (1700000, 1200000), 'Enterprise': (4000000, 3000000), 'Pro Suite': (2400000, 2000000), 'Cloud Basic': (1300000, 1130000), 'Mobile Add-on': (910000, 960000)}\ngrowth = {k: round((v[0]-v[1])/v[1]*100, 1) for k, v in products.items()}\nprint(growth)"}
_TOOL3_OUT = {"stdout": "{'Analytics Pro': 41.7, 'Enterprise': 33.3, 'Pro Suite': 20.0, 'Cloud Basic': 15.0, 'Mobile Add-on': -5.2}"}

_THINK4_IN = "Analytics Pro has the highest growth. Let me search for context."
_THINK4_OUT = "Analytics Pro has the highest growth at 41.7%. I should search for what drove this growth."
_TOOL4_IN = {"query": "Analytics Pro product features 2024 ML dashboard"}
_TOOL4_OUT = {"results": ["Analytics Pro: Released ML-powered dashboard in Q3 2024. Features: 50% faster query performance, AI-powered insights, automated anomaly detection."]}

_THINK5_IN = "Now let me get the marketing campaign data."
_THINK5_OUT = "I should look up marketing campaign data to understand the demand drivers for Analytics Pro."
_TOOL5_IN = {"campaign": "Analytics Pro Q3-Q4 2024"}
_TOOL5_OUT = {"campaigns": [{"name": "Q3 Launch", "spend": 15000, "leads": 1200}, {"name": "Black Friday", "spend": 12000, "leads": 800}, {"name": "Year-end Sale", "spend": 18000, "leads": 1200}], "total_spend": 45000, "total_leads": 3200, "conversion_rate": 0.125}

_SYN_OUT = (
    "Year-over-Year Revenue Growth — Top 5 Products:\n"
    "1. Analytics Pro: +41.7% ($1.2M to $1.7M) — Driven by ML dashboard launch "
    "in Q3 2024 and $45K marketing investment generating 3,200 leads.\n"
    "2. Enterprise: +33.3% ($3.0M to $4.0M) — Expanded enterprise sales team.\n"
    "3. Pro Suite: +20.0% ($2.0M to $2.4M) — Steady growth from existing base.\n"
    "4. Cloud Basic: +15.0% ($1.1M to $1.3M) — New SMB onboarding flow.\n"
    "5. Mobile Add-on: -5.2% ($0.96M to $0.91M) — Market saturation."
)

_TH1_U = {"input": 85, "output": 45, "total": 130, "unit": "TOKENS", "inputCost": 0.000213, "outputCost": 0.000450, "totalCost": 0.000663}
_TH2_U = {"input": 120, "output": 50, "total": 170, "unit": "TOKENS", "inputCost": 0.000300, "outputCost": 0.000500, "totalCost": 0.000800}
_TH3_U = {"input": 150, "output": 65, "total": 215, "unit": "TOKENS", "inputCost": 0.000375, "outputCost": 0.000650, "totalCost": 0.001025}
_TH4_U = {"input": 180, "output": 55, "total": 235, "unit": "TOKENS", "inputCost": 0.000450, "outputCost": 0.000550, "totalCost": 0.001000}
_TH5_U = {"input": 200, "output": 60, "total": 260, "unit": "TOKENS", "inputCost": 0.000500, "outputCost": 0.000600, "totalCost": 0.001100}
_SYN_U = {"input": 250, "output": 280, "total": 530, "unit": "TOKENS", "inputCost": 0.000625, "outputCost": 0.002800, "totalCost": 0.003425}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s03-e01"), timestamp=make_timestamp(_BASE, 12.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Revenue Growth Analysis Agent", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"agent_id": "agent-data-analyst", "conversation_id": _CONV},
        ),
    ))

    # ReAct iteration 1
    events.append(wrap_event(
        event_id=make_event_id("s03-e02"), timestamp=make_timestamp(_BASE, 12.002),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_T1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.050), end_time=make_timestamp(_BASE, 1.500),
            usage=_TH1_U, completion_start_time=make_timestamp(_BASE, 0.200),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}, {"role": "system", "content": _THINK1_IN}]},
            output={"text": _THINK1_OUT, "tool_calls": [{"name": "sql_query", "arguments": _TOOL1_IN}]},
        ),
    ))
    events.append(wrap_event(
        event_id=make_event_id("s03-e03"), timestamp=make_timestamp(_BASE, 12.003),
        event_type="span-create",
        body=make_span_create(
            span_id=_T2, trace_id=_TRACE, name="sql_query",
            start_time=make_timestamp(_BASE, 1.500), end_time=make_timestamp(_BASE, 3.000),
            input=_TOOL1_IN, output=_TOOL1_OUT,
            metadata={"tool_provider": "internal", "tool_version": "1.2.0"},
        ),
    ))

    # ReAct iteration 2
    events.append(wrap_event(
        event_id=make_event_id("s03-e04"), timestamp=make_timestamp(_BASE, 12.004),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_T3, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 3.050), end_time=make_timestamp(_BASE, 4.000),
            usage=_TH2_U, completion_start_time=make_timestamp(_BASE, 3.200),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _THINK2_IN}]},
            output={"text": _THINK2_OUT, "tool_calls": [{"name": "calculator", "arguments": _TOOL2_IN}]},
        ),
    ))
    events.append(wrap_event(
        event_id=make_event_id("s03-e05"), timestamp=make_timestamp(_BASE, 12.005),
        event_type="span-create",
        body=make_span_create(
            span_id=_T4, trace_id=_TRACE, name="calculator",
            start_time=make_timestamp(_BASE, 4.000), end_time=make_timestamp(_BASE, 4.500),
            input=_TOOL2_IN, output=_TOOL2_OUT,
        ),
    ))

    # ReAct iteration 3
    events.append(wrap_event(
        event_id=make_event_id("s03-e06"), timestamp=make_timestamp(_BASE, 12.006),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_T5, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 4.550), end_time=make_timestamp(_BASE, 6.000),
            usage=_TH3_U, completion_start_time=make_timestamp(_BASE, 4.700),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _THINK3_IN}]},
            output={"text": _THINK3_OUT, "tool_calls": [{"name": "python_interpreter", "arguments": _TOOL3_IN}]},
        ),
    ))
    events.append(wrap_event(
        event_id=make_event_id("s03-e07"), timestamp=make_timestamp(_BASE, 12.007),
        event_type="span-create",
        body=make_span_create(
            span_id=_T6, trace_id=_TRACE, name="python_interpreter",
            start_time=make_timestamp(_BASE, 6.000), end_time=make_timestamp(_BASE, 7.500),
            input=_TOOL3_IN, output=_TOOL3_OUT,
        ),
    ))

    # ReAct iteration 4
    events.append(wrap_event(
        event_id=make_event_id("s03-e08"), timestamp=make_timestamp(_BASE, 12.008),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_T7, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 7.550), end_time=make_timestamp(_BASE, 8.500),
            usage=_TH4_U, completion_start_time=make_timestamp(_BASE, 7.700),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _THINK4_IN}]},
            output={"text": _THINK4_OUT, "tool_calls": [{"name": "web_search", "arguments": _TOOL4_IN}]},
        ),
    ))
    events.append(wrap_event(
        event_id=make_event_id("s03-e09"), timestamp=make_timestamp(_BASE, 12.009),
        event_type="span-create",
        body=make_span_create(
            span_id=_T8, trace_id=_TRACE, name="web_search",
            start_time=make_timestamp(_BASE, 8.500), end_time=make_timestamp(_BASE, 10.000),
            input=_TOOL4_IN, output=_TOOL4_OUT,
        ),
    ))

    # ReAct iteration 5
    events.append(wrap_event(
        event_id=make_event_id("s03-e10"), timestamp=make_timestamp(_BASE, 12.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_T9, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 10.050), end_time=make_timestamp(_BASE, 10.800),
            usage=_TH5_U, completion_start_time=make_timestamp(_BASE, 10.200),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _THINK5_IN}]},
            output={"text": _THINK5_OUT, "tool_calls": [{"name": "data_lookup", "arguments": _TOOL5_IN}]},
        ),
    ))
    events.append(wrap_event(
        event_id=make_event_id("s03-e11"), timestamp=make_timestamp(_BASE, 12.011),
        event_type="span-create",
        body=make_span_create(
            span_id=_T10, trace_id=_TRACE, name="data_lookup",
            start_time=make_timestamp(_BASE, 10.800), end_time=make_timestamp(_BASE, 11.500),
            input=_TOOL5_IN, output=_TOOL5_OUT,
        ),
    ))

    # Final synthesis
    events.append(wrap_event(
        event_id=make_event_id("s03-e12"), timestamp=make_timestamp(_BASE, 12.012),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_T11, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 11.550), end_time=make_timestamp(_BASE, 12.000),
            usage=_SYN_U, completion_start_time=make_timestamp(_BASE, 11.700),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": "Synthesize all findings into a comprehensive answer."}]},
            output={"text": _SYN_OUT},
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
            {"index": 1, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 2, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 4, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 6, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 8, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 9, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 10, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 11, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 12, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "ReAct loop: 5 iterations of think (LLM) then act (tool). Tools: sql_query, calculator, python_interpreter, web_search, data_lookup. Final synthesis LLM call.",
    }
