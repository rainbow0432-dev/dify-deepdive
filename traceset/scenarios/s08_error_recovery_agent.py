"""Scenario 08: Error recovery agent — 4 tool errors then 1 success + RAG + synthesis.

Events (12):
  1.  trace-create       (MessageTraceInfo)
  2.  span-create        (Tool 1: web_fetch, level=ERROR)
  3.  span-create        (Tool 2: api_call, level=ERROR)
  4.  span-create        (Tool 3: db_query, level=ERROR)
  5.  span-create        (Tool 4: file_read, level=ERROR)
  6.  span-create        (Tool 5: cache_lookup, success)
  7.  span-create        (Knowledge Retrieval)
  8.  generation-create  (Synthesis: Generate)
  9.  generation-create  (Synthesis: Refine)
  10.  generation-create  (SuggestedQuestions)
  11.  trace-create       (GenerateNameTraceInfo, upsert)
  12.  span-create        (GenerateName)

VERIFY: 4 consecutive tool errors (level=ERROR) before a successful fallback.
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "08-error-recovery-agent"
SCENARIO_DESCRIPTION = "5 tool attempts (4 errors + 1 success) + RAG + synthesis + GenerateName"
APP_TYPE = "agent-chat"
DIFY_APP_MODE = "agent-chat"
EDGE_CASE = "error-recovery"
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "ToolTraceInfo", "DatasetRetrievalTraceInfo", "SuggestedQuestionTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 12
EXPECTED_SPAN_COUNT = 10
SPAN_PATTERN = "error-recovery"

_BASE = "2025-01-15T14:00:00.000000+00:00"
_TRACE = "81a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T1 = "82a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T2 = "83a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T3 = "84a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T4 = "85a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T5 = "86a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_RAG = "87a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SYN1 = "88a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SYN2 = "89a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SUGG = "8aa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_NAME = "8ba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-4a0b9c8d7e"
_CONV = "conv-7a8b9c0d1e2f"
_QUERY = "Fetch the latest sales data from the external CRM API and generate a summary"
_MODEL = "gpt-4o"
_PARAMS = {"temperature": 0.5, "max_tokens": 1500}

_T1_IN = {"url": "https://api.crm.example.com/v2/sales", "method": "GET"}
_T1_OUT = {"error": "Timeout: CRM API at api.crm.example.com did not respond within 30s", "error_type": "TimeoutError"}
_T2_IN = {"url": "https://api.crm.example.com/v2/sales", "method": "GET", "headers": {"Authorization": "Bearer <key>"}}
_T2_OUT = {"error": "401 Unauthorized: API key expired. Please refresh credentials.", "error_type": "AuthenticationError"}
_T3_IN = {"connection_string": "postgresql://sales_db:5432/sales", "query": "SELECT * FROM daily_sales WHERE date='2025-01-15'"}
_T3_OUT = {"error": "Connection refused: sales_db:5432 is unreachable. Check network configuration.", "error_type": "ConnectionError"}
_T4_IN = {"path": "/data/sales_cache.json", "mode": "r"}
_T4_OUT = {"error": "FileNotFoundError: /data/sales_cache.json does not exist", "error_type": "FileNotFoundError"}
_T5_IN = {"cache_key": "sales_daily_2025_01_15"}
_T5_OUT = {"data": {"transactions": 847, "revenue": 2300000, "avg_order": 2715, "source": "cache", "cache_age_hours": 2}}
_RAG_OUT = {"documents": [{"title": "Sales Report Template Q4", "content": "Standard template includes: revenue, transactions, AOV, top products, trends.", "score": 0.93}]}
_SYN1_OUT = "Sales Summary (from cached data): Total Revenue: $2.3M. Transactions: 847. Avg Order Value: $2,715. Data source: 2-hour-old cache (live API unavailable)."
_SYN2_OUT = "Comprehensive Sales Report — 2025-01-15\n\nRevenue: $2,300,000\nTransactions: 847\nAverage Order Value: $2,715\nTop Products: Pro Suite ($480K), Enterprise ($420K), Analytics Pro ($310K)\n\nNote: Live CRM data was unavailable due to API timeout, auth failure, database connectivity, and missing cache file. Report generated from 2-hour-old cached data. Recommended actions: 1) Refresh CRM API key, 2) Verify database network access, 3) Implement automated cache refresh."
_SUGG_OUT = {"questions": ["What caused the API failures?", "How can I refresh the CRM API key?", "When will live data be available?"]}
_NAME_OUT = "Sales Data Summary (Cached)"

_SYN1_U = {"input": 200, "output": 220, "total": 420, "unit": "TOKENS", "inputCost": 0.000500, "outputCost": 0.002200, "totalCost": 0.002700}
_SYN2_U = {"input": 280, "output": 200, "total": 480, "unit": "TOKENS", "inputCost": 0.000700, "outputCost": 0.002000, "totalCost": 0.002700}
_SUGG_U = {"input": 150, "output": 80, "total": 230, "unit": "TOKENS", "inputCost": 0.000375, "outputCost": 0.000800, "totalCost": 0.001175}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s08-e01"), timestamp=make_timestamp(_BASE, 8.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Sales Data Agent", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"agent_id": "agent-sales-fetcher", "conversation_id": _CONV},
        ),
    ))

    error_tools = [
        ("web_fetch", _T1, 0.050, 1.000, _T1_IN, _T1_OUT, "TimeoutError: CRM API did not respond within 30s"),
        ("api_call", _T2, 1.050, 2.000, _T2_IN, _T2_OUT, "AuthenticationError: API key expired"),
        ("db_query", _T3, 2.050, 3.000, _T3_IN, _T3_OUT, "ConnectionError: database unreachable"),
        ("file_read", _T4, 3.050, 4.000, _T4_IN, _T4_OUT, "FileNotFoundError: cache file does not exist"),
    ]
    for i, (name, sid, start, end, tin, tout, msg) in enumerate(error_tools):
        events.append(wrap_event(
            event_id=make_event_id(f"s08-e{i+2:02d}"),
            timestamp=make_timestamp(_BASE, 8.002 + i * 0.001),
            event_type="span-create",
            body=make_span_create(
                span_id=sid, trace_id=_TRACE, name=name,
                start_time=make_timestamp(_BASE, start), end_time=make_timestamp(_BASE, end),
                input=tin, output=tout,
                level="ERROR", status_message=msg,
            ),
        ))

    events.append(wrap_event(
        event_id=make_event_id("s08-e06"), timestamp=make_timestamp(_BASE, 8.006),
        event_type="span-create",
        body=make_span_create(
            span_id=_T5, trace_id=_TRACE, name="cache_lookup",
            start_time=make_timestamp(_BASE, 4.050), end_time=make_timestamp(_BASE, 5.000),
            input=_T5_IN, output=_T5_OUT,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s08-e07"), timestamp=make_timestamp(_BASE, 8.007),
        event_type="span-create",
        body=make_span_create(
            span_id=_RAG, trace_id=_TRACE, name="Knowledge Retrieval",
            start_time=make_timestamp(_BASE, 5.050), end_time=make_timestamp(_BASE, 5.800),
            input={"query": "sales report template"}, output=_RAG_OUT,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s08-e08"), timestamp=make_timestamp(_BASE, 8.008),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SYN1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 5.850), end_time=make_timestamp(_BASE, 7.000),
            usage=_SYN1_U, completion_start_time=make_timestamp(_BASE, 6.050),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _SYN1_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s08-e09"), timestamp=make_timestamp(_BASE, 8.009),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SYN2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 7.050), end_time=make_timestamp(_BASE, 7.700),
            usage=_SYN2_U, completion_start_time=make_timestamp(_BASE, 7.200),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": "Refine with detailed analysis and recommendations."}]},
            output={"text": _SYN2_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s08-e10"), timestamp=make_timestamp(_BASE, 8.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SUGG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 7.750), end_time=make_timestamp(_BASE, 8.000),
            usage=_SUGG_U, completion_start_time=make_timestamp(_BASE, 7.850),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": "Suggest follow-up questions."}]},
            output=_SUGG_OUT,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s08-e11"), timestamp=make_timestamp(_BASE, 8.011),
        event_type="trace-create",
        body=make_trace_create(trace_id=_TRACE, name="Generate Name", user_id=_USER),
    ))
    events.append(wrap_event(
        event_id=make_event_id("s08-e12"), timestamp=make_timestamp(_BASE, 8.012),
        event_type="span-create",
        body=make_span_create(
            span_id=_NAME, trace_id=_TRACE, name="Generate Name",
            start_time=make_timestamp(_BASE, 8.050), end_time=make_timestamp(_BASE, 8.300),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _NAME_OUT},
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
            {"index": 2, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 6, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 8, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 9, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 10, "type": "generation-create", "source_trace_type": "SuggestedQuestionTraceInfo", "dify_handler": "LangFuseDataTrace.suggested_question_trace"},
            {"index": 11, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 12, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "VERIFY: 4 consecutive tool errors (TimeoutError, AuthenticationError, ConnectionError, FileNotFoundError) before successful cache_lookup fallback. All error spans have level=ERROR.",
    }
