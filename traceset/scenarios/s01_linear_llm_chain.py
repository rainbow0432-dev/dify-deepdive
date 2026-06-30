"""Scenario 01: Linear LLM chain — 10 sequential LLM nodes in a workflow.

Events (14):
  1.  trace-create       (WorkflowTraceInfo)
  2.  span-create        (workflow root span)
  3.  span-create        (Start node, parent=workflow)
  4.  generation-create  (LLM 1: Extract Themes)
  5.  generation-create  (LLM 2: Sentiment Analysis)
  6.  generation-create  (LLM 3: Categorize Feedback)
  7.  generation-create  (LLM 4: Identify Urgency)
  8.  generation-create  (LLM 5: Extract Action Items)
  9.  generation-create  (LLM 6: Generate Statistics)
  10.  generation-create  (LLM 7: Identify Trends)
  11.  generation-create  (LLM 8: Correlate Features)
  12.  generation-create  (LLM 9: Generate Recommendations)
  13.  generation-create  (LLM 10: Compile Final Report)
  14.  span-create        (End node, parent=workflow)
"""
from traceset.helpers import (
    make_event_id,
    make_timestamp,
    make_trace_create,
    make_span_create,
    make_generation_create,
    wrap_event,
)

SCENARIO_ID = "01-linear-llm-chain"
SCENARIO_DESCRIPTION = "10 sequential LLM nodes in a straight-line workflow"
APP_TYPE = "workflow"
DIFY_APP_MODE = "workflow"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo"]
EXPECTED_EVENT_COUNT = 14
EXPECTED_SPAN_COUNT = 13
SPAN_PATTERN = "linear"

_BASE = "2025-01-15T10:30:00.000000+00:00"

_TRACE = "11a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_WF = "12a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_START = "13a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N1 = "14a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N2 = "15a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N3 = "16a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N4 = "17a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N5 = "18a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N6 = "19a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N7 = "1aa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N8 = "1ba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N9 = "1ca2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N10 = "1da2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_END = "1ea2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-7f3a2b8c4d"

_QUERY = "Analyze the Q4 2024 customer feedback dataset and generate a comprehensive report"

_MODEL = "gpt-4o"
_MODEL_PARAMS = {"temperature": 0.5, "max_tokens": 1000}

_N1_INPUT = (
    "Extract the key themes from the following 848 customer feedback responses "
    "collected during Q4 2024. Identify recurring topics and group them into "
    "thematic categories."
)
_N1_OUTPUT = (
    "Key themes identified across 848 responses: 1) Product Usability (312 mentions) "
    "— dashboard complexity, navigation improvements, mobile responsiveness. "
    "2) Pricing Concerns (198 mentions) — SMB tier pricing, value perception, "
    "competitor comparison. 3) Feature Requests (175 mentions) — Salesforce "
    "integration, workflow automation, advanced analytics. 4) Customer Support "
    "Quality (89 mentions) — response time, knowledge depth, escalation process. "
    "5) Integration Capabilities (74 mentions) — third-party API, webhook "
    "reliability, data sync."
)

_N2_INPUT = (
    "Perform sentiment analysis on the 848 customer feedback responses. "
    "Classify each as positive, neutral, or negative. Provide overall "
    "percentages and highlight trends."
)
_N2_OUTPUT = (
    "Sentiment Analysis Results: Positive: 526 responses (62%). Key positive "
    "themes: ease of use improvements, feature richness, mobile app launch. "
    "Neutral: 195 responses (23%). Primarily feature requests and suggestions. "
    "Negative: 127 responses (15%). Concentrated in pricing (SMB segment) and "
    "support response times. Quarter-over-quarter trend: positive sentiment up "
    "8 percentage points from Q3 (54%)."
)

_N3_INPUT = (
    "Categorize all 848 feedback items by department. Map each theme to the "
    "responsible team and provide item counts."
)
_N3_OUTPUT = (
    "Feedback categorized by department: Product (342 items) — usability, "
    "feature requests, mobile. Engineering (218 items) — bugs, integrations, "
    "performance. Support (198 items) — response time, escalation, knowledge. "
    "Sales (156 items) — pricing, tier comparison, onboarding. Billing (89 "
    "items) — invoice clarity, payment methods, refund process."
)

_N4_INPUT = (
    "Identify urgency levels for all feedback items. Classify as Critical, "
    "High, Medium, or Low. Provide counts and list the critical items."
)
_N4_OUTPUT = (
    "Urgency Classification: Critical: 12 items — data export bug causing data "
    "loss in v3.2 (7), billing overcharges affecting enterprise accounts (3), "
    "security vulnerability in SSO redirect (2). High: 47 items — Salesforce "
    "integration gap (15), workflow automation failures (12), API rate limit "
    "issues (20). Medium: 203 items. Low: 586 items."
)

_N5_INPUT = (
    "Extract actionable items from the feedback. Focus on the Critical and "
    "High urgency items. Provide specific, implementable recommendations."
)
_N5_OUTPUT = (
    "Action Items: 1) [Critical] Fix data export bug in v3.2 — assign to "
    "Engineering, ETA: 1 week. 2) [Critical] Resolve billing overcharges — "
    "assign to Finance, ETA: 3 days. 3) [Critical] Patch SSO redirect "
    "vulnerability — assign to Security, ETA: 24h. 4) [High] Build Salesforce "
    "integration — assign to Product, ETA: Q1 2025. 5) [High] Implement "
    "workflow automation fixes — assign to Engineering, ETA: 2 weeks. 6) [High] "
    "Increase API rate limits for enterprise — assign to Engineering, ETA: 1 week."
)

_N6_INPUT = (
    "Generate summary statistics from the feedback dataset. Include response "
    "rate, NPS, CSAT, and churn risk metrics."
)
_N6_OUTPUT = (
    "Summary Statistics: Total responses: 848 out of 2,494 customers (34% "
    "response rate). NPS: +18 (up from +12 in Q3). Promoters: 412 (49%), "
    "Passives: 312 (37%), Detractors: 124 (15%). CSAT: 4.1/5.0 (up from 3.9). "
    "Churn risk: 8% (down from 11%). Average response length: 247 characters."
)

_N7_INPUT = (
    "Identify trending topics quarter-over-quarter. Compare Q4 2024 themes "
    "with Q3 2024. Highlight significant changes."
)
_N7_OUTPUT = (
    "Trend Analysis Q3 vs Q4: Trending up: API usage (+31%), mobile adoption "
    "(+24%), workflow automation requests (+18%), analytics feature requests "
    "(+15%). Trending down: desktop-only usage (-15%), email support tickets "
    "(-22%), billing complaints (-9%). New themes in Q4: AI-powered insights "
    "(31 mentions), real-time collaboration (24 mentions)."
)

_N8_INPUT = (
    "Correlate product features with customer satisfaction. Identify which "
    "features drive NPS improvement and which correlate with churn."
)
_N8_OUTPUT = (
    "Feature-Satisfaction Correlation: Strong positive correlation (r=0.78) "
    "between Salesforce integration requests and enterprise-tier churn — "
    "customers requesting it are likely to churn without it. Workflow "
    "automation features correlate with NPS improvement (r=0.65). Mobile app "
    "usage correlates with CSAT (+0.4 points). Analytics Pro adoption "
    "correlates with retention (+12% retention for users of advanced analytics)."
)

_N9_INPUT = (
    "Based on all analysis, generate prioritized recommendations for Q1 2025. "
    "Include expected impact and effort estimates."
)
_N9_OUTPUT = (
    "Q1 2025 Prioritized Recommendations: 1) Implement Salesforce integration "
    "(Impact: -40% enterprise churn, Effort: 6 weeks, Priority: P0). 2) Launch "
    "mobile-first onboarding flow (Impact: +15% activation, Effort: 3 weeks, "
    "Priority: P1). 3) Introduce SMB pricing tier at $19/mo (Impact: +20% SMB "
    "retention, Effort: 2 weeks, Priority: P1). 4) Expand workflow automation "
    "library (Impact: +10% NPS, Effort: 4 weeks, Priority: P2). 5) Implement "
    "proactive churn alerts (Impact: -30% churn for at-risk accounts, Effort: "
    "3 weeks, Priority: P2)."
)

_N10_INPUT = (
    "Compile a final executive report summarizing all findings. Include "
    "key metrics, themes, trends, and recommendations. Format for C-suite "
    "presentation."
)
_N10_OUTPUT = (
    "Q4 2024 Customer Feedback Analysis — Executive Report: 848 responses "
    "(34% response rate). NPS improved to +18 (+6 pts QoQ). CSAT: 4.1/5.0. "
    "Five key themes: usability, pricing, features, support, integrations. "
    "Critical issues: data export bug, billing overcharges, SSO vulnerability "
    "(all resolved). Top opportunities: Salesforce integration (P0, 6 weeks), "
    "SMB pricing tier (P1, 2 weeks), mobile onboarding (P1, 3 weeks). "
    "Projected impact of recommendations: -40% enterprise churn, +15% NPS, "
    "+20% SMB retention. Investment: ~14 engineering weeks. ROI: 3.2x by Q3."
)

_FINAL_OUTPUT = {"report": _N10_OUTPUT}

_N1_USAGE = {"input": 85, "output": 120, "total": 205, "unit": "TOKENS",
             "inputCost": 0.000213, "outputCost": 0.001200, "totalCost": 0.001413}
_N2_USAGE = {"input": 95, "output": 180, "total": 275, "unit": "TOKENS",
             "inputCost": 0.000238, "outputCost": 0.001800, "totalCost": 0.002038}
_N3_USAGE = {"input": 110, "output": 90, "total": 200, "unit": "TOKENS",
             "inputCost": 0.000275, "outputCost": 0.000900, "totalCost": 0.001175}
_N4_USAGE = {"input": 130, "output": 85, "total": 215, "unit": "TOKENS",
             "inputCost": 0.000325, "outputCost": 0.000850, "totalCost": 0.001175}
_N5_USAGE = {"input": 145, "output": 210, "total": 355, "unit": "TOKENS",
             "inputCost": 0.000363, "outputCost": 0.002100, "totalCost": 0.002463}
_N6_USAGE = {"input": 90, "output": 75, "total": 165, "unit": "TOKENS",
             "inputCost": 0.000225, "outputCost": 0.000750, "totalCost": 0.000975}
_N7_USAGE = {"input": 100, "output": 130, "total": 230, "unit": "TOKENS",
             "inputCost": 0.000250, "outputCost": 0.001300, "totalCost": 0.001550}
_N8_USAGE = {"input": 115, "output": 95, "total": 210, "unit": "TOKENS",
             "inputCost": 0.000288, "outputCost": 0.000950, "totalCost": 0.001238}
_N9_USAGE = {"input": 125, "output": 220, "total": 345, "unit": "TOKENS",
             "inputCost": 0.000313, "outputCost": 0.002200, "totalCost": 0.002513}
_N10_USAGE = {"input": 180, "output": 350, "total": 530, "unit": "TOKENS",
              "inputCost": 0.000450, "outputCost": 0.003500, "totalCost": 0.003950}


def build_events():
    events = []

    # 1. trace-create (Workflow)
    events.append(wrap_event(
        event_id=make_event_id("s01-e01"),
        timestamp=make_timestamp(_BASE, 20.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE,
            name="Customer Feedback Analysis Pipeline",
            user_id=_USER,
            input={"query": _QUERY},
            metadata={
                "workflow_id": "wf-cfa-001",
                "workflow_run_id": _TRACE,
            },
        ),
    ))

    # 2. span-create (workflow root)
    events.append(wrap_event(
        event_id=make_event_id("s01-e02"),
        timestamp=make_timestamp(_BASE, 20.002),
        event_type="span-create",
        body=make_span_create(
            span_id=_WF,
            trace_id=_TRACE,
            name="workflow",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 20.000),
            input={"query": _QUERY},
            output=_FINAL_OUTPUT,
        ),
    ))

    # 3. span-create (Start node)
    events.append(wrap_event(
        event_id=make_event_id("s01-e03"),
        timestamp=make_timestamp(_BASE, 20.003),
        event_type="span-create",
        body=make_span_create(
            span_id=_START,
            trace_id=_TRACE,
            name="Start",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.050),
            parent_observation_id=_WF,
            input={"query": _QUERY},
            output={"query": _QUERY},
        ),
    ))

    # 4. generation-create (LLM 1: Extract Themes)
    events.append(wrap_event(
        event_id=make_event_id("s01-e04"),
        timestamp=make_timestamp(_BASE, 20.004),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_N1,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 2.000),
            usage=_N1_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.300),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": _N1_INPUT}]},
            output={"text": _N1_OUTPUT},
        ),
    ))

    # 5. generation-create (LLM 2: Sentiment Analysis)
    events.append(wrap_event(
        event_id=make_event_id("s01-e05"),
        timestamp=make_timestamp(_BASE, 20.005),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_N2,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 2.050),
            end_time=make_timestamp(_BASE, 4.000),
            usage=_N2_USAGE,
            completion_start_time=make_timestamp(_BASE, 2.250),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": _N2_INPUT}]},
            output={"text": _N2_OUTPUT},
        ),
    ))

    # 6. generation-create (LLM 3: Categorize)
    events.append(wrap_event(
        event_id=make_event_id("s01-e06"),
        timestamp=make_timestamp(_BASE, 20.006),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_N3,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 4.050),
            end_time=make_timestamp(_BASE, 6.000),
            usage=_N3_USAGE,
            completion_start_time=make_timestamp(_BASE, 4.200),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": _N3_INPUT}]},
            output={"text": _N3_OUTPUT},
        ),
    ))

    # 7. generation-create (LLM 4: Urgency)
    events.append(wrap_event(
        event_id=make_event_id("s01-e07"),
        timestamp=make_timestamp(_BASE, 20.007),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_N4,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 6.050),
            end_time=make_timestamp(_BASE, 8.000),
            usage=_N4_USAGE,
            completion_start_time=make_timestamp(_BASE, 6.200),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": _N4_INPUT}]},
            output={"text": _N4_OUTPUT},
        ),
    ))

    # 8. generation-create (LLM 5: Action Items)
    events.append(wrap_event(
        event_id=make_event_id("s01-e08"),
        timestamp=make_timestamp(_BASE, 20.008),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_N5,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 8.050),
            end_time=make_timestamp(_BASE, 10.500),
            usage=_N5_USAGE,
            completion_start_time=make_timestamp(_BASE, 8.250),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": _N5_INPUT}]},
            output={"text": _N5_OUTPUT},
        ),
    ))

    # 9. generation-create (LLM 6: Statistics)
    events.append(wrap_event(
        event_id=make_event_id("s01-e09"),
        timestamp=make_timestamp(_BASE, 20.009),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_N6,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 10.550),
            end_time=make_timestamp(_BASE, 12.000),
            usage=_N6_USAGE,
            completion_start_time=make_timestamp(_BASE, 10.700),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": _N6_INPUT}]},
            output={"text": _N6_OUTPUT},
        ),
    ))

    # 10. generation-create (LLM 7: Trends)
    events.append(wrap_event(
        event_id=make_event_id("s01-e10"),
        timestamp=make_timestamp(_BASE, 20.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_N7,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 12.050),
            end_time=make_timestamp(_BASE, 14.000),
            usage=_N7_USAGE,
            completion_start_time=make_timestamp(_BASE, 12.250),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": _N7_INPUT}]},
            output={"text": _N7_OUTPUT},
        ),
    ))

    # 11. generation-create (LLM 8: Correlate Features)
    events.append(wrap_event(
        event_id=make_event_id("s01-e11"),
        timestamp=make_timestamp(_BASE, 20.011),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_N8,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 14.050),
            end_time=make_timestamp(_BASE, 16.000),
            usage=_N8_USAGE,
            completion_start_time=make_timestamp(_BASE, 14.200),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": _N8_INPUT}]},
            output={"text": _N8_OUTPUT},
        ),
    ))

    # 12. generation-create (LLM 9: Recommendations)
    events.append(wrap_event(
        event_id=make_event_id("s01-e12"),
        timestamp=make_timestamp(_BASE, 20.012),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_N9,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 16.050),
            end_time=make_timestamp(_BASE, 18.500),
            usage=_N9_USAGE,
            completion_start_time=make_timestamp(_BASE, 16.300),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": _N9_INPUT}]},
            output={"text": _N9_OUTPUT},
        ),
    ))

    # 13. generation-create (LLM 10: Final Report)
    events.append(wrap_event(
        event_id=make_event_id("s01-e13"),
        timestamp=make_timestamp(_BASE, 20.013),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_N10,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 18.550),
            end_time=make_timestamp(_BASE, 19.900),
            usage=_N10_USAGE,
            completion_start_time=make_timestamp(_BASE, 18.750),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": _N10_INPUT}]},
            output={"text": _N10_OUTPUT},
        ),
    ))

    # 14. span-create (End node)
    events.append(wrap_event(
        event_id=make_event_id("s01-e14"),
        timestamp=make_timestamp(_BASE, 20.014),
        event_type="span-create",
        body=make_span_create(
            span_id=_END,
            trace_id=_TRACE,
            name="End",
            start_time=make_timestamp(_BASE, 19.950),
            end_time=make_timestamp(_BASE, 20.000),
            parent_observation_id=_WF,
            input={"report": _N10_OUTPUT},
            output=_FINAL_OUTPUT,
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
            {"index": 1, "type": "trace-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 2, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 4, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 5, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 6, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 7, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 8, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 9, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 10, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 11, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 12, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 13, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 14, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Straight-line workflow with 10 sequential LLM nodes. No branching. All nodes are children of the workflow root span.",
    }
