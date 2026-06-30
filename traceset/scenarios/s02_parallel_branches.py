"""Scenario 02: Parallel branches — 3 parallel LLM branches with fork-join.

Events (12):
  1.  trace-create       (WorkflowTraceInfo)
  2.  span-create        (workflow root)
  3.  span-create        (Start, parent=workflow)
  4.  span-create        (Fork, parent=workflow)
  5.  generation-create  (Branch A LLM 1: Security Scan)
  6.  generation-create  (Branch A LLM 2: Risk Assessment)
  7.  generation-create  (Branch B LLM 1: Performance Analysis)
  8.  generation-create  (Branch B LLM 2: Optimization Plan)
  9.  generation-create  (Branch C LLM 1: Code Quality)
  10.  generation-create  (Branch C LLM 2: Refactoring)
  11.  span-create        (Join, parent=workflow)
  12.  span-create        (End, parent=workflow)
"""
from traceset.helpers import (
    make_event_id,
    make_timestamp,
    make_trace_create,
    make_span_create,
    make_generation_create,
    wrap_event,
)

SCENARIO_ID = "02-parallel-branches"
SCENARIO_DESCRIPTION = "Forks into 3 parallel branches (2 LLM each), then joins"
APP_TYPE = "workflow"
DIFY_APP_MODE = "workflow"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo"]
EXPECTED_EVENT_COUNT = 12
EXPECTED_SPAN_COUNT = 11
SPAN_PATTERN = "parallel-fork-join"

_BASE = "2025-01-15T11:00:00.000000+00:00"

_TRACE = "21a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_WF = "22a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_START = "23a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_FORK = "24a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_A1 = "25a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_A2 = "26a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_B1 = "27a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_B2 = "28a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_C1 = "29a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_C2 = "2aa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_JOIN = "2ba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_END = "2ca2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-8a4b3c2d1e"

_QUERY = "Review the authentication module PR #432 and provide a comprehensive assessment"
_MODEL = "gpt-4o"
_PARAMS = {"temperature": 0.3, "max_tokens": 1500}

_A1_OUT = (
    "Security review identified 3 issues: 1) SQL injection risk in login query "
    "at line 45 — string concatenation used instead of parameterized queries. "
    "2) JWT secret loaded from environment without validation — no minimum "
    "length check. 3) Rate limiting absent on /auth/reset endpoint — allows "
    "brute-force attacks."
)
_A2_OUT = (
    "Risk Assessment: 1) SQL injection — Critical (CVSS 9.1). Fix: use "
    "parameterized queries with prepared statements. 2) JWT validation — High "
    "(CVSS 7.5). Fix: enforce 32-byte minimum secret length, add rotation. "
    "3) Rate limiting — Medium (CVSS 5.3). Fix: add express-rate-limit "
    "middleware (100 req/15min)."
)
_B1_OUT = (
    "Performance analysis: Login endpoint p95=847ms (target <300ms). "
    "Bottlenecks: 1) bcrypt hash rounds=12 (~400ms per hash). 2) Token "
    "verification makes 3 DB queries per request. 3) No connection pooling "
    "(max 5 connections)."
)
_B2_OUT = (
    "Optimization plan: 1) Reduce bcrypt rounds from 12 to 10 — saves ~200ms "
    "per login. 2) Cache JWT verification in Redis (TTL=300s) — eliminates "
    "3 DB queries. 3) Add connection pooling (20 connections). Expected p95: "
    "~180ms (-79%)."
)
_C1_OUT = (
    "Code quality findings: 1) No input validation on email field — accepts "
    "malformed addresses. 2) Error messages leak implementation details. "
    "3) No unit tests for token refresh flow. 4) 3 unresolved TODO comments. "
    "Maintainability index: 68/100."
)
_C2_OUT = (
    "Refactoring recommendations: 1) Add Zod schema validation for email and "
    "password inputs. 2) Sanitize error messages — use generic codes, log "
    "details server-side. 3) Add unit tests for token refresh (target 90%). "
    "4) Resolve 3 TODO comments. Estimated effort: 3 days."
)
_FINAL = {"review": "Security: 3 issues (Critical/High/Medium). Performance: p95 847ms→180ms planned. Quality: 68/100, 4 issues to fix."}

_A1_U = {"input": 140, "output": 180, "total": 320, "unit": "TOKENS", "inputCost": 0.000350, "outputCost": 0.001800, "totalCost": 0.002150}
_A2_U = {"input": 160, "output": 200, "total": 360, "unit": "TOKENS", "inputCost": 0.000400, "outputCost": 0.002000, "totalCost": 0.002400}
_B1_U = {"input": 130, "output": 170, "total": 300, "unit": "TOKENS", "inputCost": 0.000325, "outputCost": 0.001700, "totalCost": 0.002025}
_B2_U = {"input": 150, "output": 190, "total": 340, "unit": "TOKENS", "inputCost": 0.000375, "outputCost": 0.001900, "totalCost": 0.002275}
_C1_U = {"input": 120, "output": 160, "total": 280, "unit": "TOKENS", "inputCost": 0.000300, "outputCost": 0.001600, "totalCost": 0.001900}
_C2_U = {"input": 140, "output": 180, "total": 320, "unit": "TOKENS", "inputCost": 0.000350, "outputCost": 0.001800, "totalCost": 0.002150}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s02-e01"), timestamp=make_timestamp(_BASE, 4.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Code Review Pipeline", user_id=_USER,
            input={"query": _QUERY},
            metadata={"workflow_id": "wf-cr-002", "workflow_run_id": _TRACE},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e02"), timestamp=make_timestamp(_BASE, 4.002),
        event_type="span-create",
        body=make_span_create(
            span_id=_WF, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 4.050),
            input={"query": _QUERY}, output=_FINAL,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e03"), timestamp=make_timestamp(_BASE, 4.003),
        event_type="span-create",
        body=make_span_create(
            span_id=_START, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 0.050),
            parent_observation_id=_WF, input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e04"), timestamp=make_timestamp(_BASE, 4.004),
        event_type="span-create",
        body=make_span_create(
            span_id=_FORK, trace_id=_TRACE, name="Fork",
            start_time=make_timestamp(_BASE, 0.050), end_time=make_timestamp(_BASE, 0.100),
            parent_observation_id=_WF, input={"query": _QUERY},
            output={"branches": ["security", "performance", "quality"]},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e05"), timestamp=make_timestamp(_BASE, 4.005),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_A1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100), end_time=make_timestamp(_BASE, 2.000),
            usage=_A1_U, completion_start_time=make_timestamp(_BASE, 0.300),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Review PR #432 auth module for security vulnerabilities. Focus on SQL injection, JWT handling, rate limiting."}]},
            output={"text": _A1_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e06"), timestamp=make_timestamp(_BASE, 4.006),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_A2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 2.000), end_time=make_timestamp(_BASE, 3.500),
            usage=_A2_U, completion_start_time=make_timestamp(_BASE, 2.200),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Assess severity of the 3 security issues. Provide CVSS scores and fixes."}]},
            output={"text": _A2_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e07"), timestamp=make_timestamp(_BASE, 4.007),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_B1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100), end_time=make_timestamp(_BASE, 2.200),
            usage=_B1_U, completion_start_time=make_timestamp(_BASE, 0.250),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Analyze auth module for performance bottlenecks. Review DB queries, token verification, password hashing."}]},
            output={"text": _B1_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e08"), timestamp=make_timestamp(_BASE, 4.008),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_B2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 2.200), end_time=make_timestamp(_BASE, 3.800),
            usage=_B2_U, completion_start_time=make_timestamp(_BASE, 2.400),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Based on performance analysis, provide optimization plan with expected improvements."}]},
            output={"text": _B2_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e09"), timestamp=make_timestamp(_BASE, 4.009),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_C1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100), end_time=make_timestamp(_BASE, 1.800),
            usage=_C1_U, completion_start_time=make_timestamp(_BASE, 0.250),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Review auth module for code quality. Check input validation, error handling, test coverage, TODOs."}]},
            output={"text": _C1_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e10"), timestamp=make_timestamp(_BASE, 4.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_C2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 1.800), end_time=make_timestamp(_BASE, 3.200),
            usage=_C2_U, completion_start_time=make_timestamp(_BASE, 2.000),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Based on code quality findings, provide refactoring recommendations."}]},
            output={"text": _C2_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e11"), timestamp=make_timestamp(_BASE, 4.011),
        event_type="span-create",
        body=make_span_create(
            span_id=_JOIN, trace_id=_TRACE, name="Join",
            start_time=make_timestamp(_BASE, 3.800), end_time=make_timestamp(_BASE, 4.000),
            parent_observation_id=_WF,
            input={"branches": ["security", "performance", "quality"]},
            output=_FINAL,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s02-e12"), timestamp=make_timestamp(_BASE, 4.012),
        event_type="span-create",
        body=make_span_create(
            span_id=_END, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 4.000), end_time=make_timestamp(_BASE, 4.050),
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
                ["trace-create", "span-create", "span-create", "span-create",
                 "generation-create", "generation-create", "generation-create",
                 "generation-create", "generation-create", "generation-create",
                 "span-create", "span-create"], 1)
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Parallel fork-join: 3 branches (Security, Performance, Quality) each with 2 LLM nodes. Branches execute concurrently.",
    }
