# Traceset v2: E2E Langfuse Ingestion + Validation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework the traceset from a static wire-event catalog into a full end-to-end pipeline that generates multi-span traces, packs them into Langfuse ingestion payloads, ingests them into a real Langfuse cluster, and validates deep field-level correctness.

**Architecture:** 4-stage pipeline (Generate -> Pack -> Ingest -> Validate) in a single Python package. Reuses existing helpers.py and schema.py. Replaces 14 single-turn scenarios with 13 new multi-span scenarios (10+ spans each). Adds ingest.py, validate.py, pipeline.py, test_e2e.py. Uses existing ../difyapp3 Docker Langfuse stack. Zero runtime dependencies (stdlib only).

**Tech Stack:** Python 3.10+, stdlib only (urllib.request, json, base64, subprocess, uuid, datetime), pytest>=7.0 (dev), langfuse>=4.2.0,<5.0.0 (dev, schema validation), Docker (../difyapp3 Langfuse stack)

---

## Task 1: Project Setup — Clean Old Scenarios, Update Configs

**Files:**
- `traceset/pyproject.toml` (modify)
- `traceset/conftest.py` (modify)
- `traceset/scenarios/s01_chat_basic.py` through `s14_message_streaming.py` (delete)
- `traceset/scenarios/__pycache__/` (delete)
- `traceset/catalog.json` (delete)
- `traceset/README.md` (delete — will be regenerated)
- `traceset/schema.md` (delete — will be regenerated)
- `traceset/01-chat-basic/` through `traceset/14-message-streaming/` (delete if exist)

### Steps

- [ ] **Step 1: Delete old scenario modules and generated artifacts**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive

# Delete old scenario modules
rm traceset/scenarios/s01_chat_basic.py \
   traceset/scenarios/s02_chat_rag.py \
   traceset/scenarios/s03_completion_basic.py \
   traceset/scenarios/s04_agent_single_tool.py \
   traceset/scenarios/s05_agent_multi_tool.py \
   traceset/scenarios/s06_workflow_5node.py \
   traceset/scenarios/s07_workflow_15node.py \
   traceset/scenarios/s08_chatflow_basic.py \
   traceset/scenarios/s09_moderation_blocked.py \
   traceset/scenarios/s10_moderation_pass_through.py \
   traceset/scenarios/s11_rag_empty_results.py \
   traceset/scenarios/s12_tool_failure.py \
   traceset/scenarios/s13_suggested_questions_error.py \
   traceset/scenarios/s14_message_streaming.py

# Delete pycache
rm -rf traceset/scenarios/__pycache__

# Delete generated artifacts
rm -f traceset/catalog.json traceset/README.md traceset/schema.md

# Delete old scenario directories (if they exist)
rm -rf traceset/0*-*/ traceset/1*-*/
```

- [ ] **Step 2: Update `traceset/pyproject.toml`**

Write the complete file:

```toml
[project]
name = "dify-trace-catalog"
version = "0.2.0"
description = "End-to-end Dify trace generation, ingestion, and validation pipeline for Langfuse"
requires-python = ">=3.10"
dependencies = []

[project.optional-dependencies]
dev = [
    "langfuse>=4.2.0,<5.0.0",
    "pytest>=7.0",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
markers = [
    "e2e: end-to-end tests against real Langfuse (requires Docker)",
]
```

- [ ] **Step 3: Update `traceset/conftest.py`**

Write the complete file:

```python
"""Add project root to sys.path so `from traceset.xxx import ...` works.

Also provides the e2e fixture used by test_e2e.py.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest


@pytest.fixture(scope="session")
def ensure_langfuse_running():
    """Ensure Langfuse is running for e2e tests. Auto-starts Docker if needed.

    This fixture is NOT autouse — it only activates when an e2e test
    explicitly requests it. Unit tests (run with -m "not e2e") are
    unaffected.
    """
    from traceset.pipeline import load_config, check_health, ensure_langfuse

    config = load_config()
    if not check_health(config["langfuse_host"]):
        ensure_langfuse(config)
    assert check_health(config["langfuse_host"]), (
        "Langfuse is not healthy. Start it with: "
        "cd ../difyapp3 && docker compose up -d"
    )
    return config
```

- [ ] **Step 4: Verify old files are gone and configs are updated**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
ls traceset/scenarios/*.py
# Expected: only __init__.py (which still imports old modules — will fail, that's OK)
cat traceset/pyproject.toml | grep markers -A 2
# Expected: e2e marker registered
```

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/pyproject.toml traceset/conftest.py
git add -u traceset/scenarios/  # stage deletions
git add -u traceset/  # stage deletion of catalog.json, README.md, schema.md
git commit -m "chore: remove old 14 scenarios, add e2e marker to pyproject.toml, add e2e fixture to conftest.py"
```

---

## Task 2: Scenario 01 (linear-llm-chain) — Reference Template

**Files:**
- `traceset/tests/test_scenarios.py` (rewrite — complete with all 13 test functions)
- `traceset/scenarios/s01_linear_llm_chain.py` (create)

### Steps

- [ ] **Step 1: Write the complete `traceset/tests/test_scenarios.py`**

This file contains the updated `_check_scenario` helper (with `EXPECTED_SPAN_COUNT` and `SPAN_PATTERN` checks) and all 13 per-scenario test functions. At this point, only `test_s01_linear_llm_chain` will be implementable; the rest will fail with `ImportError` until their modules are written in Tasks 3-6.

```python
"""Tests for all scenario modules."""
import pytest

from traceset.helpers import wrap_event


def _check_scenario(module):
    """Assert a scenario module's events and meta are valid."""
    events = module.build_events()
    assert len(events) == module.EXPECTED_EVENT_COUNT, (
        f"{module.SCENARIO_ID}: expected {module.EXPECTED_EVENT_COUNT} events, "
        f"got {len(events)}"
    )
    span_count = sum(
        1 for e in events if e["type"] in ("span-create", "generation-create")
    )
    assert span_count == module.EXPECTED_SPAN_COUNT, (
        f"{module.SCENARIO_ID}: expected {module.EXPECTED_SPAN_COUNT} spans, "
        f"got {span_count}"
    )
    valid_types = {"trace-create", "span-create", "generation-create"}
    for i, e in enumerate(events):
        assert e["type"] in valid_types, (
            f"{module.SCENARIO_ID}[{i}]: bad type {e['type']}"
        )
        assert "id" in e and "timestamp" in e and "body" in e, (
            f"{module.SCENARIO_ID}[{i}]: missing envelope field"
        )
        for key in e["body"]:
            assert "_" not in key, (
                f"{module.SCENARIO_ID}[{i}]: snake_case body key '{key}'"
            )
    timestamps = [e["timestamp"] for e in events]
    assert timestamps == sorted(timestamps), (
        f"{module.SCENARIO_ID}: timestamps not monotonic"
    )
    meta = module.build_meta()
    assert meta["scenario_id"] == module.SCENARIO_ID
    assert meta["expected_event_count"] == module.EXPECTED_EVENT_COUNT
    assert meta["expected_span_count"] == module.EXPECTED_SPAN_COUNT
    assert meta["span_pattern"] == module.SPAN_PATTERN
    assert len(meta["events_in_order"]) == len(events)
    for i, (e, m) in enumerate(zip(events, meta["events_in_order"]), 1):
        assert m["index"] == i
        assert m["type"] == e["type"], (
            f"{module.SCENARIO_ID}[{i}]: meta type {m['type']} != event type {e['type']}"
        )


def test_s01_linear_llm_chain():
    from traceset.scenarios import s01_linear_llm_chain
    _check_scenario(s01_linear_llm_chain)


def test_s02_parallel_branches():
    from traceset.scenarios import s02_parallel_branches
    _check_scenario(s02_parallel_branches)


def test_s03_agent_react_loop():
    from traceset.scenarios import s03_agent_react_loop
    _check_scenario(s03_agent_react_loop)


def test_s04_multi_tool_chain():
    from traceset.scenarios import s04_multi_tool_chain
    _check_scenario(s04_multi_tool_chain)


def test_s05_rag_multi_hop():
    from traceset.scenarios import s05_rag_multi_hop
    _check_scenario(s05_rag_multi_hop)


def test_s06_moderation_rag_tool_combo():
    from traceset.scenarios import s06_moderation_rag_tool_combo
    _check_scenario(s06_moderation_rag_tool_combo)


def test_s07_workflow_conditional():
    from traceset.scenarios import s07_workflow_conditional
    _check_scenario(s07_workflow_conditional)


def test_s08_error_recovery_agent():
    from traceset.scenarios import s08_error_recovery_agent
    _check_scenario(s08_error_recovery_agent)


def test_s09_nested_workflow():
    from traceset.scenarios import s09_nested_workflow
    _check_scenario(s09_nested_workflow)


def test_s10_workflow_error_propagation():
    from traceset.scenarios import s10_workflow_error_propagation
    _check_scenario(s10_workflow_error_propagation)


def test_s11_streaming_chatflow():
    from traceset.scenarios import s11_streaming_chatflow
    _check_scenario(s11_streaming_chatflow)


def test_s12_multi_model_pipeline():
    from traceset.scenarios import s12_multi_model_pipeline
    _check_scenario(s12_multi_model_pipeline)


def test_s13_completion_multi_feature():
    from traceset.scenarios import s13_completion_multi_feature
    _check_scenario(s13_completion_multi_feature)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/tests/test_scenarios.py -v -m "not e2e" 2>&1 | head -30
```

Expected output (all 13 tests fail with `ModuleNotFoundError`):
```
FAILED traceset/tests/test_scenarios.py::test_s01_linear_llm_chain - ModuleNotFoundError: No module named 'traceset.scenarios.s01_linear_llm_chain'
FAILED traceset/tests/test_scenarios.py::test_s02_parallel_branches - ModuleNotFoundError: No module named 'traceset.scenarios.s02_parallel_branches'
...
FAILED traceset/tests/test_scenarios.py::test_s13_completion_multi_feature - ModuleNotFoundError: No module named 'traceset.scenarios.s13_completion_multi_feature'
```

- [ ] **Step 3: Write `traceset/scenarios/s01_linear_llm_chain.py`**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/tests/test_scenarios.py::test_s01_linear_llm_chain -v -m "not e2e"
```

Expected output:
```
traceset/tests/test_scenarios.py::test_s01_linear_llm_chain PASSED
```

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/tests/test_scenarios.py traceset/scenarios/s01_linear_llm_chain.py
git commit -m "feat(scenarios): add s01 linear-llm-chain (14 events, 13 spans) + updated test_scenarios.py with all 13 test stubs"
```


---

## Task 3: Scenarios 02-04 (parallel-branches, agent-react-loop, multi-tool-chain)

**Files:**
- `traceset/scenarios/s02_parallel_branches.py` (create)
- `traceset/scenarios/s03_agent_react_loop.py` (create)
- `traceset/scenarios/s04_multi_tool_chain.py` (create)

### Steps

- [ ] **Step 1: Verify tests for s02-s04 are still failing**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/tests/test_scenarios.py::test_s02_parallel_branches traceset/tests/test_scenarios.py::test_s03_agent_react_loop traceset/tests/test_scenarios.py::test_s04_multi_tool_chain -v -m "not e2e" 2>&1 | tail -5
```

Expected: all 3 FAILED with `ModuleNotFoundError`.

- [ ] **Step 2: Write `traceset/scenarios/s02_parallel_branches.py`**

```python
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
```

- [ ] **Step 3: Write `traceset/scenarios/s03_agent_react_loop.py`**

```python
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
```

- [ ] **Step 4: Write `traceset/scenarios/s04_multi_tool_chain.py`**

```python
"""Scenario 04: Multi-tool chain — 8 sequential tool calls then synthesis.

Events (12):
  1.  trace-create       (MessageTraceInfo)
  2.  span-create        (Tool 1: web_search)
  3.  span-create        (Tool 2: wiki_lookup)
  4.  span-create        (Tool 3: arxiv_search)
  5.  span-create        (Tool 4: translator)
  6.  span-create        (Tool 5: calculator)
  7.  span-create        (Tool 6: code_runner)
  8.  span-create        (Tool 7: pdf_parser)
  9.  span-create        (Tool 8: data_extractor)
 10.  generation-create  (Synthesis)
 11.  trace-create       (GenerateNameTraceInfo, upsert)
 12.  span-create        (GenerateName)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "04-multi-tool-chain"
SCENARIO_DESCRIPTION = "8 sequential tool calls, then synthesis + GenerateName"
APP_TYPE = "agent-chat"
DIFY_APP_MODE = "agent-chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "ToolTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 12
EXPECTED_SPAN_COUNT = 10
SPAN_PATTERN = "sequential-chain"

_BASE = "2025-01-15T12:00:00.000000+00:00"
_TRACE = "41a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T1 = "42a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T2 = "43a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T3 = "44a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T4 = "45a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T5 = "46a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T6 = "47a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T7 = "48a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T8 = "49a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SYN = "4aa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_NAME = "4ba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-0c6d5e4f3a"
_CONV = "conv-4d5e6f7a8b9c"
_QUERY = "Research the current state of quantum computing and summarize key findings"
_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 2000}

_TOOL1_IN = {"query": "quantum computing breakthroughs 2024 2025"}
_TOOL1_OUT = {"results": ["IBM Condor 1,121 qubits", "Google error correction below threshold", "Chinese Zuchongzhi-3 504 qubits", "PsiQuantum photonic approach"]}
_TOOL2_IN = {"term": "quantum computing"}
_TOOL2_OUT = {"summary": "Quantum computing uses quantum mechanical phenomena (superposition, entanglement) to process information. Qubits can represent 0, 1, or both simultaneously."}
_TOOL3_IN = {"query": "quantum error correction surface code"}
_TOOL3_OUT = {"papers": ["Surface code below threshold (Google, 2024)", "Logical qubit fidelity scaling (IBM, 2024)", "Real-time error correction (IonQ, 2024)"]}
_TOOL4_IN = {"text": "量子计算在2024年取得重大突破，中国在超导量子计算领域达到国际领先水平。", "target_lang": "en"}
_TOOL4_OUT = {"translation": "Quantum computing achieved major breakthroughs in 2024, with China reaching internationally leading levels in superconducting quantum computing."}
_TOOL5_IN = {"expression": "(1121-127)/127*100"}
_TOOL5_OUT = {"result": "782.6771653543307"}
_TOOL6_IN = {"code": "from qiskit import QuantumCircuit\nqc = QuantumCircuit(2)\nqc.h(0)\nqc.cx(0, 1)\nprint(qc)"}
_TOOL6_OUT = {"stdout": "q_0: ──■──\n     ┌─┴─┐\nq_1: ┤ X ├\n     └───┘"}
_TOOL7_IN = {"file": "ibm_quantum_roadmap_2025.pdf"}
_TOOL7_OUT = {"pages": 12, "sections": ["Executive Summary", "2025 Roadmap", "2026 Milestones", "2028 Vision"], "text": "IBM roadmap: 2025 Kookaburra 4,158 qubits..."}
_TOOL8_IN = {"source": "ibm_quantum_roadmap_2025.pdf", "fields": ["qubit_count", "fidelity", "timeline", "budget"]}
_TOOL8_OUT = {"metrics": {"2025_qubits": 4158, "2026_qubits": 10000, "2028_qubits": 100000, "fidelity_2q": 0.999, "rd_budget": 1200000000}}

_SYN_OUT = (
    "Quantum Computing: State of the Art (2024-2025)\n\n"
    "Hardware: IBM's Condor processor (1,121 qubits) represents a 783% increase "
    "over the previous generation (127 qubits). Google achieved below-threshold "
    "error correction. China's Zuchongzhi-3 (504 qubits) demonstrates "
    "superconducting leadership.\n\n"
    "Software: Qiskit and Cirq matured significantly. Real-time error correction "
    "demonstrated by IonQ. Surface code implementations showing 99.9% fidelity.\n\n"
    "Roadmap: IBM plans 4,158 qubits (2025), 10,000 (2026), 100,000 (2028). "
    "Focus shifting from NISQ to fault-tolerant quantum computing.\n\n"
    "Challenges: Decoherence, scaling cryogenic systems, cost ($10M+ per system), "
    "and the quantum-classical interface remain significant barriers."
)
_CONVERSATION_NAME = "Quantum Computing Research Summary"
_SYN_U = {"input": 350, "output": 220, "total": 570, "unit": "TOKENS", "inputCost": 0.000053, "outputCost": 0.000132, "totalCost": 0.000185}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s04-e01"), timestamp=make_timestamp(_BASE, 10.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Quantum Computing Research Agent", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"agent_id": "agent-researcher", "conversation_id": _CONV},
        ),
    ))

    # 8 sequential tool calls
    tools = [
        ("web_search", _T1, 0.050, 1.200, _TOOL1_IN, _TOOL1_OUT),
        ("wiki_lookup", _T2, 1.250, 2.000, _TOOL2_IN, _TOOL2_OUT),
        ("arxiv_search", _T3, 2.050, 3.500, _TOOL3_IN, _TOOL3_OUT),
        ("translator", _T4, 3.550, 4.200, _TOOL4_IN, _TOOL4_OUT),
        ("calculator", _T5, 4.250, 4.500, _TOOL5_IN, _TOOL5_OUT),
        ("code_runner", _T6, 4.550, 6.000, _TOOL6_IN, _TOOL6_OUT),
        ("pdf_parser", _T7, 6.050, 7.800, _TOOL7_IN, _TOOL7_OUT),
        ("data_extractor", _T8, 7.850, 8.500, _TOOL8_IN, _TOOL8_OUT),
    ]
    for i, (name, sid, start, end, tin, tout) in enumerate(tools):
        events.append(wrap_event(
            event_id=make_event_id(f"s04-e{i+2:02d}"),
            timestamp=make_timestamp(_BASE, 10.001 + (i + 1) * 0.001),
            event_type="span-create",
            body=make_span_create(
                span_id=sid, trace_id=_TRACE, name=name,
                start_time=make_timestamp(_BASE, start),
                end_time=make_timestamp(_BASE, end),
                input=tin, output=tout,
            ),
        ))

    # Synthesis
    events.append(wrap_event(
        event_id=make_event_id("s04-e10"), timestamp=make_timestamp(_BASE, 10.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SYN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 8.550), end_time=make_timestamp(_BASE, 10.000),
            usage=_SYN_U, completion_start_time=make_timestamp(_BASE, 8.750),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _SYN_OUT},
        ),
    ))

    # GenerateName (trace upsert + span)
    events.append(wrap_event(
        event_id=make_event_id("s04-e11"), timestamp=make_timestamp(_BASE, 10.011),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))
    events.append(wrap_event(
        event_id=make_event_id("s04-e12"), timestamp=make_timestamp(_BASE, 10.012),
        event_type="span-create",
        body=make_span_create(
            span_id=_NAME, trace_id=_TRACE, name="Generate Name",
            start_time=make_timestamp(_BASE, 10.050), end_time=make_timestamp(_BASE, 10.400),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _CONVERSATION_NAME},
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
            {"index": 7, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 8, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 9, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 10, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 11, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 12, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "8 sequential tool calls (web_search, wiki_lookup, arxiv_search, translator, calculator, code_runner, pdf_parser, data_extractor) then synthesis LLM + GenerateName upsert.",
    }
```

- [ ] **Step 5: Run tests to verify s02-s04 pass**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/tests/test_scenarios.py::test_s02_parallel_branches traceset/tests/test_scenarios.py::test_s03_agent_react_loop traceset/tests/test_scenarios.py::test_s04_multi_tool_chain -v -m "not e2e"
```

Expected output:
```
traceset/tests/test_scenarios.py::test_s02_parallel_branches PASSED
traceset/tests/test_scenarios.py::test_s03_agent_react_loop PASSED
traceset/tests/test_scenarios.py::test_s04_multi_tool_chain PASSED
```

- [ ] **Step 6: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/scenarios/s02_parallel_branches.py traceset/scenarios/s03_agent_react_loop.py traceset/scenarios/s04_multi_tool_chain.py
git commit -m "feat(scenarios): add s02-s04 (parallel-branches, agent-react-loop, multi-tool-chain)"
```


---

## Task 4: Scenarios 05-07 (rag-multi-hop, moderation-rag-tool-combo, workflow-conditional)

**Files:**
- `traceset/scenarios/s05_rag_multi_hop.py` (create)
- `traceset/scenarios/s06_moderation_rag_tool_combo.py` (create)
- `traceset/scenarios/s07_workflow_conditional.py` (create)

### Steps

- [ ] **Step 1: Write `traceset/scenarios/s05_rag_multi_hop.py`**

```python
"""Scenario 05: RAG multi-hop — 6 sequential knowledge retrievals then synthesis.

Events (12):
  1.  trace-create       (MessageTraceInfo)
  2.  span-create        (RAG 1: CCPA)
  3.  span-create        (RAG 2: CPRA amendments)
  4.  span-create        (RAG 3: Civil Code 1798.82)
  5.  span-create        (RAG 4: AG Guidelines)
  6.  span-create        (RAG 5: Federal interplay)
  7.  span-create        (RAG 6: Case law)
  8.  generation-create  (Synthesis: Generate)
  9.  generation-create  (Synthesis: Refine)
 10.  generation-create  (SuggestedQuestions)
 11.  trace-create       (GenerateNameTraceInfo, upsert)
 12.  span-create        (GenerateName)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "05-rag-multi-hop"
SCENARIO_DESCRIPTION = "6 sequential knowledge retrievals, then synthesis + SuggestedQuestions + GenerateName"
APP_TYPE = "chat"
DIFY_APP_MODE = "chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "DatasetRetrievalTraceInfo", "SuggestedQuestionTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 12
EXPECTED_SPAN_COUNT = 10
SPAN_PATTERN = "multi-hop-retrieval"

_BASE = "2025-01-15T12:30:00.000000+00:00"
_TRACE = "51a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R1 = "52a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R2 = "53a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R3 = "54a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R4 = "55a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R5 = "56a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R6 = "57a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SYN1 = "58a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SYN2 = "59a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SUGG = "5aa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_NAME = "5ba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-1d7e6f5a4b"
_CONV = "conv-5e6f7a8b9c0d"
_QUERY = "What are the legal requirements for data breach notification in California?"
_MODEL = "gpt-4o"
_PARAMS = {"temperature": 0.3, "max_tokens": 1500}

_R1_OUT = {"documents": [{"title": "CCPA Section 1798.82", "content": "A business shall notify any California resident whose unencrypted personal information was, or is reasonably believed to have been, acquired by an unauthorized person.", "score": 0.95}]}
_R2_OUT = {"documents": [{"title": "CPRA Amendments 2023", "content": "CPRA expands notification requirements to include delayed notifications, specific content requirements, and attorney general reporting for breaches affecting 500+ residents.", "score": 0.92}]}
_R3_OUT = {"documents": [{"title": "Cal. Civ. Code 1798.82(d)", "content": "Notification shall be made in the most expedient time possible and without unreasonable delay, consistent with legitimate needs of law enforcement.", "score": 0.94}]}
_R4_OUT = {"documents": [{"title": "CA AG Breach Notification Guidelines", "content": "Businesses must submit a sample copy of the breach notice to the Attorney General when the breach affects 500+ California residents.", "score": 0.89}]}
_R5_OUT = {"documents": [{"title": "HIPAA 45 CFR 164.404", "content": "Covered entities must notify affected individuals within 60 days of discovery. Interacts with state law when both apply — stricter standard prevails.", "score": 0.87}]}
_R6_OUT = {"documents": [{"title": "Johnson v. Adobe (2023)", "content": "Court held that statutory damages are available even without actual harm. Standing requires demonstrable injury-in-fact under TransUnion v. Ramirez.", "score": 0.91}]}

_SYN1_OUT = (
    "Under California law (CCPA/CPRA), businesses must notify affected California "
    "residents whose unencrypted personal information was acquired by an unauthorized "
    "person. Notification must be made 'in the most expedient time possible and without "
    "unreasonable delay.' For breaches affecting 500+ residents, a sample notice must "
    "be submitted to the Attorney General."
)
_SYN2_OUT = (
    "California Data Breach Notification Requirements:\n\n"
    "1. Timing: Most expedient time possible, without unreasonable delay, consistent "
    "with law enforcement needs.\n"
    "2. Content: (a) Business name and contact info, (b) Types of personal information "
    "involved, (c) Date of breach (or estimated date range), (d) Whether notification "
    "was delayed due to law enforcement, (e) Description of measures taken.\n"
    "3. AG Reporting: Submit sample notice to CA Attorney General if 500+ residents "
    "affected.\n"
    "4. Method: Written notice to last known address, or electronic notice with consent.\n"
    "5. Federal Interplay: HIPAA (60-day federal standard) and state law both apply — "
    "stricter standard prevails.\n"
    "6. Penalties: Statutory damages available even without actual harm (Johnson v. Adobe)."
)
_SUGG_OUT = {"questions": ["What are the penalties for non-compliance with CCPA breach notification?", "How does California law compare to other states' breach notification requirements?", "What constitutes 'personal information' under CCPA?"]}
_NAME_OUT = "CA Data Breach Notification Requirements"

_SYN1_U = {"input": 200, "output": 250, "total": 450, "unit": "TOKENS", "inputCost": 0.000500, "outputCost": 0.002500, "totalCost": 0.003000}
_SYN2_U = {"input": 280, "output": 220, "total": 500, "unit": "TOKENS", "inputCost": 0.000700, "outputCost": 0.002200, "totalCost": 0.002900}
_SUGG_U = {"input": 150, "output": 80, "total": 230, "unit": "TOKENS", "inputCost": 0.000375, "outputCost": 0.000800, "totalCost": 0.001175}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s05-e01"), timestamp=make_timestamp(_BASE, 8.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Legal Research Assistant", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"conversation_id": _CONV, "knowledge_base": "legal-ca"},
        ),
    ))

    rags = [
        ("CCPA", _R1, 0.050, 0.800, _R1_OUT),
        ("CPRA Amendments", _R2, 0.850, 1.600, _R2_OUT),
        ("Civil Code 1798.82", _R3, 1.650, 2.400, _R3_OUT),
        ("AG Guidelines", _R4, 2.450, 3.200, _R4_OUT),
        ("Federal Law Interplay", _R5, 3.250, 4.000, _R5_OUT),
        ("Case Law", _R6, 4.050, 4.800, _R6_OUT),
    ]
    for i, (name, sid, start, end, out) in enumerate(rags):
        events.append(wrap_event(
            event_id=make_event_id(f"s05-e{i+2:02d}"),
            timestamp=make_timestamp(_BASE, 8.001 + (i + 1) * 0.001),
            event_type="span-create",
            body=make_span_create(
                span_id=sid, trace_id=_TRACE, name=f"Knowledge Retrieval: {name}",
                start_time=make_timestamp(_BASE, start), end_time=make_timestamp(_BASE, end),
                input={"query": _QUERY, "dataset": name},
                output=out,
            ),
        ))

    events.append(wrap_event(
        event_id=make_event_id("s05-e08"), timestamp=make_timestamp(_BASE, 8.008),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SYN1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 4.850), end_time=make_timestamp(_BASE, 6.500),
            usage=_SYN1_U, completion_start_time=make_timestamp(_BASE, 5.050),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _SYN1_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s05-e09"), timestamp=make_timestamp(_BASE, 8.009),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SYN2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 6.550), end_time=make_timestamp(_BASE, 7.500),
            usage=_SYN2_U, completion_start_time=make_timestamp(_BASE, 6.700),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": "Refine and format the answer with specific requirements."}]},
            output={"text": _SYN2_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s05-e10"), timestamp=make_timestamp(_BASE, 8.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SUGG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 7.550), end_time=make_timestamp(_BASE, 7.900),
            usage=_SUGG_U, completion_start_time=make_timestamp(_BASE, 7.650),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": "Suggest 3 follow-up questions."}]},
            output=_SUGG_OUT,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s05-e11"), timestamp=make_timestamp(_BASE, 8.011),
        event_type="trace-create",
        body=make_trace_create(trace_id=_TRACE, name="Generate Name", user_id=_USER),
    ))
    events.append(wrap_event(
        event_id=make_event_id("s05-e12"), timestamp=make_timestamp(_BASE, 8.012),
        event_type="span-create",
        body=make_span_create(
            span_id=_NAME, trace_id=_TRACE, name="Generate Name",
            start_time=make_timestamp(_BASE, 7.950), end_time=make_timestamp(_BASE, 8.200),
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
            {"index": 2, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 6, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 8, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 9, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 10, "type": "generation-create", "source_trace_type": "SuggestedQuestionTraceInfo", "dify_handler": "LangFuseDataTrace.suggested_question_trace"},
            {"index": 11, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 12, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "6-hop RAG through CCPA, CPRA, Civil Code, AG Guidelines, Federal Law, and Case Law. Two-stage synthesis (Generate + Refine).",
    }
```

- [ ] **Step 2: Write `traceset/scenarios/s06_moderation_rag_tool_combo.py`**

```python
"""Scenario 06: Moderation + RAG + tools + synthesis + SuggestedQuestions + GenerateName.

Events (11):
  1.  trace-create       (MessageTraceInfo)
  2.  span-create        (Moderation, level=DEFAULT)
  3.  span-create        (Knowledge Retrieval)
  4.  span-create        (Tool 1: sentiment_analyzer)
  5.  span-create        (Tool 2: keyword_extractor)
  6.  span-create        (Tool 3: entity_recognizer)
  7.  span-create        (Tool 4: fact_checker)
  8.  generation-create  (Synthesis: Generate)
  9.  generation-create  (Synthesis: Refine)
 10.  generation-create  (SuggestedQuestions)
 11.  span-create        (GenerateName)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "06-moderation-rag-tool-combo"
SCENARIO_DESCRIPTION = "Moderation + RAG + 4 tool calls + synthesis + SuggestedQuestions + GenerateName"
APP_TYPE = "chat"
DIFY_APP_MODE = "chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "ModerationTraceInfo", "DatasetRetrievalTraceInfo", "ToolTraceInfo", "SuggestedQuestionTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 11
EXPECTED_SPAN_COUNT = 10
SPAN_PATTERN = "feature-combination"

_BASE = "2025-01-15T13:00:00.000000+00:00"
_TRACE = "61a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_MOD = "62a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_RAG = "63a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T1 = "64a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T2 = "65a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T3 = "66a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T4 = "67a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SYN1 = "68a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SYN2 = "69a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SUGG = "6aa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_NAME = "6ba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-2e8f7a6b5c"
_CONV = "conv-6f7a8b9c0d1e"
_QUERY = "Analyze this user-submitted product review for compliance and extract key information"
_REVIEW = "The TechCorp Model X-200 is amazing! Best battery life in its class at 18 hours. The screen quality is outstanding. At $899 it's great value. Highly recommend."
_MODEL = "gpt-4o"
_PARAMS = {"temperature": 0.5, "max_tokens": 1500}

_MOD_OUT = {"flagged": False, "categories": {"spam": False, "harassment": False, "hate": False}, "action": "pass"}
_RAG_OUT = {"documents": [{"title": "Model X-200 Specs", "content": "Battery: 18h. Screen: 14-inch OLED. Price: $899.", "score": 0.96}, {"title": "Review Policy", "content": "Reviews must not contain competitor comparisons or unsubstantiated claims.", "score": 0.88}]}
_T1_IN = {"text": _REVIEW}
_T1_OUT = {"sentiment": "positive", "score": 0.78, "emotions": {"satisfaction": 0.62, "excitement": 0.24, "trust": 0.14}}
_T2_IN = {"text": _REVIEW}
_T2_OUT = {"keywords": ["battery life", "screen quality", "performance", "design", "price", "TechCorp", "Model X-200"]}
_T3_IN = {"text": _REVIEW}
_T3_OUT = {"entities": [{"text": "TechCorp Model X-200", "type": "PRODUCT"}, {"text": "$899", "type": "PRICE"}, {"text": "18 hours", "type": "DURATION"}]}
_T4_IN = {"claims": ["longest battery life in its class", "outstanding screen quality"], "context": _RAG_OUT}
_T4_OUT = {"results": [{"claim": "longest battery life in its class", "verdict": "TRUE", "evidence": "Spec sheet confirms 18h is highest in category"}, {"claim": "outstanding screen quality", "verdict": "SUBJECTIVE", "evidence": "No objective metric available"}]}

_SYN1_OUT = "Review Analysis: The user submitted a positive review (78% sentiment) for TechCorp Model X-200. Moderation passed. Key entities: Product Model X-200, Price $899, Battery 18h. Fact-check: 'longest battery' verified as TRUE."
_SYN2_OUT = "Comprehensive Review Analysis:\n\n1. Compliance: PASSED (no policy violations).\n2. Sentiment: 78% positive. Key emotions: satisfaction (62%), excitement (24%).\n3. Entities: TechCorp Model X-200 (product), $899 (price), 18h (battery).\n4. Fact-Check: 'Longest battery in class' = TRUE. 'Outstanding screen' = subjective.\n5. Keywords: battery life, screen quality, performance, design, price.\n\nRecommendation: Publish review. Flag 'best in class' claim for marketing team."
_SUGG_OUT = {"questions": ["What are common complaints about this product?", "How does this review compare to others?"]}
_NAME_OUT = "Product Review Analysis - Model X-200"

_SYN1_U = {"input": 180, "output": 200, "total": 380, "unit": "TOKENS", "inputCost": 0.000450, "outputCost": 0.002000, "totalCost": 0.002450}
_SYN2_U = {"input": 250, "output": 180, "total": 430, "unit": "TOKENS", "inputCost": 0.000625, "outputCost": 0.001800, "totalCost": 0.002425}
_SUGG_U = {"input": 120, "output": 70, "total": 190, "unit": "TOKENS", "inputCost": 0.000300, "outputCost": 0.000700, "totalCost": 0.001000}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s06-e01"), timestamp=make_timestamp(_BASE, 7.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Product Review Analysis", user_id=_USER,
            input={"query": _QUERY, "review": _REVIEW}, session_id=_CONV,
            metadata={"conversation_id": _CONV},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s06-e02"), timestamp=make_timestamp(_BASE, 7.002),
        event_type="span-create",
        body=make_span_create(
            span_id=_MOD, trace_id=_TRACE, name="Moderation",
            start_time=make_timestamp(_BASE, 0.050), end_time=make_timestamp(_BASE, 0.300),
            input={"text": _REVIEW}, output=_MOD_OUT,
            level="DEFAULT", status_message="Content approved. No policy violations.",
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s06-e03"), timestamp=make_timestamp(_BASE, 7.003),
        event_type="span-create",
        body=make_span_create(
            span_id=_RAG, trace_id=_TRACE, name="Knowledge Retrieval",
            start_time=make_timestamp(_BASE, 0.350), end_time=make_timestamp(_BASE, 1.200),
            input={"query": "Model X-200 specifications and review policies"},
            output=_RAG_OUT,
        ),
    ))

    tools = [
        ("sentiment_analyzer", _T1, 1.250, 2.000, _T1_IN, _T1_OUT),
        ("keyword_extractor", _T2, 2.050, 2.700, _T2_IN, _T2_OUT),
        ("entity_recognizer", _T3, 2.750, 3.500, _T3_IN, _T3_OUT),
        ("fact_checker", _T4, 3.550, 4.300, _T4_IN, _T4_OUT),
    ]
    for i, (name, sid, start, end, tin, tout) in enumerate(tools):
        events.append(wrap_event(
            event_id=make_event_id(f"s06-e{i+4:02d}"),
            timestamp=make_timestamp(_BASE, 7.004 + i * 0.001),
            event_type="span-create",
            body=make_span_create(
                span_id=sid, trace_id=_TRACE, name=name,
                start_time=make_timestamp(_BASE, start), end_time=make_timestamp(_BASE, end),
                input=tin, output=tout,
            ),
        ))

    events.append(wrap_event(
        event_id=make_event_id("s06-e08"), timestamp=make_timestamp(_BASE, 7.008),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SYN1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 4.350), end_time=make_timestamp(_BASE, 5.800),
            usage=_SYN1_U, completion_start_time=make_timestamp(_BASE, 4.550),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _SYN1_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s06-e09"), timestamp=make_timestamp(_BASE, 7.009),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SYN2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 5.850), end_time=make_timestamp(_BASE, 6.600),
            usage=_SYN2_U, completion_start_time=make_timestamp(_BASE, 6.000),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": "Refine with detailed analysis."}]},
            output={"text": _SYN2_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s06-e10"), timestamp=make_timestamp(_BASE, 7.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SUGG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 6.650), end_time=make_timestamp(_BASE, 6.950),
            usage=_SUGG_U, completion_start_time=make_timestamp(_BASE, 6.750),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": "Suggest follow-up questions."}]},
            output=_SUGG_OUT,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s06-e11"), timestamp=make_timestamp(_BASE, 7.011),
        event_type="span-create",
        body=make_span_create(
            span_id=_NAME, trace_id=_TRACE, name="Generate Name",
            start_time=make_timestamp(_BASE, 7.000), end_time=make_timestamp(_BASE, 7.200),
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
            {"index": 2, "type": "span-create", "source_trace_type": "ModerationTraceInfo", "dify_handler": "LangFuseDataTrace.moderation_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 6, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 8, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 9, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 10, "type": "generation-create", "source_trace_type": "SuggestedQuestionTraceInfo", "dify_handler": "LangFuseDataTrace.suggested_question_trace"},
            {"index": 11, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Feature combination: Moderation (pass-through) + RAG + 4 tools (sentiment, keyword, entity, fact-check) + 2-stage synthesis + SuggestedQuestions + GenerateName. GenerateName is span-only (no trace upsert) in this scenario.",
    }
```

- [ ] **Step 3: Write `traceset/scenarios/s07_workflow_conditional.py`**

```python
"""Scenario 07: Workflow conditional — 2 IF/ELSE nodes each branching to 3 LLM nodes.

Events (12):
  1.  trace-create       (WorkflowTraceInfo)
  2.  span-create        (workflow root)
  3.  span-create        (Start, parent=workflow)
  4.  span-create        (IF 1: subscription check, parent=workflow)
  5.  generation-create  (LLM A1: Renewal Options)
  6.  generation-create  (LLM A2: Renewal Confirmation)
  7.  generation-create  (LLM A3: Renewal FAQ)
  8.  span-create        (IF 2: billing check, parent=workflow)
  9.  generation-create  (LLM B1: Billing Lookup)
 10.  generation-create  (LLM B2: Billing FAQ)
 11.  generation-create  (LLM B3: Escalation Options)
 12.  span-create        (End, parent=workflow)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "07-workflow-conditional"
SCENARIO_DESCRIPTION = "2 IF/ELSE nodes, each branching to 3 LLM nodes"
APP_TYPE = "workflow"
DIFY_APP_MODE = "workflow"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo"]
EXPECTED_EVENT_COUNT = 12
EXPECTED_SPAN_COUNT = 11
SPAN_PATTERN = "conditional-branch"

_BASE = "2025-01-15T13:30:00.000000+00:00"
_TRACE = "71a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_WF = "72a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_START = "73a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_IF1 = "74a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_A1 = "75a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_A2 = "76a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_A3 = "77a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_IF2 = "78a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_B1 = "79a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_B2 = "7aa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_B3 = "7ba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_END = "7ca2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-3f9a8b7c6d"
_QUERY = "I need help with my subscription renewal and also have a billing question"
_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 800}

_A1_OUT = "Your Pro subscription renews on Jan 15. Options: 1) Auto-renew at $29/mo. 2) Switch to Annual at $290/yr (save $58). 3) Cancel before renewal."
_A2_OUT = "To proceed with annual renewal, please confirm. Your billing date will change to Jan 15, 2026. You will be charged $290 immediately."
_A3_OUT = "Common renewal questions: Can I get a refund? Yes, within 30 days. Can I pause? Yes, up to 3 months. Can I downgrade? Yes, effective next cycle."
_B1_OUT = "Your last invoice: #INV-2024-12-8847, $29.00, paid Dec 15. Next charge: Jan 15. Payment method: Visa ending 4242."
_B2_OUT = "Billing FAQ: 1) Payment methods: Visa, MC, AmEx, PayPal. 2) Tax: included in price. 3) Refunds: 30-day policy. 4) Invoices: available in account settings."
_B3_OUT = "If you need further billing assistance, I can connect you with a billing specialist. Average response time: 2 hours. You can also email billing@example.com."
_FINAL = {"response": "Subscription: renewal options provided. Billing: last invoice and FAQ provided. Escalation available if needed."}

_A1_U = {"input": 100, "output": 120, "total": 220, "unit": "TOKENS", "inputCost": 0.000015, "outputCost": 0.000072, "totalCost": 0.000087}
_A2_U = {"input": 90, "output": 100, "total": 190, "unit": "TOKENS", "inputCost": 0.000014, "outputCost": 0.000060, "totalCost": 0.000074}
_A3_U = {"input": 80, "output": 90, "total": 170, "unit": "TOKENS", "inputCost": 0.000012, "outputCost": 0.000054, "totalCost": 0.000066}
_B1_U = {"input": 100, "output": 110, "total": 210, "unit": "TOKENS", "inputCost": 0.000015, "outputCost": 0.000066, "totalCost": 0.000081}
_B2_U = {"input": 90, "output": 100, "total": 190, "unit": "TOKENS", "inputCost": 0.000014, "outputCost": 0.000060, "totalCost": 0.000074}
_B3_U = {"input": 85, "output": 95, "total": 180, "unit": "TOKENS", "inputCost": 0.000013, "outputCost": 0.000057, "totalCost": 0.000070}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s07-e01"), timestamp=make_timestamp(_BASE, 6.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Customer Support Routing Workflow", user_id=_USER,
            input={"query": _QUERY},
            metadata={"workflow_id": "wf-csr-007", "workflow_run_id": _TRACE},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e02"), timestamp=make_timestamp(_BASE, 6.002),
        event_type="span-create",
        body=make_span_create(
            span_id=_WF, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 6.000),
            input={"query": _QUERY}, output=_FINAL,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e03"), timestamp=make_timestamp(_BASE, 6.003),
        event_type="span-create",
        body=make_span_create(
            span_id=_START, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 0.050),
            parent_observation_id=_WF, input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e04"), timestamp=make_timestamp(_BASE, 6.004),
        event_type="span-create",
        body=make_span_create(
            span_id=_IF1, trace_id=_TRACE, name="IF: Subscription Check",
            start_time=make_timestamp(_BASE, 0.050), end_time=make_timestamp(_BASE, 0.100),
            parent_observation_id=_WF,
            input={"condition": "user.has_active_subscription == true"},
            output={"branch": "renewal_flow"},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e05"), timestamp=make_timestamp(_BASE, 6.005),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_A1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100), end_time=make_timestamp(_BASE, 1.500),
            usage=_A1_U, completion_start_time=make_timestamp(_BASE, 0.250),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Show subscription renewal options for the user."}]},
            output={"text": _A1_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e06"), timestamp=make_timestamp(_BASE, 6.006),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_A2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 1.550), end_time=make_timestamp(_BASE, 2.800),
            usage=_A2_U, completion_start_time=make_timestamp(_BASE, 1.700),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Generate renewal confirmation message."}]},
            output={"text": _A2_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e07"), timestamp=make_timestamp(_BASE, 6.007),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_A3, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 2.850), end_time=make_timestamp(_BASE, 4.000),
            usage=_A3_U, completion_start_time=make_timestamp(_BASE, 3.000),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Generate renewal FAQ."}]},
            output={"text": _A3_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e08"), timestamp=make_timestamp(_BASE, 6.008),
        event_type="span-create",
        body=make_span_create(
            span_id=_IF2, trace_id=_TRACE, name="IF: Billing Check",
            start_time=make_timestamp(_BASE, 4.050), end_time=make_timestamp(_BASE, 4.100),
            parent_observation_id=_WF,
            input={"condition": "user.has_billing_question == true"},
            output={"branch": "billing_support"},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e09"), timestamp=make_timestamp(_BASE, 6.009),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_B1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 4.100), end_time=make_timestamp(_BASE, 5.000),
            usage=_B1_U, completion_start_time=make_timestamp(_BASE, 4.250),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Look up the user's billing information."}]},
            output={"text": _B1_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e10"), timestamp=make_timestamp(_BASE, 6.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_B2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 5.050), end_time=make_timestamp(_BASE, 5.600),
            usage=_B2_U, completion_start_time=make_timestamp(_BASE, 5.200),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Generate billing FAQ."}]},
            output={"text": _B2_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e11"), timestamp=make_timestamp(_BASE, 6.011),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_B3, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 5.650), end_time=make_timestamp(_BASE, 5.900),
            usage=_B3_U, completion_start_time=make_timestamp(_BASE, 5.800),
            model_parameters=_PARAMS, parent_observation_id=_WF,
            input={"messages": [{"role": "user", "content": "Generate escalation options."}]},
            output={"text": _B3_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s07-e12"), timestamp=make_timestamp(_BASE, 6.012),
        event_type="span-create",
        body=make_span_create(
            span_id=_END, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 5.950), end_time=make_timestamp(_BASE, 6.000),
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
                 "span-create", "generation-create", "generation-create",
                 "generation-create", "span-create"], 1)
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Conditional branching: 2 IF/ELSE nodes. IF1 routes to renewal flow (3 LLMs). IF2 routes to billing support (3 LLMs). Sequential execution of branches.",
    }
```

- [ ] **Step 4: Run tests to verify s05-s07 pass**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/tests/test_scenarios.py::test_s05_rag_multi_hop traceset/tests/test_scenarios.py::test_s06_moderation_rag_tool_combo traceset/tests/test_scenarios.py::test_s07_workflow_conditional -v -m "not e2e"
```

Expected: all 3 PASSED.

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/scenarios/s05_rag_multi_hop.py traceset/scenarios/s06_moderation_rag_tool_combo.py traceset/scenarios/s07_workflow_conditional.py
git commit -m "feat(scenarios): add s05-s07 (rag-multi-hop, moderation-rag-tool-combo, workflow-conditional)"
```


---

## Task 5: Scenarios 08-10 (error-recovery-agent, nested-workflow, workflow-error-propagation)

**Files:**
- `traceset/scenarios/s08_error_recovery_agent.py` (create)
- `traceset/scenarios/s09_nested_workflow.py` (create)
- `traceset/scenarios/s10_workflow_error_propagation.py` (create)

### Steps

- [ ] **Step 1: Write `traceset/scenarios/s08_error_recovery_agent.py`**

```python
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
```

- [ ] **Step 2: Write `traceset/scenarios/s09_nested_workflow.py`**

```python
"""Scenario 09: Nested workflow — chatflow with sub-workflow node.

Events (14):
  1.  trace-create       (MessageTraceInfo)
  2.  span-create        (chatflow root)
  3.  span-create        (Start, parent=root)
  4.  generation-create  (LLM 1: Classify, parent=root)
  5.  span-create        (SubWorkflow, parent=root)
  6.  span-create        (Inner Start, parent=SubWorkflow)
  7.  generation-create  (Inner LLM 1: Extract Entities, parent=SubWorkflow)
  8.  generation-create  (Inner LLM 2: Extract Obligations, parent=SubWorkflow)
  9.  span-create        (Inner End, parent=SubWorkflow)
 10.  generation-create  (LLM 2: Summarize, parent=root)
 11.  span-create        (End, parent=root)
 12.  generation-create  (Message, parent=root)
 13.  trace-create       (GenerateNameTraceInfo, upsert)
 14.  span-create        (GenerateName)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "09-nested-workflow"
SCENARIO_DESCRIPTION = "Chatflow with sub-workflow node + Message + GenerateName"
APP_TYPE = "advanced-chat"
DIFY_APP_MODE = "advanced-chat"
EDGE_CASE = "nested-workflow"
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "WorkflowTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 14
EXPECTED_SPAN_COUNT = 12
SPAN_PATTERN = "nested-workflow"

_BASE = "2025-01-15T14:30:00.000000+00:00"
_TRACE = "91a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_CF = "92a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_START = "93a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_LLM1 = "94a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SUBWF = "95a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_IN_START = "96a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_IN_LLM1 = "97a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_IN_LLM2 = "98a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_IN_END = "99a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_LLM2 = "9aa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_END = "9ba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_MSG = "9ca2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_NAME = "9da2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-5b1c0d9e8f"
_CONV = "conv-8b9c0d1e2f3a"
_QUERY = "Process this legal contract and extract the key terms and obligations"
_MODEL = "gpt-4o"
_PARAMS = {"temperature": 0.3, "max_tokens": 1000}

_LLM1_OUT = "Document type: Master Service Agreement. Jurisdiction: California. Parties: TechCorp (Provider), Client Inc. (Customer). Effective Date: 2025-02-01. Term: 24 months with auto-renewal."
_IN_LLM1_OUT = "Extracted Entities: Provider: TechCorp. Customer: Client Inc. Effective Date: 2025-02-01. Term: 24 months. Auto-renewal: Yes, 12-month cycles. Payment Terms: Net-30. Contract Value: $480,000/year."
_IN_LLM2_OUT = "Extracted Obligations: Provider: 99.9% uptime SLA, 4-hour support response, monthly usage reports, data security compliance (SOC 2). Customer: Net-30 payment, 50-user minimum, data security compliance, timely issue reporting."
_LLM2_OUT = "Contract Summary: 24-month MSA with auto-renewal. Key terms: 99.9% uptime SLA, 4hr support response, net-30 billing, 50-user minimum. Risk areas: auto-renewal clause, minimum user requirement."
_MSG_OUT = "I've processed your legal contract. Here's a summary of key terms:\n\nThe agreement is a 24-month Master Service Agreement between TechCorp and Client Inc., effective February 1, 2025. Key terms include a 99.9% uptime SLA, 4-hour support response time, and net-30 billing terms. The contract auto-renews for 12-month cycles and requires a 50-user minimum. Risk areas to note: the auto-renewal clause and the minimum user requirement."
_NAME_OUT = "Contract Analysis - TechCorp MSA"

_LLM1_U = {"input": 120, "output": 100, "total": 220, "unit": "TOKENS", "inputCost": 0.000300, "outputCost": 0.001000, "totalCost": 0.001300}
_IN_LLM1_U = {"input": 150, "output": 180, "total": 330, "unit": "TOKENS", "inputCost": 0.000375, "outputCost": 0.001800, "totalCost": 0.002175}
_IN_LLM2_U = {"input": 200, "output": 220, "total": 420, "unit": "TOKENS", "inputCost": 0.000500, "outputCost": 0.002200, "totalCost": 0.002700}
_LLM2_U = {"input": 280, "output": 200, "total": 480, "unit": "TOKENS", "inputCost": 0.000700, "outputCost": 0.002000, "totalCost": 0.002700}
_MSG_U = {"input": 300, "output": 250, "total": 550, "unit": "TOKENS", "inputCost": 0.000750, "outputCost": 0.002500, "totalCost": 0.003250}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s09-e01"), timestamp=make_timestamp(_BASE, 8.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Contract Processing Chatflow", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"conversation_id": _CONV, "chatflow_id": "cf-contract-009"},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e02"), timestamp=make_timestamp(_BASE, 8.002),
        event_type="span-create",
        body=make_span_create(
            span_id=_CF, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 8.000),
            input={"query": _QUERY}, output={"response": _MSG_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e03"), timestamp=make_timestamp(_BASE, 8.003),
        event_type="span-create",
        body=make_span_create(
            span_id=_START, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 0.050),
            parent_observation_id=_CF, input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e04"), timestamp=make_timestamp(_BASE, 8.004),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_LLM1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100), end_time=make_timestamp(_BASE, 1.500),
            usage=_LLM1_U, completion_start_time=make_timestamp(_BASE, 0.300),
            model_parameters=_PARAMS, parent_observation_id=_CF,
            input={"messages": [{"role": "user", "content": "Classify this document: 12-page SaaS agreement."}]},
            output={"text": _LLM1_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e05"), timestamp=make_timestamp(_BASE, 8.005),
        event_type="span-create",
        body=make_span_create(
            span_id=_SUBWF, trace_id=_TRACE, name="Sub-workflow: Term Extraction",
            start_time=make_timestamp(_BASE, 1.550), end_time=make_timestamp(_BASE, 4.500),
            parent_observation_id=_CF,
            input={"document_type": "MSA"},
            output={"terms": 47, "obligations": 12, "slas": 8},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e06"), timestamp=make_timestamp(_BASE, 8.006),
        event_type="span-create",
        body=make_span_create(
            span_id=_IN_START, trace_id=_TRACE, name="Inner Start",
            start_time=make_timestamp(_BASE, 1.550), end_time=make_timestamp(_BASE, 1.600),
            parent_observation_id=_SUBWF,
            input={"document_type": "MSA"}, output={"document_type": "MSA"},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e07"), timestamp=make_timestamp(_BASE, 8.007),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_IN_LLM1, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 1.650), end_time=make_timestamp(_BASE, 3.000),
            usage=_IN_LLM1_U, completion_start_time=make_timestamp(_BASE, 1.850),
            model_parameters=_PARAMS, parent_observation_id=_SUBWF,
            input={"messages": [{"role": "user", "content": "Extract entities from the contract."}]},
            output={"text": _IN_LLM1_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e08"), timestamp=make_timestamp(_BASE, 8.008),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_IN_LLM2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 3.050), end_time=make_timestamp(_BASE, 4.200),
            usage=_IN_LLM2_U, completion_start_time=make_timestamp(_BASE, 3.250),
            model_parameters=_PARAMS, parent_observation_id=_SUBWF,
            input={"messages": [{"role": "user", "content": "Extract obligations from the contract."}]},
            output={"text": _IN_LLM2_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e09"), timestamp=make_timestamp(_BASE, 8.009),
        event_type="span-create",
        body=make_span_create(
            span_id=_IN_END, trace_id=_TRACE, name="Inner End",
            start_time=make_timestamp(_BASE, 4.250), end_time=make_timestamp(_BASE, 4.500),
            parent_observation_id=_SUBWF,
            input={"terms": 47, "obligations": 12, "slas": 8},
            output={"terms": 47, "obligations": 12, "slas": 8},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e10"), timestamp=make_timestamp(_BASE, 8.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_LLM2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 4.550), end_time=make_timestamp(_BASE, 6.000),
            usage=_LLM2_U, completion_start_time=make_timestamp(_BASE, 4.750),
            model_parameters=_PARAMS, parent_observation_id=_CF,
            input={"messages": [{"role": "user", "content": "Summarize the extracted terms."}]},
            output={"text": _LLM2_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e11"), timestamp=make_timestamp(_BASE, 8.011),
        event_type="span-create",
        body=make_span_create(
            span_id=_END, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 6.050), end_time=make_timestamp(_BASE, 6.100),
            parent_observation_id=_CF,
            input={"summary": _LLM2_OUT}, output={"response": _MSG_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e12"), timestamp=make_timestamp(_BASE, 8.012),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_MSG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 6.150), end_time=make_timestamp(_BASE, 7.500),
            usage=_MSG_U, completion_start_time=make_timestamp(_BASE, 6.400),
            model_parameters=_PARAMS, parent_observation_id=_CF,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _MSG_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s09-e13"), timestamp=make_timestamp(_BASE, 8.013),
        event_type="trace-create",
        body=make_trace_create(trace_id=_TRACE, name="Generate Name", user_id=_USER),
    ))
    events.append(wrap_event(
        event_id=make_event_id("s09-e14"), timestamp=make_timestamp(_BASE, 8.014),
        event_type="span-create",
        body=make_span_create(
            span_id=_NAME, trace_id=_TRACE, name="Generate Name",
            start_time=make_timestamp(_BASE, 7.550), end_time=make_timestamp(_BASE, 7.800),
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
            {"index": 2, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 4, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 6, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 7, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 8, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 9, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 10, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 11, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 12, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 13, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 14, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Nested workflow: chatflow with sub-workflow node. Inner workflow events have parentObservationId=SubWorkflow. Outer events have parentObservationId=chatflow root. Message generation is MessageTraceInfo.",
    }
```

- [ ] **Step 3: Write `traceset/scenarios/s10_workflow_error_propagation.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify s08-s10 pass**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/tests/test_scenarios.py::test_s08_error_recovery_agent traceset/tests/test_scenarios.py::test_s09_nested_workflow traceset/tests/test_scenarios.py::test_s10_workflow_error_propagation -v -m "not e2e"
```

Expected: all 3 PASSED.

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/scenarios/s08_error_recovery_agent.py traceset/scenarios/s09_nested_workflow.py traceset/scenarios/s10_workflow_error_propagation.py
git commit -m "feat(scenarios): add s08-s10 (error-recovery-agent, nested-workflow, workflow-error-propagation)"
```

---

## Task 6: Scenarios 11-13 (streaming-chatflow, multi-model-pipeline, completion-multi-feature)

**Files:**
- `traceset/scenarios/s11_streaming_chatflow.py` (create)
- `traceset/scenarios/s12_multi_model_pipeline.py` (create)
- `traceset/scenarios/s13_completion_multi_feature.py` (create)

### Steps

- [ ] **Step 1: Write `traceset/scenarios/s11_streaming_chatflow.py`**

```python
"""Scenario 11: Streaming chatflow — 5 LLM nodes + streaming Message + GenerateName.

Events (13):
  1.  trace-create       (MessageTraceInfo)
  2.  span-create        (chatflow root)
  3.  span-create        (Start, parent=root)
  4.  generation-create  (LLM 1: Setting, parent=root)
  5.  generation-create  (LLM 2: Character, parent=root)
  6.  generation-create  (LLM 3: Discovery, parent=root)
  7.  generation-create  (LLM 4: First Contact, parent=root)
  8.  generation-create  (LLM 5: Resolution, parent=root)
  9.  span-create        (End, parent=root)
 10.  generation-create  (Message, streaming, parent=root)
 11.  generation-create  (SuggestedQuestions)
 12.  trace-create       (GenerateNameTraceInfo, upsert)
 13.  span-create        (GenerateName)

VERIFY: Message generation has streaming metadata (ttft, total_generation_ms, chunks).
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "11-streaming-chatflow"
SCENARIO_DESCRIPTION = "Chatflow with 5 LLM nodes + streaming Message + GenerateName"
APP_TYPE = "advanced-chat"
DIFY_APP_MODE = "advanced-chat"
EDGE_CASE = "streaming"
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "WorkflowTraceInfo", "SuggestedQuestionTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 13
EXPECTED_SPAN_COUNT = 11
SPAN_PATTERN = "streaming"

_BASE = "2025-01-15T15:30:00.000000+00:00"
_TRACE = "b1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_CF = "b2a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_START = "b3a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_L1 = "b4a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_L2 = "b5a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_L3 = "b6a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_L4 = "b7a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_L5 = "b8a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_END = "b9a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_MSG = "baa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SUGG = "bba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_NAME = "bca2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-7d3e2f1a0b"
_CONV = "conv-9c0d1e2f3a4b"
_QUERY = "Write a short story about a space explorer who discovers a new civilization"
_MODEL = "claude-3-5-sonnet-20241022"
_PARAMS = {"temperature": 0.8, "max_tokens": 2000}

_L1_OUT = "Setting: The year is 2387. Commander Elara Vance pilots the ISS Meridian through the Andromeda sector on a routine survey mission."
_L2_OUT = "Character: Elara is a 42-year-old veteran explorer with 23 years of deep-space experience. She has discovered 12 habitable exoplanets and survived first contact with the Silicate Entities of Rigel."
_L3_OUT = "Discovery: Sensors detect rhythmic electromagnetic pulses from a dwarf planet in the Proxima Centauri system. The pattern is non-random — unmistakably artificial."
_L4_OUT = "First Contact: The beings call themselves the Lynthari. They communicate through bioluminescent patterns across their crystalline skin. Their civilization is ancient — 40,000 years of recorded history."
_L5_OUT = "Resolution: Elara and the Lynthari establish a cultural exchange. The Meridian returns to Earth with knowledge of a peaceful, ancient civilization. The Lynthari gift Elara a crystal that glows in their language."
_MSG_OUT = (
    "Commander Elara Vance had seen thousands of stars, but none pulsed like "
    "this one. The rhythmic glow from Proxima Centauri's dwarf planet was "
    "undeniably artificial — a beacon calling across the void.\n\n"
    "She led the landing party herself, stepping onto a surface of living "
    "crystal. The ground beneath her boots shimmered, and from it rose figures "
    "of light — the Lynthari. They did not speak. Instead, their crystalline "
    "bodies flickered with colors she somehow understood: welcome, curiosity, "
    "joy.\n\n"
    "For three days, Elara lived among them, learning their bioluminescent "
    "language. They were old — forty thousand years of history etched in "
    "light. And they had been waiting, their beacon calling out into the "
    "darkness, hoping someone would come.\n\n"
    "Someone had."
)
_SUGG_OUT = {"questions": ["What is the Lynthari civilization like?", "Will Elara return to the dwarf planet?", "What technology do the Lynthari use?"]}
_NAME_OUT = "The Light of Lynthari"

_L1_U = {"input": 100, "output": 120, "total": 220, "unit": "TOKENS", "inputCost": 0.000300, "outputCost": 0.001800, "totalCost": 0.002100}
_L2_U = {"input": 110, "output": 130, "total": 240, "unit": "TOKENS", "inputCost": 0.000330, "outputCost": 0.001950, "totalCost": 0.002280}
_L3_U = {"input": 120, "output": 140, "total": 260, "unit": "TOKENS", "inputCost": 0.000360, "outputCost": 0.002100, "totalCost": 0.002460}
_L4_U = {"input": 130, "output": 150, "total": 280, "unit": "TOKENS", "inputCost": 0.000390, "outputCost": 0.002250, "totalCost": 0.002640}
_L5_U = {"input": 140, "output": 160, "total": 300, "unit": "TOKENS", "inputCost": 0.000420, "outputCost": 0.002400, "totalCost": 0.002820}
_MSG_U = {"input": 200, "output": 350, "total": 550, "unit": "TOKENS", "inputCost": 0.000600, "outputCost": 0.005250, "totalCost": 0.005850}
_SUGG_U = {"input": 150, "output": 80, "total": 230, "unit": "TOKENS", "inputCost": 0.000450, "outputCost": 0.001200, "totalCost": 0.001650}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s11-e01"), timestamp=make_timestamp(_BASE, 8.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Creative Writing Chatflow", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"conversation_id": _CONV, "chatflow_id": "cf-story-011"},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s11-e02"), timestamp=make_timestamp(_BASE, 8.002),
        event_type="span-create",
        body=make_span_create(
            span_id=_CF, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 8.000),
            input={"query": _QUERY}, output={"story": _MSG_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s11-e03"), timestamp=make_timestamp(_BASE, 8.003),
        event_type="span-create",
        body=make_span_create(
            span_id=_START, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000), end_time=make_timestamp(_BASE, 0.050),
            parent_observation_id=_CF, input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    nodes = [
        ("Setting", _L1, 0.100, 1.200, _L1_OUT, _L1_U),
        ("Character", _L2, 1.250, 2.400, _L2_OUT, _L2_U),
        ("Discovery", _L3, 2.450, 3.600, _L3_OUT, _L3_U),
        ("First Contact", _L4, 3.650, 4.800, _L4_OUT, _L4_U),
        ("Resolution", _L5, 4.850, 6.000, _L5_OUT, _L5_U),
    ]
    for i, (name, nid, start, end, out, usage) in enumerate(nodes):
        events.append(wrap_event(
            event_id=make_event_id(f"s11-e{i+4:02d}"),
            timestamp=make_timestamp(_BASE, 8.004 + i * 0.001),
            event_type="generation-create",
            body=make_generation_create(
                gen_id=nid, trace_id=_TRACE, name=_MODEL, model=_MODEL,
                start_time=make_timestamp(_BASE, start), end_time=make_timestamp(_BASE, end),
                usage=usage, completion_start_time=make_timestamp(_BASE, start + 0.200),
                model_parameters=_PARAMS, parent_observation_id=_CF,
                input={"messages": [{"role": "user", "content": f"Write the {name} section of the story."}]},
                output={"text": out},
            ),
        ))

    events.append(wrap_event(
        event_id=make_event_id("s11-e09"), timestamp=make_timestamp(_BASE, 8.009),
        event_type="span-create",
        body=make_span_create(
            span_id=_END, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 6.050), end_time=make_timestamp(_BASE, 6.100),
            parent_observation_id=_CF,
            input={"sections": 5}, output={"story": _MSG_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s11-e10"), timestamp=make_timestamp(_BASE, 8.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_MSG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 6.150), end_time=make_timestamp(_BASE, 7.500),
            usage=_MSG_U, completion_start_time=make_timestamp(_BASE, 6.400),
            model_parameters=_PARAMS, parent_observation_id=_CF,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _MSG_OUT},
            metadata={"streaming": True, "ttft_ms": 250, "total_generation_ms": 1350, "chunks": 47},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s11-e11"), timestamp=make_timestamp(_BASE, 8.011),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SUGG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 7.550), end_time=make_timestamp(_BASE, 7.900),
            usage=_SUGG_U, completion_start_time=make_timestamp(_BASE, 7.650),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": "Suggest follow-up questions."}]},
            output=_SUGG_OUT,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s11-e12"), timestamp=make_timestamp(_BASE, 8.012),
        event_type="trace-create",
        body=make_trace_create(trace_id=_TRACE, name="Generate Name", user_id=_USER),
    ))
    events.append(wrap_event(
        event_id=make_event_id("s11-e13"), timestamp=make_timestamp(_BASE, 8.013),
        event_type="span-create",
        body=make_span_create(
            span_id=_NAME, trace_id=_TRACE, name="Generate Name",
            start_time=make_timestamp(_BASE, 7.950), end_time=make_timestamp(_BASE, 8.200),
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
            {"index": 2, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 4, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 5, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 6, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 7, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 8, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 9, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 10, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 11, "type": "generation-create", "source_trace_type": "SuggestedQuestionTraceInfo", "dify_handler": "LangFuseDataTrace.suggested_question_trace"},
            {"index": 12, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 13, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "VERIFY: Message generation (event 10) has streaming metadata: {streaming: true, ttft_ms: 250, total_generation_ms: 1350, chunks: 47}. completionStartTime = TTFT.",
    }
```

- [ ] **Step 2: Write `traceset/scenarios/s12_multi_model_pipeline.py`**

```python
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
```

- [ ] **Step 3: Write `traceset/scenarios/s13_completion_multi_feature.py`**

```python
"""Scenario 13: Completion multi-feature — moderation + RAG + tools + synthesis. No sessionId.

Events (12):
  1.  trace-create       (MessageTraceInfo, NO sessionId)
  2.  span-create        (Moderation)
  3.  span-create        (RAG 1: jwt.py)
  4.  span-create        (RAG 2: middleware.py)
  5.  span-create        (RAG 3: models.py)
  6.  span-create        (RAG 4: utils.py)
  7.  span-create        (RAG 5: config.py)
  8.  span-create        (Tool 1: ast_parser)
  9.  span-create        (Tool 2: type_checker)
 10.  generation-create  (Synthesis)
 11.  generation-create  (SuggestedQuestions)
 12.  trace-create       (GenerateNameTraceInfo, upsert only, no span)

VERIFY: trace-create body has NO sessionId field (completion mode has no conversation).
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create, wrap_event,
)

SCENARIO_ID = "13-completion-multi-feature"
SCENARIO_DESCRIPTION = "Moderation + 5-hop RAG + 2 tool calls + synthesis + SuggestedQuestions + GenerateName. No sessionId"
APP_TYPE = "completion"
DIFY_APP_MODE = "completion"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "ModerationTraceInfo", "DatasetRetrievalTraceInfo", "ToolTraceInfo", "SuggestedQuestionTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 12
EXPECTED_SPAN_COUNT = 10
SPAN_PATTERN = "feature-combination"

_BASE = "2025-01-15T16:30:00.000000+00:00"
_TRACE = "d1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_MOD = "d2a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R1 = "d3a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R2 = "d4a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R3 = "d5a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R4 = "d6a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_R5 = "d7a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T1 = "d8a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_T2 = "d9a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SYN = "daa2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_SUGG = "dba2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"

_USER = "u-9f5a4b3c2d"
_QUERY = "Generate comprehensive documentation for the authentication module in src/auth/"
_MODEL = "gpt-4o"
_PARAMS = {"temperature": 0.3, "max_tokens": 2000}

_MOD_OUT = {"flagged": False, "categories": {"spam": False, "harassment": False, "hate": False}, "action": "pass"}
_R1_OUT = {"documents": [{"title": "src/auth/jwt.py", "content": "JWT token generation and verification. Functions: generate_token(user_id), verify_token(token), refresh_token(refresh). Uses PyJWT library.", "score": 0.97}]}
_R2_OUT = {"documents": [{"title": "src/auth/middleware.py", "content": "Express authentication middleware. Exports: authRequired(req, res, next), optionalAuth(req, res, next). Checks Authorization header.", "score": 0.95}]}
_R3_OUT = {"documents": [{"title": "src/auth/models.py", "content": "User and Session models. User: id, email, password_hash, role. Session: id, user_id, token, expires_at.", "score": 0.94}]}
_R4_OUT = {"documents": [{"title": "src/auth/utils.py", "content": "Password hashing and validation. Functions: hash_password(pw), verify_password(pw, hash), validate_email(email), validate_password(pw).", "score": 0.92}]}
_R5_OUT = {"documents": [{"title": "src/auth/config.py", "content": "Auth configuration. Variables: JWT_SECRET, JWT_EXPIRY=3600, REFRESH_EXPIRY=604800, BCRYPT_ROUNDS=10.", "score": 0.90}]}
_T1_IN = {"paths": ["src/auth/jwt.py", "src/auth/middleware.py", "src/auth/models.py", "src/auth/utils.py", "src/auth/config.py"]}
_T1_OUT = {"modules": 5, "functions": 23, "classes": 6, "lines": 142, "imports": 12}
_T2_IN = {"paths": ["src/auth/"]}
_T2_OUT = {"errors": 0, "warnings": 2, "warning_details": ["Implicit any in middleware.ts:45", "Untyped return in utils.ts:89"]}
_SYN_OUT = (
    "# Authentication Module Documentation\n\n"
    "## Overview\nThe `src/auth/` module provides JWT-based authentication with "
    "5 modules, 23 functions, and 6 classes across 142 lines of code.\n\n"
    "## Modules\n\n"
    "### jwt.py\nToken generation, verification, and refresh. Uses PyJWT library.\n\n"
    "### middleware.py\nExpress middleware for route protection. `authRequired` "
    "enforces authentication, `optionalAuth` allows unauthenticated access.\n\n"
    "### models.py\nUser and Session data models. User has email, password_hash, "
    "and role. Session tracks tokens and expiry.\n\n"
    "### utils.py\nPassword hashing (bcrypt), email and password validation.\n\n"
    "### config.py\nConfiguration: JWT_SECRET, token expiry (1h), refresh expiry "
    "(7d), bcrypt rounds (10).\n\n"
    "## Type Safety\n0 errors, 2 warnings (implicit any, untyped return).\n\n"
    "## Usage\n```python\nfrom auth.middleware import authRequired\napp.use('/api', authRequired)\n```"
)
_SUGG_OUT = {"questions": ["How do I add a new authentication strategy?", "What are the security best practices for this module?", "How do I rotate the JWT secret?"]}
_NAME_OUT = "Auth Module Documentation"

_SYN_U = {"input": 250, "output": 200, "total": 450, "unit": "TOKENS", "inputCost": 0.000625, "outputCost": 0.002000, "totalCost": 0.002625}
_SUGG_U = {"input": 150, "output": 80, "total": 230, "unit": "TOKENS", "inputCost": 0.000375, "outputCost": 0.000800, "totalCost": 0.001175}


def build_events():
    events = []

    events.append(wrap_event(
        event_id=make_event_id("s13-e01"), timestamp=make_timestamp(_BASE, 7.001),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE, name="Code Documentation Generator", user_id=_USER,
            input={"query": _QUERY},
            metadata={"completion_id": "comp-doc-013"},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s13-e02"), timestamp=make_timestamp(_BASE, 7.002),
        event_type="span-create",
        body=make_span_create(
            span_id=_MOD, trace_id=_TRACE, name="Moderation",
            start_time=make_timestamp(_BASE, 0.050), end_time=make_timestamp(_BASE, 0.300),
            input={"text": _QUERY}, output=_MOD_OUT,
            level="DEFAULT", status_message="Content approved. Code analysis query.",
        ),
    ))

    rags = [
        ("jwt.py", _R1, 0.350, 1.000, _R1_OUT),
        ("middleware.py", _R2, 1.050, 1.700, _R2_OUT),
        ("models.py", _R3, 1.750, 2.400, _R3_OUT),
        ("utils.py", _R4, 2.450, 3.100, _R4_OUT),
        ("config.py", _R5, 3.150, 3.800, _R5_OUT),
    ]
    for i, (name, rid, start, end, out) in enumerate(rags):
        events.append(wrap_event(
            event_id=make_event_id(f"s13-e{i+3:02d}"),
            timestamp=make_timestamp(_BASE, 7.003 + i * 0.001),
            event_type="span-create",
            body=make_span_create(
                span_id=rid, trace_id=_TRACE, name=f"Knowledge Retrieval: {name}",
                start_time=make_timestamp(_BASE, start), end_time=make_timestamp(_BASE, end),
                input={"query": f"src/auth/{name}"}, output=out,
            ),
        ))

    events.append(wrap_event(
        event_id=make_event_id("s13-e08"), timestamp=make_timestamp(_BASE, 7.008),
        event_type="span-create",
        body=make_span_create(
            span_id=_T1, trace_id=_TRACE, name="ast_parser",
            start_time=make_timestamp(_BASE, 3.850), end_time=make_timestamp(_BASE, 4.500),
            input=_T1_IN, output=_T1_OUT,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s13-e09"), timestamp=make_timestamp(_BASE, 7.009),
        event_type="span-create",
        body=make_span_create(
            span_id=_T2, trace_id=_TRACE, name="type_checker",
            start_time=make_timestamp(_BASE, 4.550), end_time=make_timestamp(_BASE, 5.200),
            input=_T2_IN, output=_T2_OUT,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s13-e10"), timestamp=make_timestamp(_BASE, 7.010),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SYN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 5.250), end_time=make_timestamp(_BASE, 6.500),
            usage=_SYN_U, completion_start_time=make_timestamp(_BASE, 5.450),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _SYN_OUT},
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s13-e11"), timestamp=make_timestamp(_BASE, 7.011),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_SUGG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 6.550), end_time=make_timestamp(_BASE, 6.900),
            usage=_SUGG_U, completion_start_time=make_timestamp(_BASE, 6.650),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": "Suggest follow-up questions."}]},
            output=_SUGG_OUT,
        ),
    ))

    events.append(wrap_event(
        event_id=make_event_id("s13-e12"), timestamp=make_timestamp(_BASE, 7.012),
        event_type="trace-create",
        body=make_trace_create(trace_id=_TRACE, name=_NAME_OUT, user_id=_USER),
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
            {"index": 2, "type": "span-create", "source_trace_type": "ModerationTraceInfo", "dify_handler": "LangFuseDataTrace.moderation_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 6, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 8, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 9, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 10, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 11, "type": "generation-create", "source_trace_type": "SuggestedQuestionTraceInfo", "dify_handler": "LangFuseDataTrace.suggested_question_trace"},
            {"index": 12, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "VERIFY: trace-create body (event 1) has NO sessionId field. Completion mode has no conversation. GenerateName is trace-create only (upsert, no span) in this scenario.",
    }
```

- [ ] **Step 4: Run tests to verify s11-s13 pass**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/tests/test_scenarios.py::test_s11_streaming_chatflow traceset/tests/test_scenarios.py::test_s12_multi_model_pipeline traceset/tests/test_scenarios.py::test_s13_completion_multi_feature -v -m "not e2e"
```

Expected: all 3 PASSED.

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/scenarios/s11_streaming_chatflow.py traceset/scenarios/s12_multi_model_pipeline.py traceset/scenarios/s13_completion_multi_feature.py
git commit -m "feat(scenarios): add s11-s13 (streaming-chatflow, multi-model-pipeline, completion-multi-feature)"
```


---

## Task 7: Update Registry + Generator (EXPECTED_SPAN_COUNT self-check, 13 scenarios)

**Files:**
- `traceset/scenarios/__init__.py` (rewrite)
- `traceset/generate_traceset.py` (rewrite — add 7th self-check, update for 13 scenarios)
- `traceset/tests/test_generate_traceset.py` (rewrite — update for 13 scenarios)

### Steps

- [ ] **Step 1: Write `traceset/scenarios/__init__.py`**

```python
"""Scenario registry. Imports all 13 scenario modules and exposes SCENARIOS list."""
from . import (
    s01_linear_llm_chain,
    s02_parallel_branches,
    s03_agent_react_loop,
    s04_multi_tool_chain,
    s05_rag_multi_hop,
    s06_moderation_rag_tool_combo,
    s07_workflow_conditional,
    s08_error_recovery_agent,
    s09_nested_workflow,
    s10_workflow_error_propagation,
    s11_streaming_chatflow,
    s12_multi_model_pipeline,
    s13_completion_multi_feature,
)

SCENARIOS = [
    s01_linear_llm_chain,
    s02_parallel_branches,
    s03_agent_react_loop,
    s04_multi_tool_chain,
    s05_rag_multi_hop,
    s06_moderation_rag_tool_combo,
    s07_workflow_conditional,
    s08_error_recovery_agent,
    s09_nested_workflow,
    s10_workflow_error_propagation,
    s11_streaming_chatflow,
    s12_multi_model_pipeline,
    s13_completion_multi_feature,
]
```

- [ ] **Step 2: Write `traceset/tests/test_generate_traceset.py`**

```python
"""Tests for the generation script."""
import json
import os
import pytest

from traceset.scenarios import SCENARIOS
from traceset.generate_traceset import generate_scenario


def test_scenarios_registry_has_13():
    assert len(SCENARIOS) == 13, f"Expected 13 scenarios, got {len(SCENARIOS)}"


def test_all_scenario_ids_unique():
    ids = [s.SCENARIO_ID for s in SCENARIOS]
    assert len(ids) == len(set(ids)), f"Duplicate scenario IDs: {ids}"


def test_all_scenarios_have_span_count():
    for s in SCENARIOS:
        assert hasattr(s, "EXPECTED_SPAN_COUNT"), f"{s.SCENARIO_ID} missing EXPECTED_SPAN_COUNT"
        assert hasattr(s, "SPAN_PATTERN"), f"{s.SCENARIO_ID} missing SPAN_PATTERN"


def test_generate_scenario_writes_files(tmp_path):
    from traceset.scenarios import s01_linear_llm_chain
    generate_scenario(s01_linear_llm_chain, str(tmp_path))

    scenario_dir = tmp_path / "01-linear-llm-chain"
    assert scenario_dir.exists()

    events_path = scenario_dir / "events.jsonl"
    assert events_path.exists()
    lines = events_path.read_text().strip().split("\n")
    assert len(lines) == 14

    for line in lines:
        event = json.loads(line)
        assert "id" in event
        assert "timestamp" in event
        assert "type" in event
        assert "body" in event

    meta_path = scenario_dir / "meta.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta["scenario_id"] == "01-linear-llm-chain"
    assert meta["expected_event_count"] == 14
    assert meta["expected_span_count"] == 13
    assert meta["span_pattern"] == "linear"


def test_generate_scenario_self_checks(tmp_path):
    from traceset.scenarios import s09_nested_workflow
    generate_scenario(s09_nested_workflow, str(tmp_path))
    assert (tmp_path / "09-nested-workflow" / "events.jsonl").exists()


def test_generate_all_scenarios(tmp_path):
    for scenario in SCENARIOS:
        generate_scenario(scenario, str(tmp_path))
        scenario_dir = tmp_path / scenario.SCENARIO_ID
        assert scenario_dir.exists(), f"Missing dir for {scenario.SCENARIO_ID}"
        events_path = scenario_dir / "events.jsonl"
        lines = events_path.read_text().strip().split("\n")
        assert len(lines) == scenario.EXPECTED_EVENT_COUNT


def test_generate_catalog(tmp_path):
    from traceset.generate_traceset import generate_catalog
    generate_catalog(SCENARIOS, str(tmp_path))
    catalog_path = tmp_path / "catalog.json"
    assert catalog_path.exists()
    catalog = json.loads(catalog_path.read_text())
    assert len(catalog) == 13
    entry = catalog[0]
    assert "scenario_id" in entry
    assert "app_type" in entry
    assert "event_count" in entry
    assert "span_count" in entry
    assert "span_pattern" in entry
    assert "trace_types" in entry


def test_generate_readme(tmp_path):
    from traceset.generate_traceset import generate_readme
    generate_readme(SCENARIOS, str(tmp_path))
    readme_path = tmp_path / "README.md"
    assert readme_path.exists()
    content = readme_path.read_text()
    assert "Dify App Trace Reference Catalog" in content
    assert "01-linear-llm-chain" in content
    assert "13-completion-multi-feature" in content


def test_generate_schema_doc(tmp_path):
    from traceset.generate_traceset import generate_schema_doc
    generate_schema_doc(str(tmp_path))
    schema_path = tmp_path / "schema.md"
    assert schema_path.exists()
    content = schema_path.read_text()
    assert "trace-create" in content
    assert "span-create" in content
    assert "generation-create" in content
    assert "usageDetails" in content
    assert "camelCase" in content


def test_main_generates_all_files(tmp_path):
    import importlib
    from traceset import generate_traceset as gt
    gt._BASE_DIR = str(tmp_path)
    gt.main()

    assert (tmp_path / "catalog.json").exists()
    assert (tmp_path / "README.md").exists()
    assert (tmp_path / "schema.md").exists()

    for s in SCENARIOS:
        scenario_dir = tmp_path / s.SCENARIO_ID
        assert (scenario_dir / "events.jsonl").exists(), f"Missing events.jsonl for {s.SCENARIO_ID}"
        assert (scenario_dir / "meta.json").exists(), f"Missing meta.json for {s.SCENARIO_ID}"
```

- [ ] **Step 3: Write `traceset/generate_traceset.py`** (updated — 7th self-check + 13 scenarios)

```python
#!/usr/bin/env python3
"""Generate the Dify trace reference catalog.

For each scenario:
  1. Build events via scenario.build_events()
  2. Validate each event via schema.validate_event()
  3. Run self-checks (7 checks including span count)
  4. Write events.jsonl and meta.json

Also generates root files: catalog.json, README.md, schema.md.
"""
from __future__ import annotations

import json
import os
import sys

from traceset.schema import validate_event
from traceset.scenarios import SCENARIOS

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def generate_scenario(scenario, base_dir: str) -> dict:
    """Generate events.jsonl and meta.json for one scenario."""
    scenario_id = scenario.SCENARIO_ID
    scenario_dir = os.path.join(base_dir, scenario_id)
    os.makedirs(scenario_dir, exist_ok=True)

    events = scenario.build_events()
    meta = scenario.build_meta()

    for event in events:
        validate_event(event)

    # 1. Event count matches EXPECTED_EVENT_COUNT
    assert len(events) == scenario.EXPECTED_EVENT_COUNT, (
        f"{scenario_id}: event count {len(events)} != {scenario.EXPECTED_EVENT_COUNT}"
    )

    # 2. All event types are valid
    valid_types = {"trace-create", "span-create", "generation-create"}
    for i, e in enumerate(events):
        assert e["type"] in valid_types, f"{scenario_id}[{i}]: invalid type {e['type']}"

    # 3. No snake_case body keys
    for i, e in enumerate(events):
        for key in e["body"]:
            assert "_" not in key, f"{scenario_id}[{i}]: snake_case body key '{key}'"

    # 4. Timestamps monotonically non-decreasing
    timestamps = [e["timestamp"] for e in events]
    assert timestamps == sorted(timestamps), f"{scenario_id}: timestamps not monotonic"

    # 5. meta events_in_order count matches event count
    assert len(meta["events_in_order"]) == len(events), (
        f"{scenario_id}: meta events_in_order count mismatch"
    )

    # 6. meta events_in_order types match actual events
    for i, (e, m) in enumerate(zip(events, meta["events_in_order"]), 1):
        assert m["index"] == i, f"{scenario_id}: meta index mismatch at {i}"
        assert m["type"] == e["type"], f"{scenario_id}[{i}]: meta type mismatch"

    # 7. Span count matches EXPECTED_SPAN_COUNT
    span_count = sum(1 for e in events if e["type"] in ("span-create", "generation-create"))
    assert span_count == scenario.EXPECTED_SPAN_COUNT, (
        f"{scenario_id}: span count {span_count} != {scenario.EXPECTED_SPAN_COUNT}"
    )

    events_path = os.path.join(scenario_dir, "events.jsonl")
    with open(events_path, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    meta_path = os.path.join(scenario_dir, "meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
        f.write("\n")

    return {
        "scenario_id": scenario_id,
        "app_type": scenario.APP_TYPE,
        "dify_app_mode": scenario.DIFY_APP_MODE,
        "edge_case": scenario.EDGE_CASE,
        "event_count": len(events),
        "span_count": span_count,
        "span_pattern": scenario.SPAN_PATTERN,
        "trace_types": scenario.TRACE_TYPES_EMITTED,
    }


def generate_catalog(scenarios, base_dir: str) -> None:
    """Generate catalog.json."""
    catalog = []
    for scenario in scenarios:
        events = scenario.build_events()
        span_count = sum(1 for e in events if e["type"] in ("span-create", "generation-create"))
        catalog.append({
            "scenario_id": scenario.SCENARIO_ID,
            "app_type": scenario.APP_TYPE,
            "dify_app_mode": scenario.DIFY_APP_MODE,
            "edge_case": scenario.EDGE_CASE,
            "event_count": len(events),
            "span_count": span_count,
            "span_pattern": scenario.SPAN_PATTERN,
            "trace_types": scenario.TRACE_TYPES_EMITTED,
        })

    catalog_path = os.path.join(base_dir, "catalog.json")
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)
        f.write("\n")


def generate_readme(scenarios, base_dir: str) -> None:
    """Generate README.md."""
    total_events = sum(len(s.build_events()) for s in scenarios)
    total_spans = sum(s.EXPECTED_SPAN_COUNT for s in scenarios)

    lines = [
        "# Dify App Trace Reference Catalog",
        "",
        f"A collection of {len(scenarios)} reference Dify app traces, captured as Langfuse wire events.",
        "",
        "## Structure",
        "",
        "Each scenario directory (`NN-slug/`) contains:",
        "- `events.jsonl` — wire events, one per line, in emission order",
        "- `meta.json` — scenario metadata",
        "",
        "Root files:",
        "- `catalog.json` — machine-readable index of all scenarios",
        "- `schema.md` — wire event field reference",
        "",
        "## Wire event format",
        "",
        "Each line in `events.jsonl` is a JSON object:",
        "```json",
        '{"id": "<uuid>", "timestamp": "<ISO8601>", "type": "trace-create|span-create|generation-create", "body": {...}}',
        "```",
        "",
        "See `schema.md` for the full field reference.",
        "",
        "## Scenarios",
        "",
        "| # | Directory | App Type | Events | Spans | Pattern |",
        "|---|---|---|---|---|---|",
    ]

    for s in scenarios:
        events = s.build_events()
        num = s.SCENARIO_ID.split("-")[0]
        lines.append(
            f"| {num} | `{s.SCENARIO_ID}` | {s.APP_TYPE} | {len(events)} | {s.EXPECTED_SPAN_COUNT} | {s.SPAN_PATTERN} |"
        )

    lines.extend([
        "",
        f"**Total**: {total_events} events, {total_spans} spans across {len(scenarios)} scenarios.",
        "",
        "## Provenance",
        "",
        "- Dify commit: `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`",
        "- Langfuse SDK: `>=4.2.0,<5.0.0`",
    ])

    readme_path = os.path.join(base_dir, "README.md")
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def generate_schema_doc(base_dir: str) -> None:
    """Generate schema.md — wire event field reference."""
    content = """# Langfuse Wire Event Schema

## Event Envelope

```json
{
  "id": "<uuid v5>",
  "timestamp": "<ISO 8601 UTC>",
  "type": "trace-create" | "span-create" | "generation-create",
  "body": { ... }
}
```

## trace-create body

```json
{
  "id": "<trace_id>",
  "name": "<string>",
  "userId": "<string>",
  "input": "<any>",
  "output": "<any>",
  "sessionId": "<string>",
  "metadata": "<any>"
}
```

## span-create body

```json
{
  "id": "<span_id>",
  "traceId": "<trace_id>",
  "name": "<string>",
  "startTime": "<ISO 8601>",
  "endTime": "<ISO 8601>",
  "input": "<any>",
  "output": "<any>",
  "metadata": "<any>",
  "level": "DEBUG" | "DEFAULT" | "WARNING" | "ERROR",
  "statusMessage": "<string>",
  "parentObservationId": "<string>"
}
```

## generation-create body

Extends span-create with:

```json
{
  "completionStartTime": "<ISO 8601>",
  "model": "<string>",
  "modelParameters": { "<key>": "<value>" },
  "usageDetails": {
    "input": "<int>",
    "output": "<int>",
    "total": "<int>",
    "unit": "TOKENS",
    "inputCost": "<float>",
    "outputCost": "<float>",
    "totalCost": "<float>"
  }
}
```

## Serialization rules

- **camelCase**: all body field names are camelCase on the wire.
- **exclude_unset + exclude_none**: only fields with non-None values appear.

## Dify trace type to wire event mapping

| Dify TraceInfo type | Wire events | Handler |
|---|---|---|
| MessageTraceInfo | 1 trace-create + 1 generation-create | `LangFuseDataTrace.message_trace` |
| WorkflowTraceInfo | 1 trace-create + 1 span-create + K node events | `LangFuseDataTrace.workflow_trace` |
| ModerationTraceInfo | 1 span-create | `LangFuseDataTrace.moderation_trace` |
| DatasetRetrievalTraceInfo | 1 span-create | `LangFuseDataTrace.dataset_retrieval_trace` |
| ToolTraceInfo | 1 span-create per tool call | `LangFuseDataTrace.tool_trace` |
| GenerateNameTraceInfo | 1 trace-create + 1 span-create | `LangFuseDataTrace.generate_name_trace` |
| SuggestedQuestionTraceInfo | 1 generation-create | `LangFuseDataTrace.suggested_question_trace` |
"""
    schema_path = os.path.join(base_dir, "schema.md")
    with open(schema_path, "w", encoding="utf-8") as f:
        f.write(content)


def main():
    base_dir = _BASE_DIR

    print("Generating Dify trace reference catalog...")
    catalog_entries = []
    for scenario in SCENARIOS:
        entry = generate_scenario(scenario, base_dir)
        catalog_entries.append(entry)
        print(f"  {entry['scenario_id']}: {entry['event_count']} events, {entry['span_count']} spans")

    generate_catalog(SCENARIOS, base_dir)
    print("  catalog.json")
    generate_readme(SCENARIOS, base_dir)
    print("  README.md")
    generate_schema_doc(base_dir)
    print("  schema.md")

    total_events = sum(e["event_count"] for e in catalog_entries)
    total_spans = sum(e["span_count"] for e in catalog_entries)
    print(f"\nTotal: {total_events} events, {total_spans} spans across {len(SCENARIOS)} scenarios")

    # Verify all 7 trace types are represented
    all_types = set()
    for s in SCENARIOS:
        all_types.update(s.TRACE_TYPES_EMITTED)
    expected_types = {
        "MessageTraceInfo", "WorkflowTraceInfo", "ModerationTraceInfo",
        "DatasetRetrievalTraceInfo", "ToolTraceInfo", "GenerateNameTraceInfo",
        "SuggestedQuestionTraceInfo",
    }
    assert all_types == expected_types, f"Missing trace types: {expected_types - all_types}"

    # Verify all 5 Dify app modes are represented
    all_modes = {s.DIFY_APP_MODE for s in SCENARIOS}
    expected_modes = {"chat", "completion", "agent-chat", "workflow", "advanced-chat"}
    assert all_modes == expected_modes, f"Missing app modes: {expected_modes - all_modes}"

    print("All self-checks passed.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run all unit tests to verify everything passes**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/tests/ -v -m "not e2e"
```

Expected: all 42 unit tests pass (13 test_scenarios + 9 test_generate_traceset + 12 test_helpers + 8 test_schema).

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/scenarios/__init__.py traceset/generate_traceset.py traceset/tests/test_generate_traceset.py
git commit -m "feat(infra): update registry + generator for 13 scenarios with EXPECTED_SPAN_COUNT self-check"
```

---

## Task 8: Write `traceset/ingest.py` (pack_batch, post_batch, ingest_all)

**Files:**
- `traceset/ingest.py` (create)

### Steps

- [ ] **Step 1: Write `traceset/ingest.py`**

```python
#!/usr/bin/env python3
"""Ingestion layer: pack events into Langfuse batch payloads and POST via HTTP.

Uses urllib.request (stdlib only — no requests dependency).
"""
from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

from traceset.scenarios import SCENARIOS

_TRACESET_DIR = os.path.dirname(os.path.abspath(__file__))


def pack_batch(events: list[dict]) -> dict:
    """Pack a list of events into a Langfuse ingestion batch payload."""
    return {"batch": events}


def post_batch(
    batch: dict,
    endpoint: str,
    public_key: str,
    secret_key: str,
    max_retries: int = 3,
) -> dict:
    """POST a batch to Langfuse /api/public/ingestion.

    Returns parsed JSON response with _http_status added.
    Raises RuntimeError on unrecoverable failures.
    """
    url = f"{endpoint.rstrip('/')}/api/public/ingestion"
    body = json.dumps(batch).encode("utf-8")
    credentials = base64.b64encode(
        f"{public_key}:{secret_key}".encode()
    ).decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {credentials}",
    }

    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url, data=body, headers=headers, method="POST"
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                status = resp.status
                resp_body = json.loads(resp.read().decode("utf-8"))
                resp_body["_http_status"] = status
                return resp_body
        except urllib.error.HTTPError as e:
            resp_text = e.read().decode("utf-8")
            if e.code == 207:
                resp_body = json.loads(resp_text)
                resp_body["_http_status"] = 207
                return resp_body
            elif 400 <= e.code < 500:
                raise RuntimeError(
                    f"HTTP {e.code} from Langfuse: {resp_text}"
                ) from e
            elif 500 <= e.code < 600:
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                raise RuntimeError(
                    f"HTTP {e.code} after {max_retries} retries: {resp_text}"
                ) from e
        except urllib.error.URLError as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            raise RuntimeError(f"Network error: {e}") from e

    raise RuntimeError(f"Max retries ({max_retries}) exceeded")


def ingest_all(
    scenarios: list,
    endpoint: str,
    public_key: str,
    secret_key: str,
    batch_mode: str = "per-scenario",
) -> dict:
    """Ingest all scenarios. Returns ingestion report dict."""
    report = {
        "scenarios": [],
        "total_successes": 0,
        "total_errors": 0,
        "batch_mode": batch_mode,
    }

    for scenario in scenarios:
        events = scenario.build_events()
        scenario_id = scenario.SCENARIO_ID
        trace_id = events[0]["body"]["id"] if events else None

        if batch_mode == "per-scenario":
            batch = pack_batch(events)
            try:
                result = post_batch(
                    batch, endpoint, public_key, secret_key
                )
                successes = len(result.get("success", []))
                errors = len(result.get("errors", []))
                report["scenarios"].append({
                    "scenario_id": scenario_id,
                    "trace_id": trace_id,
                    "http_status": result.get("_http_status"),
                    "success_count": successes,
                    "error_count": errors,
                    "errors": result.get("errors", []),
                })
                report["total_successes"] += successes
                report["total_errors"] += errors
            except RuntimeError as e:
                report["scenarios"].append({
                    "scenario_id": scenario_id,
                    "trace_id": trace_id,
                    "http_status": None,
                    "success_count": 0,
                    "error_count": len(events),
                    "errors": [{"message": str(e)}],
                })
                report["total_errors"] += len(events)

        elif batch_mode == "per-event":
            for i, event in enumerate(events):
                batch = pack_batch([event])
                try:
                    result = post_batch(
                        batch, endpoint, public_key, secret_key
                    )
                    report["total_successes"] += len(
                        result.get("success", [])
                    )
                    report["total_errors"] += len(
                        result.get("errors", [])
                    )
                except RuntimeError as e:
                    report["total_errors"] += 1
        else:
            raise ValueError(f"Unknown batch_mode: {batch_mode}")

    return report


def write_ingestion_report(report: dict, path: str | None = None) -> str:
    """Write ingestion report to JSON file. Returns path."""
    if path is None:
        path = os.path.join(_TRACESET_DIR, "ingestion_report.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path


def main():
    from traceset.pipeline import load_config, ensure_langfuse

    config = load_config()
    ensure_langfuse(config)

    print("Ingesting scenarios into Langfuse...")
    report = ingest_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )

    report_path = write_ingestion_report(report)
    print(f"  Ingestion report: {report_path}")
    print(
        f"  Total: {report['total_successes']} successes, "
        f"{report['total_errors']} errors"
    )

    if report["total_errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify ingest.py imports correctly**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -c "from traceset.ingest import pack_batch, post_batch, ingest_all; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/ingest.py
git commit -m "feat(ingest): add ingest.py with pack_batch, post_batch, ingest_all (urllib, stdlib only)"
```

---

## Task 9: Write `traceset/validate.py` (query API, ~44 assertions/scenario)

**Files:**
- `traceset/validate.py` (create)

### Steps

- [ ] **Step 1: Write `traceset/validate.py`**

```python
#!/usr/bin/env python3
"""Validation layer: query Langfuse API and assert field-level correctness.

Uses urllib.request (stdlib only). Runs ~44 assertions per scenario.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

from traceset.scenarios import SCENARIOS

_TRACESET_DIR = os.path.dirname(os.path.abspath(__file__))


def _api_get(url: str, public_key: str, secret_key: str) -> dict:
    """GET request with Basic auth. Returns parsed JSON."""
    credentials = base64.b64encode(
        f"{public_key}:{secret_key}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {credentials}"}
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def query_trace(
    trace_id: str, endpoint: str, public_key: str, secret_key: str
) -> dict:
    """GET /api/public/traces/{traceId}."""
    url = f"{endpoint.rstrip('/')}/api/public/traces/{trace_id}"
    return _api_get(url, public_key, secret_key)


def query_observations(
    trace_id: str, endpoint: str, public_key: str, secret_key: str
) -> list[dict]:
    """GET /api/public/observations?traceId={traceId}&limit=100."""
    url = (
        f"{endpoint.rstrip('/')}/api/public/observations"
        f"?traceId={trace_id}&limit=100"
    )
    result = _api_get(url, public_key, secret_key)
    if isinstance(result, dict) and "data" in result:
        return result["data"]
    if isinstance(result, list):
        return result
    return []


def wait_for_indexing(
    trace_id: str,
    expected_span_count: int,
    endpoint: str,
    public_key: str,
    secret_key: str,
    timeout: int = 30,
) -> None:
    """Poll until trace appears and observation count matches."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            trace = query_trace(trace_id, endpoint, public_key, secret_key)
            if trace and trace.get("id"):
                observations = query_observations(
                    trace_id, endpoint, public_key, secret_key
                )
                if len(observations) >= expected_span_count:
                    return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(
        f"Trace {trace_id} did not index within {timeout}s "
        f"(expected {expected_span_count} observations)"
    )


def _assert_eq(name: str, actual, expected) -> dict:
    """Compare actual vs expected. Returns assertion result dict."""
    passed = actual == expected
    result = {
        "name": name,
        "passed": passed,
    }
    if not passed:
        result["actual"] = actual
        result["expected"] = expected
    return result


def validate_scenario(
    scenario,
    endpoint: str,
    public_key: str,
    secret_key: str,
) -> dict:
    """Run ~44 assertions for one scenario. Returns validation result dict."""
    events = scenario.build_events()
    meta = scenario.build_meta()

    trace_events = [e for e in events if e["type"] == "trace-create"]
    span_events = [
        e for e in events if e["type"] in ("span-create", "generation-create")
    ]

    trace_id = trace_events[0]["body"]["id"] if trace_events else None
    if not trace_id:
        return {
            "scenario_id": scenario.SCENARIO_ID,
            "assertions": [{"name": "trace_id", "passed": False}],
            "pass_count": 0,
            "fail_count": 1,
        }

    try:
        wait_for_indexing(
            trace_id,
            scenario.EXPECTED_SPAN_COUNT,
            endpoint,
            public_key,
            secret_key,
        )
    except (TimeoutError, Exception) as e:
        return {
            "scenario_id": scenario.SCENARIO_ID,
            "assertions": [
                {"name": "indexing", "passed": False, "error": str(e)}
            ],
            "pass_count": 0,
            "fail_count": 1,
        }

    trace = query_trace(trace_id, endpoint, public_key, secret_key)
    observations = query_observations(
        trace_id, endpoint, public_key, secret_key
    )

    assertions = []

    expected_trace_body = trace_events[0]["body"]
    assertions.append(_assert_eq("trace.id", trace.get("id"), expected_trace_body["id"]))
    assertions.append(_assert_eq("trace.name", trace.get("name"), expected_trace_body["name"]))
    assertions.append(_assert_eq("trace.userId", trace.get("userId"), expected_trace_body.get("userId")))
    assertions.append(_assert_eq("trace.input", trace.get("input"), expected_trace_body.get("input")))
    assertions.append(_assert_eq("trace.metadata", trace.get("metadata"), expected_trace_body.get("metadata")))

    obs_by_id = {o["id"]: o for o in observations}

    for event in span_events:
        body = event["body"]
        obs_id = body["id"]
        obs = obs_by_id.get(obs_id)

        if event["type"] == "generation-create":
            expected_type = "GENERATION"
        else:
            expected_type = "SPAN"

        actual_type = obs.get("type") if obs else None
        assertions.append(_assert_eq(
            f"obs.{obs_id}.type", actual_type, expected_type
        ))
        assertions.append(_assert_eq(
            f"obs.{obs_id}.input",
            obs.get("input") if obs else None,
            body.get("input"),
        ))
        assertions.append(_assert_eq(
            f"obs.{obs_id}.output",
            obs.get("output") if obs else None,
            body.get("output"),
        ))

        if event["type"] == "generation-create":
            assertions.append(_assert_eq(
                f"obs.{obs_id}.model",
                obs.get("model") if obs else None,
                body.get("model"),
            ))
            assertions.append(_assert_eq(
                f"obs.{obs_id}.usageDetails",
                obs.get("usageDetails") if obs else None,
                body.get("usageDetails"),
            ))
        else:
            assertions.append(_assert_eq(
                f"obs.{obs_id}.parentObservationId",
                obs.get("parentObservationId") if obs else None,
                body.get("parentObservationId"),
            ))

    assertions.append(_assert_eq(
        "obs.count", len(observations), scenario.EXPECTED_SPAN_COUNT
    ))

    obs_ids = {o["id"] for o in observations}
    orphan_count = sum(
        1 for o in observations
        if o.get("parentObservationId")
        and o["parentObservationId"] not in obs_ids
    )
    assertions.append(_assert_eq("obs.orphans", orphan_count, 0))

    obs_times = [o.get("startTime", "") for o in observations]
    is_monotonic = obs_times == sorted(obs_times)
    assertions.append(_assert_eq("obs.timestamps_monotonic", is_monotonic, True))

    pass_count = sum(1 for a in assertions if a["passed"])
    fail_count = sum(1 for a in assertions if not a["passed"])

    return {
        "scenario_id": scenario.SCENARIO_ID,
        "assertions": assertions,
        "assertion_count": len(assertions),
        "pass_count": pass_count,
        "fail_count": fail_count,
    }


def validate_all(
    scenarios: list,
    endpoint: str,
    public_key: str,
    secret_key: str,
) -> dict:
    """Validate all scenarios. Returns validation report dict."""
    results = []
    for scenario in scenarios:
        result = validate_scenario(scenario, endpoint, public_key, secret_key)
        results.append(result)
        status = "PASS" if result["fail_count"] == 0 else "FAIL"
        print(
            f"  {status} {scenario.SCENARIO_ID}: "
            f"{result['pass_count']}/{result['assertion_count']} assertions"
        )

    total_assertions = sum(r["assertion_count"] for r in results)
    total_pass = sum(r["pass_count"] for r in results)
    total_fail = sum(r["fail_count"] for r in results)

    return {
        "scenarios": results,
        "total_assertions": total_assertions,
        "total_pass": total_pass,
        "total_fail": total_fail,
    }


def write_validation_report(report: dict, path: str | None = None) -> str:
    """Write validation report to JSON file."""
    if path is None:
        path = os.path.join(_TRACESET_DIR, "validation_report.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path


def main():
    from traceset.pipeline import load_config, ensure_langfuse

    config = load_config()
    ensure_langfuse(config)

    print("Validating scenarios against Langfuse...")
    report = validate_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )

    report_path = write_validation_report(report)
    print(f"\n  Validation report: {report_path}")

    print(f"\n{'Scenario':<30} {'Assertions':>12} {'Pass':>8} {'Fail':>8}")
    print("-" * 62)
    for s in report["scenarios"]:
        print(
            f"{s['scenario_id']:<30} {s['assertion_count']:>12} "
            f"{s['pass_count']:>8} {s['fail_count']:>8}"
        )
    print("-" * 62)
    print(
        f"{'Total':<30} {report['total_assertions']:>12} "
        f"{report['total_pass']:>8} {report['total_fail']:>8}"
    )
    if report["total_fail"] == 0:
        print("  ALL PASS")

    if report["total_fail"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify validate.py imports correctly**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -c "from traceset.validate import validate_scenario, validate_all, wait_for_indexing; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/validate.py
git commit -m "feat(validate): add validate.py with query_trace, query_observations, ~44 assertions/scenario"
```

---

## Task 10: Write `traceset/pipeline.py` (config, health check, 4-stage orchestration)

**Files:**
- `traceset/pipeline.py` (create)

### Steps

- [ ] **Step 1: Write `traceset/pipeline.py`**

```python
#!/usr/bin/env python3
"""Pipeline orchestration: generate -> pack -> ingest -> validate.

Loads config from ../difyapp3/.env, health-checks Langfuse, auto-starts
Docker if needed, and orchestrates the 4-stage pipeline.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

from traceset.scenarios import SCENARIOS

_TRACESET_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_TRACESET_DIR)
_PARENT_DIR = os.path.dirname(_REPO_ROOT)
_DIFYAPP3_DIR = os.path.join(_PARENT_DIR, "difyapp3")
_ENV_PATH = os.path.join(_DIFYAPP3_DIR, ".env")


def load_config() -> dict:
    """Load Langfuse config from ../difyapp3/.env.

    Replaces host.docker.internal with localhost for host-side access.
    """
    config = {}
    with open(_ENV_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                key, _, value = line.partition("=")
                config[key.strip()] = value.strip()

    host = config.get("LANGFUSE_HOST", "http://localhost:3000")
    host = host.replace("host.docker.internal", "localhost")

    return {
        "langfuse_host": host,
        "langfuse_public_key": config.get("LANGFUSE_PUBLIC_KEY"),
        "langfuse_secret_key": config.get("LANGFUSE_SECRET_KEY"),
    }


def check_health(endpoint: str) -> bool:
    """Check if Langfuse is healthy via GET /api/public/health."""
    url = f"{endpoint.rstrip('/')}/api/public/health"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception:
        return False


def ensure_langfuse(config: dict) -> None:
    """Ensure Langfuse is running. Auto-start Docker if needed."""
    endpoint = config["langfuse_host"]

    if check_health(endpoint):
        return

    print("Langfuse not healthy. Attempting to start Docker...")
    try:
        subprocess.run(
            [
                "docker", "compose",
                "-f", "docker-compose.yaml",
                "-f", "docker-compose.override.yml",
                "up", "-d",
            ],
            cwd=_DIFYAPP3_DIR,
            check=True,
            capture_output=True,
            timeout=60,
        )
    except FileNotFoundError:
        print(
            "Docker is not installed. Please install Docker or start Langfuse manually.",
            file=sys.stderr,
        )
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(
            f"Failed to start Docker: {e.stderr.decode() if e.stderr else e}",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Waiting for Langfuse to become healthy...")
    deadline = time.time() + 120
    while time.time() < deadline:
        if check_health(endpoint):
            print("Langfuse is healthy.")
            return
        time.sleep(2)

    print("Langfuse did not become healthy within 120s.", file=sys.stderr)
    sys.exit(1)


def clean_traces(config: dict) -> None:
    """Delete all scenario traces from Langfuse before ingesting."""
    endpoint = config["langfuse_host"]
    public_key = config["langfuse_public_key"]
    secret_key = config["langfuse_secret_key"]
    credentials = base64.b64encode(
        f"{public_key}:{secret_key}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {credentials}"}

    for scenario in SCENARIOS:
        events = scenario.build_events()
        trace_id = events[0]["body"]["id"]
        url = f"{endpoint.rstrip('/')}/api/public/traces/{trace_id}"
        req = urllib.request.Request(url, headers=headers, method="DELETE")
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            pass

    print(f"Cleaned {len(SCENARIOS)} traces.")


def run_generate() -> None:
    """Stage 1: Generate events.jsonl and meta.json for all scenarios."""
    from traceset.generate_traceset import main as gen_main

    print("\n=== Stage 1: Generate ===")
    gen_main()


def run_ingest(config: dict) -> dict:
    """Stage 2+3: Pack and ingest all scenarios."""
    from traceset.ingest import ingest_all, write_ingestion_report

    print("\n=== Stage 2+3: Pack + Ingest ===")
    report = ingest_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    write_ingestion_report(report)
    print(
        f"  Ingested: {report['total_successes']} successes, "
        f"{report['total_errors']} errors"
    )
    return report


def run_validate(config: dict) -> dict:
    """Stage 4: Validate all scenarios against Langfuse."""
    from traceset.validate import validate_all, write_validation_report

    print("\n=== Stage 4: Validate ===")
    report = validate_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    write_validation_report(report)
    return report


def main():
    parser = argparse.ArgumentParser(
        description="Traceset v2 E2E Pipeline: generate -> ingest -> validate"
    )
    parser.add_argument(
        "--stage",
        choices=["generate", "ingest", "validate", "all"],
        default="all",
        help="Which stage to run (default: all)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing traces before ingesting",
    )
    args = parser.parse_args()

    config = load_config()
    ensure_langfuse(config)

    if args.clean:
        clean_traces(config)

    if args.stage in ("generate", "all"):
        run_generate()

    if args.stage in ("ingest", "all"):
        ingest_report = run_ingest(config)
        if ingest_report["total_errors"] > 0:
            print(
                f"WARNING: {ingest_report['total_errors']} ingestion errors",
                file=sys.stderr,
            )

    if args.stage in ("validate", "all"):
        val_report = run_validate(config)
        print(f"\nTotal: {val_report['total_pass']} pass, "
              f"{val_report['total_fail']} fail")
        if val_report["total_fail"] > 0:
            sys.exit(1)

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify pipeline.py imports correctly**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -c "from traceset.pipeline import load_config, check_health, ensure_langfuse, main; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/pipeline.py
git commit -m "feat(pipeline): add pipeline.py with config loading, health check, Docker auto-start, 4-stage orchestration"
```

---

## Task 11: Write `traceset/tests/test_e2e.py` (fixture, full pipeline, parametrized)

**Files:**
- `traceset/tests/test_e2e.py` (create)

### Steps

- [ ] **Step 1: Write `traceset/tests/test_e2e.py`**

```python
"""E2E tests against real Langfuse. Auto-starts Docker if needed.

Run with: python3 -m pytest traceset/tests/test_e2e.py -v -m e2e
Skip with: python3 -m pytest traceset/ -v -m "not e2e"
"""
import pytest

from traceset.scenarios import SCENARIOS
from traceset.ingest import pack_batch, post_batch, ingest_all
from traceset.validate import wait_for_indexing, validate_scenario, validate_all


@pytest.mark.e2e
def test_e2e_full_pipeline(ensure_langfuse_running):
    """Run the complete pipeline: generate -> ingest -> validate.

    All ~570 assertions across 13 scenarios must pass.
    """
    config = ensure_langfuse_running

    # 1. Generate
    from traceset.generate_traceset import main as gen_main
    gen_main()

    # 2. Ingest all scenarios
    report = ingest_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    assert report["total_errors"] == 0, (
        f"Ingestion had {report['total_errors']} errors"
    )

    # 3. Validate all scenarios
    val_report = validate_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    assert val_report["total_fail"] == 0, (
        f"Validation had {val_report['total_fail']} failures out of "
        f"{val_report['total_assertions']} assertions"
    )


@pytest.mark.e2e
@pytest.mark.parametrize(
    "scenario",
    SCENARIOS,
    ids=[s.SCENARIO_ID for s in SCENARIOS],
)
def test_e2e_scenario(scenario, ensure_langfuse_running):
    """Per-scenario e2e: ingest one scenario's events, then validate.

    Each scenario must pass all its assertions (~44 per scenario).
    """
    config = ensure_langfuse_running

    # 1. Build events
    events = scenario.build_events()
    assert len(events) == scenario.EXPECTED_EVENT_COUNT

    # 2. Pack and POST
    batch = pack_batch(events)
    result = post_batch(
        batch,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    assert result.get("_http_status") in (200, 207), (
        f"HTTP status {result.get('_http_status')} for {scenario.SCENARIO_ID}"
    )

    # 3. Wait for indexing
    trace_id = None
    for e in events:
        if e["type"] == "trace-create":
            trace_id = e["body"]["id"]
            break
    assert trace_id is not None

    wait_for_indexing(
        trace_id,
        scenario.EXPECTED_SPAN_COUNT,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )

    # 4. Validate
    val_result = validate_scenario(
        scenario,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    assert val_result["fail_count"] == 0, (
        f"Scenario {scenario.SCENARIO_ID} had {val_result['fail_count']} "
        f"validation failures:\n"
        + "\n".join(
            f"  FAIL: {a['name']}" for a in val_result["assertions"] if not a["passed"]
        )
    )
```

- [ ] **Step 2: Verify test_e2e.py is discovered by pytest**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/tests/test_e2e.py --collect-only -m e2e 2>&1 | tail -20
```

Expected: 14 e2e tests collected (1 full pipeline + 13 parametrized).

- [ ] **Step 3: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/tests/test_e2e.py
git commit -m "feat(tests): add test_e2e.py with full pipeline test + 13 parametrized per-scenario tests"
```

---

## Task 12: Makefile + Full E2E Verification

**Files:**
- `traceset/Makefile` (create — at repo root or traceset/)

### Steps

- [ ] **Step 1: Write `traceset/Makefile`**

```makefile
.PHONY: e2e generate ingest validate health test test-unit clean

e2e:
	python3 -m traceset.pipeline

generate:
	python3 -m traceset.generate_traceset

ingest:
	python3 -m traceset.ingest

validate:
	python3 -m traceset.validate

health:
	curl -s http://localhost:3000/api/public/health

test:
	python3 -m pytest traceset/ -v

test-unit:
	python3 -m pytest traceset/ -v -m "not e2e"

clean:
	python3 -m traceset.pipeline --clean
```

- [ ] **Step 2: Run all unit tests (no Langfuse needed)**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/ -v -m "not e2e"
```

Expected output:
```
traceset/tests/test_helpers.py::test_make_event_id_deterministic PASSED
traceset/tests/test_helpers.py::test_make_event_id_unique PASSED
... (12 helper tests)
traceset/tests/test_schema.py::test_validate_trace_create PASSED
... (8 schema tests)
traceset/tests/test_scenarios.py::test_s01_linear_llm_chain PASSED
... (13 scenario tests)
traceset/tests/test_generate_traceset.py::test_scenarios_registry_has_13 PASSED
... (9 generator tests)

Total: 42 unit tests PASSED
```

- [ ] **Step 3: Run the full pipeline end-to-end**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m traceset.pipeline --clean
```

Expected output:
```
Generating Dify trace reference catalog...
  01-linear-llm-chain: 14 events, 13 spans
  02-parallel-branches: 12 events, 11 spans
  ...
  13-completion-multi-feature: 12 events, 10 spans
  catalog.json
  README.md
  schema.md

Total: 159 events, 140 spans across 13 scenarios
All self-checks passed.

=== Stage 2+3: Pack + Ingest ===
  Ingested: 159 successes, 0 errors

=== Stage 4: Validate ===
  PASS 01-linear-llm-chain: 47/47 assertions
  PASS 02-parallel-branches: 41/41 assertions
  ...
  PASS 13-completion-multi-feature: 38/38 assertions

Total: 570 pass, 0 fail
  ALL PASS

Pipeline complete.
```

- [ ] **Step 4: Run ALL tests including e2e**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
python3 -m pytest traceset/ -v
```

Expected output: 56 tests total (42 unit + 14 e2e), all PASSED.

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive
git add traceset/Makefile
git commit -m "feat: add Makefile + full e2e verification (42 unit + 14 e2e = 56 tests all pass)"
```

---

## Self-Review

### Spec Coverage

| Spec Requirement | Plan Coverage |
|---|---|
| 13 multi-span scenarios (10+ spans each) | Tasks 2-6: all 13 scenarios with complete code |
| 4-stage pipeline (Generate -> Pack -> Ingest -> Validate) | Tasks 7-10: generate_traceset.py, ingest.py, validate.py, pipeline.py |
| Reuse helpers.py and schema.py | No changes to these files — reused as-is |
| EXPECTED_SPAN_COUNT + SPAN_PATTERN constants | All 13 scenario modules include both |
| 7th self-check (span count) in generate_traceset.py | Task 7: self-check #7 added |
| 2 catalog-wide coverage assertions (7 trace types, 5 app modes) | Task 7: main() asserts both |
| Raw HTTP POST via urllib.request (no requests) | Task 8: ingest.py uses urllib.request only |
| ~44 assertions per scenario | Task 9: validate.py runs 5 trace-level + 3-5 per observation + 3 cross-cutting |
| E2E tests with auto-start Docker fixture | Task 11: test_e2e.py + conftest.py fixture |
| ../difyapp3 .env config loading + host.docker.internal substitution | Task 10: pipeline.py load_config() |
| Health check + auto-start Docker (120s timeout) | Task 10: ensure_langfuse() |
| --stage flag + --clean flag | Task 10: pipeline.py argparse |
| Makefile with e2e/generate/ingest/validate/health/test targets | Task 12: Makefile |

### Scenario Coverage

| # | Scenario | App Mode | Events | Spans | Trace Types |
|---|---|---|---|---|---|
| 01 | linear-llm-chain | workflow | 14 | 13 | WorkflowTraceInfo |
| 02 | parallel-branches | workflow | 12 | 11 | WorkflowTraceInfo |
| 03 | agent-react-loop | agent-chat | 12 | 11 | Message, Tool |
| 04 | multi-tool-chain | agent-chat | 12 | 10 | Message, Tool, GenerateName |
| 05 | rag-multi-hop | chat | 12 | 10 | Message, DatasetRetrieval, SuggestedQuestion, GenerateName |
| 06 | moderation-rag-tool-combo | chat | 11 | 10 | Message, Moderation, DatasetRetrieval, Tool, SuggestedQuestion, GenerateName |
| 07 | workflow-conditional | workflow | 12 | 11 | WorkflowTraceInfo |
| 08 | error-recovery-agent | agent-chat | 12 | 10 | Message, Tool, DatasetRetrieval, SuggestedQuestion, GenerateName |
| 09 | nested-workflow | advanced-chat | 14 | 12 | Message, Workflow, GenerateName |
| 10 | workflow-error-propagation | workflow | 11 | 10 | WorkflowTraceInfo |
| 11 | streaming-chatflow | advanced-chat | 13 | 11 | Message, Workflow, SuggestedQuestion, GenerateName |
| 12 | multi-model-pipeline | workflow | 12 | 11 | WorkflowTraceInfo |
| 13 | completion-multi-feature | completion | 12 | 10 | Message, Moderation, DatasetRetrieval, Tool, SuggestedQuestion, GenerateName |

**Totals**: 159 events, 140 spans, 13 scenarios.

**Trace type coverage**: All 7 represented (Message, Workflow, Moderation, DatasetRetrieval, Tool, GenerateName, SuggestedQuestion).

**App mode coverage**: All 5 represented (workflow: 5, agent-chat: 3, chat: 2, advanced-chat: 2, completion: 1).

### Placeholder Scan

- No "TBD", "TODO", "implement later", or "similar to Task N" in any code block.
- Every scenario module has complete `build_events()` and `build_meta()` functions with all events constructed.
- Every test function has complete assertion code.
- Every new module (ingest.py, validate.py, pipeline.py, test_e2e.py) has complete implementations.

### Type Consistency

- All 13 scenario modules export: `SCENARIO_ID`, `SCENARIO_DESCRIPTION`, `APP_TYPE`, `DIFY_APP_MODE`, `EDGE_CASE`, `TRACE_TYPES_EMITTED`, `EXPECTED_EVENT_COUNT`, `EXPECTED_SPAN_COUNT`, `SPAN_PATTERN`, `build_events()`, `build_meta()`.
- `build_events()` returns `list[dict]` where each dict is `{id, timestamp, type, body}`.
- `build_meta()` returns `dict` with `events_in_order` list matching events.
- All body keys are camelCase (enforced by helpers + self-checks).
- All envelope timestamps are monotonically non-decreasing (enforced by self-checks).
- All `EXPECTED_SPAN_COUNT` values equal span-create + generation-create count.

