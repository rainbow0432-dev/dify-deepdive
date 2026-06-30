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
