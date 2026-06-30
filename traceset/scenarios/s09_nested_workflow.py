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
