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
