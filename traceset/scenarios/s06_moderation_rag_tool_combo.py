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
