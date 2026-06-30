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
