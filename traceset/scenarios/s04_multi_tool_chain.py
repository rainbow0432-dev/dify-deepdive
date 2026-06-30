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
