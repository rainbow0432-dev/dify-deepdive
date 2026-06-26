# Dify App Trace Reference Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible Python package that generates 14 reference Dify app trace scenarios (88 wire events total) as Langfuse ingestion events, with schema validation and self-checks.

**Architecture:** A `traceset/` Python package containing scenario modules (one per Dify app type/edge case), event construction helpers (deterministic UUIDs, timestamps, camelCase body dicts), a wire schema validator (langfuse SDK with local fallback), and a generation script that validates each event, writes `events.jsonl` + `meta.json` per scenario, and produces root catalog/README/schema files.

**Tech Stack:** Python 3.10+, langfuse SDK >=4.2.0,<5.0.0 (optional, for schema validation), pytest, json (stdlib), uuid (stdlib), datetime (stdlib).

---

## File Structure

```
traceset/
├── pyproject.toml                        # Project config: deps (langfuse, pytest), pytest settings
├── __init__.py                           # Package marker (empty)
├── conftest.py                           # Adds project root to sys.path for test imports
├── schema.py                             # Wire schema validator: langfuse SDK first, local fallback
├── helpers.py                            # Event construction: UUIDs, timestamps, body dicts, wrap_event
├── generate_traceset.py                  # Main script: validate, write files, self-checks, root docs
├── scenarios/
│   ├── __init__.py                       # Registry: imports all 14 modules, exposes SCENARIOS list
│   ├── s01_chat_basic.py                 # Chatbot: Message + GenerateName (4 events)
│   ├── s02_chat_rag.py                   # Chatbot+RAG: Message+DatasetRetrieval+SuggQ+GenName (6)
│   ├── s03_completion_basic.py           # Completion: Message + GenerateName (4 events)
│   ├── s04_agent_single_tool.py          # Agent: Message + Tool×1 + GenerateName (5 events)
│   ├── s05_agent_multi_tool.py           # Agent: Message + Tool×3 + GenerateName (7 events)
│   ├── s06_workflow_5node.py             # Workflow: 5 nodes, 2 LLM (7 events)
│   ├── s07_workflow_15node.py            # Workflow: 15 nodes, 3 tool, 4 LLM (17 events, edge: high-N)
│   ├── s08_chatflow_basic.py             # Chatflow: Workflow + Message + GenerateName (11 events)
│   ├── s09_moderation_blocked.py         # Chatbot+Moderation blocked (3 events, edge: blocked)
│   ├── s10_moderation_pass_through.py    # Chatbot+Moderation pass (5 events)
│   ├── s11_rag_empty_results.py          # Chatbot+RAG empty (5 events, edge: empty)
│   ├── s12_tool_failure.py               # Agent tool error (5 events, edge: error)
│   ├── s13_suggested_questions_error.py  # SuggQ error (5 events, edge: error)
│   └── s14_message_streaming.py          # Streaming chat (4 events, edge: streaming)
├── tests/
│   ├── __init__.py                       # Package marker (empty)
│   ├── test_schema.py                    # Validator tests: valid/invalid events, snake_case rejection
│   ├── test_helpers.py                   # Helper tests: determinism, camelCase, body construction
│   ├── test_scenarios.py                 # All 14 scenario tests: count, types, camelCase, monotonic
│   └── test_generate_traceset.py         # Generation script tests: file output, self-checks
├── catalog.json                          # (generated) Machine-readable index of all 14 scenarios
├── README.md                             # (generated) Catalog overview, how to read
├── schema.md                             # (generated) Wire event field reference
├── 01-chat-basic/                        # (generated) Scenario output directory
│   ├── events.jsonl                      # Wire events, one per line, timestamp-ascending
│   └── meta.json                         # Scenario metadata
└── ...                                   # (generated) 13 more scenario directories
```

**Responsibilities:**

| File | Responsibility |
|---|---|
| `pyproject.toml` | Declares `langfuse>=4.2.0,<5.0.0` and `pytest` as dev deps; configures pytest `testpaths` |
| `conftest.py` | Inserts project root onto `sys.path` so `from traceset.xxx import ...` works without `pip install` |
| `schema.py` | `validate_event(event)`: tries langfuse SDK Pydantic models, falls back to local validator checking camelCase keys + required fields |
| `helpers.py` | `make_event_id(seed)`, `make_timestamp(base, offset)`, `make_trace_create(...)`, `make_span_create(...)`, `make_generation_create(...)`, `wrap_event(...)`, `to_camel_case(snake)` |
| `generate_traceset.py` | `main()`: iterates SCENARIOS, validates events, writes `events.jsonl`+`meta.json` per scenario, writes `catalog.json`+`README.md`+`schema.md`, runs self-checks |
| `scenarios/__init__.py` | Imports all 14 scenario modules, exposes `SCENARIOS` list of module objects |
| `scenarios/sNN_*.py` | Each defines `SCENARIO_ID`, `SCENARIO_DESCRIPTION`, `APP_TYPE`, `DIFY_APP_MODE`, `EDGE_CASE`, `TRACE_TYPES_EMITTED`, `EXPECTED_EVENT_COUNT`, `build_events()`, `build_meta()` |
| `tests/test_*.py` | TDD tests for schema, helpers, scenarios, generation script |

---

## Task 1: Project Setup

**Files:**
- Create: `traceset/pyproject.toml`
- Create: `traceset/__init__.py`
- Create: `traceset/conftest.py`
- Create: `traceset/tests/__init__.py`
- Create: `traceset/scenarios/__init__.py` (placeholder, populated in Task 13)

- [ ] **Step 1: Create project config and package markers**

Create `traceset/pyproject.toml`:
```toml
[project]
name = "dify-trace-catalog"
version = "0.1.0"
description = "Reference catalog of Dify app traces as Langfuse wire events"
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
```

Create `traceset/__init__.py`:
```python
"""Dify App Trace Reference Catalog."""
```

Create `traceset/conftest.py`:
```python
"""Add project root to sys.path so `from traceset.xxx import ...` works."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

Create `traceset/tests/__init__.py`:
```python
```

Create `traceset/scenarios/__init__.py` (placeholder — populated in Task 13):
```python
"""Scenario registry. Populated in Task 13."""
SCENARIOS = []
```

- [ ] **Step 2: Install dependencies**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && pip install -e ".[dev]"
```
Expected: Successfully installed langfuse-X.Y.Z, pytest-X.Y.Z (or "already satisfied")

- [ ] **Step 3: Verify pytest discovers the (empty) test suite**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest --collect-only
```
Expected: "no tests ran" or "collected 0 items" (no errors)

- [ ] **Step 4: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 1: project setup — pyproject.toml, package markers, conftest"
```

---

## Task 2: Wire Schema Validator

**Files:**
- Create: `traceset/schema.py`
- Create: `traceset/tests/test_schema.py`

- [ ] **Step 1: Write the failing test**

Create `traceset/tests/test_schema.py`:
```python
"""Tests for the wire schema validator."""
import pytest
from traceset.schema import validate_event


def _valid_trace_create():
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "timestamp": "2025-01-15T10:30:00.000000+00:00",
        "type": "trace-create",
        "body": {"id": "trace-1", "name": "test"},
    }


def _valid_span_create():
    return {
        "id": "00000000-0000-0000-0000-000000000002",
        "timestamp": "2025-01-15T10:30:00.000000+00:00",
        "type": "span-create",
        "body": {
            "id": "span-1",
            "traceId": "trace-1",
            "name": "test-span",
            "startTime": "2025-01-15T10:30:00.000000+00:00",
            "endTime": "2025-01-15T10:30:01.000000+00:00",
        },
    }


def _valid_generation_create():
    return {
        "id": "00000000-0000-0000-0000-000000000003",
        "timestamp": "2025-01-15T10:30:00.000000+00:00",
        "type": "generation-create",
        "body": {
            "id": "gen-1",
            "traceId": "trace-1",
            "name": "gpt-4o-mini",
            "startTime": "2025-01-15T10:30:00.000000+00:00",
            "endTime": "2025-01-15T10:30:01.000000+00:00",
            "model": "gpt-4o-mini",
            "usageDetails": {
                "input": 10,
                "output": 20,
                "total": 30,
                "unit": "TOKENS",
            },
        },
    }


def test_validate_trace_create():
    validate_event(_valid_trace_create())


def test_validate_span_create():
    validate_event(_valid_span_create())


def test_validate_generation_create():
    validate_event(_valid_generation_create())


def test_rejects_snake_case_body_key():
    event = _valid_span_create()
    event["body"]["status_message"] = "error"
    with pytest.raises(ValueError, match="snake_case"):
        validate_event(event)


def test_rejects_invalid_type():
    event = _valid_trace_create()
    event["type"] = "invalid-type"
    with pytest.raises(ValueError, match="invalid event type"):
        validate_event(event)


def test_rejects_missing_envelope_field():
    event = _valid_trace_create()
    del event["timestamp"]
    with pytest.raises(ValueError, match="missing envelope field"):
        validate_event(event)


def test_rejects_missing_required_body_field():
    event = _valid_trace_create()
    del event["body"]["name"]
    with pytest.raises(ValueError, match="missing required field"):
        validate_event(event)


def test_rejects_non_dict_body():
    event = _valid_trace_create()
    event["body"] = "not a dict"
    with pytest.raises(ValueError, match="body must be a dict"):
        validate_event(event)
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_schema.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'traceset.schema'`

- [ ] **Step 3: Write minimal implementation**

Create `traceset/schema.py`:
```python
"""Wire schema validator for Langfuse ingestion events.

Tries the langfuse SDK's Pydantic models first (Option A).
Falls back to a local validator (Option B) if the SDK is unavailable.
"""
from __future__ import annotations

_ENVELOPE_REQUIRED = {"id", "timestamp", "type", "body"}
_VALID_TYPES = {"trace-create", "span-create", "generation-create"}

_TRACE_BODY_REQUIRED = {"id", "name"}
_SPAN_BODY_REQUIRED = {"id", "traceId", "name", "startTime", "endTime"}
_GEN_BODY_REQUIRED = {
    "id", "traceId", "name", "startTime", "endTime", "model", "usageDetails",
}

_REQ_MAP = {
    "trace-create": _TRACE_BODY_REQUIRED,
    "span-create": _SPAN_BODY_REQUIRED,
    "generation-create": _GEN_BODY_REQUIRED,
}


def validate_event(event: dict) -> None:
    """Validate an event dict against the Langfuse wire schema.

    Raises ValueError on any schema violation.
    """
    try:
        _validate_with_sdk(event)
        return
    except ImportError:
        pass
    _local_validate(event)


def _validate_with_sdk(event: dict) -> None:
    """Validate using langfuse SDK Pydantic models. Raises ImportError if SDK absent."""
    from langfuse.api import (
        IngestionEvent_TraceCreate,
        IngestionEvent_SpanCreate,
        IngestionEvent_GenerationCreate,
    )

    etype = event.get("type")
    if etype == "trace-create":
        IngestionEvent_TraceCreate.model_validate(event)
    elif etype == "span-create":
        IngestionEvent_SpanCreate.model_validate(event)
    elif etype == "generation-create":
        IngestionEvent_GenerationCreate.model_validate(event)
    else:
        raise ValueError(f"invalid event type: {etype}")


def _local_validate(event: dict) -> None:
    """Fallback local validator: checks envelope, type, required fields, camelCase."""
    for field in _ENVELOPE_REQUIRED:
        if field not in event:
            raise ValueError(f"missing envelope field: {field}")

    etype = event["type"]
    if etype not in _VALID_TYPES:
        raise ValueError(f"invalid event type: {etype}")

    body = event["body"]
    if not isinstance(body, dict):
        raise ValueError("body must be a dict")

    required = _REQ_MAP[etype]
    for field in required:
        if field not in body:
            raise ValueError(f"{etype} body missing required field: {field}")

    for key in body:
        if "_" in key:
            raise ValueError(f"snake_case key in {etype} body: {key}")
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_schema.py -v
```
Expected: PASS — 8 tests passed

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 2: wire schema validator — langfuse SDK with local fallback"
```

---

## Task 3: Event Construction Helpers

**Files:**
- Create: `traceset/helpers.py`
- Create: `traceset/tests/test_helpers.py`

- [ ] **Step 1: Write the failing test**

Create `traceset/tests/test_helpers.py`:
```python
"""Tests for event construction helpers."""
import uuid
import pytest
from traceset.helpers import (
    make_event_id,
    make_timestamp,
    make_trace_create,
    make_span_create,
    make_generation_create,
    wrap_event,
    to_camel_case,
)


def test_make_event_id_deterministic():
    assert make_event_id("seed-1") == make_event_id("seed-1")


def test_make_event_id_different_seeds():
    assert make_event_id("seed-1") != make_event_id("seed-2")


def test_make_event_id_valid_uuid():
    id_str = make_event_id("test")
    parsed = uuid.UUID(id_str)
    assert str(parsed) == id_str


def test_make_timestamp_no_offset():
    ts = make_timestamp("2025-01-15T10:30:00.000000+00:00", 0.0)
    assert ts == "2025-01-15T10:30:00.000000+00:00"


def test_make_timestamp_with_offset():
    ts = make_timestamp("2025-01-15T10:30:00.000000+00:00", 1.5)
    assert ts == "2025-01-15T10:30:01.500000+00:00"


def test_to_camel_case():
    assert to_camel_case("user_id") == "userId"
    assert to_camel_case("session_id") == "sessionId"
    assert to_camel_case("parent_observation_id") == "parentObservationId"
    assert to_camel_case("completion_start_time") == "completionStartTime"
    assert to_camel_case("name") == "name"
    assert to_camel_case("model_parameters") == "modelParameters"


def test_make_trace_create_basic():
    body = make_trace_create(trace_id="t1", name="test", user_id="u1")
    assert body == {"id": "t1", "name": "test", "userId": "u1"}


def test_make_trace_create_no_user_id():
    body = make_trace_create(trace_id="t1", name="test")
    assert "userId" not in body


def test_make_trace_create_with_kwargs():
    body = make_trace_create(
        trace_id="t1", name="test", user_id="u1",
        session_id="s1", input={"q": "hello"}, metadata={"k": "v"},
    )
    assert body["sessionId"] == "s1"
    assert body["input"] == {"q": "hello"}
    assert body["metadata"] == {"k": "v"}


def test_make_span_create():
    body = make_span_create(
        span_id="sp1", trace_id="t1", name="span",
        start_time="2025-01-15T10:30:00.000000+00:00",
        end_time="2025-01-15T10:30:01.000000+00:00",
        parent_observation_id="parent-1",
        input={"q": "in"}, output={"r": "out"},
    )
    assert body["id"] == "sp1"
    assert body["traceId"] == "t1"
    assert body["startTime"] == "2025-01-15T10:30:00.000000+00:00"
    assert body["endTime"] == "2025-01-15T10:30:01.000000+00:00"
    assert body["parentObservationId"] == "parent-1"
    assert body["input"] == {"q": "in"}
    assert body["output"] == {"r": "out"}


def test_make_generation_create():
    usage = {"input": 10, "output": 20, "total": 30, "unit": "TOKENS"}
    body = make_generation_create(
        gen_id="g1", trace_id="t1", name="gpt-4o-mini",
        model="gpt-4o-mini",
        start_time="2025-01-15T10:30:00.000000+00:00",
        end_time="2025-01-15T10:30:01.000000+00:00",
        usage=usage,
        completion_start_time="2025-01-15T10:30:00.100000+00:00",
        model_parameters={"temperature": 0.7},
        input={"messages": []}, output={"text": "hello"},
    )
    assert body["model"] == "gpt-4o-mini"
    assert body["usageDetails"] == usage
    assert body["completionStartTime"] == "2025-01-15T10:30:00.100000+00:00"
    assert body["modelParameters"] == {"temperature": 0.7}
    assert body["input"] == {"messages": []}
    assert body["output"] == {"text": "hello"}


def test_wrap_event():
    body = {"id": "t1", "name": "test"}
    event = wrap_event("e1", "2025-01-15T10:30:00.000000+00:00", "trace-create", body)
    assert event == {
        "id": "e1",
        "timestamp": "2025-01-15T10:30:00.000000+00:00",
        "type": "trace-create",
        "body": body,
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_helpers.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'traceset.helpers'`

- [ ] **Step 3: Write minimal implementation**

Create `traceset/helpers.py`:
```python
"""Event construction helpers for Langfuse wire events.

All make_*_create functions return body dicts with camelCase keys.
wrap_event wraps a body dict into the full event envelope.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta


def make_event_id(seed: str) -> str:
    """Deterministic UUID (v5) from a seed string."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def make_timestamp(base: str, offset_seconds: float = 0.0) -> str:
    """ISO 8601 UTC timestamp from a base time + offset in seconds."""
    dt = datetime.fromisoformat(base) + timedelta(seconds=offset_seconds)
    return dt.isoformat()


def to_camel_case(snake: str) -> str:
    """Convert snake_case to camelCase."""
    parts = snake.split("_")
    return parts[0] + "".join(p.title() for p in parts[1:])


def make_trace_create(
    trace_id: str,
    name: str,
    user_id: str | None = None,
    **kwargs,
) -> dict:
    """Build a trace-create body dict with camelCase keys."""
    body: dict = {"id": trace_id, "name": name}
    if user_id is not None:
        body["userId"] = user_id
    for k, v in kwargs.items():
        body[to_camel_case(k)] = v
    return body


def make_span_create(
    span_id: str,
    trace_id: str,
    name: str,
    start_time: str,
    end_time: str,
    **kwargs,
) -> dict:
    """Build a span-create body dict with camelCase keys."""
    body: dict = {
        "id": span_id,
        "traceId": trace_id,
        "name": name,
        "startTime": start_time,
        "endTime": end_time,
    }
    for k, v in kwargs.items():
        body[to_camel_case(k)] = v
    return body


def make_generation_create(
    gen_id: str,
    trace_id: str,
    name: str,
    model: str,
    start_time: str,
    end_time: str,
    usage: dict,
    **kwargs,
) -> dict:
    """Build a generation-create body dict with camelCase keys."""
    body: dict = {
        "id": gen_id,
        "traceId": trace_id,
        "name": name,
        "startTime": start_time,
        "endTime": end_time,
        "model": model,
        "usageDetails": usage,
    }
    for k, v in kwargs.items():
        body[to_camel_case(k)] = v
    return body


def wrap_event(
    event_id: str,
    timestamp: str,
    event_type: str,
    body: dict,
) -> dict:
    """Wrap a body dict into the full event envelope."""
    return {
        "id": event_id,
        "timestamp": timestamp,
        "type": event_type,
        "body": body,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_helpers.py -v
```
Expected: PASS — 12 tests passed

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 3: event construction helpers — UUIDs, timestamps, camelCase body dicts"
```

---

## Task 4: Scenario 01 — chat-basic (Complete Reference)

**Files:**
- Create: `traceset/scenarios/s01_chat_basic.py`
- Create: `traceset/tests/test_scenarios.py`

- [ ] **Step 1: Write the failing test**

Create `traceset/tests/test_scenarios.py`:
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
    valid_types = {"trace-create", "span-create", "generation-create"}
    for i, e in enumerate(events):
        assert e["type"] in valid_types, f"{module.SCENARIO_ID}[{i}]: bad type {e['type']}"
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
    assert len(meta["events_in_order"]) == len(events)
    for i, (e, m) in enumerate(zip(events, meta["events_in_order"]), 1):
        assert m["index"] == i
        assert m["type"] == e["type"], (
            f"{module.SCENARIO_ID}[{i}]: meta type {m['type']} != event type {e['type']}"
        )


def test_s01_chat_basic():
    from traceset.scenarios import s01_chat_basic
    _check_scenario(s01_chat_basic)
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s01_chat_basic -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'traceset.scenarios.s01_chat_basic'`

- [ ] **Step 3: Write the implementation**

Create `traceset/scenarios/s01_chat_basic.py`:
```python
"""Scenario 01: Basic chatbot, single-turn Q&A, no extras.

Events (4):
  1. trace-create       (MessageTraceInfo)
  2. generation-create  (MessageTraceInfo)
  3. trace-create       (GenerateNameTraceInfo, upsert)
  4. span-create        (GenerateNameTraceInfo)

This scenario is the COMPLETE REFERENCE TEMPLATE — all field values are
shown explicitly below. Other scenarios use the same helper functions
with different values.
"""
from traceset.helpers import (
    make_event_id,
    make_timestamp,
    make_trace_create,
    make_span_create,
    make_generation_create,
    wrap_event,
)

SCENARIO_ID = "01-chat-basic"
SCENARIO_DESCRIPTION = "Basic chatbot, single-turn Q&A, no extras"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 4

# ── Deterministic constants ──────────────────────────────────────────
_BASE = "2025-01-15T10:30:00.000000+00:00"

# Trace ID = Dify message_id (UUID v4 format)
_TRACE = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"

# User / conversation
_USER = "u-7f3a2b8c4d"
_CONV = "conv-2b3c4d5e6f7a"

# Generation span ID (for the LLM generation-create event)
_GEN = "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"

# Generate Name span ID
_NAME_SPAN = "c3d4e5f6-a7b8-4c9d-0e1f-2a3b4c5d6e7f"

# ── Realistic content ────────────────────────────────────────────────
_USER_QUERY = "What are the key differences between Redis and Memcached?"

_LLM_RESPONSE = (
    "Redis and Memcached are both in-memory key-value stores, but they differ "
    "in several key areas. Redis supports diverse data structures (lists, sets, "
    "sorted sets, hashes, streams) while Memcached only supports strings. "
    "Redis offers built-in persistence via RDB snapshots and AOF logs; Memcached "
    "has no persistence. Redis is single-threaded (multiplexed I/O); Memcached is "
    "multi-threaded. Redis supports pub/sub, Lua scripting, and clustering; "
    "Memcached is simpler and excels at raw cache performance for simple key-value "
    "lookups."
)

_CONV_NAME = "Redis vs Memcached Comparison"

# gpt-4o-mini pricing: $0.15/1M input, $0.60/1M output
# Spec reference values (section 5 example)
_MODEL = "gpt-4o-mini"
_MODEL_PARAMS = {"temperature": 0.7, "max_tokens": 500}
_USAGE = {
    "input": 42,
    "output": 156,
    "total": 198,
    "unit": "TOKENS",
    "inputCost": 0.000063,
    "outputCost": 0.000234,
    "totalCost": 0.000297,
}


def build_events():
    events = []

    # ── Event 1: trace-create (MessageTraceInfo) ────────────────────
    #    Fires after the LLM returns. trace_id = message_id.
    events.append(wrap_event(
        event_id=make_event_id("s01-e01"),
        timestamp=make_timestamp(_BASE, 2.123),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE,
            name="Dify Chatbot",
            user_id=_USER,
            input={"query": _USER_QUERY},
            session_id=_CONV,
            metadata={
                "user_id": _USER,
                "conversation_id": _CONV,
            },
        ),
    ))

    # ── Event 2: generation-create (MessageTraceInfo) ───────────────
    #    The LLM call itself. Same trace_id.
    events.append(wrap_event(
        event_id=make_event_id("s01-e02"),
        timestamp=make_timestamp(_BASE, 2.124),
        event_type="generation-create",
        body=make_generation_create(
            gen_id=_GEN,
            trace_id=_TRACE,
            name=_MODEL,
            model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 2.023),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.250),
            model_parameters=_MODEL_PARAMS,
            input={
                "messages": [
                    {"role": "user", "content": _USER_QUERY}
                ]
            },
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # ── Event 3: trace-create (GenerateNameTraceInfo, upsert) ───────
    #    Upserts the same trace_id with a new name.
    events.append(wrap_event(
        event_id=make_event_id("s01-e03"),
        timestamp=make_timestamp(_BASE, 3.500),
        event_type="trace-create",
        body=make_trace_create(
            trace_id=_TRACE,
            name="Generate Name",
            user_id=_USER,
        ),
    ))

    # ── Event 4: span-create (GenerateNameTraceInfo) ────────────────
    #    The name-generation LLM call as a span.
    events.append(wrap_event(
        event_id=make_event_id("s01-e04"),
        timestamp=make_timestamp(_BASE, 3.501),
        event_type="span-create",
        body=make_span_create(
            span_id=_NAME_SPAN,
            trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 3.000),
            end_time=make_timestamp(_BASE, 3.450),
            input={
                "messages": [
                    {"role": "user", "content": _USER_QUERY}
                ]
            },
            output={"text": _CONV_NAME},
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
        "events_in_order": [
            {
                "index": 1,
                "type": "trace-create",
                "source_trace_type": "MessageTraceInfo",
                "dify_handler": "LangFuseDataTrace.message_trace",
            },
            {
                "index": 2,
                "type": "generation-create",
                "source_trace_type": "MessageTraceInfo",
                "dify_handler": "LangFuseDataTrace.message_trace",
            },
            {
                "index": 3,
                "type": "trace-create",
                "source_trace_type": "GenerateNameTraceInfo",
                "dify_handler": "LangFuseDataTrace.generate_name_trace",
            },
            {
                "index": 4,
                "type": "span-create",
                "source_trace_type": "GenerateNameTraceInfo",
                "dify_handler": "LangFuseDataTrace.generate_name_trace",
            },
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Simplest chat run. GenerateName upserts the same trace ID.",
    }
```

**Reference: the 4 events produced by `build_events()` (full data):**

Event 1:
```json
{
  "id": "<uuid5 from 's01-e01'>",
  "timestamp": "2025-01-15T10:30:02.123000+00:00",
  "type": "trace-create",
  "body": {
    "id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
    "name": "Dify Chatbot",
    "userId": "u-7f3a2b8c4d",
    "input": {"query": "What are the key differences between Redis and Memcached?"},
    "sessionId": "conv-2b3c4d5e6f7a",
    "metadata": {"user_id": "u-7f3a2b8c4d", "conversation_id": "conv-2b3c4d5e6f7a"}
  }
}
```

Event 2:
```json
{
  "id": "<uuid5 from 's01-e02'>",
  "timestamp": "2025-01-15T10:30:02.124000+00:00",
  "type": "generation-create",
  "body": {
    "id": "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e",
    "traceId": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
    "name": "gpt-4o-mini",
    "startTime": "2025-01-15T10:30:00.100000+00:00",
    "endTime": "2025-01-15T10:30:02.023000+00:00",
    "model": "gpt-4o-mini",
    "usageDetails": {"input": 42, "output": 156, "total": 198, "unit": "TOKENS", "inputCost": 0.000063, "outputCost": 0.000234, "totalCost": 0.000297},
    "completionStartTime": "2025-01-15T10:30:00.250000+00:00",
    "modelParameters": {"temperature": 0.7, "max_tokens": 500},
    "input": {"messages": [{"role": "user", "content": "What are the key differences between Redis and Memcached?"}]},
    "output": {"text": "Redis and Memcached are both in-memory key-value stores..."}
  }
}
```

Event 3:
```json
{
  "id": "<uuid5 from 's01-e03'>",
  "timestamp": "2025-01-15T10:30:03.500000+00:00",
  "type": "trace-create",
  "body": {"id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d", "name": "Generate Name", "userId": "u-7f3a2b8c4d"}
}
```

Event 4:
```json
{
  "id": "<uuid5 from 's01-e04'>",
  "timestamp": "2025-01-15T10:30:03.501000+00:00",
  "type": "span-create",
  "body": {
    "id": "c3d4e5f6-a7b8-4c9d-0e1f-2a3b4c5d6e7f",
    "traceId": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
    "name": "Generate Name",
    "startTime": "2025-01-15T10:30:03.000000+00:00",
    "endTime": "2025-01-15T10:30:03.450000+00:00",
    "input": {"messages": [{"role": "user", "content": "What are the key differences between Redis and Memcached?"}]},
    "output": {"text": "Redis vs Memcached Comparison"}
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s01_chat_basic -v
```
Expected: PASS — 1 test passed

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 4: scenario 01 chat-basic — complete reference template (4 events)"
```

---

## Task 5: Scenario 02 — chat-rag

**Files:**
- Create: `traceset/scenarios/s02_chat_rag.py`
- Modify: `traceset/tests/test_scenarios.py` (add test function)

**Event order (from spec section 5):**
trace-create(msg) → generation-create(msg) → span-create(rag) → generation-create(sugg-q) → trace-create(name) → span-create(name)

- [ ] **Step 1: Write the failing test**

Add to `traceset/tests/test_scenarios.py` (after `test_s01_chat_basic`):
```python
def test_s02_chat_rag():
    from traceset.scenarios import s02_chat_rag
    _check_scenario(s02_chat_rag)
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s02_chat_rag -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'traceset.scenarios.s02_chat_rag'`

- [ ] **Step 3: Write the implementation**

Create `traceset/scenarios/s02_chat_rag.py`:
```python
"""Scenario 02: Chatbot with RAG, suggested questions, and auto-name.

Events (6):
  1. trace-create       (MessageTraceInfo)
  2. generation-create  (MessageTraceInfo)
  3. span-create        (DatasetRetrievalTraceInfo)
  4. generation-create  (SuggestedQuestionTraceInfo)
  5. trace-create       (GenerateNameTraceInfo, upsert)
  6. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "02-chat-rag"
SCENARIO_DESCRIPTION = "Chatbot with knowledge base retrieval, suggested questions, auto-name"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = [
    "MessageTraceInfo", "DatasetRetrievalTraceInfo",
    "SuggestedQuestionTraceInfo", "GenerateNameTraceInfo",
]
EXPECTED_EVENT_COUNT = 6

_BASE = "2025-01-15T11:00:00.000000+00:00"
_TRACE = "d4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f8a"
_USER = "u-9a1b2c3d4e"
_CONV = "conv-5e6f7a8b9c0d"
_GEN = "e5f6a7b8-c9d0-4e1f-2a3b-4c5d6e7f8a9b"
_RAG_SPAN = "f6a7b8c9-d0e1-4f2a-3b4c-5d6e7f8a9b0c"
_SUGG_GEN = "a7b8c9d0-e1f2-4a3b-4c5d-6e7f8a9b0c1d"
_NAME_SPAN = "b8c9d0e1-f2a3-4b4c-5d6e-7f8a9b0c1d2e"

_QUERY = "How does Redis handle persistence compared to Memcached?"

_LLM_RESPONSE = (
    "Redis provides two main persistence mechanisms: RDB (Redis Database) "
    "snapshots that save point-in-time copies of the dataset at intervals, "
    "and AOF (Append-Only File) logs that record every write operation. "
    "You can use either or both. Memcached has no built-in persistence — "
    "data is lost on restart. This makes Redis suitable for scenarios "
    "where data durability matters, while Memcached is purely for "
    "transient caching."
)

_RAG_DOCS = [
    {"title": "Redis Persistence Guide", "content": "Redis supports RDB and AOF...", "score": 0.95},
    {"title": "Memcached vs Redis", "content": "Memcached has no persistence...", "score": 0.89},
    {"title": "In-Memory Databases Comparison", "content": "Redis and Memcached compared...", "score": 0.82},
]

_SUGG_QUESTIONS = [
    "What are the performance trade-offs of RDB vs AOF?",
    "How do I configure Redis persistence for my use case?",
    "Can Memcached be made persistent with external tools?",
]

_MODEL = "gpt-4o-mini"
_MODEL_PARAMS = {"temperature": 0.7, "max_tokens": 800}
_MSG_USAGE = {"input": 58, "output": 234, "total": 292, "unit": "TOKENS",
              "inputCost": 0.000009, "outputCost": 0.000140, "totalCost": 0.000149}
_SUGG_USAGE = {"input": 85, "output": 45, "total": 130, "unit": "TOKENS",
               "inputCost": 0.000013, "outputCost": 0.000027, "totalCost": 0.000040}
_CONV_NAME = "Redis Persistence vs Memcached"


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s02-e01"), make_timestamp(_BASE, 3.200),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV},
        ),
    ))

    # 2. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s02-e02"), make_timestamp(_BASE, 3.201),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.150),
            end_time=make_timestamp(_BASE, 3.100),
            usage=_MSG_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.300),
            model_parameters=_MODEL_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}],
                   "context": _RAG_DOCS},
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 3. span-create (DatasetRetrieval)
    events.append(wrap_event(
        make_event_id("s02-e03"), make_timestamp(_BASE, 3.202),
        "span-create",
        make_span_create(
            span_id=_RAG_SPAN, trace_id=_TRACE,
            name="dataset_retrieval",
            start_time=make_timestamp(_BASE, 0.050),
            end_time=make_timestamp(_BASE, 0.120),
            input={"query": _QUERY},
            output={"documents": _RAG_DOCS},
        ),
    ))

    # 4. generation-create (SuggestedQuestion)
    events.append(wrap_event(
        make_event_id("s02-e04"), make_timestamp(_BASE, 4.000),
        "generation-create",
        make_generation_create(
            gen_id=_SUGG_GEN, trace_id=_TRACE,
            name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 3.500),
            end_time=make_timestamp(_BASE, 3.950),
            usage=_SUGG_USAGE,
            completion_start_time=make_timestamp(_BASE, 3.600),
            model_parameters=_MODEL_PARAMS,
            input={"messages": [{"role": "assistant", "content": _LLM_RESPONSE}]},
            output={"questions": _SUGG_QUESTIONS},
        ),
    ))

    # 5. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s02-e05"), make_timestamp(_BASE, 4.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 6. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s02-e06"), make_timestamp(_BASE, 4.501),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 4.100),
            end_time=make_timestamp(_BASE, 4.450),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _CONV_NAME},
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
        "events_in_order": [
            {"index": 1, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 2, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 4, "type": "generation-create", "source_trace_type": "SuggestedQuestionTraceInfo", "dify_handler": "LangFuseDataTrace.suggested_question_trace"},
            {"index": 5, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 6, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Chat with knowledge base. RAG retrieval + suggested questions + auto-name.",
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s02_chat_rag -v
```
Expected: PASS — 1 test passed

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 5: scenario 02 chat-rag — Message+RAG+SuggQ+GenName (6 events)"
```

---

## Task 6: Scenario 03 — completion-basic

**Files:**
- Create: `traceset/scenarios/s03_completion_basic.py`
- Modify: `traceset/tests/test_scenarios.py` (add test function)

**Event order:** trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name)

- [ ] **Step 1: Write the failing test**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s03_completion_basic():
    from traceset.scenarios import s03_completion_basic
    _check_scenario(s03_completion_basic)
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s03_completion_basic -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

Create `traceset/scenarios/s03_completion_basic.py`:
```python
"""Scenario 03: Basic completion app, single-turn, no extras.

Events (4):
  1. trace-create       (MessageTraceInfo)
  2. generation-create  (MessageTraceInfo)
  3. trace-create       (GenerateNameTraceInfo, upsert)
  4. span-create        (GenerateNameTraceInfo)

Same structure as s01 but app_mode = 'completion' (no conversation context).
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "03-completion-basic"
SCENARIO_DESCRIPTION = "Basic completion app, single-turn text generation, no extras"
APP_TYPE = "completion"
DIFY_APP_MODE = "completion"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 4

_BASE = "2025-01-15T11:30:00.000000+00:00"
_TRACE = "c9d0e1f2-a3b4-4c5d-6e7f-8a9b0c1d2e3f"
_USER = "u-1a2b3c4d5e"
_GEN = "d0e1f2a3-b4c5-4d6e-7f8a-9b0c1d2e3f4a"
_NAME_SPAN = "e1f2a3b4-c5d6-4e7f-8a9b-0c1d2e3f4a5b"

_PROMPT = "Explain the concept of ACID compliance in database transactions."

_LLM_RESPONSE = (
    "ACID compliance ensures reliable database transactions through four "
    "properties: Atomicity (all operations in a transaction succeed or "
    "none do), Consistency (transactions move the database from one valid "
    "state to another), Isolation (concurrent transactions don't interfere "
    "with each other), and Durability (committed transactions persist even "
    "after crashes). These guarantees make relational databases trustworthy "
    "for financial and mission-critical applications."
)

_TITLE = "ACID Compliance Explained"

_MODEL = "gpt-4o-mini"
_MODEL_PARAMS = {"temperature": 0.3, "max_tokens": 500}
_USAGE = {"input": 35, "output": 120, "total": 155, "unit": "TOKENS",
          "inputCost": 0.000005, "outputCost": 0.000072, "totalCost": 0.000077}


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s03-e01"), make_timestamp(_BASE, 2.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Completion",
            user_id=_USER,
            input={"prompt": _PROMPT},
        ),
    ))

    # 2. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s03-e02"), make_timestamp(_BASE, 2.501),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 2.400),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.200),
            model_parameters=_MODEL_PARAMS,
            input={"messages": [{"role": "user", "content": _PROMPT}]},
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 3. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s03-e03"), make_timestamp(_BASE, 3.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 4. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s03-e04"), make_timestamp(_BASE, 3.801),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 3.400),
            end_time=make_timestamp(_BASE, 3.750),
            input={"messages": [{"role": "user", "content": _PROMPT}]},
            output={"text": _TITLE},
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
        "events_in_order": [
            {"index": 1, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 2, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Completion app (no conversation). Same trace structure as chat but app_mode=completion.",
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s03_completion_basic -v
```
Expected: PASS — 1 test passed

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 6: scenario 03 completion-basic — Message+GenName (4 events)"
```

---

## Task 7: Scenarios 04-05 — Agent Variants

**Files:**
- Create: `traceset/scenarios/s04_agent_single_tool.py`
- Create: `traceset/scenarios/s05_agent_multi_tool.py`
- Modify: `traceset/tests/test_scenarios.py` (add 2 test functions)

### Scenario 04: agent-single-tool (5 events)

**Event order:** trace-create(msg) → span-create(tool) → generation-create(msg) → trace-create(name) → span-create(name)

- [ ] **Step 1a: Write failing test for s04**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s04_agent_single_tool():
    from traceset.scenarios import s04_agent_single_tool
    _check_scenario(s04_agent_single_tool)
```

- [ ] **Step 2a: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s04_agent_single_tool -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3a: Write s04 implementation**

Create `traceset/scenarios/s04_agent_single_tool.py`:
```python
"""Scenario 04: Agent with a single tool call.

Events (5):
  1. trace-create       (MessageTraceInfo)
  2. span-create        (ToolTraceInfo)
  3. generation-create  (MessageTraceInfo)
  4. trace-create       (GenerateNameTraceInfo, upsert)
  5. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "04-agent-single-tool"
SCENARIO_DESCRIPTION = "Agent app, single tool call before LLM response"
APP_TYPE = "agent"
DIFY_APP_MODE = "agent-chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "ToolTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 5

_BASE = "2025-01-15T12:00:00.000000+00:00"
_TRACE = "f2a3b4c5-d6e7-4f8a-9b0c-1d2e3f4a5b6c"
_USER = "u-2b3c4d5e6f"
_CONV = "conv-6f7a8b9c0d1e"
_GEN = "a3b4c5d6-e7f8-4a9b-0c1d-2e3f4a5b6c7d"
_TOOL_SPAN = "b4c5d6e7-f8a9-4b0c-1d2e-3f4a5b6c7d8e"
_NAME_SPAN = "c5d6e7f8-a9b0-4c1d-2e3f-4a5b6c7d8e9f"

_QUERY = "What's the current weather in San Francisco?"
_TOOL_INPUT = {"location": "San Francisco, CA", "unit": "fahrenheit"}
_TOOL_OUTPUT = {
    "location": "San Francisco, CA",
    "temperature": 62,
    "unit": "fahrenheit",
    "condition": "Partly Cloudy",
    "humidity": 65,
    "wind_speed": 12,
}
_LLM_RESPONSE = (
    "The current weather in San Francisco is partly cloudy with a "
    "temperature of 62°F. Humidity is at 65% and wind speed is 12 mph. "
    "It's a typical San Francisco day — cool and mild."
)
_CONV_NAME = "SF Weather Query"

_MODEL = "gpt-4o-mini"
_MODEL_PARAMS = {"temperature": 0.7, "max_tokens": 300}
_USAGE = {"input": 65, "output": 89, "total": 154, "unit": "TOKENS",
          "inputCost": 0.000010, "outputCost": 0.000053, "totalCost": 0.000063}


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s04-e01"), make_timestamp(_BASE, 3.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Agent", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV},
        ),
    ))

    # 2. span-create (Tool)
    events.append(wrap_event(
        make_event_id("s04-e02"), make_timestamp(_BASE, 3.501),
        "span-create",
        make_span_create(
            span_id=_TOOL_SPAN, trace_id=_TRACE,
            name="weather_api",
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 0.850),
            input=_TOOL_INPUT, output=_TOOL_OUTPUT,
        ),
    ))

    # 3. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s04-e03"), make_timestamp(_BASE, 3.502),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.900),
            end_time=make_timestamp(_BASE, 3.400),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 1.050),
            model_parameters=_MODEL_PARAMS,
            input={
                "messages": [
                    {"role": "user", "content": _QUERY},
                    {"role": "tool", "content": str(_TOOL_OUTPUT)},
                ],
            },
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 4. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s04-e04"), make_timestamp(_BASE, 4.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 5. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s04-e05"), make_timestamp(_BASE, 4.801),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 4.400),
            end_time=make_timestamp(_BASE, 4.750),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _CONV_NAME},
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
        "events_in_order": [
            {"index": 1, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 2, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 3, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 4, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Agent with 1 tool call. Tool span emitted between trace-create and generation-create.",
    }
```

- [ ] **Step 4a: Run s04 test**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s04_agent_single_tool -v
```
Expected: PASS

### Scenario 05: agent-multi-tool (7 events)

**Event order:** trace-create(msg) → span-create(tool1) → span-create(tool2) → span-create(tool3) → generation-create(msg) → trace-create(name) → span-create(name)

- [ ] **Step 1b: Write failing test for s05**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s05_agent_multi_tool():
    from traceset.scenarios import s05_agent_multi_tool
    _check_scenario(s05_agent_multi_tool)
```

- [ ] **Step 2b: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s05_agent_multi_tool -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3b: Write s05 implementation**

Create `traceset/scenarios/s05_agent_multi_tool.py`:
```python
"""Scenario 05: Agent with three sequential tool calls.

Events (7):
  1. trace-create       (MessageTraceInfo)
  2. span-create        (ToolTraceInfo — search)
  3. span-create        (ToolTraceInfo — fetch)
  4. span-create        (ToolTraceInfo — calculate)
  5. generation-create  (MessageTraceInfo)
  6. trace-create       (GenerateNameTraceInfo, upsert)
  7. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "05-agent-multi-tool"
SCENARIO_DESCRIPTION = "Agent app, three sequential tool calls before LLM response"
APP_TYPE = "agent"
DIFY_APP_MODE = "agent-chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "ToolTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 7

_BASE = "2025-01-15T12:30:00.000000+00:00"
_TRACE = "d6e7f8a9-b0c1-4d2e-3f4a-5b6c7d8e9f0a"
_USER = "u-3c4d5e6f7a"
_CONV = "conv-7a8b9c0d1e2f"
_GEN = "e7f8a9b0-c1d2-4e3f-4a5b-6c7d8e9f0a1b"
_TOOL1 = "f8a9b0c1-d2e3-4f4a-5b6c-7d8e9f0a1b2c"
_TOOL2 = "a9b0c1d2-e3f4-4a5b-6c7d-8e9f0a1b2c3d"
_TOOL3 = "b0c1d2e3-f4a5-4b6c-7d8e-9f0a1b2c3d4e"
_NAME_SPAN = "c1d2e3f4-a5b6-4c7d-8e9f-0a1b2c3d4e5f"

_QUERY = "Research the population of Tokyo, fetch GDP data, and calculate GDP per capita."
_TOOL1_INPUT = {"query": "Tokyo population 2024"}
_TOOL1_OUTPUT = {"population": 13960000, "year": 2024, "source": "World Bank"}
_TOOL2_INPUT = {"query": "Tokyo GDP 2024 USD"}
_TOOL2_OUTPUT = {"gdp_usd": 1100000000000, "year": 2024, "source": "IMF"}
_TOOL3_INPUT = {"gdp": 1100000000000, "population": 13960000}
_TOOL3_OUTPUT = {"gdp_per_capita_usd": 78796.56, "currency": "USD"}
_LLM_RESPONSE = (
    "Based on the research: Tokyo has a population of approximately "
    "13.96 million (2024, World Bank). The GDP is approximately $1.1 "
    "trillion USD (2024, IMF). The calculated GDP per capita is "
    "approximately $78,797 USD. Tokyo remains one of the most "
    "economically productive metropolitan areas in the world."
)
_CONV_NAME = "Tokyo GDP Per Capita Research"

_MODEL = "gpt-4o"
_MODEL_PARAMS = {"temperature": 0.3, "max_tokens": 600}
_USAGE = {"input": 180, "output": 145, "total": 325, "unit": "TOKENS",
          "inputCost": 0.000450, "outputCost": 0.001450, "totalCost": 0.001900}


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s05-e01"), make_timestamp(_BASE, 5.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Agent", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV},
        ),
    ))

    # 2. span-create (Tool 1: search)
    events.append(wrap_event(
        make_event_id("s05-e02"), make_timestamp(_BASE, 5.501),
        "span-create",
        make_span_create(
            span_id=_TOOL1, trace_id=_TRACE, name="web_search",
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 1.200),
            input=_TOOL1_INPUT, output=_TOOL1_OUTPUT,
        ),
    ))

    # 3. span-create (Tool 2: fetch)
    events.append(wrap_event(
        make_event_id("s05-e03"), make_timestamp(_BASE, 5.502),
        "span-create",
        make_span_create(
            span_id=_TOOL2, trace_id=_TRACE, name="data_fetch",
            start_time=make_timestamp(_BASE, 1.300),
            end_time=make_timestamp(_BASE, 2.500),
            input=_TOOL2_INPUT, output=_TOOL2_OUTPUT,
        ),
    ))

    # 4. span-create (Tool 3: calculate)
    events.append(wrap_event(
        make_event_id("s05-e04"), make_timestamp(_BASE, 5.503),
        "span-create",
        make_span_create(
            span_id=_TOOL3, trace_id=_TRACE, name="calculator",
            start_time=make_timestamp(_BASE, 2.600),
            end_time=make_timestamp(_BASE, 2.750),
            input=_TOOL3_INPUT, output=_TOOL3_OUTPUT,
        ),
    ))

    # 5. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s05-e05"), make_timestamp(_BASE, 5.504),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 2.800),
            end_time=make_timestamp(_BASE, 5.400),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 2.950),
            model_parameters=_MODEL_PARAMS,
            input={
                "messages": [
                    {"role": "user", "content": _QUERY},
                    {"role": "tool", "content": str(_TOOL1_OUTPUT)},
                    {"role": "tool", "content": str(_TOOL2_OUTPUT)},
                    {"role": "tool", "content": str(_TOOL3_OUTPUT)},
                ],
            },
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 6. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s05-e06"), make_timestamp(_BASE, 6.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 7. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s05-e07"), make_timestamp(_BASE, 6.801),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 6.400),
            end_time=make_timestamp(_BASE, 6.750),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _CONV_NAME},
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
        "events_in_order": [
            {"index": 1, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 2, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 5, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 6, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Agent with 3 sequential tool calls. Uses gpt-4o for the final synthesis.",
    }
```

- [ ] **Step 4b: Run s05 test**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s05_agent_multi_tool -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 7: scenarios 04-05 agent variants — single-tool (5 events) + multi-tool (7 events)"
```

---

## Task 8: Scenarios 06-07 — Workflow Variants

**Files:**
- Create: `traceset/scenarios/s06_workflow_5node.py`
- Create: `traceset/scenarios/s07_workflow_15node.py`
- Modify: `traceset/tests/test_scenarios.py` (add 2 test functions)

### Scenario 06: workflow-5node (7 events)

**Event order:** trace-create(wf) → span-create(wf-span) → span-create(node1) → span-create(node2) → generation-create(node3-llm) → generation-create(node4-llm) → span-create(node5)

- [ ] **Step 1a: Write failing test for s06**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s06_workflow_5node():
    from traceset.scenarios import s06_workflow_5node
    _check_scenario(s06_workflow_5node)
```

- [ ] **Step 2a: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s06_workflow_5node -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3a: Write s06 implementation**

Create `traceset/scenarios/s06_workflow_5node.py`:
```python
"""Scenario 06: Workflow with 5 nodes (2 LLM, 1 knowledge retrieval, start, end).

Events (7):
  1. trace-create       (WorkflowTraceInfo)
  2. span-create        (workflow-level span)
  3. span-create        (node 1: Start)
  4. span-create        (node 2: Knowledge Retrieval)
  5. generation-create  (node 3: LLM — Generate Answer)
  6. generation-create  (node 4: LLM — Refine Answer)
  7. span-create        (node 5: End)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "06-workflow-5node"
SCENARIO_DESCRIPTION = "Workflow app, 5 nodes, 2 LLM nodes, 1 knowledge retrieval"
APP_TYPE = "workflow"
DIFY_APP_MODE = "workflow"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo"]
EXPECTED_EVENT_COUNT = 7

_BASE = "2025-01-15T13:00:00.000000+00:00"
_TRACE = "e3f4a5b6-c7d8-4e9f-0a1b-2c3d4e5f6a7b"
_USER = "u-4d5e6f7a8b"
_WF_SPAN = "f4a5b6c7-d8e9-4f0a-1b2c-3d4e5f6a7b8c"
_N1 = "a5b6c7d8-e9f0-4a1b-2c3d-4e5f6a7b8c9d"
_N2 = "b6c7d8e9-f0a1-4b2c-3d4e-5f6a7b8c9d0e"
_N3 = "c7d8e9f0-a1b2-4c3d-4e5f-6a7b8c9d0e1f"
_N4 = "d8e9f0a1-b2c3-4d4e-5f6a-7b8c9d0e1f2a"
_N5 = "e9f0a1b2-c3d4-4e5f-6a7b-8c9d0e1f2a3b"

_QUERY = "Summarize the key points of microservices architecture."
_RAG_DOCS = [
    {"title": "Microservices Patterns", "content": "Key patterns include...", "score": 0.91},
    {"title": "Monolith vs Microservices", "content": "Trade-offs of...", "score": 0.85},
]
_N3_RESPONSE = (
    "Microservices architecture breaks applications into small, independent "
    "services that communicate via APIs. Key points include: service autonomy, "
    "decentralized data management, independent deployment, and technology diversity."
)
_N4_RESPONSE = (
    "Refined summary: Microservices architecture decomposes applications into "
    "autonomous services with independent data stores, deployable independently, "
    "using diverse technologies, communicating via lightweight APIs. Benefits "
    "include scalability and fault isolation; challenges include operational "
    "complexity and distributed data management."
)
_FINAL_OUTPUT = {"summary": _N4_RESPONSE}

_MODEL = "gpt-4o"
_MODEL_PARAMS = {"temperature": 0.5, "max_tokens": 800}
_N3_USAGE = {"input": 120, "output": 85, "total": 205, "unit": "TOKENS",
             "inputCost": 0.000300, "outputCost": 0.000850, "totalCost": 0.001150}
_N4_USAGE = {"input": 210, "output": 95, "total": 305, "unit": "TOKENS",
             "inputCost": 0.000525, "outputCost": 0.000950, "totalCost": 0.001475}


def build_events():
    events = []

    # 1. trace-create (Workflow)
    events.append(wrap_event(
        make_event_id("s06-e01"), make_timestamp(_BASE, 6.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Content Summarization Workflow",
            user_id=_USER,
            input={"query": _QUERY},
            metadata={"workflow_id": "wf-001", "workflow_run_id": _TRACE},
        ),
    ))

    # 2. span-create (workflow-level span)
    events.append(wrap_event(
        make_event_id("s06-e02"), make_timestamp(_BASE, 6.501),
        "span-create",
        make_span_create(
            span_id=_WF_SPAN, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 6.400),
            input={"query": _QUERY}, output=_FINAL_OUTPUT,
        ),
    ))

    # 3. span-create (node 1: Start)
    events.append(wrap_event(
        make_event_id("s06-e03"), make_timestamp(_BASE, 6.502),
        "span-create",
        make_span_create(
            span_id=_N1, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.010),
            parent_observation_id=_WF_SPAN,
            input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    # 4. span-create (node 2: Knowledge Retrieval)
    events.append(wrap_event(
        make_event_id("s06-e04"), make_timestamp(_BASE, 6.503),
        "span-create",
        make_span_create(
            span_id=_N2, trace_id=_TRACE, name="Knowledge Retrieval",
            start_time=make_timestamp(_BASE, 0.050),
            end_time=make_timestamp(_BASE, 0.800),
            parent_observation_id=_WF_SPAN,
            input={"query": _QUERY}, output={"documents": _RAG_DOCS},
        ),
    ))

    # 5. generation-create (node 3: LLM — Generate Answer)
    events.append(wrap_event(
        make_event_id("s06-e05"), make_timestamp(_BASE, 6.504),
        "generation-create",
        make_generation_create(
            gen_id=_N3, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.850),
            end_time=make_timestamp(_BASE, 3.200),
            usage=_N3_USAGE,
            completion_start_time=make_timestamp(_BASE, 1.000),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}],
                   "context": _RAG_DOCS},
            output={"text": _N3_RESPONSE},
        ),
    ))

    # 6. generation-create (node 4: LLM — Refine Answer)
    events.append(wrap_event(
        make_event_id("s06-e06"), make_timestamp(_BASE, 6.505),
        "generation-create",
        make_generation_create(
            gen_id=_N4, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 3.300),
            end_time=make_timestamp(_BASE, 6.300),
            usage=_N4_USAGE,
            completion_start_time=make_timestamp(_BASE, 3.450),
            model_parameters=_MODEL_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [
                {"role": "user", "content": _QUERY},
                {"role": "assistant", "content": _N3_RESPONSE},
            ]},
            output={"text": _N4_RESPONSE},
        ),
    ))

    # 7. span-create (node 5: End)
    events.append(wrap_event(
        make_event_id("s06-e07"), make_timestamp(_BASE, 6.506),
        "span-create",
        make_span_create(
            span_id=_N5, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 6.350),
            end_time=make_timestamp(_BASE, 6.400),
            parent_observation_id=_WF_SPAN,
            input={"summary": _N4_RESPONSE}, output=_FINAL_OUTPUT,
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
        "events_in_order": [
            {"index": 1, "type": "trace-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 2, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 5, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 6, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "5-node workflow: Start -> KnowledgeRetrieval -> LLM(Generate) -> LLM(Refine) -> End.",
    }
```

- [ ] **Step 4a: Run s06 test**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s06_workflow_5node -v
```
Expected: PASS

### Scenario 07: workflow-15node (17 events, edge: high-N)

**Event order:** trace-create(wf) → span-create(wf-span) → [15 node events: 4 generation-create + 11 span-create]

- [ ] **Step 1b: Write failing test for s07**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s07_workflow_15node():
    from traceset.scenarios import s07_workflow_15node
    _check_scenario(s07_workflow_15node)
```

- [ ] **Step 2b: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s07_workflow_15node -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3b: Write s07 implementation**

Create `traceset/scenarios/s07_workflow_15node.py`:
```python
"""Scenario 07: High-N workflow with 15 nodes (4 LLM, 3 tool, 8 other).

Events (17):
  1.  trace-create       (WorkflowTraceInfo)
  2.  span-create        (workflow-level span)
  3.  span-create        (node 1: Start)
  4.  generation-create  (node 2: LLM — Analyze Query, gpt-4o)
  5.  span-create        (node 3: Knowledge Retrieval)
  6.  span-create        (node 4: Tool — Web Search)
  7.  generation-create  (node 5: LLM — Synthesize Answer, gpt-4o)
  8.  span-create        (node 6: IF/ELSE — Check Confidence)
  9.  span-create        (node 7: Tool — Fetch Additional Data)
  10. generation-create  (node 8: LLM — Refine Response, claude-3-5-sonnet)
  11. span-create        (node 9: Template Transform — Format Output)
  12. span-create        (node 10: Tool — Validate Output)
  13. span-create        (node 11: Variable Aggregator — Merge)
  14. generation-create  (node 12: LLM — Final Polish, gpt-4o)
  15. span-create        (node 13: IF/ELSE — Quality Check)
  16. span-create        (node 14: Code — Post-process)
  17. span-create        (node 15: End)

Edge case: high-N (many sequential HTTP POSTs from one Celery task).
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "07-workflow-15node"
SCENARIO_DESCRIPTION = "High-N workflow, 15 nodes, 4 LLM, 3 tool calls, edge case"
APP_TYPE = "workflow"
DIFY_APP_MODE = "workflow"
EDGE_CASE = "high-n"
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo"]
EXPECTED_EVENT_COUNT = 17

_BASE = "2025-01-15T13:30:00.000000+00:00"
_TRACE = "f5a6b7c8-d9e0-4f1a-2b3c-4d5e6f7a8b9c"
_USER = "u-5e6f7a8b9c"
_WF_SPAN = "a6b7c8d9-e0f1-4a2b-3c4d-5e6f7a8b9c0d"

_N1  = "b7c8d9e0-f1a2-4b3c-4d5e-6f7a8b9c0d1e"
_N2  = "c8d9e0f1-a2b3-4c4d-5e6f-7a8b9c0d1e2f"
_N3  = "d9e0f1a2-b3c4-4d5e-6f7a-8b9c0d1e2f3a"
_N4  = "e0f1a2b3-c4d5-4e6f-7a8b-9c0d1e2f3a4b"
_N5  = "f1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
_N6  = "a2b3c4d5-e6f7-4a8b-9c0d-1e2f3a4b5c6d"
_N7  = "b3c4d5e6-f7a8-4b9c-0d1e-2f3a4b5c6d7e"
_N8  = "c4d5e6f7-a8b9-4c0d-1e2f-3a4b5c6d7e8f"
_N9  = "d5e6f7a8-b9c0-4d1e-2f3a-4b5c6d7e8f9a"
_N10 = "e6f7a8b9-c0d1-4e2f-3a4b-5c6d7e8f9a0b"
_N11 = "f7a8b9c0-d1e2-4f3a-4b5c-6d7e8f9a0b1c"
_N12 = "a8b9c0d1-e2f3-4a4b-5c6d-7e8f9a0b1c2d"
_N13 = "b9c0d1e2-f3a4-4b5c-6d7e-8f9a0b1c2d3e"
_N14 = "c0d1e2f3-a4b5-4c6d-7e8f-9a0b1c2d3e4f"
_N15 = "d1e2f3a4-b5c6-4d7e-8f9a-0b1c2d3e4f5a"

_QUERY = "Produce a comprehensive research report on renewable energy adoption trends."
_GPT4O = "gpt-4o"
_CLAUDE = "claude-3-5-sonnet-20241022"
_PARAMS = {"temperature": 0.5, "max_tokens": 1000}
_N2_USAGE = {"input": 85, "output": 120, "total": 205, "unit": "TOKENS",
             "inputCost": 0.000213, "outputCost": 0.001200, "totalCost": 0.001413}
_N5_USAGE = {"input": 350, "output": 280, "total": 630, "unit": "TOKENS",
             "inputCost": 0.000875, "outputCost": 0.002800, "totalCost": 0.003675}
_N8_USAGE = {"input": 450, "output": 320, "total": 770, "unit": "TOKENS",
             "inputCost": 0.001350, "outputCost": 0.004800, "totalCost": 0.006150}
_N12_USAGE = {"input": 550, "output": 190, "total": 740, "unit": "TOKENS",
              "inputCost": 0.001375, "outputCost": 0.001900, "totalCost": 0.003275}


def build_events():
    events = []

    # 1. trace-create (Workflow)
    events.append(wrap_event(
        make_event_id("s07-e01"), make_timestamp(_BASE, 15.000),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Research Report Workflow",
            user_id=_USER,
            input={"query": _QUERY},
            metadata={"workflow_id": "wf-007", "workflow_run_id": _TRACE},
        ),
    ))

    # 2. span-create (workflow-level span)
    events.append(wrap_event(
        make_event_id("s07-e02"), make_timestamp(_BASE, 15.001),
        "span-create",
        make_span_create(
            span_id=_WF_SPAN, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 14.900),
            input={"query": _QUERY},
            output={"report": "Renewable energy adoption is accelerating globally..."},
        ),
    ))

    # 3. span-create (node 1: Start)
    events.append(wrap_event(
        make_event_id("s07-e03"), make_timestamp(_BASE, 15.002),
        "span-create",
        make_span_create(
            span_id=_N1, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.010),
            parent_observation_id=_WF_SPAN,
            input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    # 4. generation-create (node 2: LLM — Analyze Query)
    events.append(wrap_event(
        make_event_id("s07-e04"), make_timestamp(_BASE, 15.003),
        "generation-create",
        make_generation_create(
            gen_id=_N2, trace_id=_TRACE, name=_GPT4O, model=_GPT4O,
            start_time=make_timestamp(_BASE, 0.050),
            end_time=make_timestamp(_BASE, 1.500),
            usage=_N2_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.200),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": "Query analysis: The user wants a comprehensive report on renewable energy adoption trends, covering solar, wind, hydro, and policy dimensions."},
        ),
    ))

    # 5. span-create (node 3: Knowledge Retrieval)
    events.append(wrap_event(
        make_event_id("s07-e05"), make_timestamp(_BASE, 15.004),
        "span-create",
        make_span_create(
            span_id=_N3, trace_id=_TRACE, name="Knowledge Retrieval",
            start_time=make_timestamp(_BASE, 1.550),
            end_time=make_timestamp(_BASE, 2.300),
            parent_observation_id=_WF_SPAN,
            input={"query": "renewable energy adoption trends 2024"},
            output={"documents": [{"title": "IEA Renewables Report", "score": 0.93}]},
        ),
    ))

    # 6. span-create (node 4: Tool — Web Search)
    events.append(wrap_event(
        make_event_id("s07-e06"), make_timestamp(_BASE, 15.005),
        "span-create",
        make_span_create(
            span_id=_N4, trace_id=_TRACE, name="web_search",
            start_time=make_timestamp(_BASE, 2.350),
            end_time=make_timestamp(_BASE, 3.500),
            parent_observation_id=_WF_SPAN,
            input={"query": "renewable energy statistics 2024"},
            output={"results": [{"url": "https://example.com/iea", "snippet": "Global renewable capacity increased 50% in 2024"}]},
        ),
    ))

    # 7. generation-create (node 5: LLM — Synthesize Answer)
    events.append(wrap_event(
        make_event_id("s07-e07"), make_timestamp(_BASE, 15.006),
        "generation-create",
        make_generation_create(
            gen_id=_N5, trace_id=_TRACE, name=_GPT4O, model=_GPT4O,
            start_time=make_timestamp(_BASE, 3.550),
            end_time=make_timestamp(_BASE, 7.000),
            usage=_N5_USAGE,
            completion_start_time=make_timestamp(_BASE, 3.700),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": "Renewable energy adoption is accelerating. Solar leads with 50% growth, wind at 20%, and policy support is strong in EU and China."},
        ),
    ))

    # 8. span-create (node 6: IF/ELSE — Check Confidence)
    events.append(wrap_event(
        make_event_id("s07-e08"), make_timestamp(_BASE, 15.007),
        "span-create",
        make_span_create(
            span_id=_N6, trace_id=_TRACE, name="Check Confidence",
            start_time=make_timestamp(_BASE, 7.050),
            end_time=make_timestamp(_BASE, 7.060),
            parent_observation_id=_WF_SPAN,
            input={"confidence": 0.85}, output={"branch": "high_confidence"},
        ),
    ))

    # 9. span-create (node 7: Tool — Fetch Additional Data)
    events.append(wrap_event(
        make_event_id("s07-e09"), make_timestamp(_BASE, 15.008),
        "span-create",
        make_span_create(
            span_id=_N7, trace_id=_TRACE, name="data_fetch",
            start_time=make_timestamp(_BASE, 7.100),
            end_time=make_timestamp(_BASE, 8.200),
            parent_observation_id=_WF_SPAN,
            input={"endpoint": "/api/stats/renewable"},
            output={"data": {"solar_gw": 1400, "wind_gw": 900, "hydro_gw": 1300}},
        ),
    ))

    # 10. generation-create (node 8: LLM — Refine Response, claude-3-5-sonnet)
    events.append(wrap_event(
        make_event_id("s07-e10"), make_timestamp(_BASE, 15.009),
        "generation-create",
        make_generation_create(
            gen_id=_N8, trace_id=_TRACE, name=_CLAUDE, model=_CLAUDE,
            start_time=make_timestamp(_BASE, 8.250),
            end_time=make_timestamp(_BASE, 12.000),
            usage=_N8_USAGE,
            completion_start_time=make_timestamp(_BASE, 8.400),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": "Refined report: Global renewable capacity reached 3.6 TW in 2024. Solar 1.4 TW, wind 0.9 TW, hydro 1.3 TW. EU Green Deal and China's 14th Five-Year Plan drive growth."},
        ),
    ))

    # 11. span-create (node 9: Template Transform)
    events.append(wrap_event(
        make_event_id("s07-e11"), make_timestamp(_BASE, 15.010),
        "span-create",
        make_span_create(
            span_id=_N9, trace_id=_TRACE, name="Format Output",
            start_time=make_timestamp(_BASE, 12.050),
            end_time=make_timestamp(_BASE, 12.100),
            parent_observation_id=_WF_SPAN,
            input={"text": "Refined report..."},
            output={"formatted": "# Renewable Energy Adoption Report\n\n## Key Findings\n..."},
        ),
    ))

    # 12. span-create (node 10: Tool — Validate Output)
    events.append(wrap_event(
        make_event_id("s07-e12"), make_timestamp(_BASE, 15.011),
        "span-create",
        make_span_create(
            span_id=_N10, trace_id=_TRACE, name="validate_output",
            start_time=make_timestamp(_BASE, 12.150),
            end_time=make_timestamp(_BASE, 12.400),
            parent_observation_id=_WF_SPAN,
            input={"text": "# Renewable Energy Adoption Report..."},
            output={"valid": True, "issues": []},
        ),
    ))

    # 13. span-create (node 11: Variable Aggregator)
    events.append(wrap_event(
        make_event_id("s07-e13"), make_timestamp(_BASE, 15.012),
        "span-create",
        make_span_create(
            span_id=_N11, trace_id=_TRACE, name="Merge Variables",
            start_time=make_timestamp(_BASE, 12.450),
            end_time=make_timestamp(_BASE, 12.460),
            parent_observation_id=_WF_SPAN,
            input={"variables": ["report", "stats", "validation"]},
            output={"merged": {"report": "...", "stats": {}, "valid": True}},
        ),
    ))

    # 14. generation-create (node 12: LLM — Final Polish, gpt-4o)
    events.append(wrap_event(
        make_event_id("s07-e14"), make_timestamp(_BASE, 15.013),
        "generation-create",
        make_generation_create(
            gen_id=_N12, trace_id=_TRACE, name=_GPT4O, model=_GPT4O,
            start_time=make_timestamp(_BASE, 12.500),
            end_time=make_timestamp(_BASE, 14.500),
            usage=_N12_USAGE,
            completion_start_time=make_timestamp(_BASE, 12.650),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": "Polish the final report"}]},
            output={"text": "Final polished report: Renewable energy adoption surged in 2024 with global capacity reaching 3.6 TW..."},
        ),
    ))

    # 15. span-create (node 13: IF/ELSE — Quality Check)
    events.append(wrap_event(
        make_event_id("s07-e15"), make_timestamp(_BASE, 15.014),
        "span-create",
        make_span_create(
            span_id=_N13, trace_id=_TRACE, name="Quality Check",
            start_time=make_timestamp(_BASE, 14.550),
            end_time=make_timestamp(_BASE, 14.560),
            parent_observation_id=_WF_SPAN,
            input={"quality_score": 0.92}, output={"passed": True},
        ),
    ))

    # 16. span-create (node 14: Code — Post-process)
    events.append(wrap_event(
        make_event_id("s07-e16"), make_timestamp(_BASE, 15.015),
        "span-create",
        make_span_create(
            span_id=_N14, trace_id=_TRACE, name="Post-process",
            start_time=make_timestamp(_BASE, 14.600),
            end_time=make_timestamp(_BASE, 14.800),
            parent_observation_id=_WF_SPAN,
            input={"report": "Final polished report..."},
            output={"report": "## Renewable Energy Adoption Report\n\nFinal version with formatting..."},
        ),
    ))

    # 17. span-create (node 15: End)
    events.append(wrap_event(
        make_event_id("s07-e17"), make_timestamp(_BASE, 15.016),
        "span-create",
        make_span_create(
            span_id=_N15, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 14.850),
            end_time=make_timestamp(_BASE, 14.900),
            parent_observation_id=_WF_SPAN,
            input={"report": "## Renewable Energy Adoption Report..."},
            output={"report": "## Renewable Energy Adoption Report\n\nFinal version..."},
        ),
    ))

    return events


def build_meta():
    nodes = [
        (3, "span-create", "Start"),
        (4, "generation-create", "Analyze Query (gpt-4o)"),
        (5, "span-create", "Knowledge Retrieval"),
        (6, "span-create", "Web Search (tool)"),
        (7, "generation-create", "Synthesize Answer (gpt-4o)"),
        (8, "span-create", "Check Confidence (IF/ELSE)"),
        (9, "span-create", "Fetch Additional Data (tool)"),
        (10, "generation-create", "Refine Response (claude-3-5-sonnet)"),
        (11, "span-create", "Format Output (Template)"),
        (12, "span-create", "Validate Output (tool)"),
        (13, "span-create", "Merge Variables (Aggregator)"),
        (14, "generation-create", "Final Polish (gpt-4o)"),
        (15, "span-create", "Quality Check (IF/ELSE)"),
        (16, "span-create", "Post-process (Code)"),
        (17, "span-create", "End"),
    ]
    return {
        "scenario_id": SCENARIO_ID,
        "scenario_description": SCENARIO_DESCRIPTION,
        "app_type": APP_TYPE,
        "dify_app_mode": DIFY_APP_MODE,
        "edge_case": EDGE_CASE,
        "trace_types_emitted": TRACE_TYPES_EMITTED,
        "expected_event_count": EXPECTED_EVENT_COUNT,
        "events_in_order": [
            {"index": 1, "type": "trace-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 2, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
        ] + [
            {"index": idx, "type": etype, "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"}
            for idx, etype, _name in nodes
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: high-N. 15-node workflow with 4 LLM nodes (gpt-4o x3, claude-3-5-sonnet x1), 3 tool calls, 8 other nodes. 17 sequential HTTP POSTs from one WorkflowTraceInfo task.",
    }
```

- [ ] **Step 4b: Run s07 test**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s07_workflow_15node -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 8: scenarios 06-07 workflow variants — 5node (7 events) + 15node high-N (17 events)"
```

---

## Task 9: Scenario 08 — chatflow-basic (11 events)

**Files:**
- Create: `traceset/scenarios/s08_chatflow_basic.py`
- Modify: `traceset/tests/test_scenarios.py` (add test function)

**Event order:** trace-create(wf) → span-create(wf-span) → [5 node events] → trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name)

- [ ] **Step 1: Write the failing test**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s08_chatflow_basic():
    from traceset.scenarios import s08_chatflow_basic
    _check_scenario(s08_chatflow_basic)
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s08_chatflow_basic -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

Create `traceset/scenarios/s08_chatflow_basic.py`:
```python
"""Scenario 08: Chatflow (advanced-chat), workflow + message + generate name.

Events (11):
  1.  trace-create       (WorkflowTraceInfo)
  2.  span-create        (workflow-level span)
  3.  span-create        (node 1: Start)
  4.  generation-create  (node 2: LLM — Classify Intent, gpt-4o-mini)
  5.  span-create        (node 3: Knowledge Retrieval)
  6.  generation-create  (node 4: LLM — Generate Response, gpt-4o-mini)
  7.  span-create        (node 5: End)
  8.  trace-create       (MessageTraceInfo)
  9.  generation-create  (MessageTraceInfo)
  10. trace-create       (GenerateNameTraceInfo, upsert)
  11. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "08-chatflow-basic"
SCENARIO_DESCRIPTION = "Chatflow (advanced-chat), workflow + message + generate name"
APP_TYPE = "chatflow"
DIFY_APP_MODE = "advanced-chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["WorkflowTraceInfo", "MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 11

_BASE = "2025-01-15T14:00:00.000000+00:00"
_TRACE = "a0b1c2d3-e4f5-4a6b-7c8d-9e0f1a2b3c4d"
_USER = "u-6f7a8b9c0d"
_CONV = "conv-8b9c0d1e2f3a"
_WF_SPAN = "b1c2d3e4-f5a6-4b7c-8d9e-0f1a2b3c4d5e"
_GEN_MSG = "c2d3e4f5-a6b7-4c8d-9e0f-1a2b3c4d5e6f"
_N1 = "d3e4f5a6-b7c8-4d9e-0f1a-2b3c4d5e6f7a"
_N2 = "e4f5a6b7-c8d9-4e0f-1a2b-3c4d5e6f7a8b"
_N3 = "f5a6b7c8-d9e0-4f1a-2b3c-4d5e6f7a8b9c"
_N4 = "a6b7c8d9-e0f1-4a2b-3c4d-5e6f7a8b9c0d"
_N5 = "b7c8d9e0-f1a2-4b3c-4d5e-6f7a8b9c0d1e"
_NAME_SPAN = "c8d9e0f1-a2b3-4c4d-5e6f-7a8b9c0d1e2f"

_QUERY = "What are the best practices for API versioning?"
_RAG_DOCS = [{"title": "API Versioning Guide", "content": "Use semantic versioning...", "score": 0.88}]
_N2_RESPONSE = "Intent: technical_question. Category: api_design. Confidence: 0.92."
_N4_RESPONSE = (
    "Best practices for API versioning: 1) Use semantic versioning (v1, v2). "
    "2) Version in the URL path (/api/v1/resource) or header. "
    "3) Deprecate old versions with clear timelines. 4) Maintain backward compatibility. "
    "5) Document breaking changes thoroughly."
)
_CONV_NAME = "API Versioning Best Practices"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 400}
_N2_USAGE = {"input": 45, "output": 25, "total": 70, "unit": "TOKENS",
             "inputCost": 0.000007, "outputCost": 0.000015, "totalCost": 0.000022}
_N4_USAGE = {"input": 120, "output": 110, "total": 230, "unit": "TOKENS",
             "inputCost": 0.000018, "outputCost": 0.000066, "totalCost": 0.000084}
_MSG_USAGE = {"input": 130, "output": 110, "total": 240, "unit": "TOKENS",
              "inputCost": 0.000020, "outputCost": 0.000066, "totalCost": 0.000086}


def build_events():
    events = []

    # 1. trace-create (Workflow)
    events.append(wrap_event(
        make_event_id("s08-e01"), make_timestamp(_BASE, 5.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Chatflow: API Q&A",
            user_id=_USER,
            input={"query": _QUERY},
            metadata={"workflow_id": "wf-008", "workflow_run_id": _TRACE,
                      "conversation_id": _CONV},
        ),
    ))

    # 2. span-create (workflow-level span)
    events.append(wrap_event(
        make_event_id("s08-e02"), make_timestamp(_BASE, 5.501),
        "span-create",
        make_span_create(
            span_id=_WF_SPAN, trace_id=_TRACE, name="workflow",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 5.400),
            input={"query": _QUERY}, output={"text": _N4_RESPONSE},
        ),
    ))

    # 3. span-create (node 1: Start)
    events.append(wrap_event(
        make_event_id("s08-e03"), make_timestamp(_BASE, 5.502),
        "span-create",
        make_span_create(
            span_id=_N1, trace_id=_TRACE, name="Start",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.010),
            parent_observation_id=_WF_SPAN,
            input={"query": _QUERY}, output={"query": _QUERY},
        ),
    ))

    # 4. generation-create (node 2: LLM — Classify Intent)
    events.append(wrap_event(
        make_event_id("s08-e04"), make_timestamp(_BASE, 5.503),
        "generation-create",
        make_generation_create(
            gen_id=_N2, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.050),
            end_time=make_timestamp(_BASE, 1.200),
            usage=_N2_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.150),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _N2_RESPONSE},
        ),
    ))

    # 5. span-create (node 3: Knowledge Retrieval)
    events.append(wrap_event(
        make_event_id("s08-e05"), make_timestamp(_BASE, 5.504),
        "span-create",
        make_span_create(
            span_id=_N3, trace_id=_TRACE, name="Knowledge Retrieval",
            start_time=make_timestamp(_BASE, 1.250),
            end_time=make_timestamp(_BASE, 2.000),
            parent_observation_id=_WF_SPAN,
            input={"query": "API versioning best practices"},
            output={"documents": _RAG_DOCS},
        ),
    ))

    # 6. generation-create (node 4: LLM — Generate Response)
    events.append(wrap_event(
        make_event_id("s08-e06"), make_timestamp(_BASE, 5.505),
        "generation-create",
        make_generation_create(
            gen_id=_N4, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 2.050),
            end_time=make_timestamp(_BASE, 5.300),
            usage=_N4_USAGE,
            completion_start_time=make_timestamp(_BASE, 2.200),
            model_parameters=_PARAMS,
            parent_observation_id=_WF_SPAN,
            input={"messages": [{"role": "user", "content": _QUERY}],
                   "context": _RAG_DOCS},
            output={"text": _N4_RESPONSE},
        ),
    ))

    # 7. span-create (node 5: End)
    events.append(wrap_event(
        make_event_id("s08-e07"), make_timestamp(_BASE, 5.506),
        "span-create",
        make_span_create(
            span_id=_N5, trace_id=_TRACE, name="End",
            start_time=make_timestamp(_BASE, 5.350),
            end_time=make_timestamp(_BASE, 5.400),
            parent_observation_id=_WF_SPAN,
            input={"text": _N4_RESPONSE}, output={"text": _N4_RESPONSE},
        ),
    ))

    # 8. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s08-e08"), make_timestamp(_BASE, 5.510),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatflow", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV,
                      "workflow_run_id": _TRACE},
        ),
    ))

    # 9. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s08-e09"), make_timestamp(_BASE, 5.511),
        "generation-create",
        make_generation_create(
            gen_id=_GEN_MSG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.050),
            end_time=make_timestamp(_BASE, 5.350),
            usage=_MSG_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.200),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _N4_RESPONSE},
        ),
    ))

    # 10. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s08-e10"), make_timestamp(_BASE, 6.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 11. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s08-e11"), make_timestamp(_BASE, 6.801),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 6.400),
            end_time=make_timestamp(_BASE, 6.750),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _CONV_NAME},
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
        "events_in_order": [
            {"index": 1, "type": "trace-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 2, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 3, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 4, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 6, "type": "generation-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 7, "type": "span-create", "source_trace_type": "WorkflowTraceInfo", "dify_handler": "LangFuseDataTrace.workflow_trace"},
            {"index": 8, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 9, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 10, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 11, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Chatflow = workflow + message. The workflow produces the response; the message trace records it for the conversation.",
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s08_chatflow_basic -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 9: scenario 08 chatflow-basic — Workflow+Message+GenName (11 events)"
```

---

## Task 10: Scenarios 09-10 — Moderation

**Files:**
- Create: `traceset/scenarios/s09_moderation_blocked.py`
- Create: `traceset/scenarios/s10_moderation_pass_through.py`
- Modify: `traceset/tests/test_scenarios.py` (add 2 test functions)

> **Open question (spec section 5):** Scenario 09 (moderation-blocked) may not emit a `generation-create` event since there's no real LLM call. The spec table assumes 3 events. This plan follows that assumption. **Verify against `message_service.py` / `input_moderation.py` source** during implementation. If no generation is emitted, adjust `EXPECTED_EVENT_COUNT` to 2 and remove event 3.

### Scenario 09: moderation-blocked (3 events, edge: blocked)

**Event order:** span-create(mod) → trace-create(msg) → generation-create(msg, preset response)

- [ ] **Step 1a: Write failing test for s09**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s09_moderation_blocked():
    from traceset.scenarios import s09_moderation_blocked
    _check_scenario(s09_moderation_blocked)
```

- [ ] **Step 2a: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s09_moderation_blocked -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3a: Write s09 implementation**

Create `traceset/scenarios/s09_moderation_blocked.py`:
```python
"""Scenario 09: Moderation blocks the user input (edge: blocked).

Events (3):
  1. span-create        (ModerationTraceInfo — blocked)
  2. trace-create       (MessageTraceInfo — preset response)
  3. generation-create  (MessageTraceInfo — preset response, no real LLM call)

VERIFY: does MessageTraceInfo emit generation-create when moderation blocks?
If not, this scenario has 2 events.
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "09-moderation-blocked"
SCENARIO_DESCRIPTION = "Chatbot with moderation that blocks the input, preset response returned"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = "moderation-blocked"
TRACE_TYPES_EMITTED = ["ModerationTraceInfo", "MessageTraceInfo"]
EXPECTED_EVENT_COUNT = 3

_BASE = "2025-01-15T14:30:00.000000+00:00"
_TRACE = "d1e2f3a4-b5c6-4d7e-8f9a-0b1c2d3e4f5a"
_USER = "u-7a8b9c0d1e"
_CONV = "conv-9c0d1e2f3a4b"
_MOD_SPAN = "e2f3a4b5-c6d7-4e8f-9a0b-1c2d3e4f5a6b"
_GEN = "f3a4b5c6-d7e8-4f9a-0b1c-2d3e4f5a6b7c"

_QUERY = "Write something inappropriate that triggers moderation."
_PRESET_RESPONSE = (
    "Sorry, your message has been flagged by our moderation system "
    "and cannot be processed. Please rephrase your message and try again."
)

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.0, "max_tokens": 100}
_USAGE = {"input": 15, "output": 40, "total": 55, "unit": "TOKENS",
          "inputCost": 0.000002, "outputCost": 0.000024, "totalCost": 0.000026}


def build_events():
    events = []

    # 1. span-create (Moderation — blocked)
    events.append(wrap_event(
        make_event_id("s09-e01"), make_timestamp(_BASE, 0.500),
        "span-create",
        make_span_create(
            span_id=_MOD_SPAN, trace_id=_TRACE,
            name="input_moderation",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.350),
            input={"query": _QUERY},
            output={"flagged": True, "action": "blocked",
                    "categories": ["violence", "hate"]},
            level="WARNING",
            status_message="Input blocked by moderation: flagged categories [violence, hate]",
        ),
    ))

    # 2. trace-create (Message — preset response)
    events.append(wrap_event(
        make_event_id("s09-e02"), make_timestamp(_BASE, 0.501),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV,
                      "moderation_blocked": True},
        ),
    ))

    # 3. generation-create (Message — preset response)
    events.append(wrap_event(
        make_event_id("s09-e03"), make_timestamp(_BASE, 0.502),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.350),
            end_time=make_timestamp(_BASE, 0.400),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.360),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _PRESET_RESPONSE},
            metadata={"preset_response": True, "moderation_blocked": True},
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
        "events_in_order": [
            {"index": 1, "type": "span-create", "source_trace_type": "ModerationTraceInfo", "dify_handler": "LangFuseDataTrace.moderation_trace"},
            {"index": 2, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: moderation-blocked. Preset response without real LLM call. VERIFY: does generation-create fire?",
    }
```

- [ ] **Step 4a: Run s09 test**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s09_moderation_blocked -v
```
Expected: PASS

### Scenario 10: moderation-pass-through (5 events)

**Event order:** span-create(mod) → trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name)

- [ ] **Step 1b: Write failing test for s10**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s10_moderation_pass_through():
    from traceset.scenarios import s10_moderation_pass_through
    _check_scenario(s10_moderation_pass_through)
```

- [ ] **Step 2b: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s10_moderation_pass_through -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3b: Write s10 implementation**

Create `traceset/scenarios/s10_moderation_pass_through.py`:
```python
"""Scenario 10: Moderation passes, normal chat proceeds.

Events (5):
  1. span-create        (ModerationTraceInfo — passed)
  2. trace-create       (MessageTraceInfo)
  3. generation-create  (MessageTraceInfo)
  4. trace-create       (GenerateNameTraceInfo, upsert)
  5. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "10-moderation-pass-through"
SCENARIO_DESCRIPTION = "Chatbot with moderation that passes, normal chat proceeds"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = None
TRACE_TYPES_EMITTED = ["ModerationTraceInfo", "MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 5

_BASE = "2025-01-15T15:00:00.000000+00:00"
_TRACE = "a4b5c6d7-e8f9-4a0b-1c2d-3e4f5a6b7c8d"
_USER = "u-8b9c0d1e2f"
_CONV = "conv-0d1e2f3a4b5c"
_MOD_SPAN = "b5c6d7e8-f9a0-4b1c-2d3e-4f5a6b7c8d9e"
_GEN = "c6d7e8f9-a0b1-4c2d-3e4f-5a6b7c8d9e0f"
_NAME_SPAN = "d7e8f9a0-b1c2-4d3e-4f5a-6b7c8d9e0f1a"

_QUERY = "What are the safety considerations for rock climbing?"
_LLM_RESPONSE = (
    "Key safety considerations for rock climbing: 1) Always wear a helmet "
    "and harness. 2) Check all gear before each climb. 3) Use proper "
    "communication signals between climber and belayer. 4) Inspect anchors "
    "and knots. 5) Know your limits and don't push beyond your skill level. "
    "6) Check weather conditions. 7) Have a first aid kit and emergency plan."
)
_CONV_NAME = "Rock Climbing Safety"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 400}
_USAGE = {"input": 42, "output": 130, "total": 172, "unit": "TOKENS",
          "inputCost": 0.000006, "outputCost": 0.000078, "totalCost": 0.000084}


def build_events():
    events = []

    # 1. span-create (Moderation — passed)
    events.append(wrap_event(
        make_event_id("s10-e01"), make_timestamp(_BASE, 2.800),
        "span-create",
        make_span_create(
            span_id=_MOD_SPAN, trace_id=_TRACE,
            name="input_moderation",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.200),
            input={"query": _QUERY},
            output={"flagged": False, "action": "pass"},
        ),
    ))

    # 2. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s10-e02"), make_timestamp(_BASE, 2.801),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV},
        ),
    ))

    # 3. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s10-e03"), make_timestamp(_BASE, 2.802),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.250),
            end_time=make_timestamp(_BASE, 2.700),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.400),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 4. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s10-e04"), make_timestamp(_BASE, 4.000),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 5. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s10-e05"), make_timestamp(_BASE, 4.001),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 3.600),
            end_time=make_timestamp(_BASE, 3.950),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _CONV_NAME},
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
        "events_in_order": [
            {"index": 1, "type": "span-create", "source_trace_type": "ModerationTraceInfo", "dify_handler": "LangFuseDataTrace.moderation_trace"},
            {"index": 2, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 4, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Moderation passes (level=DEFAULT), normal chat proceeds with GenerateName.",
    }
```

- [ ] **Step 4b: Run s10 test**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s10_moderation_pass_through -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 10: scenarios 09-10 moderation — blocked (3 events, edge) + pass-through (5 events)"
```

---

## Task 11: Scenarios 11-12 — RAG-Empty + Tool-Failure

**Files:**
- Create: `traceset/scenarios/s11_rag_empty_results.py`
- Create: `traceset/scenarios/s12_tool_failure.py`
- Modify: `traceset/tests/test_scenarios.py` (add 2 test functions)

### Scenario 11: rag-empty-results (5 events, edge: empty)

**Event order:** span-create(rag, empty docs) → trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name)

- [ ] **Step 1a: Write failing test for s11**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s11_rag_empty_results():
    from traceset.scenarios import s11_rag_empty_results
    _check_scenario(s11_rag_empty_results)
```

- [ ] **Step 2a: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s11_rag_empty_results -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3a: Write s11 implementation**

Create `traceset/scenarios/s11_rag_empty_results.py`:
```python
"""Scenario 11: RAG retrieval returns no documents (edge: empty-rag).

Events (5):
  1. span-create        (DatasetRetrievalTraceInfo — empty results)
  2. trace-create       (MessageTraceInfo)
  3. generation-create  (MessageTraceInfo — LLM answers without context)
  4. trace-create       (GenerateNameTraceInfo, upsert)
  5. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "11-rag-empty-results"
SCENARIO_DESCRIPTION = "Chatbot with RAG that returns zero documents, LLM answers without context"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = "empty-rag"
TRACE_TYPES_EMITTED = ["DatasetRetrievalTraceInfo", "MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 5

_BASE = "2025-01-15T15:30:00.000000+00:00"
_TRACE = "b5c6d7e8-f9a0-4b1c-2d3e-4f5a6b7c8d9e"
_USER = "u-9c0d1e2f3a"
_CONV = "conv-1e2f3a4b5c6d"
_RAG_SPAN = "c6d7e8f9-a0b1-4c2d-3e4f-5a6b7c8d9e0f"
_GEN = "d7e8f9a0-b1c2-4d3e-4f5a-6b7c8d9e0f1a"
_NAME_SPAN = "e8f9a0b1-c2d3-4e4f-5a6b-7c8d9e0f1a2b"

_QUERY = "What is the internal policy for sabbatical leave?"
_LLM_RESPONSE = (
    "I don't have specific information about internal sabbatical leave policies "
    "in my knowledge base. I'd recommend checking your company's HR portal or "
    "contacting your HR representative directly for the most accurate and "
    "up-to-date information regarding sabbatical leave eligibility, duration, "
    "and application procedures."
)
_CONV_NAME = "Sabbatical Leave Policy Inquiry"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 300}
_USAGE = {"input": 38, "output": 95, "total": 133, "unit": "TOKENS",
          "inputCost": 0.000006, "outputCost": 0.000057, "totalCost": 0.000063}


def build_events():
    events = []

    # 1. span-create (DatasetRetrieval — empty results)
    events.append(wrap_event(
        make_event_id("s11-e01"), make_timestamp(_BASE, 2.500),
        "span-create",
        make_span_create(
            span_id=_RAG_SPAN, trace_id=_TRACE,
            name="dataset_retrieval",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 0.150),
            input={"query": _QUERY},
            output={"documents": [], "result_count": 0},
        ),
    ))

    # 2. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s11-e02"), make_timestamp(_BASE, 2.501),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV,
                      "rag_empty": True},
        ),
    ))

    # 3. generation-create (Message — LLM answers without context)
    events.append(wrap_event(
        make_event_id("s11-e03"), make_timestamp(_BASE, 2.502),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.200),
            end_time=make_timestamp(_BASE, 2.400),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.350),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 4. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s11-e04"), make_timestamp(_BASE, 3.700),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 5. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s11-e05"), make_timestamp(_BASE, 3.701),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 3.300),
            end_time=make_timestamp(_BASE, 3.650),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _CONV_NAME},
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
        "events_in_order": [
            {"index": 1, "type": "span-create", "source_trace_type": "DatasetRetrievalTraceInfo", "dify_handler": "LangFuseDataTrace.dataset_retrieval_trace"},
            {"index": 2, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 4, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: empty-rag. DatasetRetrieval returns 0 documents. LLM answers without context. RAG span emitted before message events.",
    }
```

- [ ] **Step 4a: Run s11 test**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s11_rag_empty_results -v
```
Expected: PASS

### Scenario 12: tool-failure (5 events, edge: error)

**Event order:** span-create(tool, level=ERROR, statusMessage=error) → trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name)

- [ ] **Step 1b: Write failing test for s12**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s12_tool_failure():
    from traceset.scenarios import s12_tool_failure
    _check_scenario(s12_tool_failure)
```

- [ ] **Step 2b: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s12_tool_failure -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3b: Write s12 implementation**

Create `traceset/scenarios/s12_tool_failure.py`:
```python
"""Scenario 12: Agent tool call fails (edge: tool-error).

Events (5):
  1. span-create        (ToolTraceInfo — level=ERROR, statusMessage=error)
  2. trace-create       (MessageTraceInfo)
  3. generation-create  (MessageTraceInfo — LLM responds despite tool failure)
  4. trace-create       (GenerateNameTraceInfo, upsert)
  5. span-create        (GenerateNameTraceInfo)
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "12-tool-failure"
SCENARIO_DESCRIPTION = "Agent app where the tool call fails, LLM responds without tool data"
APP_TYPE = "agent"
DIFY_APP_MODE = "agent-chat"
EDGE_CASE = "tool-error"
TRACE_TYPES_EMITTED = ["ToolTraceInfo", "MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 5

_BASE = "2025-01-15T16:00:00.000000+00:00"
_TRACE = "c6d7e8f9-a0b1-4c2d-3e4f-5a6b7c8d9e0f"
_USER = "u-0d1e2f3a4b"
_CONV = "conv-2f3a4b5c6d7e"
_TOOL_SPAN = "d7e8f9a0-b1c2-4d3e-4f5a-6b7c8d9e0f1a"
_GEN = "e8f9a0b1-c2d3-4e4f-5a6b-7c8d9e0f1a2b"
_NAME_SPAN = "f9a0b1c2-d3e4-4f5a-6b7c-8d9e0f1a2b3c"

_QUERY = "Look up the stock price for AAPL."
_TOOL_INPUT = {"symbol": "AAPL", "endpoint": "/api/v1/stocks"}
_TOOL_ERROR_MSG = "ConnectionError: Failed to connect to stock API (timeout after 5000ms)"

_LLM_RESPONSE = (
    "I attempted to look up the stock price for AAPL, but the stock data "
    "service is currently unavailable due to a connection timeout. Please "
    "try again in a moment, or check a financial website like Yahoo Finance "
    "or Google Finance for the latest AAPL stock price."
)
_CONV_NAME = "AAPL Stock Price Lookup Error"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 200}
_USAGE = {"input": 55, "output": 72, "total": 127, "unit": "TOKENS",
          "inputCost": 0.000008, "outputCost": 0.000043, "totalCost": 0.000051}


def build_events():
    events = []

    # 1. span-create (Tool — ERROR)
    events.append(wrap_event(
        make_event_id("s12-e01"), make_timestamp(_BASE, 5.200),
        "span-create",
        make_span_create(
            span_id=_TOOL_SPAN, trace_id=_TRACE,
            name="stock_api",
            start_time=make_timestamp(_BASE, 0.000),
            end_time=make_timestamp(_BASE, 5.000),
            input=_TOOL_INPUT,
            output={"error": _TOOL_ERROR_MSG},
            level="ERROR",
            status_message=_TOOL_ERROR_MSG,
        ),
    ))

    # 2. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s12-e02"), make_timestamp(_BASE, 5.201),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Agent", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV,
                      "tool_failed": True},
        ),
    ))

    # 3. generation-create (Message — LLM responds despite tool failure)
    events.append(wrap_event(
        make_event_id("s12-e03"), make_timestamp(_BASE, 5.202),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 5.050),
            end_time=make_timestamp(_BASE, 5.150),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 5.080),
            model_parameters=_PARAMS,
            input={
                "messages": [
                    {"role": "user", "content": _QUERY},
                    {"role": "tool", "content": _TOOL_ERROR_MSG},
                ],
            },
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 4. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s12-e04"), make_timestamp(_BASE, 6.400),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 5. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s12-e05"), make_timestamp(_BASE, 6.401),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 6.000),
            end_time=make_timestamp(_BASE, 6.350),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _CONV_NAME},
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
        "events_in_order": [
            {"index": 1, "type": "span-create", "source_trace_type": "ToolTraceInfo", "dify_handler": "LangFuseDataTrace.tool_trace"},
            {"index": 2, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 4, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 5, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: tool-error. Tool span has level=ERROR and statusMessage. LLM responds with an apology message despite the tool failure.",
    }
```

- [ ] **Step 4b: Run s12 test**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s12_tool_failure -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 11: scenarios 11-12 — rag-empty (5 events, edge) + tool-failure (5 events, edge)"
```

---

## Task 12: Scenarios 13-14 — Suggested-Questions-Error + Streaming

**Files:**
- Create: `traceset/scenarios/s13_suggested_questions_error.py`
- Create: `traceset/scenarios/s14_message_streaming.py`
- Modify: `traceset/tests/test_scenarios.py` (add 2 test functions)

### Scenario 13: suggested-questions-error (5 events, edge: error)

**Event order:** trace-create(msg) → generation-create(msg) → trace-create(name) → span-create(name) → generation-create(sugg-q, level=ERROR)

- [ ] **Step 1a: Write failing test for s13**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s13_suggested_questions_error():
    from traceset.scenarios import s13_suggested_questions_error
    _check_scenario(s13_suggested_questions_error)
```

- [ ] **Step 2a: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s13_suggested_questions_error -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3a: Write s13 implementation**

Create `traceset/scenarios/s13_suggested_questions_error.py`:
```python
"""Scenario 13: Suggested questions generation fails (edge: sugg-q-error).

Events (5):
  1. trace-create       (MessageTraceInfo)
  2. generation-create  (MessageTraceInfo)
  3. trace-create       (GenerateNameTraceInfo, upsert)
  4. span-create        (GenerateNameTraceInfo)
  5. generation-create  (SuggestedQuestionTraceInfo — level=ERROR)

The suggested-questions LLM call fails. The generation-create event for it
has level=ERROR and a statusMessage with the error.
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "13-suggested-questions-error"
SCENARIO_DESCRIPTION = "Chatbot where suggested questions generation fails with an error"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = "suggested-questions-error"
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "GenerateNameTraceInfo", "SuggestedQuestionTraceInfo"]
EXPECTED_EVENT_COUNT = 5

_BASE = "2025-01-15T16:30:00.000000+00:00"
_TRACE = "d7e8f9a0-b1c2-4d3e-4f5a-6b7c8d9e0f1a"
_USER = "u-1e2f3a4b5c"
_CONV = "conv-3a4b5c6d7e8f"
_GEN_MSG = "e8f9a0b1-c2d3-4e4f-5a6b-7c8d9e0f1a2b"
_NAME_SPAN = "f9a0b1c2-d3e4-4f5a-6b7c-8d9e0f1a2b3c"
_SUGG_GEN = "a0b1c2d3-e4f5-4a6b-7c8d-9e0f1a2b3c4d"

_QUERY = "What is the difference between TCP and UDP?"
_LLM_RESPONSE = (
    "TCP (Transmission Control Protocol) is connection-oriented, ensuring "
    "reliable, ordered delivery of data through handshakes, acknowledgments, "
    "and retransmission. UDP (User Datagram Protocol) is connectionless, "
    "prioritizing speed over reliability — it sends datagrams without "
    "guaranteeing delivery or order. TCP is used for web browsing, email, "
    "and file transfer; UDP for streaming, gaming, and DNS."
)
_CONV_NAME = "TCP vs UDP"

_SUGG_ERROR_MSG = "RateLimitError: Rate limit exceeded for model gpt-4o-mini (429 Too Many Requests)"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.5, "max_tokens": 400}
_MSG_USAGE = {"input": 42, "output": 130, "total": 172, "unit": "TOKENS",
              "inputCost": 0.000006, "outputCost": 0.000078, "totalCost": 0.000084}
# Suggested questions failed — minimal usage (input tokens consumed before error)
_SUGG_USAGE = {"input": 85, "output": 0, "total": 85, "unit": "TOKENS",
               "inputCost": 0.000013, "outputCost": 0.000000, "totalCost": 0.000013}


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s13-e01"), make_timestamp(_BASE, 2.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV},
        ),
    ))

    # 2. generation-create (Message)
    events.append(wrap_event(
        make_event_id("s13-e02"), make_timestamp(_BASE, 2.801),
        "generation-create",
        make_generation_create(
            gen_id=_GEN_MSG, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 2.700),
            usage=_MSG_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.250),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _LLM_RESPONSE},
        ),
    ))

    # 3. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s13-e03"), make_timestamp(_BASE, 4.000),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 4. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s13-e04"), make_timestamp(_BASE, 4.001),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 3.600),
            end_time=make_timestamp(_BASE, 3.950),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _CONV_NAME},
        ),
    ))

    # 5. generation-create (SuggestedQuestion — ERROR)
    events.append(wrap_event(
        make_event_id("s13-e05"), make_timestamp(_BASE, 5.200),
        "generation-create",
        make_generation_create(
            gen_id=_SUGG_GEN, trace_id=_TRACE,
            name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 4.100),
            end_time=make_timestamp(_BASE, 5.100),
            usage=_SUGG_USAGE,
            completion_start_time=make_timestamp(_BASE, 4.150),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "assistant", "content": _LLM_RESPONSE}]},
            output={"error": _SUGG_ERROR_MSG},
            level="ERROR",
            status_message=_SUGG_ERROR_MSG,
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
        "events_in_order": [
            {"index": 1, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 2, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 5, "type": "generation-create", "source_trace_type": "SuggestedQuestionTraceInfo", "dify_handler": "LangFuseDataTrace.suggested_question_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: suggested-questions-error. SuggestedQuestion generation-create has level=ERROR and statusMessage with rate limit error. The LLM call consumed input tokens before failing (output=0).",
    }
```

- [ ] **Step 4a: Run s13 test**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s13_suggested_questions_error -v
```
Expected: PASS

### Scenario 14: message-streaming (4 events, edge: streaming)

**Event order:** trace-create(msg) → generation-create(msg, streaming fields) → trace-create(name) → span-create(name)

> **Open question (spec section 7):** The Dify-internal `gen_ai_server_time_to_first_token` and `llm_streaming_time_to_generate` fields do NOT have direct Langfuse wire equivalents — they are carried in the generation body's `metadata` field. The exact metadata key names should be verified against the `LangFuseDataTrace.message_trace` source. The implementation below uses the key names from the spec's description.

- [ ] **Step 1b: Write failing test for s14**

Add to `traceset/tests/test_scenarios.py`:
```python
def test_s14_message_streaming():
    from traceset.scenarios import s14_message_streaming
    _check_scenario(s14_message_streaming)
```

- [ ] **Step 2b: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s14_message_streaming -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3b: Write s14 implementation**

Create `traceset/scenarios/s14_message_streaming.py`:
```python
"""Scenario 14: Streaming chat with TTFT/TTG metadata (edge: streaming).

Events (4):
  1. trace-create       (MessageTraceInfo)
  2. generation-create  (MessageTraceInfo — streaming fields)
  3. trace-create       (GenerateNameTraceInfo, upsert)
  4. span-create        (GenerateNameTraceInfo)

The generation-create event includes:
  - completionStartTime (set for all generations, but especially relevant for streaming)
  - metadata with streaming latency metrics:
    - gen_ai_server_time_to_first_token (TTFT in milliseconds)
    - llm_streaming_time_to_generate (total generation time in milliseconds)

VERIFY: the exact metadata key names against LangFuseDataTrace.message_trace source.
"""
from traceset.helpers import (
    make_event_id, make_timestamp,
    make_trace_create, make_span_create, make_generation_create,
    wrap_event,
)

SCENARIO_ID = "14-message-streaming"
SCENARIO_DESCRIPTION = "Streaming chatbot response with TTFT/TTG latency metadata"
APP_TYPE = "chatbot"
DIFY_APP_MODE = "chat"
EDGE_CASE = "streaming"
TRACE_TYPES_EMITTED = ["MessageTraceInfo", "GenerateNameTraceInfo"]
EXPECTED_EVENT_COUNT = 4

_BASE = "2025-01-15T17:00:00.000000+00:00"
_TRACE = "e8f9a0b1-c2d3-4e4f-5a6b-7c8d9e0f1a2b"
_USER = "u-2f3a4b5c6d"
_CONV = "conv-4b5c6d7e8f9a"
_GEN = "f9a0b1c2-d3e4-4f5a-6b7c-8d9e0f1a2b3c"
_NAME_SPAN = "a0b1c2d3-e4f5-4a6b-7c8d-9e0f1a2b3c4d"

_QUERY = "Write a short essay about the importance of open source software."
_LLM_RESPONSE = (
    "Open source software is the backbone of modern technology infrastructure. "
    "From the Linux kernel that powers most of the internet's servers to the "
    "Apache web server, from the Python and JavaScript languages that drive "
    "application development to the Kubernetes platform that orchestrates "
    "cloud-native deployments, open source projects enable innovation at a "
    "scale that no single company could match. The collaborative model of "
    "open source development — where anyone can read, modify, and contribute "
    "code — produces software that is often more secure, more reliable, and "
    "more adaptable than proprietary alternatives. The transparency enables "
    "peer review, the license freedom prevents vendor lock-in, and the "
    "community-driven roadmap ensures that features serve users rather than "
    "shareholders. Open source is not just a licensing model; it is a "
    "philosophy of knowledge sharing that has transformed how we build software."
)
_CONV_NAME = "Open Source Software Essay"

_MODEL = "gpt-4o-mini"
_PARAMS = {"temperature": 0.7, "max_tokens": 1000}
_USAGE = {"input": 48, "output": 285, "total": 333, "unit": "TOKENS",
          "inputCost": 0.000007, "outputCost": 0.000171, "totalCost": 0.000178}

# Streaming latency metrics (in milliseconds)
_TTFT_MS = 320       # Time to first token
_TTG_MS = 4150       # Total time to generate


def build_events():
    events = []

    # 1. trace-create (Message)
    events.append(wrap_event(
        make_event_id("s14-e01"), make_timestamp(_BASE, 4.500),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Dify Chatbot", user_id=_USER,
            input={"query": _QUERY}, session_id=_CONV,
            metadata={"user_id": _USER, "conversation_id": _CONV,
                      "streaming": True},
        ),
    ))

    # 2. generation-create (Message — streaming fields)
    #    completionStartTime = base + 0.320s (TTFT)
    #    metadata carries Dify-internal streaming latency metrics
    events.append(wrap_event(
        make_event_id("s14-e02"), make_timestamp(_BASE, 4.501),
        "generation-create",
        make_generation_create(
            gen_id=_GEN, trace_id=_TRACE, name=_MODEL, model=_MODEL,
            start_time=make_timestamp(_BASE, 0.100),
            end_time=make_timestamp(_BASE, 4.250),
            usage=_USAGE,
            completion_start_time=make_timestamp(_BASE, 0.320),
            model_parameters=_PARAMS,
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _LLM_RESPONSE},
            metadata={
                "streaming": True,
                "gen_ai_server_time_to_first_token": _TTFT_MS,
                "llm_streaming_time_to_generate": _TTG_MS,
            },
        ),
    ))

    # 3. trace-create (GenerateName, upsert)
    events.append(wrap_event(
        make_event_id("s14-e03"), make_timestamp(_BASE, 5.800),
        "trace-create",
        make_trace_create(
            trace_id=_TRACE, name="Generate Name", user_id=_USER,
        ),
    ))

    # 4. span-create (GenerateName)
    events.append(wrap_event(
        make_event_id("s14-e04"), make_timestamp(_BASE, 5.801),
        "span-create",
        make_span_create(
            span_id=_NAME_SPAN, trace_id=_TRACE,
            name="Generate Name",
            start_time=make_timestamp(_BASE, 5.400),
            end_time=make_timestamp(_BASE, 5.750),
            input={"messages": [{"role": "user", "content": _QUERY}]},
            output={"text": _CONV_NAME},
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
        "events_in_order": [
            {"index": 1, "type": "trace-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 2, "type": "generation-create", "source_trace_type": "MessageTraceInfo", "dify_handler": "LangFuseDataTrace.message_trace"},
            {"index": 3, "type": "trace-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
            {"index": 4, "type": "span-create", "source_trace_type": "GenerateNameTraceInfo", "dify_handler": "LangFuseDataTrace.generate_name_trace"},
        ],
        "dify_commit": "b33e8f0ddb1189427548b0e1206cedcdc17d9bb6",
        "langfuse_sdk_version": ">=4.2.0,<5.0.0",
        "notes": "Edge: streaming. generation-create includes completionStartTime and metadata with gen_ai_server_time_to_first_token (320ms TTFT) and llm_streaming_time_to_generate (4150ms TTG). VERIFY: exact metadata key names against LangFuseDataTrace.message_trace source.",
    }
```

- [ ] **Step 4b: Run s14 test**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_scenarios.py::test_s14_message_streaming -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 12: scenarios 13-14 — sugg-q-error (5 events, edge) + streaming (4 events, edge)"
```

---

## Task 13: Scenario Registry + Generation Script

**Files:**
- Modify: `traceset/scenarios/__init__.py` (replace placeholder with full registry)
- Create: `traceset/generate_traceset.py`
- Create: `traceset/tests/test_generate_traceset.py`

- [ ] **Step 1: Write the failing test**

Create `traceset/tests/test_generate_traceset.py`:
```python
"""Tests for the generation script."""
import json
import os
import tempfile
import pytest

from traceset.scenarios import SCENARIOS
from traceset.generate_traceset import generate_scenario


def test_scenarios_registry_has_14():
    assert len(SCENARIOS) == 14, f"Expected 14 scenarios, got {len(SCENARIOS)}"


def test_all_scenario_ids_unique():
    ids = [s.SCENARIO_ID for s in SCENARIOS]
    assert len(ids) == len(set(ids)), f"Duplicate scenario IDs: {ids}"


def test_generate_scenario_writes_files(tmp_path):
    from traceset.scenarios import s01_chat_basic
    generate_scenario(s01_chat_basic, str(tmp_path))

    scenario_dir = tmp_path / "01-chat-basic"
    assert scenario_dir.exists()

    events_path = scenario_dir / "events.jsonl"
    assert events_path.exists()
    lines = events_path.read_text().strip().split("\n")
    assert len(lines) == 4

    for line in lines:
        event = json.loads(line)
        assert "id" in event
        assert "timestamp" in event
        assert "type" in event
        assert "body" in event

    meta_path = scenario_dir / "meta.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta["scenario_id"] == "01-chat-basic"
    assert meta["expected_event_count"] == 4


def test_generate_scenario_self_checks(tmp_path):
    """Verify the self-checks (event count, monotonic, no snake_case) pass."""
    from traceset.scenarios import s07_workflow_15node
    # This should not raise
    generate_scenario(s07_workflow_15node, str(tmp_path))
    assert (tmp_path / "07-workflow-15node" / "events.jsonl").exists()


def test_generate_all_scenarios(tmp_path):
    for scenario in SCENARIOS:
        generate_scenario(scenario, str(tmp_path))
        scenario_dir = tmp_path / scenario.SCENARIO_ID
        assert scenario_dir.exists(), f"Missing dir for {scenario.SCENARIO_ID}"
        events_path = scenario_dir / "events.jsonl"
        lines = events_path.read_text().strip().split("\n")
        assert len(lines) == scenario.EXPECTED_EVENT_COUNT, (
            f"{scenario.SCENARIO_ID}: {len(lines)} events != {scenario.EXPECTED_EVENT_COUNT}"
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_generate_traceset.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'traceset.generate_traceset'` (and `SCENARIOS` is still empty `[]`)

- [ ] **Step 3: Write the implementation**

Replace `traceset/scenarios/__init__.py` with:
```python
"""Scenario registry. Imports all 14 scenario modules and exposes SCENARIOS list."""
from . import (
    s01_chat_basic,
    s02_chat_rag,
    s03_completion_basic,
    s04_agent_single_tool,
    s05_agent_multi_tool,
    s06_workflow_5node,
    s07_workflow_15node,
    s08_chatflow_basic,
    s09_moderation_blocked,
    s10_moderation_pass_through,
    s11_rag_empty_results,
    s12_tool_failure,
    s13_suggested_questions_error,
    s14_message_streaming,
)

SCENARIOS = [
    s01_chat_basic,
    s02_chat_rag,
    s03_completion_basic,
    s04_agent_single_tool,
    s05_agent_multi_tool,
    s06_workflow_5node,
    s07_workflow_15node,
    s08_chatflow_basic,
    s09_moderation_blocked,
    s10_moderation_pass_through,
    s11_rag_empty_results,
    s12_tool_failure,
    s13_suggested_questions_error,
    s14_message_streaming,
]
```

Create `traceset/generate_traceset.py`:
```python
#!/usr/bin/env python3
"""Generate the Dify trace reference catalog.

For each scenario:
  1. Build events via scenario.build_events()
  2. Validate each event via schema.validate_event()
  3. Run self-checks (event count, monotonic timestamps, no snake_case)
  4. Write events.jsonl and meta.json

Also generates root files: catalog.json, README.md, schema.md (Task 14).
"""
from __future__ import annotations

import json
import os
import sys

from traceset.schema import validate_event
from traceset.scenarios import SCENARIOS

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def generate_scenario(scenario, base_dir: str) -> dict:
    """Generate events.jsonl and meta.json for one scenario.

    Returns a catalog entry dict.
    """
    scenario_id = scenario.SCENARIO_ID
    scenario_dir = os.path.join(base_dir, scenario_id)
    os.makedirs(scenario_dir, exist_ok=True)

    events = scenario.build_events()
    meta = scenario.build_meta()

    # ── Validate each event against the wire schema ─────────────────
    for event in events:
        validate_event(event)

    # ── Self-checks ─────────────────────────────────────────────────
    # 1. Event count matches EXPECTED_EVENT_COUNT
    assert len(events) == scenario.EXPECTED_EVENT_COUNT, (
        f"{scenario_id}: event count {len(events)} != "
        f"{scenario.EXPECTED_EVENT_COUNT}"
    )

    # 2. All event types are valid
    valid_types = {"trace-create", "span-create", "generation-create"}
    for i, e in enumerate(events):
        assert e["type"] in valid_types, (
            f"{scenario_id}[{i}]: invalid type {e['type']}"
        )

    # 3. No snake_case body keys
    for i, e in enumerate(events):
        for key in e["body"]:
            assert "_" not in key, (
                f"{scenario_id}[{i}]: snake_case body key '{key}'"
            )

    # 4. Timestamps monotonically non-decreasing
    timestamps = [e["timestamp"] for e in events]
    assert timestamps == sorted(timestamps), (
        f"{scenario_id}: timestamps not monotonic"
    )

    # 5. meta events_in_order count matches event count
    assert len(meta["events_in_order"]) == len(events), (
        f"{scenario_id}: meta events_in_order count "
        f"{len(meta['events_in_order'])} != event count {len(events)}"
    )

    # 6. meta events_in_order types match actual events
    for i, (e, m) in enumerate(zip(events, meta["events_in_order"]), 1):
        assert m["index"] == i, f"{scenario_id}: meta index mismatch at {i}"
        assert m["type"] == e["type"], (
            f"{scenario_id}[{i}]: meta type {m['type']} != event type {e['type']}"
        )

    # ── Write events.jsonl ──────────────────────────────────────────
    events_path = os.path.join(scenario_dir, "events.jsonl")
    with open(events_path, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    # ── Write meta.json ─────────────────────────────────────────────
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
        "trace_types": scenario.TRACE_TYPES_EMITTED,
    }


def main():
    base_dir = _BASE_DIR

    print("Generating Dify trace reference catalog...")
    catalog_entries = []
    for scenario in SCENARIOS:
        entry = generate_scenario(scenario, base_dir)
        catalog_entries.append(entry)
        print(f"  {entry['scenario_id']}: {entry['event_count']} events")

    total_events = sum(e["event_count"] for e in catalog_entries)
    print(f"\nTotal: {total_events} events across {len(SCENARIOS)} scenarios")

    # Verify all 7 trace types are represented
    all_types = set()
    for s in SCENARIOS:
        all_types.update(s.TRACE_TYPES_EMITTED)
    expected_types = {
        "MessageTraceInfo", "WorkflowTraceInfo", "ModerationTraceInfo",
        "DatasetRetrievalTraceInfo", "ToolTraceInfo", "GenerateNameTraceInfo",
        "SuggestedQuestionTraceInfo",
    }
    assert all_types == expected_types, (
        f"Missing trace types: {expected_types - all_types}"
    )

    # Verify all 5 Dify app modes are represented
    all_modes = {s.DIFY_APP_MODE for s in SCENARIOS}
    expected_modes = {"chat", "completion", "agent-chat", "workflow", "advanced-chat"}
    assert all_modes == expected_modes, (
        f"Missing app modes: {expected_modes - all_modes}"
    )

    print("All self-checks passed.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_generate_traceset.py -v
```
Expected: PASS — 5 tests passed

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 13: scenario registry + generation script — 14 scenarios, validation, self-checks"
```

---

## Task 14: Root Files Generation

**Files:**
- Modify: `traceset/generate_traceset.py` (add `generate_catalog`, `generate_readme`, `generate_schema_doc`)
- Modify: `traceset/tests/test_generate_traceset.py` (add tests for root files)

- [ ] **Step 1: Write the failing test**

Add to `traceset/tests/test_generate_traceset.py`:
```python
def test_generate_catalog(tmp_path):
    from traceset.generate_traceset import generate_catalog
    generate_catalog(SCENARIOS, str(tmp_path))
    catalog_path = tmp_path / "catalog.json"
    assert catalog_path.exists()
    catalog = json.loads(catalog_path.read_text())
    assert len(catalog) == 14
    entry = catalog[0]
    assert "scenario_id" in entry
    assert "app_type" in entry
    assert "event_count" in entry
    assert "trace_types" in entry


def test_generate_readme(tmp_path):
    from traceset.generate_traceset import generate_readme
    generate_readme(SCENARIOS, str(tmp_path))
    readme_path = tmp_path / "README.md"
    assert readme_path.exists()
    content = readme_path.read_text()
    assert "Dify App Trace Reference Catalog" in content
    assert "01-chat-basic" in content
    assert "14-message-streaming" in content


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
    """Run main() in a temp dir and verify all outputs exist."""
    import importlib
    from traceset import generate_traceset as gt
    gt._BASE_DIR = str(tmp_path)
    gt.main()

    # Root files
    assert (tmp_path / "catalog.json").exists()
    assert (tmp_path / "README.md").exists()
    assert (tmp_path / "schema.md").exists()

    # All 14 scenario directories
    for s in SCENARIOS:
        scenario_dir = tmp_path / s.SCENARIO_ID
        assert (scenario_dir / "events.jsonl").exists(), f"Missing events.jsonl for {s.SCENARIO_ID}"
        assert (scenario_dir / "meta.json").exists(), f"Missing meta.json for {s.SCENARIO_ID}"
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_generate_traceset.py -v
```
Expected: FAIL — `ImportError: cannot import name 'generate_catalog' from 'traceset.generate_traceset'`

- [ ] **Step 3: Write the implementation**

Add the following functions to `traceset/generate_traceset.py` (after `generate_scenario`, before `main`):
```python
def generate_catalog(scenarios, base_dir: str) -> None:
    """Generate catalog.json — machine-readable index of all scenarios."""
    catalog = []
    for scenario in scenarios:
        events = scenario.build_events()
        catalog.append({
            "scenario_id": scenario.SCENARIO_ID,
            "app_type": scenario.APP_TYPE,
            "dify_app_mode": scenario.DIFY_APP_MODE,
            "edge_case": scenario.EDGE_CASE,
            "event_count": len(events),
            "trace_types": scenario.TRACE_TYPES_EMITTED,
        })

    catalog_path = os.path.join(base_dir, "catalog.json")
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)
        f.write("\n")


def generate_readme(scenarios, base_dir: str) -> None:
    """Generate README.md — catalog overview and how-to-read guide."""
    total_events = sum(len(s.build_events()) for s in scenarios)

    lines = [
        "# Dify App Trace Reference Catalog",
        "",
        "A collection of 14 reference Dify app traces, captured as Langfuse wire events.",
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
        "| # | Directory | App Type | Events | Edge? |",
        "|---|---|---|---|---|",
    ]

    for s in scenarios:
        events = s.build_events()
        edge = s.EDGE_CASE or ""
        num = s.SCENARIO_ID.split("-")[0]
        lines.append(
            f"| {num} | `{s.SCENARIO_ID}` | {s.APP_TYPE} | {len(events)} | {edge} |"
        )

    lines.extend([
        "",
        f"**Total**: {total_events} events across {len(scenarios)} scenarios.",
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

This document describes the wire event format used in the Dify trace catalog.

## Event Envelope

Every event in `events.jsonl` has this structure:

```json
{
  "id": "<uuid v4>",
  "timestamp": "<ISO 8601 UTC, e.g. 2025-01-15T10:30:00.123456+00:00>",
  "type": "trace-create" | "span-create" | "generation-create",
  "body": { ... type-specific ... }
}
```

- `id`: event ID (UUID, generated by Dify's `_make_event_id()`)
- `timestamp`: event creation time (ISO 8601 UTC, from Dify's `_now_iso()`)
- `type`: one of `trace-create`, `span-create`, `generation-create`
- `body`: type-specific payload (see below)

## trace-create body

```json
{
  "id": "<trace_id>",
  "name": "<string>",
  "userId": "<string>",
  "input": "<any>",
  "output": "<any>",
  "sessionId": "<string>",
  "version": "<string>",
  "release": "<string>",
  "metadata": "<any>",
  "tags": ["<string>"],
  "public": "<bool>"
}
```

All fields optional; only populated fields appear on the wire.

## span-create body

```json
{
  "id": "<span_id>",
  "traceId": "<trace_id>",
  "name": "<string>",
  "startTime": "<ISO 8601>",
  "endTime": "<ISO 8601>",
  "metadata": "<any>",
  "input": "<any>",
  "output": "<any>",
  "level": "DEBUG" | "DEFAULT" | "WARNING" | "ERROR",
  "statusMessage": "<string>",
  "parentObservationId": "<string>",
  "version": "<string>",
  "environment": "<string>"
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
    "unit": "CHARACTERS" | "TOKENS",
    "inputCost": "<float>",
    "outputCost": "<float>",
    "totalCost": "<float>"
  },
  "costDetails": { "<key>": "<float>" },
  "promptName": "<string>",
  "promptVersion": "<int>"
}
```

## Serialization rules

- **camelCase**: all body field names are camelCase on the wire.
- **exclude_unset + exclude_none**: only fields with non-None values appear.
- **1 event per POST**: Dify sends one event per HTTP POST to `/api/public/ingestion`.

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
```

Also update `main()` to call the new functions. Replace the existing `main()` function with:
```python
def main():
    base_dir = _BASE_DIR

    print("Generating Dify trace reference catalog...")
    catalog_entries = []
    for scenario in SCENARIOS:
        entry = generate_scenario(scenario, base_dir)
        catalog_entries.append(entry)
        print(f"  {entry['scenario_id']}: {entry['event_count']} events")

    # Generate root files
    generate_catalog(SCENARIOS, base_dir)
    print("  catalog.json")
    generate_readme(SCENARIOS, base_dir)
    print("  README.md")
    generate_schema_doc(base_dir)
    print("  schema.md")

    total_events = sum(e["event_count"] for e in catalog_entries)
    print(f"\nTotal: {total_events} events across {len(SCENARIOS)} scenarios")

    # Verify all 7 trace types are represented
    all_types = set()
    for s in SCENARIOS:
        all_types.update(s.TRACE_TYPES_EMITTED)
    expected_types = {
        "MessageTraceInfo", "WorkflowTraceInfo", "ModerationTraceInfo",
        "DatasetRetrievalTraceInfo", "ToolTraceInfo", "GenerateNameTraceInfo",
        "SuggestedQuestionTraceInfo",
    }
    assert all_types == expected_types, (
        f"Missing trace types: {expected_types - all_types}"
    )

    # Verify all 5 Dify app modes are represented
    all_modes = {s.DIFY_APP_MODE for s in SCENARIOS}
    expected_modes = {"chat", "completion", "agent-chat", "workflow", "advanced-chat"}
    assert all_modes == expected_modes, (
        f"Missing app modes: {expected_modes - all_modes}"
    )

    print("All self-checks passed.")
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/test_generate_traceset.py -v
```
Expected: PASS — 9 tests passed

- [ ] **Step 5: Commit**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 14: root files generation — catalog.json, README.md, schema.md"
```

---

## Task 15: Full Run + Verification

**Files:**
- None (verification only — no new files created)

- [ ] **Step 1: Run the full test suite**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m pytest tests/ -v
```
Expected: PASS — all tests pass (schema: 8, helpers: 12, scenarios: 14, generate: 9 = 43 tests)

- [ ] **Step 2: Run the generation script**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -m traceset.generate_traceset
```
Expected output:
```
Generating Dify trace reference catalog...
  01-chat-basic: 4 events
  02-chat-rag: 6 events
  03-completion-basic: 4 events
  04-agent-single-tool: 5 events
  05-agent-multi-tool: 7 events
  06-workflow-5node: 7 events
  07-workflow-15node: 17 events
  08-chatflow-basic: 11 events
  09-moderation-blocked: 3 events
  10-moderation-pass-through: 5 events
  11-rag-empty-results: 5 events
  12-tool-failure: 5 events
  13-suggested-questions-error: 5 events
  14-message-streaming: 4 events
  catalog.json
  README.md
  schema.md

Total: 88 events across 14 scenarios
All self-checks passed.
```

- [ ] **Step 3: Verify all 14 scenario directories exist**

Run:
```bash
ls -d /Users/bruce/Projects/opencode-go/dify-deepdive/traceset/[0-9]*-*
```
Expected: 14 directories listed (01-chat-basic through 14-message-streaming)

- [ ] **Step 4: Verify event counts match expected**

Run:
```bash
for d in /Users/bruce/Projects/opencode-go/dify-deepdive/traceset/[0-9]*-*/; do
  scenario=$(basename "$d")
  count=$(wc -l < "$d/events.jsonl" | tr -d ' ')
  echo "$scenario: $count events"
done
```
Expected:
```
01-chat-basic: 4 events
02-chat-rag: 6 events
03-completion-basic: 4 events
04-agent-single-tool: 5 events
05-agent-multi-tool: 7 events
06-workflow-5node: 7 events
07-workflow-15node: 17 events
08-chatflow-basic: 11 events
09-moderation-blocked: 3 events
10-moderation-pass-through: 5 events
11-rag-empty-results: 5 events
12-tool-failure: 5 events
13-suggested-questions-error: 5 events
14-message-streaming: 4 events
```
Total: 88 events.

- [ ] **Step 5: Verify no snake_case keys in any events.jsonl**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -c "
import json, glob
for path in sorted(glob.glob('[0-9]*-*/events.jsonl')):
    with open(path) as f:
        for i, line in enumerate(f, 1):
            event = json.loads(line)
            for key in event['body']:
                if '_' in key:
                    print(f'SNAKE_CASE: {path}:{i} body key {key}')
                    exit(1)
print('No snake_case body keys found in any scenario.')
"
```
Expected: "No snake_case body keys found in any scenario."

- [ ] **Step 6: Verify catalog.json has 14 entries**

Run:
```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive/traceset && python -c "
import json
catalog = json.load(open('catalog.json'))
assert len(catalog) == 14, f'Expected 14, got {len(catalog)}'
total = sum(e['event_count'] for e in catalog)
assert total == 88, f'Expected 88 total events, got {total}'
print(f'catalog.json: {len(catalog)} scenarios, {total} total events. OK.')
"
```
Expected: "catalog.json: 14 scenarios, 88 total events. OK."

- [ ] **Step 7: Commit generated files**

```bash
cd /Users/bruce/Projects/opencode-go/dify-deepdive && git add traceset/ && git commit -m "task 15: full run + verification — 14 scenarios, 88 events, all self-checks pass"
```

---

## Self-Review

### 1. Spec Coverage

| Spec Section | Task(s) |
|---|---|
| §1 Overview (7 trace types, 5 app types, 6 edge cases) | Tasks 4-12 (14 scenarios), Task 15 (verification) |
| §2 Scenario catalog (14 scenarios, 88 events) | Tasks 4-12 (14 scenarios), Task 13 (registry), Task 15 (count verification) |
| §3 Directory structure (traceset/ layout) | Task 1 (setup), Task 13 (generate_traceset.py), Task 14 (root files) |
| §4 Wire event format (envelope + 3 body types) | Task 2 (schema validator), Task 3 (helpers), Task 14 (schema.md) |
| §5 Per-scenario event composition (emission order) | Tasks 4-12 (per-scenario event ordering), Task 13 (self-check: monotonic timestamps) |
| §6 meta.json schema | Task 3 (helpers), Tasks 4-12 (build_meta per scenario), Task 13 (self-check: meta/events consistency) |
| §7 Realism conventions | Tasks 4-12 (real model names, token counts, UUIDs, timestamps, text content, costs) |
| §8 Generation & validation method | Task 2 (schema validation), Task 3 (deterministic UUIDs/timestamps), Task 13 (generation script + self-checks) |
| §9 Scope boundaries | All tasks (wire events only, no TraceInfo, no latency replay) |
| §10 References | Task 1 (pyproject.toml with langfuse dep), Task 14 (schema.md with source mapping) |

### 2. Placeholder Scan

- **TBD/TODO**: None found. Every step has complete code.
- **"similar to Task N"**: None found. Each scenario has full build_events() and build_meta() code.
- **"add error handling"**: None found. All error handling is explicit (try/except in schema.py, assertions in generate_traceset.py).
- **Ellipsis (...) in code**: None in implementation code. Some LLM response text uses "..." for readability in JSON reference blocks (Task 4), but the actual Python string constants are complete.

### 3. Type Consistency

| Helper function | Signature | Used consistently across tasks |
|---|---|---|
| `make_event_id(seed: str) -> str` | Task 3 | Tasks 4-14: all call as `make_event_id("sNN-eMM")` |
| `make_timestamp(base: str, offset_seconds: float) -> str` | Task 3 | Tasks 4-14: all call as `make_timestamp(_BASE, X.XXX)` |
| `make_trace_create(trace_id, name, user_id=None, **kwargs) -> dict` | Task 3 | Tasks 4-14: kwargs include `input`, `session_id`, `metadata` — all converted to camelCase |
| `make_span_create(span_id, trace_id, name, start_time, end_time, **kwargs) -> dict` | Task 3 | Tasks 4-14: kwargs include `parent_observation_id`, `level`, `status_message`, `input`, `output` |
| `make_generation_create(gen_id, trace_id, name, model, start_time, end_time, usage, **kwargs) -> dict` | Task 3 | Tasks 4-14: kwargs include `completion_start_time`, `model_parameters`, `input`, `output`, `metadata`, `level`, `status_message` |
| `wrap_event(event_id, timestamp, event_type, body) -> dict` | Task 3 | Tasks 4-14: all events wrapped consistently |
| `validate_event(event: dict) -> None` | Task 2 | Task 13: called in generate_scenario for each event |
| `to_camel_case(snake: str) -> str` | Task 3 | Used internally by make_*_create functions |

### 4. Event Count Verification

| Scenario | Expected | Trace Types |
|---|---|---|
| 01-chat-basic | 4 | Message + GenerateName |
| 02-chat-rag | 6 | Message + DatasetRetrieval + SuggestedQuestion + GenerateName |
| 03-completion-basic | 4 | Message + GenerateName |
| 04-agent-single-tool | 5 | Message + Tool + GenerateName |
| 05-agent-multi-tool | 7 | Message + Tool×3 + GenerateName |
| 06-workflow-5node | 7 | WorkflowTraceInfo (5 nodes) |
| 07-workflow-15node | 17 | WorkflowTraceInfo (15 nodes, edge: high-N) |
| 08-chatflow-basic | 11 | Workflow + Message + GenerateName |
| 09-moderation-blocked | 3 | Moderation + Message (edge: blocked) |
| 10-moderation-pass-through | 5 | Moderation + Message + GenerateName |
| 11-rag-empty-results | 5 | DatasetRetrieval + Message + GenerateName (edge: empty) |
| 12-tool-failure | 5 | Tool + Message + GenerateName (edge: error) |
| 13-suggested-questions-error | 5 | Message + GenerateName + SuggestedQuestion (edge: error) |
| 14-message-streaming | 4 | Message + GenerateName (edge: streaming) |
| **Total** | **88** | All 7 trace types, all 5 app modes, 6 edge cases |

### 5. Open Questions Flagged for Verification

1. **Scenario 09 (moderation-blocked)**: Does `MessageTraceInfo` emit `generation-create` when moderation blocks? Spec assumes yes (3 events). If not, adjust to 2 events. (Flagged in Task 10, Step 3a.)
2. **Scenario 14 (streaming)**: Exact metadata key names for TTFT/TTG (`gen_ai_server_time_to_first_token`, `llm_streaming_time_to_generate`). Verify against `LangFuseDataTrace.message_trace` source. (Flagged in Task 12, Step 3b.)

Both are noted in the scenario module docstrings and meta.json notes fields.
