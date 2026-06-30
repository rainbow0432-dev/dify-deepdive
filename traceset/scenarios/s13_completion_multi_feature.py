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
