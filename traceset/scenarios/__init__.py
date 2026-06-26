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
