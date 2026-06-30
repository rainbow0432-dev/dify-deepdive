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
