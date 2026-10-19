"""Configuration sub-package for the Dragon AI Agent framework."""

from .agent_config import AgentConfig, MCPServerConfig, OrchestratorConfig
from .memory_config import MemoryConfig, MemoryStrategy
from .pipeline import TaskStatus, PipelineNode, Pipeline, TaskResult
from .dispatch import DispatchHeader
from .ddict_keys import (
    RESULT_KEY,
    STATUS_KEY,
    DISPATCH_ID_KEY,
    USER_INPUT_KEY,
    GLOBAL_STATE_KEY,
    GLOBAL_STATE_ENTRY_KEY,
    LLM_EVENT_KEY,
    LLM_EVENT_COUNT_KEY,
    TOOL_EVENT_KEY,
    TOOL_EVENT_COUNT_KEY,
    HITL_EVENT_KEY,
    HITL_EVENT_COUNT_KEY,
    MEMORY_EVENT_KEY,
    MEMORY_EVENT_COUNT_KEY,
    EVENT_CURSOR_KEY,
    TRACE_KEY,
    TRACE_META_KEY,
    TRACE_INDEX_SLOT_KEY,
    HITL_REQUEST_KEY,
    HITL_RESPONSE_KEY,
    HITL_QUEUE_KEY,
)

__all__ = [
    # User-facing config
    "AgentConfig",
    "MCPServerConfig",
    "OrchestratorConfig",
    "MemoryConfig",
    "MemoryStrategy",
    # Pipeline
    "TaskStatus",
    "PipelineNode",
    "Pipeline",
    "TaskResult",
    # Internal dispatch
    "DispatchHeader",
    # DDict keys
    "RESULT_KEY",
    "STATUS_KEY",
    "DISPATCH_ID_KEY",
    "USER_INPUT_KEY",
    "GLOBAL_STATE_KEY",
    "GLOBAL_STATE_ENTRY_KEY",
    "LLM_EVENT_KEY",
    "LLM_EVENT_COUNT_KEY",
    "TOOL_EVENT_KEY",
    "TOOL_EVENT_COUNT_KEY",
    "HITL_EVENT_KEY",
    "HITL_EVENT_COUNT_KEY",
    "MEMORY_EVENT_KEY",
    "MEMORY_EVENT_COUNT_KEY",
    "EVENT_CURSOR_KEY",
    "TRACE_KEY",
    "TRACE_META_KEY",
    "TRACE_INDEX_SLOT_KEY",
    "HITL_REQUEST_KEY",
    "HITL_RESPONSE_KEY",
    "HITL_QUEUE_KEY",
]

