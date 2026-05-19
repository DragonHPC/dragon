"""Shared test setup for agent integration tests.

This module sets up Dragon multiprocessing.  Shared helpers (FakeLLMEngine,
FailingLLMEngine, IntegrationTestCase) are importable by test modules.
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp
import json
import uuid
from typing import Any, Optional
from unittest import TestCase

from dragon.data.ddict import DDict

from dragon.ai.agent.config import (
    AgentConfig,
    MCPServerConfig,
    OrchestratorConfig,
    MemoryConfig,
    MemoryStrategy,
    DispatchHeader,
    TaskStatus,
    PipelineNode,
    Pipeline,
    TaskResult,
    RESULT_KEY,
    STATUS_KEY,
    DISPATCH_ID_KEY,
    GLOBAL_STATE_KEY,
    GLOBAL_STATE_ENTRY_KEY,
    USER_INPUT_KEY,
    LLM_EVENT_COUNT_KEY,
    TOOL_EVENT_COUNT_KEY,
    MEMORY_EVENT_COUNT_KEY,
    HITL_EVENT_COUNT_KEY,
    TRACE_KEY,
)
from dragon.ai.agent.ddict import DDictAccessor
from dragon.ai.agent.communication.message import Message
from dragon.ai.agent.tools.registry import ToolRegistry
from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher
from dragon.ai.agent.core.sub_agent import SubAgent


mp.set_start_method("dragon")


# ---------------------------------------------------------------------------
# Fake LLM engine (runs in-process, no inference pipeline)
# ---------------------------------------------------------------------------

class FakeLLMEngine:
    """Deterministic async LLM engine for integration tests.

    Returns pre-configured responses in order.  Supports both the plain-chat
    path (no tools) and the structured-output tool-call path.
    """

    def __init__(self, responses: Optional[list] = None) -> None:
        self._responses = list(responses or [])
        self._call_count = 0
        self.calls: list = []

    async def chat(self, prompts, tools=None, json_schema=None,
                   continue_final_message=False) -> str:
        self.calls.append({
            "prompts": prompts,
            "tools": tools,
            "json_schema": json_schema,
        })
        if self._call_count < len(self._responses):
            resp = self._responses[self._call_count]
        else:
            resp = '{"response": {"type": "final_answer", "content": "default"}}'
        self._call_count += 1
        return resp


class FailingLLMEngine:
    """LLM engine that raises on the Nth call (1-indexed).

    Calls before *fail_on* succeed with the corresponding response.
    The *fail_on*-th call raises *exc_type*.
    """

    def __init__(
        self,
        responses: Optional[list] = None,
        fail_on: int = 1,
        exc_type: type = RuntimeError,
        exc_msg: str = "LLM inference failure",
    ) -> None:
        self._responses = list(responses or [])
        self._fail_on = fail_on
        self._exc_type = exc_type
        self._exc_msg = exc_msg
        self._call_count = 0
        self.calls: list = []

    async def chat(self, prompts, tools=None, json_schema=None,
                   continue_final_message=False) -> str:
        self._call_count += 1
        self.calls.append({"prompts": prompts, "tools": tools})
        if self._call_count == self._fail_on:
            raise self._exc_type(self._exc_msg)
        idx = self._call_count - 1
        if idx < len(self._responses):
            return self._responses[idx]
        return '{"response": {"type": "final_answer", "content": "default"}}'


# ---------------------------------------------------------------------------
# Integration test base class (mirrors inference IntegrationTestCase)
# ---------------------------------------------------------------------------

class IntegrationTestCase(TestCase):
    """Base class for agent integration tests.

    Provides DDict lifecycle management and factory methods for configs,
    agents, and registries — eliminates boilerplate across test files.
    """

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    # -- config factories ---------------------------------------------------

    @staticmethod
    def get_agent_config(
        agent_id: str = "agent",
        role: str = "You are a helpful assistant.",
        memory: Optional[MemoryConfig] = None,
        approval_filter=None,
        max_tool_call_iterations: int = 20,
    ) -> AgentConfig:
        return AgentConfig(
            agent_id=agent_id,
            name=f"Test {agent_id}",
            role=role,
            memory=memory,
            approval_filter=approval_filter,
            max_tool_call_iterations=max_tool_call_iterations,
        )

    @staticmethod
    def get_memory_config_sliding(max_kept_turns: int = 2) -> MemoryConfig:
        return MemoryConfig(
            strategy=MemoryStrategy.SLIDING_WINDOW,
            max_kept_turns=max_kept_turns,
        )

    @staticmethod
    def get_memory_config_summarize(
        max_kept_turns: int = 4,
        summarize_after_turns: int = 3,
    ) -> MemoryConfig:
        return MemoryConfig(
            strategy=MemoryStrategy.SUMMARIZE,
            max_kept_turns=max_kept_turns,
            summarize_after_turns=summarize_after_turns,
        )

    @staticmethod
    def get_memory_config_full() -> MemoryConfig:
        return MemoryConfig(strategy=MemoryStrategy.FULL)

    # -- agent factory ------------------------------------------------------

    def build_sub_agent(
        self,
        agent_id: str,
        llm_engine,
        role: str = "assistant",
        tool_registry: Optional[ToolRegistry] = None,
        approval_filter=None,
        memory: Optional[MemoryConfig] = None,
    ) -> SubAgent:
        """Build a SubAgent with injected LLM engine (no inference pipeline)."""
        config = self.get_agent_config(
            agent_id=agent_id, role=role,
            memory=memory, approval_filter=approval_filter,
        )
        registry = tool_registry or ToolRegistry()
        context_manager = None
        if memory:
            from dragon.ai.agent.memory.context_manager import ContextManager
            context_manager = ContextManager(memory)
        dispatcher = ToolDispatcher(
            llm_engine, registry,
            approval_filter=approval_filter,
            context_manager=context_manager,
        )

        agent = object.__new__(SubAgent)
        agent.config = config
        agent.comm = None
        agent.tool_registry = registry
        agent.llm = llm_engine
        agent.tool_dispatcher = dispatcher
        agent.shutdown_event = None
        agent._pending_mcp_servers = []
        agent._system_prompt = agent._build_system_prompt()
        return agent

    # -- DDict seeding helpers ----------------------------------------------

    def seed_ddict(self, agent_id: str, user_input: str = "Do the task."):
        """Write the minimal DDict keys an agent expects before processing."""
        accessor = DDictAccessor(
            self.ddict, agent_id=agent_id, task_id=self.task_id,
        )
        accessor.put(
            USER_INPUT_KEY.format(task_id=self.task_id), user_input,
        )
        accessor.put(
            GLOBAL_STATE_KEY.format(task_id=self.task_id),
            [{"agent_id": "user", "answer": user_input}],
        )
        return accessor

    def make_header(
        self,
        task: str = "Do the task.",
        dispatch_id: Optional[str] = None,
        upstream_agent_ids: Optional[list] = None,
        tracing: bool = False,
        completion_event=None,
    ) -> DispatchHeader:
        return DispatchHeader(
            task=task,
            serialized_ddict=self.ddict.serialize(),
            dispatch_id=dispatch_id or str(uuid.uuid4()),
            upstream_agent_ids=upstream_agent_ids or [],
            tracing=tracing,
            completion_event=completion_event,
        )

    # -- response builders --------------------------------------------------

    @staticmethod
    def tool_request_response(tool_name: str, args: dict) -> str:
        return json.dumps({
            "response": {
                "type": "tool_request",
                "tool_calls": [{"name": tool_name, "args": args}],
            }
        })

    @staticmethod
    def final_answer_response(content: str) -> str:
        return json.dumps({
            "response": {"type": "final_answer", "content": content}
        })
