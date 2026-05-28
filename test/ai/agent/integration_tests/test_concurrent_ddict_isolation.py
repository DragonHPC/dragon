"""Integration tests — concurrent multi-task DDict isolation on a single agent.

Verifies that when an agent handles multiple tasks concurrently (via
asyncio.create_task inside listen), each task's DDict reads/writes are
fully isolated by dispatch_id — no cross-contamination of results, status,
or event counters.

Run with:  dragon python -m unittest test.ai.agent.integration_tests.test_concurrent_ddict_isolation -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp


import asyncio
import json
import uuid
from unittest import TestCase, main

from dragon.data.ddict import DDict
from dragon.native.event import Event

from dragon.ai.agent.config import (
    AgentConfig,
    DispatchHeader,
    TaskStatus,
    RESULT_KEY,
    STATUS_KEY,
    DISPATCH_ID_KEY,
    GLOBAL_STATE_KEY,
    GLOBAL_STATE_ENTRY_KEY,
    USER_INPUT_KEY,
    LLM_EVENT_COUNT_KEY,
    TOOL_EVENT_COUNT_KEY,
    TOOL_EVENT_KEY,
)
from dragon.ai.agent.ddict import DDictAccessor
from dragon.ai.agent.communication.message import Message
from dragon.ai.agent.tools.registry import ToolRegistry
from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher
from dragon.ai.agent.core.sub_agent import SubAgent

from .conftest import FakeLLMEngine


# ---------------------------------------------------------------------------
# Helper — build a SubAgent with injected FakeLLMEngine
# ---------------------------------------------------------------------------

def _build_sub_agent(agent_id, role, llm_engine, tool_registry=None):
    """Construct a SubAgent bypassing __init__ (no inference pipeline)."""
    config = AgentConfig(agent_id=agent_id, name=f"Test {agent_id}", role=role)
    registry = tool_registry or ToolRegistry()
    dispatcher = ToolDispatcher(llm_engine, registry)

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


def _seed_ddict(ddict, task_id, agent_id, user_input="Do task."):
    """Write the minimal DDict keys the agent expects before processing."""
    accessor = DDictAccessor(ddict, agent_id=agent_id, task_id=task_id)
    accessor.put(USER_INPUT_KEY.format(task_id=task_id), user_input)
    accessor.put(
        GLOBAL_STATE_KEY.format(task_id=task_id),
        [{"agent_id": "user", "answer": user_input}],
    )
    return accessor


# ========================================================================
# Concurrent _handle_message — dispatch_id key isolation
# ========================================================================

class TestConcurrentDispatchIsolation(TestCase):
    """Two concurrent _handle_message calls on the same agent must not collide."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_two_concurrent_tasks_isolated_results(self):
        """Two concurrent tasks write different results under their own dispatch_ids."""
        agent_id = "worker"
        dispatch_a = str(uuid.uuid4())
        dispatch_b = str(uuid.uuid4())

        _seed_ddict(self.ddict, self.task_id, agent_id, "Task A input")

        # Each task gets its own LLM engine with distinct answers
        llm_a = FakeLLMEngine(responses=["Answer A"])
        llm_b = FakeLLMEngine(responses=["Answer B"])
        agent_a = _build_sub_agent(agent_id, "worker", llm_a)
        agent_b = _build_sub_agent(agent_id, "worker", llm_b)

        accessor = DDictAccessor(self.ddict, agent_id=agent_id, task_id=self.task_id)

        header_a = DispatchHeader(
            task="Do task A.",
            serialized_ddict=self.ddict.serialize(),
            dispatch_id=dispatch_a,
        )
        header_b = DispatchHeader(
            task="Do task B.",
            serialized_ddict=self.ddict.serialize(),
            dispatch_id=dispatch_b,
        )

        # Run both concurrently
        async def run_both():
            task_a = asyncio.create_task(
                agent_a.process(self.task_id, header_a, accessor)
            )
            task_b = asyncio.create_task(
                agent_b.process(self.task_id, header_b, accessor)
            )
            return await asyncio.gather(task_a, task_b)

        result_a, result_b = asyncio.run(run_both())

        self.assertEqual(result_a["response"], "Answer A")
        self.assertEqual(result_b["response"], "Answer B")

    def test_concurrent_tasks_independent_status(self):
        """Each concurrent task writes its own STATUS_KEY independently."""
        agent_id = "worker"
        dispatch_a = str(uuid.uuid4())
        dispatch_b = str(uuid.uuid4())

        _seed_ddict(self.ddict, self.task_id, agent_id)

        llm = FakeLLMEngine(responses=["Done A", "Done B"])
        agent = _build_sub_agent(agent_id, "worker", llm)

        accessor = DDictAccessor(self.ddict, agent_id=agent_id, task_id=self.task_id)

        msg_a = Message(
            task_id=self.task_id, sender_id="orch", recipient_id=agent_id,
            header=DispatchHeader(
                task="A", serialized_ddict=self.ddict.serialize(),
                dispatch_id=dispatch_a,
            ),
        )
        msg_b = Message(
            task_id=self.task_id, sender_id="orch", recipient_id=agent_id,
            header=DispatchHeader(
                task="B", serialized_ddict=self.ddict.serialize(),
                dispatch_id=dispatch_b,
            ),
        )

        async def run_both():
            await asyncio.gather(
                agent._handle_message(msg_a),
                agent._handle_message(msg_b),
            )

        asyncio.run(run_both())

        status_a = accessor.get(STATUS_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_a,
        ))
        status_b = accessor.get(STATUS_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_b,
        ))
        self.assertEqual(status_a, TaskStatus.DONE)
        self.assertEqual(status_b, TaskStatus.DONE)


class TestConcurrentHandleMessageWithEvents(TestCase):
    """Concurrent _handle_message calls each signal their own Event."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_concurrent_handle_message_events(self):
        """Two concurrent _handle_message calls each fire their own completion Event."""
        agent_id = "worker"
        dispatch_a = str(uuid.uuid4())
        dispatch_b = str(uuid.uuid4())

        _seed_ddict(self.ddict, self.task_id, agent_id)

        llm = FakeLLMEngine(responses=["Result"] * 2)
        agent = _build_sub_agent(agent_id, "worker", llm)

        accessor = DDictAccessor(self.ddict, agent_id=agent_id, task_id=self.task_id)

        event_a = Event()
        event_b = Event()

        msg_a = Message(
            task_id=self.task_id, sender_id="orch", recipient_id=agent_id,
            header=DispatchHeader(
                task="A", serialized_ddict=self.ddict.serialize(),
                dispatch_id=dispatch_a, completion_event=event_a,
            ),
        )
        msg_b = Message(
            task_id=self.task_id, sender_id="orch", recipient_id=agent_id,
            header=DispatchHeader(
                task="B", serialized_ddict=self.ddict.serialize(),
                dispatch_id=dispatch_b, completion_event=event_b,
            ),
        )

        async def run_both():
            await asyncio.gather(
                agent._handle_message(msg_a),
                agent._handle_message(msg_b),
            )

        asyncio.run(run_both())

        self.assertTrue(event_a.is_set)
        self.assertTrue(event_b.is_set)

        # Clean up Events
        event_a.destroy()
        event_b.destroy()


class TestConcurrentToolCallIsolation(TestCase):
    """Concurrent tasks with tool calls write independent event counters."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_tool_event_counts_isolated(self):
        """Two concurrent tasks with tool calls maintain separate event counters."""
        agent_id = "analyst"
        dispatch_a = str(uuid.uuid4())
        dispatch_b = str(uuid.uuid4())

        _seed_ddict(self.ddict, self.task_id, agent_id)

        registry = ToolRegistry()

        @registry.tool
        def double(n: int) -> int:
            """Double a number."""
            return n * 2

        tool_call = json.dumps({
            "response": {
                "type": "tool_request",
                "tool_calls": [{"name": "double", "args": {"n": 5}}],
            }
        })
        final_answer = json.dumps({
            "response": {"type": "final_answer", "content": "Result: 10"}
        })

        llm_a = FakeLLMEngine(responses=[tool_call, final_answer])
        llm_b = FakeLLMEngine(responses=[tool_call, final_answer])
        agent_a = _build_sub_agent(agent_id, "analyst", llm_a, tool_registry=registry)
        agent_b = _build_sub_agent(agent_id, "analyst", llm_b, tool_registry=registry)

        accessor = DDictAccessor(self.ddict, agent_id=agent_id, task_id=self.task_id)

        header_a = DispatchHeader(
            task="Double 5", serialized_ddict=self.ddict.serialize(),
            dispatch_id=dispatch_a, tracing=True,
        )
        header_b = DispatchHeader(
            task="Double 5", serialized_ddict=self.ddict.serialize(),
            dispatch_id=dispatch_b, tracing=True,
        )

        async def run_both():
            return await asyncio.gather(
                agent_a.process(self.task_id, header_a, accessor),
                agent_b.process(self.task_id, header_b, accessor),
            )

        asyncio.run(run_both())

        # Each dispatch should have its own tool event count
        count_key_a = TOOL_EVENT_COUNT_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_a,
        )
        count_key_b = TOOL_EVENT_COUNT_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_b,
        )
        count_a = accessor.get(count_key_a)
        count_b = accessor.get(count_key_b)
        self.assertEqual(count_a, 1)
        self.assertEqual(count_b, 1)

        # Verify each dispatch wrote its own tool event
        event_key_a = TOOL_EVENT_KEY.format(
            task_id=self.task_id, agent_id=agent_id,
            dispatch_id=dispatch_a, index=0,
        )
        event_key_b = TOOL_EVENT_KEY.format(
            task_id=self.task_id, agent_id=agent_id,
            dispatch_id=dispatch_b, index=0,
        )
        event_a = accessor.get(event_key_a)
        event_b = accessor.get(event_key_b)
        self.assertEqual(event_a["name"], "double")
        self.assertEqual(event_b["name"], "double")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
