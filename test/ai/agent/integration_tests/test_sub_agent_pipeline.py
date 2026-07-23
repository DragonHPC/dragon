"""Integration tests for SubAgent end-to-end pipeline.

These tests verify the full agent flow: message arrives on queue → LLM
reasoning → tool dispatch → result written to DDict — using real Dragon
Queues, DDicts, and Events.

Run with: dragon python -m unittest test.ai.agent.integration_tests.test_sub_agent_pipeline -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp


import asyncio
import json
import uuid
from typing import Optional
from unittest import TestCase, main

from dragon.data.ddict import DDict
from dragon.native.queue import Queue
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
)
from dragon.ai.agent.ddict import DDictAccessor
from dragon.ai.agent.communication.message import Message
from dragon.ai.agent.tools.registry import ToolRegistry
from dragon.ai.agent.tools.function_tool import FunctionTool
from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher
from dragon.ai.agent.core.sub_agent import SubAgent

from .conftest import FakeLLMEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_sub_agent(
    agent_id: str,
    role: str,
    llm_engine: FakeLLMEngine,
    tool_registry: Optional[ToolRegistry] = None,
    approval_filter=None,
) -> SubAgent:
    """Build a SubAgent with a fake LLM engine (no inference pipeline).

    Bypasses the normal DragonAgent.__init__ which expects an inference_queue
    to auto-create a proxy.  Instead we inject the FakeLLMEngine directly
    into a ToolDispatcher.
    """
    config = AgentConfig(
        agent_id=agent_id,
        name=f"Test {agent_id}",
        role=role,
        approval_filter=approval_filter,
    )
    registry = tool_registry or ToolRegistry()
    dispatcher = ToolDispatcher(
        llm_engine, registry, approval_filter=approval_filter,
    )

    # Construct SubAgent without invoking __init__ (which requires inference_queue),
    # then wire fields manually.
    agent = object.__new__(SubAgent)
    agent.config = config
    agent.comm = None  # will use direct process() calls, not listen()
    agent.tool_registry = registry
    agent.llm = llm_engine
    agent.tool_dispatcher = dispatcher
    agent.shutdown_event = None
    agent._pending_mcp_servers = []
    agent._system_prompt = agent._build_system_prompt()
    return agent


# ========================================================================
# Test: single agent, no tools, plain LLM answer
# ========================================================================

class TestSubAgentNoTools(TestCase):
    """End-to-end: agent receives task, calls LLM (no tools), writes result to DDict."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_plain_llm_answer(self):
        """Agent with no tools does one LLM call and writes result."""
        agent_id = "researcher"
        dispatch_id = str(uuid.uuid4())

        # Seed DDict with global state (as orchestrator would)
        accessor = DDictAccessor(self.ddict, agent_id=agent_id, task_id=self.task_id)
        user_input_key = USER_INPUT_KEY.format(task_id=self.task_id)
        accessor.put(user_input_key, "What is quantum computing?")
        global_key = GLOBAL_STATE_KEY.format(task_id=self.task_id)
        accessor.put(global_key, [{"agent_id": "user", "answer": "What is quantum computing?"}])

        # Build agent with a simple plain-text response
        llm = FakeLLMEngine(responses=["Quantum computing uses qubits."])
        agent = _build_sub_agent(agent_id, "researcher", llm)

        # Build header and run
        header = DispatchHeader(
            task="Explain quantum computing.",
            serialized_ddict=self.ddict.serialize(),
            dispatch_id=dispatch_id,
        )
        result = asyncio.run(agent.process(self.task_id, header, accessor))

        self.assertEqual(result["response"], "Quantum computing uses qubits.")

        # Verify DDict writes
        entry_key = GLOBAL_STATE_ENTRY_KEY.format(task_id=self.task_id, agent_id=agent_id)
        entry = accessor.get(entry_key)
        self.assertEqual(entry["agent_id"], agent_id)
        self.assertIn("qubits", entry["answer"])


# ========================================================================
# Test: single agent with tools — tool call then final answer
# ========================================================================

class TestSubAgentWithTools(TestCase):
    """End-to-end: agent uses a tool, gets result, then produces final answer."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_tool_call_then_answer(self):
        """Agent calls a tool, receives result, produces final answer in DDict."""
        agent_id = "analyst"
        dispatch_id = str(uuid.uuid4())

        # Seed DDict
        accessor = DDictAccessor(self.ddict, agent_id=agent_id, task_id=self.task_id)
        user_input_key = USER_INPUT_KEY.format(task_id=self.task_id)
        accessor.put(user_input_key, "Count words in 'hello world'")
        global_key = GLOBAL_STATE_KEY.format(task_id=self.task_id)
        accessor.put(global_key, [{"agent_id": "user", "answer": "Count words in 'hello world'"}])

        # Register a local tool
        registry = ToolRegistry()

        def word_count(text: str) -> dict:
            """Count words in text."""
            return {"count": len(text.split())}

        registry.register(word_count)

        # LLM responses: first requests the tool, second gives final answer
        tool_call_response = json.dumps({
            "response": {
                "type": "tool_request",
                "tool_calls": [{"name": "word_count", "args": {"text": "hello world"}}],
            }
        })
        final_response = json.dumps({
            "response": {
                "type": "final_answer",
                "content": "The text has 2 words.",
            }
        })
        llm = FakeLLMEngine(responses=[tool_call_response, final_response])
        agent = _build_sub_agent(agent_id, "text analyst", llm, tool_registry=registry)

        header = DispatchHeader(
            task="Count the words.",
            serialized_ddict=self.ddict.serialize(),
            dispatch_id=dispatch_id,
        )
        result = asyncio.run(agent.process(self.task_id, header, accessor))

        self.assertIn("2 words", result["response"])

        # Verify the tool was actually invoked (LLM was called twice)
        self.assertEqual(llm._call_count, 2)


# ========================================================================
# Test: upstream dependency reads
# ========================================================================

class TestSubAgentUpstreamReads(TestCase):
    """End-to-end: agent reads upstream agent results from DDict."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_reads_upstream_results(self):
        """Agent with upstream_agent_ids reads parent results before processing."""
        agent_id = "synthesizer"
        parent_id = "researcher"
        dispatch_id = str(uuid.uuid4())
        parent_dispatch_id = str(uuid.uuid4())

        accessor = DDictAccessor(self.ddict, agent_id=agent_id, task_id=self.task_id)

        # Write user input
        accessor.put(USER_INPUT_KEY.format(task_id=self.task_id), "Summarize findings.")

        # Write parent's dispatch_id and result (as the parent agent would)
        accessor.put(
            DISPATCH_ID_KEY.format(task_id=self.task_id, agent_id=parent_id),
            parent_dispatch_id,
        )
        accessor.put(
            RESULT_KEY.format(task_id=self.task_id, agent_id=parent_id, dispatch_id=parent_dispatch_id),
            {"response": "Quantum computing is fast."},
        )

        llm = FakeLLMEngine(responses=["Summary: Quantum computing is fast."])
        agent = _build_sub_agent(agent_id, "synthesizer", llm)

        header = DispatchHeader(
            task="Synthesize results.",
            serialized_ddict=self.ddict.serialize(),
            dispatch_id=dispatch_id,
            upstream_agent_ids=[parent_id],
        )
        result = asyncio.run(agent.process(self.task_id, header, accessor))

        self.assertIn("Quantum computing", result["response"])

        # Verify the LLM received the parent's result in its context
        llm_prompts = llm.calls[0]["prompts"]
        parent_msg = [m for m in llm_prompts if "Result from researcher" in m.get("content", "")]
        self.assertEqual(len(parent_msg), 1)


# ========================================================================
# Test: _handle_message sets status and signals completion event
# ========================================================================

class TestSubAgentHandleMessage(TestCase):
    """End-to-end: _handle_message writes status to DDict and signals Event."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_handle_message_lifecycle(self):
        """_handle_message sets PROCESSING→DONE status and fires completion_event."""
        agent_id = "worker"
        dispatch_id = str(uuid.uuid4())

        accessor = DDictAccessor(self.ddict, agent_id=agent_id, task_id=self.task_id)
        accessor.put(USER_INPUT_KEY.format(task_id=self.task_id), "Do work.")
        accessor.put(
            GLOBAL_STATE_KEY.format(task_id=self.task_id),
            [{"agent_id": "user", "answer": "Do work."}],
        )

        llm = FakeLLMEngine(responses=["Work done."])
        agent = _build_sub_agent(agent_id, "worker", llm)

        # Create a real Dragon Event for completion signaling
        completion_event = Event()

        header = DispatchHeader(
            task="Do the work.",
            serialized_ddict=self.ddict.serialize(),
            completion_event=completion_event,
            dispatch_id=dispatch_id,
        )
        msg = Message(
            task_id=self.task_id,
            sender_id="dispatcher",
            recipient_id=agent_id,
            header=header,
        )

        asyncio.run(agent._handle_message(msg))

        # Verify status
        status_key = STATUS_KEY.format(task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        self.assertEqual(self.ddict[status_key], TaskStatus.DONE)

        # Verify result
        result_key = RESULT_KEY.format(task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        result = self.ddict[result_key]
        self.assertEqual(result["response"], "Work done.")

        # Verify completion event was set
        self.assertTrue(completion_event.is_set())

        completion_event.destroy()

    def test_handle_message_error_sets_error_status(self):
        """_handle_message writes ERROR status when LLM raises an exception."""
        agent_id = "failing_agent"
        dispatch_id = str(uuid.uuid4())

        accessor = DDictAccessor(self.ddict, agent_id=agent_id, task_id=self.task_id)
        accessor.put(USER_INPUT_KEY.format(task_id=self.task_id), "Fail please.")
        accessor.put(
            GLOBAL_STATE_KEY.format(task_id=self.task_id),
            [{"agent_id": "user", "answer": "Fail please."}],
        )

        # LLM that raises on chat()
        class FailingLLM:
            async def chat(self, prompts, **kwargs):
                raise RuntimeError("LLM crashed")

        agent = _build_sub_agent(agent_id, "worker", FailingLLM())

        completion_event = Event()
        header = DispatchHeader(
            task="Do something.",
            serialized_ddict=self.ddict.serialize(),
            completion_event=completion_event,
            dispatch_id=dispatch_id,
        )
        msg = Message(
            task_id=self.task_id,
            sender_id="dispatcher",
            recipient_id=agent_id,
            header=header,
        )

        # _handle_message catches exceptions (task isolation)
        asyncio.run(agent._handle_message(msg))

        # Verify ERROR status
        status_key = STATUS_KEY.format(task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        self.assertEqual(self.ddict[status_key], TaskStatus.ERROR)

        # Verify completion event was still set (so dispatcher unblocks)
        self.assertTrue(completion_event.is_set())

        completion_event.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
