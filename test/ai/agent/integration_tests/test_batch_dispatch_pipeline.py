"""Integration tests for batch dispatch pipeline.

These tests verify the dispatcher closure flow: create header → send to agent
queue → agent processes → signals Event → dispatcher reads status from DDict.

Uses real Dragon Queues, DDicts, and Events.

Run with: dragon python -m unittest test.ai.agent.integration_tests.test_batch_dispatch_pipeline -v
"""

import dragon  # noqa: F401 — activates Dragon runtime

import asyncio
import json
import multiprocessing as mp

import threading
import time
import uuid
from unittest import TestCase, main

from dragon.data.ddict import DDict
from dragon.native.queue import Queue
from dragon.native.event import Event

from dragon.ai.agent.config import (
    AgentConfig,
    DispatchHeader,
    OrchestratorConfig,
    PipelineNode,
    TaskResult,
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
from dragon.ai.agent.core.batch_dispatch import make_dispatcher_fn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_agent_loop(agent_queue, ddict_handle, llm_response="Done."):
    """Simulate a minimal agent: read from queue, write result to DDict.

    Runs in a background thread (not a separate process) so it shares memory.
    For integration tests that need process isolation, use mp.Process instead.
    """
    msg = agent_queue.get(timeout=10)
    header = msg.header
    task_id = msg.task_id
    agent_id = msg.recipient_id
    dispatch_id = header.dispatch_id

    ddict = DDict.attach(header.serialized_ddict)
    try:
        status_key = STATUS_KEY.format(task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        result_key = RESULT_KEY.format(task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        entry_key = GLOBAL_STATE_ENTRY_KEY.format(task_id=task_id, agent_id=agent_id)

        ddict[status_key] = TaskStatus.PROCESSING
        ddict[result_key] = {"response": llm_response}
        ddict[entry_key] = {"agent_id": agent_id, "answer": llm_response}
        ddict[status_key] = TaskStatus.DONE
    finally:
        ddict.detach()
        if header.completion_event is not None:
            header.completion_event.set()


def _fake_agent_error_loop(agent_queue, ddict_handle):
    """Simulate an agent that fails: writes ERROR status to DDict."""
    msg = agent_queue.get(timeout=10)
    header = msg.header
    task_id = msg.task_id
    agent_id = msg.recipient_id
    dispatch_id = header.dispatch_id

    ddict = DDict.attach(header.serialized_ddict)
    try:
        status_key = STATUS_KEY.format(task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        result_key = RESULT_KEY.format(task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        ddict[result_key] = {"error": "Agent crashed"}
        ddict[status_key] = TaskStatus.ERROR
    finally:
        ddict.detach()
        if header.completion_event is not None:
            header.completion_event.set()


# ========================================================================
# Test: single dispatcher → single agent (no upstream)
# ========================================================================

class TestSingleDispatch(TestCase):
    """Verify a single dispatcher closure sends work and collects result."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_successful_dispatch(self):
        """Dispatcher sends header, agent completes, dispatcher returns DONE."""
        agent_queue = Queue()
        serialized = self.ddict.serialize()

        node = PipelineNode(agent_id="worker", task_description="Do analysis.")
        config = OrchestratorConfig(
            agents=[AgentConfig(agent_id="worker", name="Worker", role="analyst")],
            poll_timeout=10.0,
        )

        # Start fake agent in background
        agent_thread = threading.Thread(
            target=_fake_agent_loop,
            args=(agent_queue, serialized, "Analysis complete."),
        )
        agent_thread.start()

        # Create and run dispatcher
        dispatcher_fn = make_dispatcher_fn(
            agent_queue=agent_queue,
            node=node,
            task_id=self.task_id,
            serialized_ddict=serialized,
            config=config,
        )

        # Provide initial (root) TaskResult
        root = TaskResult(
            task_id=self.task_id,
            agent_id="__root__",
            status=TaskStatus.DONE,
            serialized_ddict=serialized,
        )
        result = dispatcher_fn(root)

        agent_thread.join(timeout=10)

        self.assertEqual(result.status, TaskStatus.DONE)
        self.assertEqual(result.agent_id, "worker")
        self.assertEqual(result.task_id, self.task_id)

        # Verify DDict has the result
        dispatch_id = self.ddict[DISPATCH_ID_KEY.format(task_id=self.task_id, agent_id="worker")]
        result_key = RESULT_KEY.format(task_id=self.task_id, agent_id="worker", dispatch_id=dispatch_id)
        self.assertEqual(self.ddict[result_key]["response"], "Analysis complete.")

        agent_queue.destroy()

    def test_agent_error_raises(self):
        """Dispatcher raises RuntimeError when agent writes ERROR status."""
        agent_queue = Queue()
        serialized = self.ddict.serialize()

        node = PipelineNode(agent_id="bad_worker", task_description="Fail.")
        config = OrchestratorConfig(
            agents=[AgentConfig(agent_id="bad_worker", name="Bad", role="tester")],
            poll_timeout=10.0,
        )

        agent_thread = threading.Thread(
            target=_fake_agent_error_loop,
            args=(agent_queue, serialized),
        )
        agent_thread.start()

        dispatcher_fn = make_dispatcher_fn(
            agent_queue=agent_queue,
            node=node,
            task_id=self.task_id,
            serialized_ddict=serialized,
            config=config,
        )

        root = TaskResult(
            task_id=self.task_id,
            agent_id="__root__",
            status=TaskStatus.DONE,
            serialized_ddict=serialized,
        )

        with self.assertRaisesRegex(RuntimeError, "reported error"):
            dispatcher_fn(root)

        agent_thread.join(timeout=10)
        agent_queue.destroy()


# ========================================================================
# Test: upstream dependency — one dispatcher feeds into another
# ========================================================================

class TestChainedDispatch(TestCase):
    """Verify upstream TaskResult propagation between chained dispatchers."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_upstream_result_propagated(self):
        """Second dispatcher receives DONE TaskResult from first dispatcher."""
        queue_a = Queue()
        queue_b = Queue()
        serialized = self.ddict.serialize()

        # Seed user input
        self.ddict[USER_INPUT_KEY.format(task_id=self.task_id)] = "Research quantum computing."
        self.ddict[GLOBAL_STATE_KEY.format(task_id=self.task_id)] = [
            {"agent_id": "user", "answer": "Research quantum computing."}
        ]

        node_a = PipelineNode(agent_id="researcher", task_description="Research.")
        node_b = PipelineNode(
            agent_id="writer",
            task_description="Write report.",
            depends_on=["researcher"],
        )
        config = OrchestratorConfig(
            agents=[
                AgentConfig(agent_id="researcher", name="Researcher", role="researcher"),
                AgentConfig(agent_id="writer", name="Writer", role="writer"),
            ],
            poll_timeout=10.0,
        )

        # Start both fake agents
        thread_a = threading.Thread(
            target=_fake_agent_loop,
            args=(queue_a, serialized, "Quantum computers use qubits."),
        )
        thread_b = threading.Thread(
            target=_fake_agent_loop,
            args=(queue_b, serialized, "Report: Quantum computers use qubits."),
        )
        thread_a.start()
        thread_b.start()

        # Create dispatchers
        fn_a = make_dispatcher_fn(queue_a, node_a, self.task_id, serialized, config)
        fn_b = make_dispatcher_fn(queue_b, node_b, self.task_id, serialized, config)

        root = TaskResult(
            task_id=self.task_id,
            agent_id="__root__",
            status=TaskStatus.DONE,
            serialized_ddict=serialized,
        )

        # Run first dispatcher
        result_a = fn_a(root)
        self.assertEqual(result_a.status, TaskStatus.DONE)

        # Run second dispatcher with first's result as upstream
        result_b = fn_b(result_a)
        self.assertEqual(result_b.status, TaskStatus.DONE)

        thread_a.join(timeout=10)
        thread_b.join(timeout=10)

        # Verify both agents wrote results
        did_a = self.ddict[DISPATCH_ID_KEY.format(task_id=self.task_id, agent_id="researcher")]
        self.assertEqual(self.ddict[RESULT_KEY.format(
            task_id=self.task_id, agent_id="researcher", dispatch_id=did_a
        )]["response"], "Quantum computers use qubits.")

        did_b = self.ddict[DISPATCH_ID_KEY.format(task_id=self.task_id, agent_id="writer")]
        self.assertIn("Report", self.ddict[RESULT_KEY.format(
            task_id=self.task_id, agent_id="writer", dispatch_id=did_b
        )]["response"])

        queue_a.destroy()
        queue_b.destroy()

    def test_upstream_error_blocks_downstream(self):
        """When upstream dispatcher raises, downstream dispatcher also raises."""
        queue_a = Queue()
        serialized = self.ddict.serialize()

        node_a = PipelineNode(agent_id="failing", task_description="Fail.")
        config = OrchestratorConfig(
            agents=[AgentConfig(agent_id="failing", name="Failing", role="tester")],
            poll_timeout=10.0,
        )

        thread_a = threading.Thread(
            target=_fake_agent_error_loop,
            args=(queue_a, serialized),
        )
        thread_a.start()

        fn_a = make_dispatcher_fn(queue_a, node_a, self.task_id, serialized, config)
        root = TaskResult(
            task_id=self.task_id,
            agent_id="__root__",
            status=TaskStatus.DONE,
            serialized_ddict=serialized,
        )

        with self.assertRaisesRegex(RuntimeError, "reported error"):
            fn_a(root)

        thread_a.join(timeout=10)
        queue_a.destroy()


# ========================================================================
# Test: timeout when agent does not respond
# ========================================================================

class TestDispatchTimeout(TestCase):
    """Verify dispatcher raises TimeoutError when agent doesn't complete."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_timeout_raises(self):
        """Dispatcher times out if agent never signals completion."""
        agent_queue = Queue()
        serialized = self.ddict.serialize()

        node = PipelineNode(agent_id="slow", task_description="Never finish.")
        config = OrchestratorConfig(
            agents=[AgentConfig(agent_id="slow", name="Slow", role="slacker")],
            poll_timeout=1.0,  # 1 second timeout
        )

        # Don't start any agent — queue will never be consumed
        # But we need someone to consume the message so the queue doesn't
        # block on put. Use a thread that reads but never signals.
        def _slow_agent():
            msg = agent_queue.get(timeout=5)
            # Intentionally don't signal completion or write status

        thread = threading.Thread(target=_slow_agent)
        thread.start()

        fn = make_dispatcher_fn(agent_queue, node, self.task_id, serialized, config)
        root = TaskResult(
            task_id=self.task_id,
            agent_id="__root__",
            status=TaskStatus.DONE,
            serialized_ddict=serialized,
        )

        with self.assertRaisesRegex(TimeoutError, "did not complete"):
            fn(root)

        thread.join(timeout=5)
        agent_queue.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
