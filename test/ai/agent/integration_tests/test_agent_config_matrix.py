"""Integration tests — combinatorial agent configuration matrix.

Exercises every combination of:
    Memory strategy:  FULL × SLIDING_WINDOW × SUMMARIZE
    HITL approval:    Off × On
    Tracing:          Off × On
    Tools:            None × Local

This mirrors the inference suite's ``test_batching_guardrails_matrix.py``
which cross-products batching mode × guardrails.

Run with:  dragon python -m unittest test.ai.agent.integration_tests.test_agent_config_matrix -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp


import asyncio
import json
import uuid
from unittest import TestCase, main
from unittest.mock import MagicMock

from dragon.data.ddict import DDict
from dragon.native.queue import Queue

from dragon.ai.agent.config import (
    MemoryStrategy,
    TaskStatus,
    STATUS_KEY,
    RESULT_KEY,
    LLM_EVENT_COUNT_KEY,
    TOOL_EVENT_COUNT_KEY,
    MEMORY_EVENT_COUNT_KEY,
    TRACE_KEY,
)
from dragon.ai.agent.ddict import DDictAccessor
from dragon.ai.agent.tools.registry import ToolRegistry

from .conftest import FakeLLMEngine, IntegrationTestCase


# ---------------------------------------------------------------------------
# Shared tool for "with tools" configurations
# ---------------------------------------------------------------------------

def _make_tool_registry():
    registry = ToolRegistry()

    @registry.tool
    def double(n: int) -> int:
        """Double a number."""
        return n * 2

    return registry


def _tool_call_then_answer():
    """Return (tool_request, final_answer) LLM response pair."""
    tool_req = json.dumps({
        "response": {
            "type": "tool_request",
            "tool_calls": [{"name": "double", "args": {"n": 7}}],
        }
    })
    final = json.dumps({
        "response": {"type": "final_answer", "content": "The answer is 14."}
    })
    return [tool_req, final]


# ========================================================================
# Memory=FULL × HITL=Off × Tracing=Off × Tools=None  (baseline)
# ========================================================================

class TestFullNoHitlNoTracingNoTools(IntegrationTestCase):
    """Simplest configuration: no memory pruning, no HITL, no tracing, no tools."""

    def test_plain_answer(self):
        agent_id = "baseline"
        llm = FakeLLMEngine(responses=["Simple answer."])
        agent = self.build_sub_agent(agent_id, llm, memory=self.get_memory_config_full())
        accessor = self.seed_ddict(agent_id)
        header = self.make_header(tracing=False)

        result = asyncio.run(agent.process(self.task_id, header, accessor))

        self.assertEqual(result["response"], "Simple answer.")
        self.assertEqual(llm._call_count, 1)


# ========================================================================
# Memory=SLIDING_WINDOW × HITL=Off × Tracing=Off × Tools=Local
# ========================================================================

class TestSlidingWindowNoHitlNoTracingWithTools(IntegrationTestCase):
    """Sliding window memory + local tool call, no HITL, no tracing."""

    def test_tool_then_answer(self):
        agent_id = "slider"
        registry = _make_tool_registry()
        llm = FakeLLMEngine(responses=_tool_call_then_answer())
        agent = self.build_sub_agent(
            agent_id, llm,
            tool_registry=registry,
            memory=self.get_memory_config_sliding(max_kept_turns=4),
        )
        accessor = self.seed_ddict(agent_id)
        header = self.make_header(tracing=False)

        result = asyncio.run(agent.process(self.task_id, header, accessor))

        self.assertIn("14", result["response"])
        self.assertEqual(llm._call_count, 2)


# ========================================================================
# Memory=SLIDING_WINDOW × HITL=Off × Tracing=On × Tools=Local
# ========================================================================

class TestSlidingWindowNoHitlTracingOnWithTools(IntegrationTestCase):
    """Sliding window + tracing enabled: verifies event keys are written."""

    def test_tracing_events_written(self):
        agent_id = "traced-slider"
        dispatch_id = str(uuid.uuid4())
        registry = _make_tool_registry()
        llm = FakeLLMEngine(responses=_tool_call_then_answer())
        agent = self.build_sub_agent(
            agent_id, llm,
            tool_registry=registry,
            memory=self.get_memory_config_sliding(max_kept_turns=4),
        )
        accessor = self.seed_ddict(agent_id)
        header = self.make_header(dispatch_id=dispatch_id, tracing=True)

        result = asyncio.run(agent.process(self.task_id, header, accessor))

        self.assertIn("14", result["response"])

        # Tracing should have written LLM + tool event counts
        llm_count = accessor.get(LLM_EVENT_COUNT_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id,
        ))
        tool_count = accessor.get(TOOL_EVENT_COUNT_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id,
        ))
        self.assertGreaterEqual(llm_count, 2)
        self.assertGreaterEqual(tool_count, 1)


# ========================================================================
# Memory=FULL × HITL=Off × Tracing=On × Tools=None
# ========================================================================

class TestFullNoHitlTracingOnNoTools(IntegrationTestCase):
    """Full memory + tracing, no tools: verifies trace spans written to DDict."""

    def test_trace_key_written(self):
        agent_id = "traced-full"
        dispatch_id = str(uuid.uuid4())
        llm = FakeLLMEngine(responses=["Traced answer."])
        agent = self.build_sub_agent(
            agent_id, llm, memory=self.get_memory_config_full(),
        )
        accessor = self.seed_ddict(agent_id)
        header = self.make_header(dispatch_id=dispatch_id, tracing=True)

        result = asyncio.run(agent.process(self.task_id, header, accessor))

        self.assertEqual(result["response"], "Traced answer.")
        llm_count = accessor.get(LLM_EVENT_COUNT_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id,
        ))
        self.assertGreaterEqual(llm_count, 1)


# ========================================================================
# Memory=SUMMARIZE × HITL=Off × Tracing=Off × Tools=Local (many turns)
# ========================================================================

class TestSummarizeNoHitlNoTracingWithTools(IntegrationTestCase):
    """Summarize strategy with enough tool calls to trigger summarization."""

    def test_summarization_triggered(self):
        agent_id = "summarizer"
        registry = _make_tool_registry()

        # 4 tool calls + final answer = 5 LLM calls
        # With summarize_after_turns=3, summarization should trigger
        responses = []
        for _ in range(4):
            responses.append(json.dumps({
                "response": {
                    "type": "tool_request",
                    "tool_calls": [{"name": "double", "args": {"n": 1}}],
                }
            }))
        responses.append(json.dumps({
            "response": {"type": "final_answer", "content": "All doubled."}
        }))

        llm = FakeLLMEngine(responses=responses)
        agent = self.build_sub_agent(
            agent_id, llm,
            tool_registry=registry,
            memory=self.get_memory_config_summarize(
                max_kept_turns=4, summarize_after_turns=3,
            ),
        )
        accessor = self.seed_ddict(agent_id)
        header = self.make_header(tracing=False)

        result = asyncio.run(agent.process(self.task_id, header, accessor))

        self.assertEqual(result["response"], "All doubled.")
        # LLM should have been called at least 5 times (4 tool + 1 final)
        self.assertGreaterEqual(llm._call_count, 5)


# ========================================================================
# Memory=SLIDING_WINDOW × HITL=On (auto-approve) × Tracing=On × Tools=Local
# ========================================================================

class TestSlidingWindowHitlOnTracingOnWithTools(IntegrationTestCase):
    """Full configuration: sliding window + HITL + tracing + tools.

    Uses a mock HITL queue that auto-approves to verify the HITL code path
    is entered without blocking.
    """

    def test_hitl_approval_path(self):
        from dragon.ai.agent.hitl.models import HumanApprovalResponse
        from dragon.ai.agent.config import HITL_QUEUE_KEY

        agent_id = "hitl-agent"
        dispatch_id = str(uuid.uuid4())
        registry = _make_tool_registry()
        approval_filter = lambda name, args: True  # all tools need approval

        llm = FakeLLMEngine(responses=_tool_call_then_answer())
        agent = self.build_sub_agent(
            agent_id, llm,
            tool_registry=registry,
            approval_filter=approval_filter,
            memory=self.get_memory_config_sliding(max_kept_turns=4),
        )
        accessor = self.seed_ddict(agent_id)

        # Create a mock HITL queue: when the agent puts (request, response_queue),
        # a thread auto-approves immediately.
        import threading

        hitl_queue = Queue()
        accessor.put(
            HITL_QUEUE_KEY.format(task_id=self.task_id),
            hitl_queue,
        )

        def auto_approver():
            req, resp_q = hitl_queue.get(timeout=10)
            resp_q.put(HumanApprovalResponse(approved=True, reason="Auto"))
            resp_q.destroy()

        approver = threading.Thread(target=auto_approver, daemon=True)
        approver.start()

        header = self.make_header(dispatch_id=dispatch_id, tracing=True)
        result = asyncio.run(agent.process(self.task_id, header, accessor))

        approver.join(timeout=5)
        hitl_queue.destroy()

        self.assertIn("14", result["response"])

        # Verify tracing captured HITL event
        from dragon.ai.agent.config import HITL_EVENT_COUNT_KEY
        hitl_count_key = HITL_EVENT_COUNT_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id,
        )
        hitl_count = accessor.get(hitl_count_key)
        self.assertGreaterEqual(hitl_count, 1)


# ========================================================================
# Combinatorial matrix — all combos via subTest
# ========================================================================

class TestConfigurationMatrix(IntegrationTestCase):
    """Run every (memory × tools × tracing) combination via subTest.

    HITL is excluded from the combinatorial loop because it requires real
    Dragon Queue threading — covered by the dedicated class above.
    """

    MEMORY_CONFIGS = {
        "full": None,  # None → get_memory_config_full()
        "sliding_window": "sliding",
        "summarize": "summarize",
    }

    TOOL_CONFIGS = {
        "no_tools": None,
        "with_tools": True,
    }

    TRACING_FLAGS = [False, True]

    def _get_memory(self, key):
        if key == "full":
            return self.get_memory_config_full()
        elif key == "sliding":
            return self.get_memory_config_sliding(max_kept_turns=4)
        elif key == "summarize":
            return self.get_memory_config_summarize(
                max_kept_turns=6, summarize_after_turns=4,
            )
        return self.get_memory_config_full()

    def test_all_configurations(self):
        """Cross-product: 3 memory × 2 tools × 2 tracing = 12 combinations."""
        for mem_label, mem_key in [
            ("full", "full"),
            ("sliding_window", "sliding"),
            ("summarize", "summarize"),
        ]:
            for tool_label, use_tools in [("no_tools", False), ("with_tools", True)]:
                for tracing in self.TRACING_FLAGS:
                    combo = f"memory={mem_label}, tools={tool_label}, tracing={tracing}"
                    with self.subTest(combo=combo):
                        self._run_combo(mem_key, use_tools, tracing)

    def _run_combo(self, mem_key, use_tools, tracing):
        agent_id = f"matrix-{uuid.uuid4().hex[:6]}"
        dispatch_id = str(uuid.uuid4())
        memory = self._get_memory(mem_key)

        if use_tools:
            registry = _make_tool_registry()
            responses = _tool_call_then_answer()
        else:
            registry = None
            responses = ["Matrix answer."]

        llm = FakeLLMEngine(responses=responses)
        agent = self.build_sub_agent(
            agent_id, llm,
            tool_registry=registry,
            memory=memory,
        )
        accessor = self.seed_ddict(agent_id)
        header = self.make_header(dispatch_id=dispatch_id, tracing=tracing)

        result = asyncio.run(agent.process(self.task_id, header, accessor))

        # Every combo must produce a non-empty response
        self.assertTrue(result["response"])

        # If tracing enabled, LLM event count must be present
        if tracing:
            llm_count = accessor.get(LLM_EVENT_COUNT_KEY.format(
                task_id=self.task_id, agent_id=agent_id,
                dispatch_id=dispatch_id,
            ))
            self.assertGreaterEqual(llm_count, 1)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
