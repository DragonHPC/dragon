"""Integration tests for MCP tool routing through the agent pipeline.

These tests verify that the ToolDispatcher correctly routes tool calls
between local registry tools and remote MCP server tools, using real
Dragon DDicts for event recording.

Run with: dragon python -m unittest test.ai.agent.integration_tests.test_mcp_tool_integration -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp


import asyncio
import json
import uuid
from typing import List, Optional, Tuple
from unittest import TestCase, main

from dragon.data.ddict import DDict

from dragon.ai.agent.config import (
    LLM_EVENT_KEY,
    LLM_EVENT_COUNT_KEY,
    TOOL_EVENT_KEY,
    TOOL_EVENT_COUNT_KEY,
)
from dragon.ai.agent.ddict import DDictAccessor
from dragon.ai.agent.tools.registry import ToolRegistry
from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher

from .conftest import FakeLLMEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dispatcher(
    responses: List[str],
    registry: Optional[ToolRegistry] = None,
    approval_filter=None,
) -> Tuple[ToolDispatcher, FakeLLMEngine]:
    """Build a ToolDispatcher with a FakeLLMEngine."""
    llm = FakeLLMEngine(responses)
    reg = registry or ToolRegistry()
    dispatcher = ToolDispatcher(llm, reg, approval_filter=approval_filter)
    return dispatcher, llm


def _tool_request(tool_name: str, args: dict) -> str:
    """Build a JSON tool request response string."""
    return json.dumps({
        "response": {
            "type": "tool_request",
            "tool_calls": [{"name": tool_name, "args": args}],
        }
    })


def _final_answer(content: str) -> str:
    """Build a JSON final answer response string."""
    return json.dumps({
        "response": {"type": "final_answer", "content": content}
    })


# ========================================================================
# Test: local tool call with DDict event recording
# ========================================================================

class TestLocalToolWithDDict(TestCase):
    """Verify local tool calls write events to real DDict."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_tool_event_written_to_ddict(self):
        """Tool call event is written to DDict when tracing is enabled."""
        agent_id = "analyst"
        dispatch_id = str(uuid.uuid4())

        registry = ToolRegistry()

        def word_count(text: str) -> dict:
            """Count words in text."""
            return {"count": len(text.split())}

        registry.register(word_count)

        dispatcher, llm = _make_dispatcher(
            responses=[
                _tool_request("word_count", {"text": "hello world"}),
                _final_answer("The text has 2 words."),
            ],
            registry=registry,
        )

        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Count words in 'hello world'."},
        ]

        result = asyncio.run(dispatcher.chat(
            messages,
            ddict=self.ddict,
            tracing=True,
            task_id=self.task_id,
            agent_id=agent_id,
            dispatch_id=dispatch_id,
        ))

        conversation = result[0]
        self.assertTrue(any("2 words" in (m.get("content") or "") for m in conversation))

        # Verify LLM events written to DDict
        fmt = dict(task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        llm_count_key = LLM_EVENT_COUNT_KEY.format(**fmt)
        self.assertGreaterEqual(self.ddict[llm_count_key], 2)  # at least 2 LLM calls

        # Verify tool events written to DDict
        tool_count_key = TOOL_EVENT_COUNT_KEY.format(**fmt)
        self.assertGreaterEqual(self.ddict[tool_count_key], 1)

        # Read the actual tool event
        tool_event_key = TOOL_EVENT_KEY.format(index=0, **fmt)
        tool_event = self.ddict[tool_event_key]
        self.assertEqual(tool_event["name"], "word_count")
        self.assertEqual(tool_event["result"]["count"], 2)


# ========================================================================
# Test: multiple local tools in sequence
# ========================================================================

class TestMultipleToolCalls(TestCase):
    """Verify multiple sequential tool calls with DDict event recording."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_two_tools_in_sequence(self):
        """Agent calls two tools before producing final answer."""
        agent_id = "multi_tool_agent"
        dispatch_id = str(uuid.uuid4())

        registry = ToolRegistry()

        def get_weather(city: str) -> dict:
            """Get weather for a city."""
            return {"city": city, "temp": 72, "condition": "sunny"}

        def get_population(city: str) -> dict:
            """Get population for a city."""
            return {"city": city, "population": 1_000_000}

        registry.register(get_weather)
        registry.register(get_population)

        dispatcher, llm = _make_dispatcher(
            responses=[
                _tool_request("get_weather", {"city": "Denver"}),
                _tool_request("get_population", {"city": "Denver"}),
                _final_answer("Denver: 72°F, sunny, population 1M."),
            ],
            registry=registry,
        )

        messages = [
            {"role": "system", "content": "You are a city info assistant."},
            {"role": "user", "content": "Tell me about Denver."},
        ]

        result = asyncio.run(dispatcher.chat(
            messages,
            ddict=self.ddict,
            tracing=True,
            task_id=self.task_id,
            agent_id=agent_id,
            dispatch_id=dispatch_id,
        ))

        conversation = result[0]
        self.assertTrue(any("Denver" in (m.get("content") or "") for m in conversation))

        # Verify two tool events
        fmt = dict(task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        self.assertEqual(self.ddict[TOOL_EVENT_COUNT_KEY.format(**fmt)], 2)

        # Verify tool event details
        evt0 = self.ddict[TOOL_EVENT_KEY.format(index=0, **fmt)]
        evt1 = self.ddict[TOOL_EVENT_KEY.format(index=1, **fmt)]
        tool_names = {evt0["name"], evt1["name"]}
        self.assertEqual(tool_names, {"get_weather", "get_population"})


# ========================================================================
# Test: tool error fed back to LLM
# ========================================================================

class TestToolErrorFeedback(TestCase):
    """Verify tool errors are fed back to the LLM as tool messages."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_tool_error_recovery(self):
        """Agent recovers from tool error by producing final answer."""
        agent_id = "error_handler"
        dispatch_id = str(uuid.uuid4())

        registry = ToolRegistry()

        call_count = 0

        def flaky_tool(query: str) -> dict:
            """A tool that fails on first call, succeeds on second."""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Service unavailable")
            return {"result": f"Found: {query}"}

        registry.register(flaky_tool)

        dispatcher, llm = _make_dispatcher(
            responses=[
                _tool_request("flaky_tool", {"query": "test"}),
                # After error, LLM retries
                _tool_request("flaky_tool", {"query": "test"}),
                _final_answer("Found: test"),
            ],
            registry=registry,
        )

        messages = [
            {"role": "system", "content": "You search for data."},
            {"role": "user", "content": "Find 'test'."},
        ]

        result = asyncio.run(dispatcher.chat(
            messages,
            ddict=self.ddict,
            tracing=True,
            task_id=self.task_id,
            agent_id=agent_id,
            dispatch_id=dispatch_id,
        ))

        conversation = result[0]
        self.assertTrue(any("Found: test" in (m.get("content") or "") for m in conversation))
        self.assertEqual(call_count, 2)  # tool was called twice


# ========================================================================
# Test: MCP tool routing (simulated — no live server)
# ========================================================================

class TestMCPToolRouting(TestCase):
    """Verify MCP tool routing without a live server.

    Manually injects a mock MCP client into the dispatcher to test routing
    logic. For tests against a live MCP server, add a skip decorator.
    """

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_mcp_tool_routed_correctly(self):
        """Tool with alias prefix is routed to the correct MCP client."""
        from unittest.mock import AsyncMock, MagicMock

        agent_id = "mcp_agent"
        dispatch_id = str(uuid.uuid4())

        registry = ToolRegistry()
        dispatcher, llm = _make_dispatcher(
            responses=[
                _tool_request("jupyter__run_cell", {"code": "print('hi')"}),
                _final_answer("Cell output: hi"),
            ],
            registry=registry,
        )

        # Inject a mock MCP client
        mock_mcp = MagicMock()
        mock_mcp.tools_schemas = [
            {
                "type": "function",
                "function": {
                    "name": "jupyter__run_cell",
                    "description": "Run a Jupyter cell",
                    "parameters": {"type": "object", "properties": {"code": {"type": "string"}}},
                },
            }
        ]
        mock_mcp.scoped_names = {"jupyter__run_cell"}
        mock_mcp.call_tool = AsyncMock(return_value={"output": "hi"})
        dispatcher._mcp_clients["jupyter"] = mock_mcp

        messages = [
            {"role": "system", "content": "You run code."},
            {"role": "user", "content": "Run print('hi')."},
        ]

        result = asyncio.run(dispatcher.chat(
            messages,
            ddict=self.ddict,
            tracing=True,
            task_id=self.task_id,
            agent_id=agent_id,
            dispatch_id=dispatch_id,
        ))

        # Verify MCP client was called
        mock_mcp.call_tool.assert_called_once_with(
            "jupyter__run_cell", {"code": "print('hi')"}
        )

        # Verify tool event in DDict records MCP source
        fmt = dict(task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id)
        tool_evt = self.ddict[TOOL_EVENT_KEY.format(index=0, **fmt)]
        self.assertEqual(tool_evt["source"], "mcp")
        self.assertEqual(tool_evt["name"], "jupyter__run_cell")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
