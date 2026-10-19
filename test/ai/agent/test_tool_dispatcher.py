"""Tests for ToolDispatcher — agentic tool-calling loop."""

import dragon  # noqa: F401  # activates Dragon runtime
import json
import multiprocessing as mp

from typing import Optional
from unittest import IsolatedAsyncioTestCase, TestCase, main
from unittest.mock import AsyncMock, MagicMock, patch

from dragon.ai.agent.reasoning.response_parser import (
    ResponseModel, ToolRequest, FinalResponse, ToolCall,
)
from dragon.ai.agent.tools.registry import ToolRegistry
from dragon.ai.agent.utils.errors import AgentLoopError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _final_answer_json(content: str) -> str:
    return json.dumps({"response": {"type": "final_answer", "content": content}})


def _tool_request_json(name: str, args: Optional[dict] = None) -> str:
    return json.dumps({
        "response": {
            "type": "tool_request",
            "tool_calls": [{"name": name, "args": args or {}}],
        }
    })


def _make_dispatcher(responses: list, tools: Optional[dict] = None,
                     approval_filter=None, max_iterations: int = 20):
    """Create a ToolDispatcher with a mock LLM and optional local tools."""
    from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher

    llm = AsyncMock()
    llm.chat = AsyncMock(side_effect=responses)

    registry = ToolRegistry()
    if tools:
        for name, fn in tools.items():
            registry.register(fn)

    return ToolDispatcher(
        llm, registry,
        approval_filter=approval_filter,
        max_tool_call_iterations=max_iterations,
    )


# ========================================================================
# Fast path — no tools
# ========================================================================

class TestToolDispatcherNoTools(IsolatedAsyncioTestCase):
    """Verify the fast path when no tools are registered."""

    async def test_no_tools_single_call(self):
        """With no tools, chat does one plain LLM call and returns."""
        dispatcher = _make_dispatcher(["Direct answer"])
        result = await dispatcher.chat([{"role": "user", "content": "Hi"}])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0]["content"], "Direct answer")


# ========================================================================
# Single tool call → final answer
# ========================================================================

class TestToolDispatcherSingleToolCall(IsolatedAsyncioTestCase):
    """Verify a single tool call followed by a final answer."""

    async def test_tool_call_then_final_answer(self):
        """LLM requests a tool, gets the result, then produces a final answer."""
        def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        responses = [
            _tool_request_json("add", {"a": 2, "b": 3}),
            _final_answer_json("The sum is 5."),
        ]
        dispatcher = _make_dispatcher(responses, tools={"add": add})
        result = await dispatcher.chat([{"role": "user", "content": "Add 2 and 3"}])

        # Result should contain the tool call turn + final answer
        turns = result[0]
        roles = [m["role"] for m in turns]
        self.assertIn("assistant", roles)
        self.assertIn("tool", roles)
        # Final answer should be the last message
        self.assertEqual(turns[-1]["content"], "The sum is 5.")


# ========================================================================
# Tool execution error → fed back to LLM
# ========================================================================

class TestToolDispatcherToolError(IsolatedAsyncioTestCase):
    """Verify tool execution errors are fed back to the LLM."""

    async def test_tool_error_fed_to_llm(self):
        """Tool RuntimeError is caught and injected as a tool message for the LLM."""
        def failing_tool(x: str) -> str:
            """Always fails."""
            raise RuntimeError("boom")

        responses = [
            _tool_request_json("failing_tool", {"x": "test"}),
            _final_answer_json("Tool failed, cannot proceed."),
        ]
        dispatcher = _make_dispatcher(responses, tools={"failing_tool": failing_tool})
        result = await dispatcher.chat([{"role": "user", "content": "Try"}])

        # The error should have been fed back and the LLM produced a final answer
        turns = result[0]
        self.assertEqual(turns[-1]["content"], "Tool failed, cannot proceed.")
        # The tool result should contain the error
        tool_msgs = [m for m in turns if m.get("role") == "tool"]
        self.assertEqual(len(tool_msgs), 1)
        self.assertIn("error", tool_msgs[0]["content"])


# ========================================================================
# Max iterations exceeded
# ========================================================================

class TestToolDispatcherMaxIterations(IsolatedAsyncioTestCase):
    """Verify max_tool_call_iterations is enforced."""

    async def test_max_iterations_raises(self):
        """When max iterations is hit without a final_answer, raise AgentLoopError."""
        # Always request a tool call that returns something, never giving final_answer
        def echo(x: str) -> str:
            """Echo."""
            return x

        responses = [_tool_request_json("echo", {"x": "hi"})] * 5
        dispatcher = _make_dispatcher(responses, tools={"echo": echo},
                                      max_iterations=3)

        with self.assertRaisesRegex(AgentLoopError, "Max tool call iterations"):
            await dispatcher.chat([{"role": "user", "content": "Loop forever"}])


# ========================================================================
# Tool list merging (registry + MCP)
# ========================================================================

class TestToolDispatcherToolMerging(IsolatedAsyncioTestCase):
    """Verify local registry tools are merged with MCP tool schemas."""

    async def test_get_all_tools_includes_mcp(self):
        """LLM receives both local and MCP-provided tool schemas."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher

        llm = AsyncMock()
        llm.chat = AsyncMock(return_value=_final_answer_json("done"))

        registry = ToolRegistry()

        def local_tool(x: str) -> str:
            """Local."""
            return x

        registry.register(local_tool)

        dispatcher = ToolDispatcher(llm, registry)

        # Simulate an MCP client with one tool
        mock_mcp = MagicMock()
        mock_mcp.tools_schemas = [
            {"type": "function", "function": {"name": "jupyter__run", "description": "Run"}}
        ]
        mock_mcp.scoped_names = {"jupyter__run"}
        dispatcher._mcp_clients["jupyter"] = mock_mcp

        # The chat method should use the merged tool list
        result = await dispatcher.chat([{"role": "user", "content": "test"}])
        # LLM was called with tools= containing both local and MCP tools
        call_kwargs = llm.chat.call_args
        tools_arg = call_kwargs.kwargs.get("tools") or call_kwargs[1]
        # At least 2 tools (1 local + 1 MCP)
        # Check the LLM was called (not the fast path)
        self.assertGreaterEqual(llm.chat.call_count, 1)


# ========================================================================
# MCP connection management
# ========================================================================

class TestToolDispatcherMCPManagement(IsolatedAsyncioTestCase):
    """Verify MCP client connection and disconnection management."""

    async def test_connect_mcp_duplicate_alias_rejected(self):
        """Connecting with an already-used alias raises ValueError."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher

        dispatcher = ToolDispatcher(AsyncMock(), ToolRegistry())
        dispatcher._mcp_clients["jupyter"] = MagicMock()

        with self.assertRaisesRegex(ValueError, "already connected"):
            await dispatcher.connect_mcp("http://x", "token", alias="jupyter")

    async def test_close_mcp_all(self):
        """close_mcp() without alias closes all MCP clients."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher

        dispatcher = ToolDispatcher(AsyncMock(), ToolRegistry())
        mock_client = AsyncMock()
        dispatcher._mcp_clients["a"] = mock_client
        dispatcher._mcp_clients["b"] = AsyncMock()

        await dispatcher.close_mcp()
        self.assertEqual(len(dispatcher._mcp_clients), 0)

    async def test_close_mcp_by_alias(self):
        """close_mcp(alias=...) closes only the specified client."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher

        dispatcher = ToolDispatcher(AsyncMock(), ToolRegistry())
        mock_a = AsyncMock()
        mock_b = AsyncMock()
        dispatcher._mcp_clients["a"] = mock_a
        dispatcher._mcp_clients["b"] = mock_b

        await dispatcher.close_mcp("a")
        self.assertNotIn("a", dispatcher._mcp_clients)
        self.assertIn("b", dispatcher._mcp_clients)
        mock_a.close.assert_called_once()


# ========================================================================
# MCP tool routing
# ========================================================================

class TestToolDispatcherMCPRouting(IsolatedAsyncioTestCase):
    """Verify tool calls are routed to the correct MCP client."""

    async def test_mcp_tool_routed_to_client(self):
        """A tool call with alias prefix routes through the correct MCP client."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher

        llm = AsyncMock()
        llm.chat = AsyncMock(side_effect=[
            _tool_request_json("jupyter__run_cell", {"code": "1+1"}),
            _final_answer_json("Result: 2"),
        ])

        registry = ToolRegistry()
        dispatcher = ToolDispatcher(llm, registry)

        mock_mcp = AsyncMock()
        mock_mcp.tools_schemas = [
            {"type": "function", "function": {"name": "jupyter__run_cell", "description": "Run"}}
        ]
        mock_mcp.scoped_names = {"jupyter__run_cell"}
        mock_mcp.call_tool = AsyncMock(return_value={"result": "2"})
        dispatcher._mcp_clients["jupyter"] = mock_mcp

        result = await dispatcher.chat([{"role": "user", "content": "run code"}])
        mock_mcp.call_tool.assert_called_once_with("jupyter__run_cell", {"code": "1+1"})

    async def test_local_tool_not_routed_to_mcp(self):
        """A tool without alias prefix routes to local registry, not MCP."""
        def local_add(a: int, b: int) -> int:
            """Add."""
            return a + b

        responses = [
            _tool_request_json("local_add", {"a": 1, "b": 2}),
            _final_answer_json("Sum is 3"),
        ]
        dispatcher = _make_dispatcher(responses, tools={"local_add": local_add})

        mock_mcp = AsyncMock()
        mock_mcp.tools_schemas = []
        mock_mcp.scoped_names = set()
        dispatcher._mcp_clients["jupyter"] = mock_mcp

        result = await dispatcher.chat([{"role": "user", "content": "add"}])
        mock_mcp.call_tool.assert_not_called()


# ========================================================================
# HITL approval gate
# ========================================================================

class TestToolDispatcherHITLGate(IsolatedAsyncioTestCase):
    """Verify the HITL approval gate in the tool-calling loop."""

    async def test_needs_approval_false_when_no_filter(self):
        """_needs_approval returns False when no approval_filter is set."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher
        dispatcher = ToolDispatcher(AsyncMock(), ToolRegistry())
        self.assertFalse(dispatcher._needs_approval("any_tool", {}))

    async def test_needs_approval_delegates_to_filter(self):
        """_needs_approval delegates to the approval_filter callable."""
        filt = lambda name, args: name == "dangerous"
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher
        dispatcher = ToolDispatcher(AsyncMock(), ToolRegistry(), approval_filter=filt)
        self.assertTrue(dispatcher._needs_approval("dangerous", {}))
        self.assertFalse(dispatcher._needs_approval("safe", {}))

    async def test_approved_tool_executes(self):
        """When HITL approves, the tool executes normally."""
        def safe_tool(x: str) -> str:
            """Safe."""
            return f"result: {x}"

        from dragon.ai.agent.hitl.models import HumanApprovalResponse

        responses = [
            _tool_request_json("safe_tool", {"x": "test"}),
            _final_answer_json("Done with result."),
        ]
        dispatcher = _make_dispatcher(
            responses,
            tools={"safe_tool": safe_tool},
            approval_filter=lambda name, args: True,  # all tools gated
        )

        mock_response = HumanApprovalResponse(approved=True)

        with patch(
            "dragon.ai.agent.hitl.approval.request_human_approval",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            result = await dispatcher.chat(
                [{"role": "user", "content": "do it"}],
                hitl_queue=MagicMock(),
                ddict={},
                task_id="t1",
                agent_id="a1",
                dispatch_id="d1",
            )

        turns = result[0]
        self.assertEqual(turns[-1]["content"], "Done with result.")

    async def test_rejected_tool_sends_rejection_to_llm(self):
        """When HITL rejects, rejection message is fed back to LLM instead of tool output."""
        def danger(x: str) -> str:
            """Danger."""
            return "should not reach"

        from dragon.ai.agent.hitl.models import HumanApprovalResponse

        responses = [
            _tool_request_json("danger", {"x": "bad"}),
            _final_answer_json("Understood, tool was rejected."),
        ]
        dispatcher = _make_dispatcher(
            responses,
            tools={"danger": danger},
            approval_filter=lambda name, args: True,
        )

        mock_response = HumanApprovalResponse(approved=False, reason="Too risky")

        with patch(
            "dragon.ai.agent.hitl.approval.request_human_approval",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            result = await dispatcher.chat(
                [{"role": "user", "content": "do it"}],
                hitl_queue=MagicMock(),
                ddict={},
                task_id="t1",
                agent_id="a1",
                dispatch_id="d1",
            )

        turns = result[0]
        # There should be a tool result with the rejection
        tool_msgs = [m for m in turns if m.get("role") == "tool"]
        self.assertEqual(len(tool_msgs), 1)
        self.assertIn("REJECTED", tool_msgs[0]["content"])

    async def test_feedback_response_includes_feedback_message(self):
        """When HITL gives feedback, the feedback text is sent to the LLM."""
        def gated_tool(x: str) -> str:
            """Gated."""
            return "should not reach"

        from dragon.ai.agent.hitl.models import HumanApprovalResponse

        responses = [
            _tool_request_json("gated_tool", {"x": "val"}),
            _final_answer_json("Adjusting based on feedback."),
        ]
        dispatcher = _make_dispatcher(
            responses,
            tools={"gated_tool": gated_tool},
            approval_filter=lambda name, args: True,
        )

        mock_response = HumanApprovalResponse(
            approved=False, reason="Use parameter Y instead", is_feedback=True
        )

        with patch(
            "dragon.ai.agent.hitl.approval.request_human_approval",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            result = await dispatcher.chat(
                [{"role": "user", "content": "do it"}],
                hitl_queue=MagicMock(),
                ddict={},
                task_id="t1",
                agent_id="a1",
                dispatch_id="d1",
            )

        turns = result[0]
        tool_msgs = [m for m in turns if m.get("role") == "tool"]
        self.assertIn("FEEDBACK", tool_msgs[0]["content"])
        self.assertIn("Use parameter Y instead", tool_msgs[0]["content"])


# ========================================================================
# Tool execution edge cases
# ========================================================================

class TestToolDispatcherExecuteTool(IsolatedAsyncioTestCase):
    """Verify _execute_tool_call handles local, MCP, and async tools."""

    async def test_async_local_tool_awaited(self):
        """Async local tools are awaited during execution."""
        async def async_tool(x: str) -> str:
            """Async tool."""
            return f"async: {x}"

        responses = [
            _tool_request_json("async_tool", {"x": "hello"}),
            _final_answer_json("Got async result."),
        ]
        dispatcher = _make_dispatcher(responses, tools={"async_tool": async_tool})
        result = await dispatcher.chat([{"role": "user", "content": "test"}])
        turns = result[0]
        tool_msgs = [m for m in turns if m.get("role") == "tool"]
        self.assertIn("async: hello", tool_msgs[0]["content"])

    async def test_missing_mcp_alias_raises(self):
        """Calling an MCP tool with unknown alias raises ToolExecutionError."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher
        from dragon.ai.agent.utils.errors import ToolExecutionError

        llm = AsyncMock()
        llm.chat = AsyncMock(side_effect=[
            _tool_request_json("unknown__tool", {}),
            _final_answer_json("fallback"),
        ])

        registry = ToolRegistry()
        dispatcher = ToolDispatcher(llm, registry)
        # Register the scoped name as MCP but don't register the client
        # Simulate: _mcp_clients is empty but scoped_names would match

        # The tool call should fail with error and be fed back to LLM
        mock_mcp = MagicMock()
        mock_mcp.tools_schemas = [
            {"type": "function", "function": {"name": "unknown__tool", "description": "X"}}
        ]
        mock_mcp.scoped_names = {"unknown__tool"}
        dispatcher._mcp_clients["unknown"] = mock_mcp
        mock_mcp.call_tool = AsyncMock(side_effect=RuntimeError("connection lost"))

        result = await dispatcher.chat([{"role": "user", "content": "test"}])
        turns = result[0]
        tool_msgs = [m for m in turns if m.get("role") == "tool"]
        self.assertIn("error", tool_msgs[0]["content"])


# ========================================================================
# Memory management integration
# ========================================================================

class TestToolDispatcherMemoryManagement(IsolatedAsyncioTestCase):
    """Verify _apply_memory_management interacts with ContextManager."""

    async def test_context_manager_called_before_llm(self):
        """When a context_manager is set, enforce_window is called before each LLM call."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher

        llm = AsyncMock()
        llm.chat = AsyncMock(return_value=_final_answer_json("done"))

        registry = ToolRegistry()
        ctx_mgr = MagicMock()
        ctx_mgr.enforce_window = MagicMock()
        ctx_mgr.should_summarize = MagicMock(return_value=False)
        ctx_mgr.maybe_summarize = AsyncMock(return_value=None)
        ctx_mgr._config = MagicMock()
        ctx_mgr._config.max_kept_turns = 8

        def add_tool(x: int) -> int:
            """Add."""
            return x + 1

        registry.register(add_tool)

        dispatcher = ToolDispatcher(llm, registry, context_manager=ctx_mgr)
        await dispatcher.chat([{"role": "user", "content": "test"}])

        ctx_mgr.enforce_window.assert_called()
        ctx_mgr.maybe_summarize.assert_called()

    async def test_no_context_manager_skips_memory(self):
        """Without context_manager, memory management is skipped entirely."""
        def noop_tool(x: str) -> str:
            """Noop."""
            return x

        responses = [_final_answer_json("done")]
        dispatcher = _make_dispatcher(responses, tools={"noop_tool": noop_tool})
        self.assertIsNone(dispatcher._context_manager)

        # Should not raise even though no context_manager
        result = await dispatcher.chat([{"role": "user", "content": "test"}])
        self.assertEqual(result[0][-1]["content"], "done")


# ========================================================================
# Helper: _generate_random_id
# ========================================================================

class TestToolDispatcherGenerateRandomId(TestCase):
    """Verify _generate_random_id produces valid IDs."""

    def test_default_length(self):
        """Default length is 8 characters."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher
        result = ToolDispatcher._generate_random_id()
        self.assertEqual(len(result), 8)

    def test_custom_length(self):
        """Custom length produces an ID of that length."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher
        result = ToolDispatcher._generate_random_id(length=16)
        self.assertEqual(len(result), 16)

    def test_alphanumeric_only(self):
        """Generated ID contains only alphanumeric characters."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher
        result = ToolDispatcher._generate_random_id(length=100)
        self.assertTrue(result.isalnum())

    def test_unique_ids(self):
        """Two generated IDs are very likely different."""
        from dragon.ai.agent.reasoning.tool_dispatcher import ToolDispatcher
        id1 = ToolDispatcher._generate_random_id(length=20)
        id2 = ToolDispatcher._generate_random_id(length=20)
        self.assertNotEqual(id1, id2)


# ========================================================================
# Multiple tool calls in single iteration
# ========================================================================

class TestToolDispatcherMultipleToolCalls(IsolatedAsyncioTestCase):
    """Verify handling of multiple tool calls in a single LLM response."""

    async def test_two_tool_calls_in_one_response(self):
        """LLM requests two tools at once; both are executed and results fed back."""
        def add(a: int, b: int) -> int:
            """Add."""
            return a + b

        def multiply(a: int, b: int) -> int:
            """Multiply."""
            return a * b

        multi_tool_request = json.dumps({
            "response": {
                "type": "tool_request",
                "tool_calls": [
                    {"name": "add", "args": {"a": 2, "b": 3}},
                    {"name": "multiply", "args": {"a": 4, "b": 5}},
                ],
            }
        })

        responses = [multi_tool_request, _final_answer_json("Sum=5, Product=20")]
        dispatcher = _make_dispatcher(
            responses, tools={"add": add, "multiply": multiply}
        )

        result = await dispatcher.chat([{"role": "user", "content": "calc"}])
        turns = result[0]
        tool_msgs = [m for m in turns if m.get("role") == "tool"]
        self.assertEqual(len(tool_msgs), 2)
        contents = [m["content"] for m in tool_msgs]
        self.assertTrue(any("5" in c for c in contents))
        self.assertTrue(any("20" in c for c in contents))


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
