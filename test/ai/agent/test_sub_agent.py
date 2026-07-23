"""Tests for SubAgent — system prompt, process, task isolation, and error paths."""

import dragon
import multiprocessing as mp


import asyncio
import json
import types
import sys
from typing import Optional
from unittest import TestCase, IsolatedAsyncioTestCase, main
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from dragon.ai.agent.config import (
    AgentConfig, TaskStatus, DispatchHeader,
    STATUS_KEY, RESULT_KEY, GLOBAL_STATE_ENTRY_KEY,
    GLOBAL_STATE_KEY, USER_INPUT_KEY, DISPATCH_ID_KEY,
)
from dragon.ai.agent.tools.registry import ToolRegistry
from dragon.ai.agent.ddict.accessor import DDictAccessor


# ---------------------------------------------------------------------------
# Ensure ``inference.llm_proxy`` is importable (lazy import inside base.py)
# ---------------------------------------------------------------------------
if "inference" not in sys.modules:
    _inf = types.ModuleType("inference")
    _inf.__path__ = []
    _inf.__package__ = "inference"
    sys.modules["inference"] = _inf

    _proxy_mod = types.ModuleType("inference.llm_proxy")

    class _FakeLLMProxy:
        """Lightweight stub returned by DragonQueueLLMProxy(...)."""
        def __init__(self, *args, **kwargs):
            pass

    _proxy_mod.DragonQueueLLMProxy = _FakeLLMProxy
    sys.modules["inference.llm_proxy"] = _proxy_mod
    _inf.llm_proxy = _proxy_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sub_agent(tools: Optional[dict] = None, role: str = "Analyze data"):
    """Create a SubAgent with mocked LLM and optional tools."""
    from dragon.ai.agent.core.sub_agent import SubAgent

    registry = ToolRegistry()
    if tools:
        for name, fn in tools.items():
            registry.register(fn)

    config = AgentConfig(
        agent_id="test_agent",
        name="Test Agent",
        role=role,
        inference_queue=MagicMock(),
    )

    agent = SubAgent(config, registry)
    return agent


# ========================================================================
# _build_system_prompt
# ========================================================================

class TestBuildSystemPrompt(TestCase):
    """Verify _build_system_prompt assembles role, name, and tool info."""

    def test_includes_role(self):
        """System prompt contains the agent's configured role string."""
        agent = _make_sub_agent(role="Find research papers")
        prompt = agent._system_prompt
        self.assertIn("Find research papers", prompt)

    def test_includes_agent_name(self):
        """System prompt contains the agent's display name."""
        agent = _make_sub_agent()
        self.assertIn("Test Agent", agent._system_prompt)

    def test_includes_tool_schemas_when_tools_present(self):
        """When tools are registered, the prompt lists their schemas."""
        def search(query: str) -> list:
            """Search for results."""
            return []

        agent = _make_sub_agent(tools={"search": search})
        self.assertIn("search", agent._system_prompt)
        self.assertIn("tools", agent._system_prompt.lower())

    def test_no_tools_section_when_empty(self):
        """With no tools registered, the prompt omits the tools section."""
        agent = _make_sub_agent()
        self.assertNotIn("Available tools:", agent._system_prompt)


# ========================================================================
# process
# ========================================================================

class TestSubAgentProcess(IsolatedAsyncioTestCase):
    """Verify process() drives the LLM loop and returns/stores results."""

    async def test_process_returns_response(self):
        """process() returns {'response': <final_answer>} from the LLM."""
        agent = _make_sub_agent()

        # Mock tool_dispatcher.chat to return a final answer
        agent.tool_dispatcher = MagicMock()
        agent.tool_dispatcher.chat = AsyncMock(return_value=[
            [{"role": "assistant", "content": "The answer is 42."}]
        ])

        ddict = {
            GLOBAL_STATE_KEY.format(task_id="t1"): [],
        }
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        header = DispatchHeader(task="What is 42?", serialized_ddict="fake")
        result = await agent.process("t1", header, accessor)

        self.assertIn("response", result)
        self.assertEqual(result["response"], "The answer is 42.")

    async def test_process_writes_global_state_entry(self):
        """After LLM answers, the result is written to the global state key in DDict."""
        agent = _make_sub_agent()

        agent.tool_dispatcher = MagicMock()
        agent.tool_dispatcher.chat = AsyncMock(return_value=[
            [{"role": "assistant", "content": "Done."}]
        ])

        ddict = {GLOBAL_STATE_KEY.format(task_id="t1"): []}
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        header = DispatchHeader(task="Do stuff", serialized_ddict="fake")
        await agent.process("t1", header, accessor)

        entry_key = GLOBAL_STATE_ENTRY_KEY.format(task_id="t1", agent_id="test_agent")
        self.assertIn(entry_key, ddict)
        self.assertEqual(ddict[entry_key]["answer"], "Done.")


# ========================================================================
# _invoke_llm_with_tools — upstream result reading
# ========================================================================

class TestInvokeLLMWithTools(IsolatedAsyncioTestCase):
    """Verify _invoke_llm_with_tools reads upstream context correctly."""

    async def test_direct_read_upstream_results(self):
        """With upstream_agent_ids, reads each parent's result from DDict and injects it into the LLM messages."""
        agent = _make_sub_agent()

        agent.tool_dispatcher = MagicMock()
        agent.tool_dispatcher.chat = AsyncMock(return_value=[
            [{"role": "assistant", "content": "Combined result."}]
        ])

        ddict = {
            USER_INPUT_KEY.format(task_id="t1"): "Analyze X",
            DISPATCH_ID_KEY.format(task_id="t1", agent_id="parent1"): "d_parent1",
            RESULT_KEY.format(task_id="t1", agent_id="parent1", dispatch_id="d_parent1"): "Parent found X",
            GLOBAL_STATE_KEY.format(task_id="t1"): [],
        }
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        answer = await agent._invoke_llm_with_tools(
            task_id="t1", dispatch_id="d1", task="Combine results",
            accessor=accessor, upstream_agent_ids=["parent1"],
        )
        self.assertEqual(answer, "Combined result.")

        # Verify the LLM received messages containing the parent's result
        call_args = agent.tool_dispatcher.chat.call_args
        messages = call_args[0][0]
        user_msgs = [m for m in messages if m["role"] == "user"]
        # Should have: user_input msg, parent result msg, task msg
        self.assertTrue(any("Parent found X" in m["content"] for m in user_msgs))

    async def test_fallback_global_state(self):
        """Root agents (no upstream) read global_state."""
        agent = _make_sub_agent()

        agent.tool_dispatcher = MagicMock()
        agent.tool_dispatcher.chat = AsyncMock(return_value=[
            [{"role": "assistant", "content": "Root answer."}]
        ])

        ddict = {
            GLOBAL_STATE_KEY.format(task_id="t1"): [
                {"agent_id": "other", "answer": "Prior work."}
            ],
        }
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        answer = await agent._invoke_llm_with_tools(
            task_id="t1", dispatch_id="d1", task="Start work",
            accessor=accessor, upstream_agent_ids=[],
        )
        self.assertEqual(answer, "Root answer.")

    async def test_missing_upstream_raises(self):
        """RuntimeError raised when a declared upstream agent has no dispatch_id in DDict."""
        agent = _make_sub_agent()
        ddict = {
            USER_INPUT_KEY.format(task_id="t1"): "X",
        }
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        with self.assertRaisesRegex(RuntimeError, "dispatch_id"):
            await agent._invoke_llm_with_tools(
                task_id="t1", dispatch_id="d1", task="X",
                accessor=accessor, upstream_agent_ids=["missing_parent"],
            )


# ========================================================================
# Task isolation — _handle_message
# ========================================================================

class TestTaskIsolation(IsolatedAsyncioTestCase):
    """Verify _handle_message provides task-isolation guarantees."""

    async def test_exception_does_not_propagate(self):
        """_handle_message must swallow exceptions and publish ERROR to DDict."""
        agent = _make_sub_agent()

        # Make process() raise
        agent.process = AsyncMock(side_effect=RuntimeError("LLM exploded"))

        ddict = {}
        mock_msg = MagicMock()
        mock_msg.task_id = "t1"
        mock_msg.header = DispatchHeader(
            task="Fail", serialized_ddict="fake", dispatch_id="d1",
        )
        mock_msg.header.completion_event = MagicMock()
        mock_msg.header.completion_event.set = MagicMock()

        with patch("dragon.ai.agent.core.sub_agent.DDict") as ddict_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock(side_effect=ddict.__setitem__)
            mock_ddict.__getitem__ = MagicMock(side_effect=ddict.__getitem__)
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            # _handle_message should NOT raise
            await agent._handle_message(mock_msg)

        # Completion event should be set (so dispatcher unblocks)
        mock_msg.header.completion_event.set.assert_called()

    async def test_handle_message_sets_processing_status(self):
        """_handle_message writes PROCESSING status before calling process()."""
        agent = _make_sub_agent()

        statuses_written = []
        original_process = agent.process

        async def tracking_process(task_id, header, accessor):
            # At this point, status should already be PROCESSING
            status_key = STATUS_KEY.format(
                task_id=task_id,
                agent_id="test_agent",
                dispatch_id=header.dispatch_id,
            )
            try:
                val = accessor.get(status_key)
                statuses_written.append(val)
            except KeyError:
                statuses_written.append("NOT_SET")
            return {"response": "ok"}

        agent.process = tracking_process

        agent.tool_dispatcher = MagicMock()
        agent.tool_dispatcher.chat = AsyncMock(return_value=[
            [{"role": "assistant", "content": "ok"}]
        ])

        mock_msg = MagicMock()
        mock_msg.task_id = "t1"
        mock_msg.header = DispatchHeader(
            task="Work", serialized_ddict="fake", dispatch_id="d1",
        )
        mock_msg.header.completion_event = MagicMock()
        mock_msg.header.completion_event.set = MagicMock()

        with patch("dragon.ai.agent.core.sub_agent.DDict") as ddict_cls:
            mock_ddict = {}
            real_ddict = MagicMock()
            real_ddict.__setitem__ = MagicMock(side_effect=mock_ddict.__setitem__)
            real_ddict.__getitem__ = MagicMock(side_effect=mock_ddict.__getitem__)
            real_ddict.__contains__ = MagicMock(side_effect=mock_ddict.__contains__)
            real_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = real_ddict

            await agent._handle_message(mock_msg)

        self.assertEqual(statuses_written, [TaskStatus.PROCESSING])

    async def test_handle_message_writes_done_on_success(self):
        """On success, _handle_message writes result and DONE status."""
        agent = _make_sub_agent()
        agent.process = AsyncMock(return_value={"response": "great"})

        ddict = {}
        mock_msg = MagicMock()
        mock_msg.task_id = "t1"
        mock_msg.header = DispatchHeader(
            task="Work", serialized_ddict="fake", dispatch_id="d1",
        )
        mock_msg.header.completion_event = MagicMock()
        mock_msg.header.completion_event.set = MagicMock()

        with patch("dragon.ai.agent.core.sub_agent.DDict") as ddict_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock(side_effect=ddict.__setitem__)
            mock_ddict.__getitem__ = MagicMock(side_effect=ddict.__getitem__)
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            await agent._handle_message(mock_msg)

        status_key = STATUS_KEY.format(task_id="t1", agent_id="test_agent", dispatch_id="d1")
        result_key = RESULT_KEY.format(task_id="t1", agent_id="test_agent", dispatch_id="d1")
        self.assertEqual(ddict[status_key], TaskStatus.DONE)
        self.assertEqual(ddict[result_key], {"response": "great"})

    async def test_handle_message_writes_error_on_failure(self):
        """On process failure, _handle_message writes ERROR status and error result."""
        agent = _make_sub_agent()
        agent.process = AsyncMock(side_effect=RuntimeError("boom"))

        ddict = {}
        mock_msg = MagicMock()
        mock_msg.task_id = "t1"
        mock_msg.header = DispatchHeader(
            task="Work", serialized_ddict="fake", dispatch_id="d1",
        )
        mock_msg.header.completion_event = MagicMock()
        mock_msg.header.completion_event.set = MagicMock()

        with patch("dragon.ai.agent.core.sub_agent.DDict") as ddict_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock(side_effect=ddict.__setitem__)
            mock_ddict.__getitem__ = MagicMock(side_effect=ddict.__getitem__)
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            await agent._handle_message(mock_msg)

        status_key = STATUS_KEY.format(task_id="t1", agent_id="test_agent", dispatch_id="d1")
        result_key = RESULT_KEY.format(task_id="t1", agent_id="test_agent", dispatch_id="d1")
        self.assertEqual(ddict[status_key], TaskStatus.ERROR)
        self.assertIn("error", ddict[result_key])

    async def test_handle_message_detaches_ddict(self):
        """DDict.detach() is always called in the finally block."""
        agent = _make_sub_agent()
        agent.process = AsyncMock(return_value={"response": "ok"})

        mock_msg = MagicMock()
        mock_msg.task_id = "t1"
        mock_msg.header = DispatchHeader(
            task="Work", serialized_ddict="fake", dispatch_id="d1",
        )
        mock_msg.header.completion_event = MagicMock()
        mock_msg.header.completion_event.set = MagicMock()

        with patch("dragon.ai.agent.core.sub_agent.DDict") as ddict_cls:
            mock_ddict = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            await agent._handle_message(mock_msg)

        mock_ddict.detach.assert_called_once()

    async def test_handle_message_detaches_ddict_on_error(self):
        """DDict.detach() is called even when process() raises."""
        agent = _make_sub_agent()
        agent.process = AsyncMock(side_effect=RuntimeError("fail"))

        mock_msg = MagicMock()
        mock_msg.task_id = "t1"
        mock_msg.header = DispatchHeader(
            task="Fail", serialized_ddict="fake", dispatch_id="d1",
        )
        mock_msg.header.completion_event = MagicMock()
        mock_msg.header.completion_event.set = MagicMock()

        with patch("dragon.ai.agent.core.sub_agent.DDict") as ddict_cls:
            mock_ddict = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            await agent._handle_message(mock_msg)

        mock_ddict.detach.assert_called_once()


# ========================================================================
# _invoke_llm_with_tools — additional error paths
# ========================================================================

class TestInvokeLLMWithToolsErrors(IsolatedAsyncioTestCase):
    """Verify error paths in _invoke_llm_with_tools."""

    async def test_missing_user_input_raises(self):
        """RuntimeError raised when USER_INPUT_KEY is missing with upstream agents."""
        agent = _make_sub_agent()
        ddict = {}
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        with self.assertRaisesRegex(RuntimeError, "Missing user input"):
            await agent._invoke_llm_with_tools(
                task_id="t1", dispatch_id="d1", task="X",
                accessor=accessor, upstream_agent_ids=["parent"],
            )

    async def test_missing_upstream_result_raises(self):
        """RuntimeError raised when upstream agent has dispatch_id but no result."""
        agent = _make_sub_agent()
        ddict = {
            USER_INPUT_KEY.format(task_id="t1"): "request",
            DISPATCH_ID_KEY.format(task_id="t1", agent_id="parent"): "dp1",
            # No RESULT_KEY — parent has no result
        }
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        with self.assertRaisesRegex(RuntimeError, "no result"):
            await agent._invoke_llm_with_tools(
                task_id="t1", dispatch_id="d1", task="X",
                accessor=accessor, upstream_agent_ids=["parent"],
            )

    async def test_dict_upstream_result_serialized_as_json(self):
        """Dict upstream results are serialized as JSON for the LLM context."""
        agent = _make_sub_agent()
        agent.tool_dispatcher = MagicMock()
        agent.tool_dispatcher.chat = AsyncMock(return_value=[
            [{"role": "assistant", "content": "got it."}]
        ])

        ddict = {
            USER_INPUT_KEY.format(task_id="t1"): "analyze",
            DISPATCH_ID_KEY.format(task_id="t1", agent_id="p1"): "dp1",
            RESULT_KEY.format(task_id="t1", agent_id="p1", dispatch_id="dp1"): {"key": "value"},
            GLOBAL_STATE_KEY.format(task_id="t1"): [],
        }
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        await agent._invoke_llm_with_tools(
            task_id="t1", dispatch_id="d1", task="Analyze",
            accessor=accessor, upstream_agent_ids=["p1"],
        )

        call_args = agent.tool_dispatcher.chat.call_args
        messages = call_args[0][0]
        user_msgs = [m for m in messages if m["role"] == "user"]
        # The dict should be serialized as JSON (indented)
        self.assertTrue(any('"key": "value"' in m["content"] for m in user_msgs))

    async def test_string_upstream_result_used_as_is(self):
        """String upstream results are used directly (not JSON-serialized)."""
        agent = _make_sub_agent()
        agent.tool_dispatcher = MagicMock()
        agent.tool_dispatcher.chat = AsyncMock(return_value=[
            [{"role": "assistant", "content": "ok."}]
        ])

        ddict = {
            USER_INPUT_KEY.format(task_id="t1"): "analyze",
            DISPATCH_ID_KEY.format(task_id="t1", agent_id="p1"): "dp1",
            RESULT_KEY.format(task_id="t1", agent_id="p1", dispatch_id="dp1"): "plain text result",
            GLOBAL_STATE_KEY.format(task_id="t1"): [],
        }
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        await agent._invoke_llm_with_tools(
            task_id="t1", dispatch_id="d1", task="Analyze",
            accessor=accessor, upstream_agent_ids=["p1"],
        )

        call_args = agent.tool_dispatcher.chat.call_args
        messages = call_args[0][0]
        user_msgs = [m for m in messages if m["role"] == "user"]
        self.assertTrue(any("plain text result" in m["content"] for m in user_msgs))

    async def test_empty_upstream_list_uses_global_state(self):
        """Empty upstream_agent_ids list triggers global_state fallback."""
        agent = _make_sub_agent()
        agent.tool_dispatcher = MagicMock()
        agent.tool_dispatcher.chat = AsyncMock(return_value=[
            [{"role": "assistant", "content": "from global."}]
        ])

        state_entry = {"agent_id": "other", "answer": "prior work"}
        ddict = {
            GLOBAL_STATE_KEY.format(task_id="t1"): [state_entry],
        }
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        answer = await agent._invoke_llm_with_tools(
            task_id="t1", dispatch_id="d1", task="Continue",
            accessor=accessor, upstream_agent_ids=[],
        )

        call_args = agent.tool_dispatcher.chat.call_args
        messages = call_args[0][0]
        user_msgs = [m for m in messages if m["role"] == "user"]
        self.assertTrue(any("prior work" in m["content"] for m in user_msgs))

    async def test_multiple_upstream_results_injected(self):
        """Multiple upstream agents each have their results injected into messages."""
        agent = _make_sub_agent()
        agent.tool_dispatcher = MagicMock()
        agent.tool_dispatcher.chat = AsyncMock(return_value=[
            [{"role": "assistant", "content": "combined."}]
        ])

        ddict = {
            USER_INPUT_KEY.format(task_id="t1"): "combine",
            DISPATCH_ID_KEY.format(task_id="t1", agent_id="p1"): "dp1",
            DISPATCH_ID_KEY.format(task_id="t1", agent_id="p2"): "dp2",
            RESULT_KEY.format(task_id="t1", agent_id="p1", dispatch_id="dp1"): "result_A",
            RESULT_KEY.format(task_id="t1", agent_id="p2", dispatch_id="dp2"): "result_B",
            GLOBAL_STATE_KEY.format(task_id="t1"): [],
        }
        accessor = DDictAccessor(ddict, agent_id="test_agent", task_id="t1")

        await agent._invoke_llm_with_tools(
            task_id="t1", dispatch_id="d1", task="Merge",
            accessor=accessor, upstream_agent_ids=["p1", "p2"],
        )

        call_args = agent.tool_dispatcher.chat.call_args
        messages = call_args[0][0]
        user_msgs = [m for m in messages if m["role"] == "user"]
        contents = " ".join(m["content"] for m in user_msgs)
        self.assertIn("result_A", contents)
        self.assertIn("result_B", contents)


# ========================================================================
# SubAgent construction edge cases
# ========================================================================

class TestSubAgentConstruction(TestCase):
    """Verify SubAgent construction validation."""

    def test_no_llm_raises_runtime_error(self):
        """SubAgent without inference_queue raises RuntimeError."""
        from dragon.ai.agent.core.sub_agent import SubAgent

        config = AgentConfig(
            agent_id="broken",
            name="Broken Agent",
            role="Nothing",
            # No inference_queue → tool_dispatcher will be None
        )
        with self.assertRaisesRegex(RuntimeError, "no LLM configured"):
            SubAgent(config, ToolRegistry())

    def test_system_prompt_built_once(self):
        """_system_prompt is built during __init__ and is immutable."""
        agent = _make_sub_agent(role="Research papers")
        prompt1 = agent._system_prompt
        prompt2 = agent._system_prompt
        self.assertIs(prompt1, prompt2)
        self.assertIn("Research papers", prompt1)


# ========================================================================
# DragonAgent.list_tools
# ========================================================================

class TestDragonAgentListTools(TestCase):
    """Verify the list_tools() method on the base DragonAgent."""

    def test_list_tools_with_registry_only(self):
        """list_tools returns registry tools and empty mcp dict."""
        def search(q: str) -> list:
            """Search."""
            return []

        agent = _make_sub_agent(tools={"search": search})
        result = agent.list_tools()
        self.assertIn("registry", result)
        self.assertIn("mcp", result)
        self.assertEqual(len(result["registry"]), 1)
        self.assertEqual(result["mcp"], {})

    def test_list_tools_with_mcp(self):
        """list_tools includes MCP tools grouped by alias."""
        agent = _make_sub_agent()
        mock_mcp = MagicMock()
        mock_mcp.tools_schemas = [
            {"type": "function", "function": {"name": "jupyter__run", "description": "Run"}}
        ]
        agent.tool_dispatcher._mcp_clients["jupyter"] = mock_mcp

        result = agent.list_tools()
        self.assertIn("jupyter", result["mcp"])
        self.assertEqual(len(result["mcp"]["jupyter"]), 1)

    def test_list_tools_empty(self):
        """list_tools with no tools returns empty registry and empty mcp."""
        agent = _make_sub_agent()
        result = agent.list_tools()
        self.assertEqual(result["registry"], [])
        self.assertEqual(result["mcp"], {})


# ========================================================================
# _log_task_exception — done callback
# ========================================================================

class TestLogTaskException(TestCase):
    """Verify _log_task_exception done-callback behavior."""

    def test_no_crash_on_cancelled_task(self):
        """Cancelled task does not raise in the callback."""
        from dragon.ai.agent.core.sub_agent import _log_task_exception

        mock_task = MagicMock()
        mock_task.cancelled.return_value = True
        _log_task_exception(mock_task)  # should not raise

    def test_no_crash_on_successful_task(self):
        """Successful task (no exception) does not raise."""
        from dragon.ai.agent.core.sub_agent import _log_task_exception

        mock_task = MagicMock()
        mock_task.cancelled.return_value = False
        mock_task.exception.return_value = None
        _log_task_exception(mock_task)  # should not raise

    def test_logs_exception_from_failed_task(self):
        """Failed task's exception is logged (not re-raised)."""
        from dragon.ai.agent.core.sub_agent import _log_task_exception

        mock_task = MagicMock()
        mock_task.cancelled.return_value = False
        mock_task.exception.return_value = RuntimeError("task failed")
        mock_task.get_name.return_value = "test-task"
        # Should not raise — just log
        _log_task_exception(mock_task)


# ========================================================================
# DragonAgent — protocol validation
# ========================================================================

class TestDragonAgentProtocol(TestCase):
    """Verify DragonAgent rejects unsupported protocols."""

    def test_non_dragon_protocol_raises(self):
        """Passing a non-DragonQueueProtocol class raises NotImplementedError."""
        config = AgentConfig(agent_id="a", name="A", role="r")
        with self.assertRaises(NotImplementedError):
            from dragon.ai.agent.core.sub_agent import SubAgent
            SubAgent(config, ToolRegistry(), protocol=object)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
