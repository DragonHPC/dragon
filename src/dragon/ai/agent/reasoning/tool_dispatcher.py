"""ToolDispatcher — agentic tool-calling loop over LLMInferenceEngine.

This module implements the structured-output reasoning loop that sits between
the raw LLM (:class:`~dragon.ai.inference.llm_engine.LLMInferenceEngine`) and
the world of tools (:class:`~dragon.ai.agent.tools.registry.ToolRegistry` +
:class:`~dragon.ai.agent.tools.mcp_tool.MCPServerClient` instances).

JSON parsing / repair lives in :mod:`response_parser`; DDict event writing
lives in :mod:`event_writer`.

Typical usage::

    proxy = DragonQueueLLMProxy(input_queue)

    registry = ToolRegistry()
    registry.register(my_python_fn)

    dispatcher = ToolDispatcher(proxy, registry)
    await dispatcher.connect_mcp("http://server1/mcp", token1, alias="jupyter")
    await dispatcher.connect_mcp("http://server2/mcp", token2, alias="filesystem")

    outputs = await dispatcher.chat(messages)

    await dispatcher.close_mcp()        # close all MCP connections
"""

from __future__ import annotations

import inspect
import json
import random
import string
from typing import Any, Dict, List

from ..utils.errors import ToolExecutionError, AgentLoopError
from ..utils.logging import get_agent_logger
from ..config.memory_config import MemoryStrategy
from ..observability.tracer import trace_span
from ..observability.trace_protocol import (
    SpanKind,
    ATTR_LLM_ITERATION, ATTR_LLM_EVENT_INDEX,
    ATTR_TOOL_NAME, ATTR_TOOL_SOURCE, ATTR_TOOL_EVENT_INDEX,
    ATTR_HITL_TOOL_GATED, ATTR_HITL_APPROVED, ATTR_HITL_EVENT_INDEX,
    ATTR_MEMORY_EVENT_INDEX,
    ATTR_MEMORY_TURNS_PRUNED, ATTR_MEMORY_TURNS_BEFORE, ATTR_MEMORY_TURNS_KEPT,
    ATTR_MEMORY_TURNS_SUMMARIZED, ATTR_MEMORY_SUMMARY_CHARS,
    ATTR_MEMORY_STRATEGY,
    EVT_MEMORY_INPUT, EVT_MEMORY_OUTPUT,
)
from ..ddict import DDictAccessor

from .response_parser import ResponseModel, parse_llm_response, handle_final_answer
from .event_writer import (
    write_llm_event,
    write_tool_event,
    write_hitl_event,
    write_memory_event,
)

log = get_agent_logger("tool_dispatcher")


class ToolDispatcher:
    """Drives the structured-output agentic loop for a single LLM engine.

    Responsibilities:

    - Owns and manages any number of :class:`~dragon.ai.agent.tools.mcp_tool.MCPServerClient`
      connections (one per MCP server, keyed by user-supplied alias).
    - Combines local tool schemas (from :class:`~dragon.ai.agent.tools.registry.ToolRegistry`)
      with remote tool schemas (from each ``MCPServerClient``) into a single
      tool list for the LLM.
    - Routes tool calls: ``{alias}__{tool_name}`` → the matching
      ``MCPServerClient``; everything else → the local ``ToolRegistry``.

    Parameters
    ----------
    llm_engine:
        An :class:`~dragon.ai.inference.llm_proxy.LLMProxy` instance
        (or any object exposing a compatible ``.chat(messages, tools,
        json_schema, continue_final_message)`` method).
    registry:
        Registry of local :class:`~dragon.ai.agent.tools.base.BaseTool`
        instances.  Pass an empty ``ToolRegistry()`` if no local tools
        are needed.

    Example
    -------
    ::

        dispatcher = ToolDispatcher(llm_engine, registry)
        await dispatcher.connect_mcp(url, token, alias="jupyter")
        outputs = await dispatcher.chat(messages)
        await dispatcher.close_mcp()
    """

    def __init__(self, llm_engine, registry, approval_filter=None,
                 max_tool_call_iterations: int = 20,
                 context_manager=None) -> None:
        self._llm_engine = llm_engine
        self._registry = registry
        # alias → MCPServerClient
        self._mcp_clients: dict[str, Any] = {}
        # HITL — optional callable(tool_name, tool_args) → bool
        self._approval_filter = approval_filter
        self._max_tool_call_iterations = max_tool_call_iterations
        # Memory management — optional ContextManager instance
        self._context_manager = context_manager

    # ------------------------------------------------------------------
    # MCP connection management
    # ------------------------------------------------------------------

    async def connect_mcp(
        self,
        url: str,
        token: str,
        alias: str,
        *,
        max_retries: int = 3,
        retry_delay: float = 0.5,
        timeout: float = 5.0,
    ) -> None:
        """Connect to an MCP server and register it under *alias*.

        :param url: MCP server URL.
        :param token: Authentication token.
        :param alias: Short unique label (e.g. ``"jupyter"``).  Used as the
            routing prefix: tools from this server will be named
            ``{alias}__{tool_name}``.
        :param max_retries: Number of connection attempts before raising
            ``ConnectionError``.  Defaults to ``3``.
        :param retry_delay: Seconds to wait between retry attempts.
            Defaults to ``0.5``.
        :param timeout: Per-attempt connection timeout in seconds.
            Defaults to ``5.0``.
        :raises ValueError: If *alias* is already in use.
        :raises ConnectionError: If all retry attempts fail.
        """
        from ..tools.mcp_tool import MCPServerClient

        if alias in self._mcp_clients:
            raise ValueError(
                f"MCP alias '{alias}' is already connected. "
                "Use a different alias or call close_mcp(alias) first."
            )
        client = MCPServerClient(
            alias=alias,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
        )
        await client.start(url, token)
        self._mcp_clients[alias] = client
        log.debug(f"ToolDispatcher: MCP server '{alias}' connected.")

    async def close_mcp(self, alias: str | None = None) -> None:
        """Close one or all MCP connections.

        :param alias: If given, close only that connection.  If ``None``
            (default), close **all** connections.
        """
        if alias is not None:
            client = self._mcp_clients.pop(alias, None)
            if client:
                await client.close()
        else:
            for client in list(self._mcp_clients.values()):
                await client.close()
            self._mcp_clients.clear()

    # ------------------------------------------------------------------
    # Agentic loop
    # ------------------------------------------------------------------

    async def chat(self, prompts: List[Dict[str, Any]], **dispatch_ctx) -> List[List[str]]:
        """Run the structured-output agentic tool-calling loop.

        Builds a combined tool list from the ``ToolRegistry`` and all
        connected ``MCPServerClient`` instances, then iterates up to
        ``max_tool_call_iterations`` times.  On each iteration the LLM
        either requests one or more tool calls or signals a final answer.
        Tool calls are dispatched to the correct backend, and the results
        are appended to the conversation.  Once the LLM signals
        ``final_answer`` a second, unstructured call produces the
        natural-language response.

        Parameters
        ----------
        prompts:
            Conversation messages in OpenAI chat format (list of message dicts).
        **dispatch_ctx:
            DDict context forwarded from ``_invoke_llm_with_tools``:
            ``ddict``, ``task_id``, ``agent_id``, ``dispatch_id``.
            When ``hitl_queue`` is also present **and** ``self._approval_filter``
            is set, the HITL approval gate is active.
            Per-event keys (``LLM_EVENT_KEY``, ``TOOL_EVENT_KEY``,
            ``HITL_EVENT_KEY``) are written to DDict in real time using
            these context values.

        Returns
        -------
        list[list[str]]
            List of conversation turns added after the initial prompt.
        """
        # Build combined tool schema list
        all_tools: list[dict[str, Any]] = list(self._registry.list_tools())
        for client in self._mcp_clients.values():
            all_tools.extend(client.tools_schemas)

        # ------------------------------------------------------------------
        # Fast path: no tools registered and no MCP servers connected.
        # Skip the structured-output loop entirely — passing an empty tools
        # list still activates the JSON-schema prompt, causing the model to
        # hallucinate tool names it was pre-trained on (e.g. "web_search").
        # Instead do a single plain chat call and return immediately.
        # ------------------------------------------------------------------
        if not all_tools:
            final_text = await self._llm_engine.chat(prompts)
            # Write LLM event for tracing even on the no-tools fast path
            _tracing = dispatch_ctx.get("tracing", False)
            if _tracing:
                _ddict = dispatch_ctx.get("ddict")
                _task_id = dispatch_ctx.get("task_id", "")
                _agent_id = dispatch_ctx.get("agent_id", "")
                _dispatch_id = dispatch_ctx.get("dispatch_id", "")
                _accessor = DDictAccessor(_ddict, agent_id=_agent_id, task_id=_task_id) if _ddict is not None else None
                _fmt = dict(task_id=_task_id, agent_id=_agent_id, dispatch_id=_dispatch_id)
                write_llm_event(_accessor, True, _fmt, 0, prompts, final_text, 0)
            return [[{"role": "assistant", "content": final_text}]]

        # Build set of all MCP-scoped names for fast routing
        all_mcp_scoped_names: set[str] = set()
        for client in self._mcp_clients.values():
            all_mcp_scoped_names.update(client.scoped_names)

        # Build the JSON schema for structured output.
        union_schema = ResponseModel.model_json_schema()

        # Prepend the reasoning system prompt (agent-owned, not user-facing)
        copy_prompts: list[dict[str, Any]] = [
            {
                "role": "system",
                "content": (
                    "You are a Precise Data Retrieval Assistant. "
                    "Your goal is to satisfy the user request by using tools.\n\n"

                    "## OUTPUT FORMAT\n"
                    "You MUST respond with a single JSON object and NOTHING else — "
                    "no markdown, no commentary, no extra text before or after the JSON.\n\n"

                    "Choose exactly ONE of the two formats below:\n\n"

                    "FORMAT A — call a tool:\n"
                    '{"response": {"type": "tool_request", "tool_calls": '
                    '[{"name": "<tool_name>", "args": {<arguments>}}]}}\n\n'

                    "FORMAT B — give the final answer:\n"
                    '{"response": {"type": "final_answer", '
                    '"content": "<your complete answer here>"}}\n\n'

                    "## RULES\n"
                    "1. If ANY required data is still missing AND a tool exists "
                    "to retrieve it, you MUST use FORMAT A.\n"
                    "2. If ALL required data has been retrieved (or no suitable "
                    "tool exists), you MUST use FORMAT B.\n"
                    "3. Make ONE tool call per turn. After receiving the result, "
                    "re-evaluate what is still missing.\n"
                    "4. Use ONLY data from tool results in your final answer. "
                    "Never fabricate data.\n"
                    "5. Never invent tool names that are not in the available "
                    "tools list.\n\n"

                    "## EXAMPLES\n\n"

                    "Example 1 — data still missing, call a tool:\n"
                    "User: Check status for Order A and Order B.\n"
                    "History: tool result for Order A already received.\n"
                    "Response:\n"
                    '{"response": {"type": "tool_request", '
                    '"tool_calls": [{"name": "get_order", '
                    '"args": {"id": "Order B"}}]}}\n\n'

                    "Example 2 — all data present, give final answer:\n"
                    "User: Check status for Order A and Order B.\n"
                    "History: tool results for both Order A and Order B received.\n"
                    "Response:\n"
                    '{"response": {"type": "final_answer", '
                    '"content": "Order A is shipped and Order B is processing."}}'
                ),
            }
        ]
        copy_prompts.extend(prompts)

        max_tool_call_iterations = self._max_tool_call_iterations
        len_initial_prompt = len(copy_prompts)

        # DDict context for per-event real-time writes
        _ddict = dispatch_ctx.get("ddict")
        _tracing = dispatch_ctx.get("tracing", False)
        _task_id = dispatch_ctx.get("task_id", "")
        _agent_id = dispatch_ctx.get("agent_id", "")
        _dispatch_id = dispatch_ctx.get("dispatch_id", "")
        _accessor = DDictAccessor(_ddict, agent_id=_agent_id, task_id=_task_id) if _ddict is not None else None
        _fmt = dict(task_id=_task_id, agent_id=_agent_id, dispatch_id=_dispatch_id)
        _llm_event_idx = 0
        _tool_event_idx = 0
        _hitl_event_idx = 0
        _memory_event_idx = 0

        for i in range(max_tool_call_iterations):
            # =============================================================
            # Memory management — runs before each LLM call.
            # =============================================================
            if self._context_manager is not None:
                _memory_event_idx = await self._apply_memory_management(
                    copy_prompts, len_initial_prompt,
                    _tracing, _accessor,
                    _memory_event_idx, _fmt,
                )

            async with trace_span(None, "llm", SpanKind.LLM_CALL, {
                ATTR_LLM_ITERATION: i,
                ATTR_LLM_EVENT_INDEX: _llm_event_idx,
            }) as llm_span:
                output_text = await self._llm_engine.chat(
                    copy_prompts,
                    tools=all_tools if all_tools else None,
                    json_schema=union_schema,
                )
                output_text = output_text.strip()

            # Write LLM event to DDict
            write_llm_event(
                _accessor, _tracing, _fmt,
                i, copy_prompts, output_text, _llm_event_idx,
            )
            _llm_event_idx += 1

            log.debug(f"ToolDispatcher iteration {i}: {output_text[:200]}")

            # -- Parse LLM response (may repair truncated JSON) -------------
            data, truncated_prefix = parse_llm_response(output_text, i)

            # -- Final answer branch ----------------------------------------
            if data.response.type == "final_answer":
                final_text = await handle_final_answer(
                    data, truncated_prefix, copy_prompts, self._llm_engine,
                )
                copy_prompts.append({"role": "assistant", "content": final_text})
                log.debug(f"ToolDispatcher iteration {i}: final answer produced.")
                return [copy_prompts[len_initial_prompt:]]

            # -- Tool-call branch -------------------------------------------
            if hasattr(data.response, "tool_calls") and data.response.tool_calls:
                assistant_tool_calls = []
                tool_results = []

                for call in data.response.tool_calls:
                    tool_call_id = self._generate_random_id(length=9)
                    name = call.name
                    args = call.args or {}

                    rejected, _hitl_event_idx = await self._check_hitl_gate(
                        name, args, tool_call_id,
                        dispatch_ctx, _tracing, _accessor,
                        _hitl_event_idx, _fmt,
                    )
                    if rejected is not None:
                        tool_results.append(rejected["tool_result"])
                        assistant_tool_calls.append(rejected["assistant_call"])
                        continue

                    try:
                        tool_answer, _tool_event_idx = await self._execute_tool_call(
                            name, args, tool_call_id, all_mcp_scoped_names,
                            _tracing, _accessor,
                            _tool_event_idx, _fmt,
                        )
                    except ToolExecutionError as exc:
                        # Feed the error back to the LLM as a tool result so
                        # it can reason about the failure and retry or produce
                        # a final answer — instead of killing the entire task.
                        log.warning(
                            "Tool '%s' failed; feeding error to LLM: %s",
                            name, exc,
                        )
                        tool_answer = {"error": str(exc)}

                    tool_result_msg = {
                        "role": "tool",
                        "tool_call_id": tool_call_id,
                        "name": str(name),
                        "content": json.dumps(tool_answer),
                    }
                    tool_results.append(tool_result_msg)
                    assistant_tool_calls.append({
                        "id": tool_call_id,
                        "type": "function",
                        "function": {
                            "name": str(name),
                            "arguments": json.dumps(args),
                        },
                    })

                if assistant_tool_calls:
                    copy_prompts.extend([
                        {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": assistant_tool_calls,
                        },
                        *tool_results,
                    ])

        raise AgentLoopError(
            f"Max tool call iterations ({max_tool_call_iterations}) exceeded for "
            f"agent '{_agent_id}'. Split the request to reduce tool call depth."
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _apply_memory_management(
        self,
        copy_prompts: list[dict[str, Any]],
        len_initial_prompt: int,
        tracing: bool,
        accessor: DDictAccessor | None,
        memory_event_idx: int,
        fmt: dict[str, str],
    ) -> int:
        """Run sliding-window pruning and optional summarization.

        Returns the updated *memory_event_idx*.
        """
        # Phase 1: sliding-window prune
        _turns_before = len(self._context_manager._parse_turns(
            copy_prompts, len_initial_prompt,
        ))
        self._context_manager.enforce_window(copy_prompts, len_initial_prompt)
        _turns_after = len(self._context_manager._parse_turns(
            copy_prompts, len_initial_prompt,
        ))
        _turns_pruned = _turns_before - _turns_after
        if _turns_pruned > 0:
            async with trace_span(None, "memory_prune", SpanKind.MEMORY_PRUNE, {
                ATTR_MEMORY_STRATEGY: MemoryStrategy.SLIDING_WINDOW,
                ATTR_MEMORY_TURNS_PRUNED: _turns_pruned,
                ATTR_MEMORY_TURNS_BEFORE: _turns_before,
                ATTR_MEMORY_TURNS_KEPT: _turns_after,
            }):
                pass  # marker only — pruning already done above

        # Phase 2: summarization
        if self._context_manager.should_summarize(copy_prompts, len_initial_prompt):
            async with trace_span(None, "memory_summarize", SpanKind.MEMORY_SUMMARIZE, {
                ATTR_MEMORY_STRATEGY: MemoryStrategy.SUMMARIZE,
                ATTR_MEMORY_TURNS_KEPT: self._context_manager._config.max_kept_turns,
                ATTR_MEMORY_EVENT_INDEX: memory_event_idx,
            }) as summ_span:
                _pre_len2 = len(copy_prompts)
                _summ_result = await self._context_manager.maybe_summarize(
                    copy_prompts, len_initial_prompt, self._llm_engine,
                )
                _shrunk = _pre_len2 - len(copy_prompts)
                summ_span.attributes[ATTR_MEMORY_TURNS_SUMMARIZED] = \
                    max(0, (_shrunk - 1)) // 2
                if _summ_result:
                    summ_span.attributes[ATTR_MEMORY_SUMMARY_CHARS] = \
                        len(_summ_result["output"])

            if _summ_result:
                write_memory_event(accessor, tracing, fmt, _summ_result, memory_event_idx)
            memory_event_idx += 1
        else:
            await self._context_manager.maybe_summarize(
                copy_prompts, len_initial_prompt, self._llm_engine,
            )

        return memory_event_idx

    async def _check_hitl_gate(
        self,
        name: str,
        args: dict,
        tool_call_id: str,
        dispatch_ctx: dict,
        tracing: bool,
        accessor: DDictAccessor | None,
        hitl_event_idx: int,
        fmt: dict[str, str],
    ) -> tuple[dict | None, int]:
        """Run HITL approval gate if active; return rejection payload or None."""
        if not (self._needs_approval(name, args) and dispatch_ctx.get("hitl_queue")):
            return None, hitl_event_idx

        async with trace_span(None, name, SpanKind.HITL_APPROVAL, {
            ATTR_HITL_TOOL_GATED: name,
            ATTR_HITL_EVENT_INDEX: hitl_event_idx,
        }) as hitl_span:
            from ..hitl.approval import request_human_approval  # noqa: PLC0415
            response = await request_human_approval(
                ddict=dispatch_ctx["ddict"],
                hitl_queue=dispatch_ctx["hitl_queue"],
                task_id=dispatch_ctx["task_id"],
                agent_id=dispatch_ctx["agent_id"],
                dispatch_id=dispatch_ctx["dispatch_id"],
                tool_name=name,
                tool_args=args,
                context=f"LLM requested tool call: {name}({json.dumps(args)[:200]})",
            )
            hitl_span.attributes[ATTR_HITL_APPROVED] = response.approved

        write_hitl_event(accessor, tracing, fmt, name, args, response, hitl_event_idx)
        hitl_event_idx += 1

        if response.approved:
            return None, hitl_event_idx

        # Build rejection payload
        if response.is_feedback:
            content = json.dumps({
                "feedback": "FEEDBACK from operator — NOT APPROVED",
                "message": response.reason,
                "action_required": (
                    "Apply the requested changes and call the appropriate tool. "
                    "Do NOT produce a final answer until the operator approves."
                ),
            })
        else:
            content = json.dumps({
                "error": "REJECTED by operator",
                "reason": response.reason,
                "action_required": (
                    "The operator has rejected this action. "
                    "Produce a final answer explaining the rejection."
                ),
            })
        return {
            "tool_result": {
                "role": "tool",
                "tool_call_id": tool_call_id,
                "name": str(name),
                "content": content,
            },
            "assistant_call": {
                "id": tool_call_id,
                "type": "function",
                "function": {
                    "name": str(name),
                    "arguments": json.dumps(args),
                },
            },
        }, hitl_event_idx

    async def _execute_tool_call(
        self,
        name: str,
        args: dict,
        tool_call_id: str,
        all_mcp_scoped_names: set[str],
        tracing: bool,
        accessor: DDictAccessor | None,
        tool_event_idx: int,
        fmt: dict[str, str],
    ) -> tuple[Any, int]:
        """Route a single tool call (MCP or local), write trace + DDict event."""
        _tool_source = "mcp" if name in all_mcp_scoped_names else "local"
        async with trace_span(None, name, SpanKind.TOOL_CALL, {
            ATTR_TOOL_NAME: name,
            ATTR_TOOL_SOURCE: _tool_source,
            ATTR_TOOL_EVENT_INDEX: tool_event_idx,
        }) as tool_span:
            try:
                if name in all_mcp_scoped_names:
                    alias = name.split("__", 1)[0]
                    client = self._mcp_clients.get(alias)
                    if client is None:
                        raise RuntimeError(
                            f"No MCP client found for alias '{alias}'. "
                            "Was connect_mcp() called?"
                        )
                    tool_answer = await client.call_tool(name, args)
                else:
                    tool = self._registry.get(name)
                    if inspect.iscoroutinefunction(tool.run):
                        tool_answer = await tool.run(args)
                    else:
                        tool_answer = tool.run(args)
            except Exception as exc:
                log.error(
                    "Tool '%s' raised %s: %s",
                    name, type(exc).__name__, exc,
                    exc_info=True,
                )
                raise ToolExecutionError(name, args, exc) from exc

        write_tool_event(
            accessor, tracing, fmt,
            name, args, tool_answer, _tool_source, tool_call_id, tool_event_idx,
        )
        tool_event_idx += 1
        return tool_answer, tool_event_idx

    def _needs_approval(self, tool_name: str, tool_args: dict) -> bool:
        """Return True if this tool call requires human approval."""
        if self._approval_filter is None:
            return False
        return self._approval_filter(tool_name, tool_args)

    @staticmethod
    def _generate_random_id(length: int = 8) -> str:
        """Generate a random alphanumeric ID string."""
        characters = string.ascii_letters + string.digits
        return "".join(random.choice(characters) for _ in range(length))
