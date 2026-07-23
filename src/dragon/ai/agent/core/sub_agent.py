"""SubAgent — concrete stateless persistent agent."""

from __future__ import annotations

import asyncio
import json
import traceback
from typing import Any

from ....data.ddict import DDict

from ..utils.logging import setup_agent_logging, get_agent_logger

log = get_agent_logger("sub_agent")

from ..core.base import DragonAgent
from ..config import (
    AgentConfig, DispatchHeader, MCPServerConfig,
    RESULT_KEY, STATUS_KEY, TaskStatus,
    GLOBAL_STATE_KEY, GLOBAL_STATE_ENTRY_KEY, DISPATCH_ID_KEY, USER_INPUT_KEY, HITL_QUEUE_KEY,
)
from ..ddict import DDictAccessor
from ..tools.registry import ToolRegistry
from ..observability.tracer import (
    trace_span,
    _current_trace_id, _current_processor,
)
from ..observability.trace_protocol import SpanKind, ATTR_AGENT_ID, ATTR_DISPATCH_ID
from ..observability.ddict_tracer import DictTracingProcessor


def _log_task_exception(task: asyncio.Task) -> None:
    """Done-callback for asyncio tasks: log unhandled exceptions.

    Attached via ``task.add_done_callback(_log_task_exception)`` so that
    exceptions from ``_handle_message`` are never silently lost.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.error(
            "Unhandled exception in agent task %s: %s: %s",
            task.get_name(), type(exc).__name__, exc,
        )


class SubAgent(DragonAgent):
    """A stateless sub-agent that processes tasks via LLM reasoning + tools.

    All per-task state (upstream context, tool history, results) is stored in
    the shared Dragon Distributed Dictionary — the agent itself holds nothing
    between tasks.

    Behavior is fully determined by :attr:`AgentConfig.role`, which becomes
    the LLM system prompt persona.

    Execution modes:

    * **Persistent listen loop** — call :meth:`listen` (inherited from
      :class:`DragonAgent`).  The agent blocks on its input queue, processes
      each incoming message, and writes results back to DDict.  This is the
      primary path used by Dragon Batch dispatchers.

    * **Direct call** — call :meth:`process` directly for testing or
      non-batch workflows.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if self.tool_dispatcher is None:
            raise RuntimeError(
                f"Agent '{self.config.agent_id}' has no LLM configured "
                f"(tool_dispatcher is None). Set config.inference_queue "
                f"to the inference pipeline's input queue."
            )
        # System prompt depends only on config + tool_registry, both immutable
        # after construction.  Build once and reuse for every task.
        self._system_prompt: str = self._build_system_prompt()

    # -- LLM + tool-call loop ------------------------------------------------

    async def _invoke_llm_with_tools(
        self,
        task_id: str,
        dispatch_id: str,
        task: str,
        accessor: DDictAccessor,
        upstream_agent_ids: list[str] | None = None,
        tracing: bool = False,
    ) -> str:
        """Run the LLM in a tool-call loop until a final answer is produced.

        When *upstream_agent_ids* is provided (non-empty), the method performs
        **targeted DDict reads** — fetching only the direct parents' results
        via ``DISPATCH_ID_KEY`` → ``RESULT_KEY``.

        When *upstream_agent_ids* is empty or ``None`` (root agents), the
        method falls back to reading ``global_state``.

        After the LLM produces a final answer, appends
        ``{"agent_id": ..., "answer": ...}`` to the global state list.

        Returns
        -------
        str
            The final answer string from the LLM.
        """
        agent_id = self.config.agent_id
        global_state_key = GLOBAL_STATE_KEY.format(task_id=task_id)

        messages: list[dict[str, str]] = [
            {"role": "system", "content": self._system_prompt},
        ]

        if upstream_agent_ids:
            # -- 1a. DIRECT READS: fetch only the listed parents' results ---
            user_input_key = USER_INPUT_KEY.format(task_id=task_id)
            try:
                user_input = accessor.get(user_input_key)
            except KeyError:
                raise RuntimeError(
                    f"[{agent_id}] Missing user input in DDict "
                    f"(key={user_input_key!r}). The orchestrator should "
                    f"write this before dispatching any agent."
                )
            messages.append({"role": "user", "content": f"User request:\n{user_input}"})

            log.info(
                "[%s] [%s] direct-read upstream (%d parents): %s",
                agent_id, task_id[:8], len(upstream_agent_ids), upstream_agent_ids,
            )
            for parent_id in upstream_agent_ids:
                dispatch_key = DISPATCH_ID_KEY.format(task_id=task_id, agent_id=parent_id)
                try:
                    parent_dispatch_id = accessor.get(dispatch_key)
                except KeyError:
                    raise RuntimeError(
                        f"[{agent_id}] Upstream agent '{parent_id}' has no "
                        f"dispatch_id in DDict (key={dispatch_key!r}). "
                        f"Verify DAG dependencies — the parent must complete "
                        f"before this node runs."
                    )
                result_key = RESULT_KEY.format(
                    task_id=task_id,
                    agent_id=parent_id,
                    dispatch_id=parent_dispatch_id,
                )
                try:
                    parent_result = accessor.get(result_key)
                except KeyError:
                    raise RuntimeError(
                        f"[{agent_id}] Upstream agent '{parent_id}' has no "
                        f"result in DDict (key={result_key!r}). The parent "
                        f"may have failed without writing a result."
                    )

                # Serialize the result for the LLM context message.
                if isinstance(parent_result, dict):
                    result_text = json.dumps(parent_result, indent=2)
                else:
                    result_text = str(parent_result)

                _preview = result_text[:200].replace('\n', ' ')
                _trunc = '\u2026' if len(result_text) > 200 else ''  # \u2026 = … (ellipsis)
                log.debug("  [%s] %r%s", parent_id, _preview, _trunc)

                messages.append({
                    "role": "user",
                    "content": f"Result from {parent_id}:\n{result_text}",
                })
        else:
            # -- 1b. FALLBACK: read full global state (root agents) ---------
            global_state_fallback: list = accessor.get_list(global_state_key)

            log.info(
                "[%s] [%s] global-state fallback (%d entries)",
                agent_id, task_id[:8], len(global_state_fallback),
            )
            for entry in global_state_fallback:
                _ans = entry['answer']
                _preview = _ans[:200].replace('\n', ' ')
                _trunc = '\u2026' if len(_ans) > 200 else ''  # \u2026 = … (ellipsis)
                log.debug("  [%s] %r%s", entry['agent_id'], _preview, _trunc)

            global_state_text = json.dumps(global_state_fallback, indent=2)
            messages.append({
                "role": "user",
                "content": f"Shared state (answers from agents so far):\n{global_state_text}",
            })

        messages.append({"role": "user", "content": task})

        dispatch_ctx: dict = dict(
            ddict=accessor.raw,
            tracing=tracing,
            task_id=task_id,
            agent_id=agent_id,
            dispatch_id=dispatch_id,
        )
        # Add HITL queue handle if approval_filter is configured
        if self.config.approval_filter is not None:
            try:
                dispatch_ctx["hitl_queue"] = accessor.get(HITL_QUEUE_KEY)
            except KeyError:
                log.warning(
                    "approval_filter set for '%s' but no HITL queue found in DDict. Running ungated.",
                    agent_id,
                )

        result = await self.tool_dispatcher.chat(messages, **dispatch_ctx)
        conversation_turns = result[0] if result else []
        answer = conversation_turns[-1].get("content", "") if conversation_turns else ""

        # -- Write this agent's answer to a per-agent key (atomic, no race) -
        entry = {"agent_id": agent_id, "answer": answer}
        entry_key = GLOBAL_STATE_ENTRY_KEY.format(task_id=task_id, agent_id=agent_id)
        accessor.put(entry_key, entry)

        return answer

    # -- prompt construction -------------------------------------------------

    def _build_system_prompt(self) -> str:
        """Assemble the system prompt from the agent's role and available tools."""
        parts = [
            f"You are {self.config.name}.",
            f"Role: {self.config.role}",
        ]

        tool_schemas = self.tool_registry.list_tools()
        if tool_schemas:
            parts.append(
                "You have access to the following tools. To call a tool, "
                "respond with a JSON object: "
                '{"tool_calls": [{"name": "<tool_name>", "arguments": {...}}]}'
            )
            parts.append(f"Available tools:\n{json.dumps(tool_schemas, indent=2)}")

            parts.append(
                "When you have the final answer and do not need to call any more "
                "tools, respond with the answer directly (not wrapped in JSON)."
            )

        return "\n\n".join(parts)

    # -- process -------------------------------------------------------------

    async def process(
        self, task_id: str, header: DispatchHeader, accessor: DDictAccessor,
    ) -> dict[str, Any]:
        """Process a single task by reading upstream context and invoking the LLM.

        Steps:

        1. Read upstream results from DDict using the ``task_id`` / ``agent_id``
           references in ``header.task``.
        2. Invoke the LLM in a tool-call loop via
           :meth:`_invoke_llm_with_tools`.
        3. Return the result dict.

        Parameters
        ----------
        task_id:
            Unique identifier for this batch run / user request.
        header:
            :class:`DispatchHeader` with task description, serialized DDict
            handle, and optional completion event.
        accessor:
            :class:`DDictAccessor` wrapping the shared Dragon Distributed
            Dictionary for this batch run.

        Returns
        -------
        dict
            ``{"response": <final_answer_string>}``.
        """
        dispatch_id = header.dispatch_id
        task = header.task
        upstream_agent_ids = header.upstream_agent_ids
        tracing = header.tracing

        answer = await self._invoke_llm_with_tools(
            task_id=task_id,
            dispatch_id=dispatch_id,
            task=task,
            accessor=accessor,
            upstream_agent_ids=upstream_agent_ids,
            tracing=tracing,
        )

        return {"response": answer}

    # -- persistent listen loop ----------------------------------------------

    async def _handle_message(self, msg: Any) -> None:
        """Process a single message as a concurrent asyncio Task.

        Dispatched via ``asyncio.create_task()`` so the listen loop can
        accept the next message immediately while this task is awaiting I/O.

        Steps:

        1. Attach to the shared DDict.
        2. Set status to ``PROCESSING``.
        3. Await :meth:`process` (LLM + tool-call loop).
        4. Write result and ``DONE`` status, or ``ERROR`` status on failure.
        5. Signal the dispatcher's completion event (in a thread — Dragon
           native events involve cross-process IPC and must not block the
           event loop).
        6. Detach from DDict.

        **Task isolation:** exceptions are caught and published to DDict as
        ``ERROR`` status but are **never** re-raised past this method.  This
        ensures a single failing task cannot crash the agent process or
        affect other concurrent tasks.
        """
        header = msg.header
        task_id = msg.task_id
        agent_id = self.config.agent_id
        dispatch_id = header.dispatch_id

        # ``ddict`` starts as None so that the finally block can safely
        # skip detach() if DDict.attach() itself failed.
        ddict = None
        trace_id_token = None
        processor_token = None

        try:
            # Resolve the tracer for this task.
            # The tracing flag lives on the DispatchHeader (pipeline-level
            # setting propagated by the orchestrator).
            # ``tracing=True`` → create a per-task DictTracingProcessor so
            # spans and per-event keys are written to DDict in real time.
            # ``tracing=False`` → no tracing, zero overhead.
            tracing_enabled = header.tracing
            task_desc = header.task
            serialized_ddict = header.serialized_ddict
            completion_event = header.completion_event

            log.info(
                ">>> AGENT START: %s  [%s]  dispatch=%s  Task: %.120s%s",
                agent_id, task_id[:8], dispatch_id,
                task_desc.replace('\n', ' '),
                '...' if len(task_desc) > 120 else '',
            )

            # Attach to the per-run shared DDict from the serialized handle
            ddict = DDict.attach(serialized_ddict)
            accessor = DDictAccessor(ddict, agent_id=agent_id, task_id=task_id)

            # Only create DictTracingProcessor when the user explicitly asked
            # for tracing via ``tracing=True``.  Otherwise skip all
            # tracing work — zero overhead.
            if tracing_enabled:
                tracer = DictTracingProcessor(ddict, task_id, agent_id, dispatch_id)
            else:
                tracer = None

            status_key = STATUS_KEY.format(task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id)
            result_key = RESULT_KEY.format(task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id)

            accessor.put(status_key, TaskStatus.PROCESSING)

            # Set task-scoped ContextVars so child spans (in ToolDispatcher etc.)
            # automatically inherit the correct trace_id and processor.
            if tracer is not None:
                trace_id_token = _current_trace_id.set(task_id)
                processor_token = _current_processor.set(tracer)

            async with trace_span(tracer, agent_id, SpanKind.TASK, {ATTR_AGENT_ID: agent_id, ATTR_DISPATCH_ID: dispatch_id}) as task_span:
                try:
                    result = await self.process(task_id, header, accessor)
                    log.info(
                        "<<< AGENT DONE: %s [%s] dispatch=%s Result: %s",
                        agent_id, task_id[:8], dispatch_id, str(result)[:500],
                    )
                    accessor.put(result_key, result)
                    accessor.put(status_key, TaskStatus.DONE)
                except Exception as exc:
                    tb = traceback.format_exc()
                    log.error(
                        "AGENT ERROR: %s [%s] dispatch=%s %s\n%s",
                        agent_id, task_id[:8], dispatch_id, exc, tb,
                    )
                    # Publish error status to DDict so the batch dispatcher
                    # knows this task failed.  DDict itself may be broken
                    # (e.g. manager crash), so guard these writes — a secondary
                    # failure here must not mask the original exception.
                    try:
                        accessor.put(result_key, {"error": str(exc)})
                        accessor.put(status_key, TaskStatus.ERROR)
                        # Write an error entry to global state so downstream
                        # agents (and the orchestrator's result collector) can
                        # see that this agent ran and failed, rather than
                        # appearing to have never executed.
                        error_entry = {"agent_id": agent_id, "dispatch_id": dispatch_id, "answer": f"[ERROR] {exc}"}
                        entry_key = GLOBAL_STATE_ENTRY_KEY.format(
                            task_id=task_id, agent_id=agent_id,
                        )
                        accessor.put(entry_key, error_entry)
                    except Exception as pub_exc:
                        log.error(
                            "Failed to publish ERROR status to DDict for "
                            "agent '%s' task [%s]: %s",
                            agent_id, task_id[:8], pub_exc, exc_info=True,
                        )
                    raise  # re-raise so trace_span captures the error
        except Exception as exc:
            # Task-isolation boundary: do NOT re-raise here.
            #
            # Why: each message is an independent asyncio.Task.  Re-raising
            # would crash the agent process, killing ALL concurrent tasks.
            #
            # The error IS surfaced to the user through a different path:
            #   1. Inner except (above) publishes status=ERROR to DDict.
            #   2. batch_dispatch.py reads ERROR → raises RuntimeError.
            #   3. task_handle.get() propagates to user's try/except.
            #
            # If the failure happened during setup (before status=ERROR was
            # published), the dispatcher will still unblock via the
            # completion_event in the finally block below and time out or
            # detect the missing status.
            #
            # This log.error() goes through Dragon's three-tier logging
            # (stderr / dragon-file / actor-file) for operator visibility.
            log.error(
                "Agent '%s' task [%s] dispatch=%s failed (error propagated via DDict): %s",
                agent_id, task_id[:8], dispatch_id, exc,
                exc_info=True,
            )
        finally:
            if trace_id_token is not None:
                _current_trace_id.reset(trace_id_token)
            if processor_token is not None:
                _current_processor.reset(processor_token)
            # Always detach from DDict first — all reads/writes are done.
            if ddict is not None:
                try:
                    ddict.detach()
                except Exception as exc:
                    log.debug(
                        "DDict detach failed for agent '%s' task [%s]: %s",
                        agent_id, task_id[:8], exc,
                    )
            # Signal the dispatcher that work is complete (set on both success
            # and error paths so the dispatcher always unblocks).
            # completion_event.set() is a Dragon cross-process IPC call —
            # run in a thread pool so it never blocks the event loop.
            #
            # On failure we log but do NOT raise — the agent is a long-lived
            # service process and raising from finally would crash this task
            # (bypassing the task-isolation boundary above).  The dispatcher
            # will time out and surface the error to the caller.
            if completion_event is not None:
                try:
                    await asyncio.to_thread(completion_event.set)
                except Exception as exc:
                    log.error(
                        "completion_event.set() failed for agent '%s' task [%s] dispatch=%s: %s. "
                        "The batch dispatcher will time out.",
                        agent_id, task_id[:8], dispatch_id, exc,
                    )

    async def listen(self) -> None:
        """Dispatch tasks from the input queue as concurrent asyncio Tasks.

        Flow:

        1. Connect any pending MCP servers (deferred from ``__init__``).
        2. Await :meth:`receive` — blocks up to 1 s when queue is idle
           (accepted stall; see :class:`DragonQueueProtocol`).
        3. On message arrival, create an :func:`asyncio.Task` for
           :meth:`_handle_message` and immediately loop back to receive.
        4. Multiple tasks run concurrently — the event loop is free during
           all LLM, MCP, and tool I/O waits.
        5. Loop exits cleanly when ``self.shutdown_event`` is set.

        Worst-case shutdown latency is ~1 s (the queue-poll timeout).
        """
        # Connect MCP servers inside the agent's own event loop so that
        # the fastmcp.Client async context stays alive for call_tool().
        if self._pending_mcp_servers and self.tool_dispatcher is not None:
            for srv in self._pending_mcp_servers:
                await self.tool_dispatcher.connect_mcp(
                    srv.url, srv.token, alias=srv.alias,
                    max_retries=srv.max_retries,
                    retry_delay=srv.retry_delay,
                    timeout=srv.timeout,
                )
            self._pending_mcp_servers = []

        # Track in-flight tasks so we can await them on shutdown — ensures
        # every _handle_message completes its DDict writes and fires its
        # completion_event before the process exits.
        inflight: set[asyncio.Task] = set()

        try:
            while self.shutdown_event is None or not self.shutdown_event.is_set():
                # receive() offloads queue.get() to a thread via
                # asyncio.to_thread(), so the event loop stays free for
                # in-flight _handle_message tasks while the thread waits
                # up to 1 s on the Dragon queue.
                msg = await self.comm.receive(timeout=1.0)

                if msg is None:
                    # Queue was empty for the poll interval — loop back to
                    # re-check shutdown_event and poll again.
                    continue

                # Dispatch as a concurrent Task; loop immediately returns to
                # polling for the next message.
                task = asyncio.create_task(self._handle_message(msg))
                inflight.add(task)
                task.add_done_callback(inflight.discard)
                task.add_done_callback(_log_task_exception)
        finally:
            # Graceful drain: let every in-flight task finish so completion
            # events are signalled and DDict writes are flushed.
            if inflight:
                log.info(
                    "Shutdown: awaiting %d in-flight task(s)…", len(inflight),
                )
                # Wait for every in-flight task to finish its DDict writes
                # and completion_event.set() before the process exits.
                # return_exceptions=True prevents one failed task from
                # aborting the wait for the rest; errors are already logged
                # by the _log_task_exception done-callback.
                await asyncio.gather(*inflight, return_exceptions=True)
            # Close MCP client connections.
            if self.tool_dispatcher is not None:
                try:
                    await self.tool_dispatcher.close_mcp()
                except Exception as exc:
                    log.debug("MCP close failed during shutdown: %s", exc)



def create_sub_agent(
    config: AgentConfig,
    tool_registry: ToolRegistry = None,
    mcp_servers: "list[MCPServerConfig] | None" = None,
    shutdown_event=None,
    reply_queue=None,
):
    """Entry point for a sub-agent process.

    The agent's communication queue is created inside this process
    (lifecycle tied to the agent).  If *reply_queue* is provided, the
    serialized queue handle is put onto it immediately after construction
    so the parent can retrieve it.

    The LLM proxy is auto-created from ``config.inference_queue`` inside
    the agent process — no pre-built proxy needs to be passed across
    process boundaries.  The proxy's ``ResponseQueuePool`` lives entirely
    in this process and its event loop.

    Parameters
    ----------
    config:
        Agent identity, role, and inference pipeline reference.
        ``config.inference_queue`` must be set to the Dragon Queue
        feeding the inference backend.
    tool_registry:
        Local tools available for inline invocation.
    mcp_servers:
        Optional list of :class:`MCPServerConfig` instances to connect on
        startup.
    shutdown_event:
        A :class:`dragon.native.event.Event` that, when set by the parent,
        causes the agent's listen loop to exit cleanly.
    reply_queue:
        If given, the agent's input :class:`Queue` is put here immediately
        after construction so the parent process can retrieve it.
    """
    # Configure Dragon-native three-tier logging for this agent process.
    # Must happen before any logger usage — mirrors DDict.setup_logging().
    setup_agent_logging(label=config.agent_id)

    try:
        sub_agent = SubAgent(
            config=config,
            tool_registry=tool_registry,
            mcp_servers=mcp_servers,
            shutdown_event=shutdown_event,
        )
        if reply_queue is not None:
            reply_queue.put(sub_agent.comm.queue)
        asyncio.run(sub_agent.listen())
    except Exception as ex:
        tb = traceback.format_exc()
        log.error("create_sub_agent caught exception: %s\n%s", ex, tb)
        raise RuntimeError(f"caught exception {ex} in create_sub_agent\ntraceback: {tb}\n")
