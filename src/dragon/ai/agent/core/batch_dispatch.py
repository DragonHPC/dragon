"""Batch dispatcher callable factory (``batch_dispatch``).

Each Dragon Batch node runs a lightweight *dispatcher* closure — **not** the
agent itself.  The dispatcher:

1. Checks upstream statuses from the ``TaskResult`` Python objects it receives
   as arguments (no DDict read for this).
2. Builds a header containing the task description, a completion
   :class:`~dragon.native.event.Event`, and the serialized DDict handle.
3. Sends the header directly to the target agent's input queue.
4. Blocks on ``event.wait()`` until the agent signals completion.
5. Returns a ``TaskResult`` Python object that Dragon Batch passes to
   downstream dispatcher nodes.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Callable

from ....native.queue import Queue
from ....native.event import Event
from ....data.ddict import DDict

from ..utils.logging import get_agent_logger

from ..config import (
    OrchestratorConfig,
    PipelineNode,
    TaskResult,
    TaskStatus,
    DispatchHeader,
    STATUS_KEY,
    DISPATCH_ID_KEY,
    TRACE_KEY,
    TRACE_INDEX_SLOT_KEY,
)
from ..ddict import DDictAccessor
from ..communication.message import Message

_log = get_agent_logger("batch_dispatch")


def make_dispatcher_fn(
    agent_queue: Queue,
    node: PipelineNode,
    task_id: str,
    serialized_ddict: str,
    config: OrchestratorConfig,
) -> Callable[..., TaskResult]:
    """Create a batch-compatible dispatcher closure for *node*.

    The returned callable accepts ``*upstream_results: TaskResult`` and returns
    a ``TaskResult``.  Dragon Batch threads these Python objects between nodes
    automatically.

    Parameters
    ----------
    agent_queue:
        The input queue handle for the target agent.
    node:
        The :class:`PipelineNode` this dispatcher represents.
    task_id:
        Unique id for the current batch run.
    serialized_ddict:
        Serialized handle of the shared DDict for this run.
    config:
        Orchestrator config (poll interval and timeout).

    Returns
    -------
    Callable
        ``dispatcher(*upstream_results) -> TaskResult``
    """

    def _safe_repr(r: Any) -> Any:
        """Safely represent a result — handles both dataclass and raw values."""
        if is_dataclass(r) and not isinstance(r, type):
            return asdict(r)
        return f"<{type(r).__name__}: {r!r}>"

    def dispatcher(*upstream_results: TaskResult) -> TaskResult:
        # Attach to the shared DDict immediately — all subsequent steps
        # (status writes, dispatch_id registration, result reads) need it.
        ddict = DDict.attach(serialized_ddict)
        try:
            accessor = DDictAccessor(ddict, agent_id=node.agent_id, task_id=task_id)

            # -- 1. Coerce upstream results to TaskResult (Dragon Batch may
            #        deserialize them as dicts instead of dataclass instances) ----
            # If Dragon Batch passes an upstream exception as an argument (which
            # it does when an upstream dispatcher raised), re-raise it immediately
            # so this node also fails and no further downstream nodes run.
            for r in upstream_results:
                if isinstance(r, BaseException):
                    raise RuntimeError(
                        f"Upstream node failed before reaching agent '{node.agent_id}': "
                        f"{type(r).__name__}: {r}"
                    ) from r

            # Dragon Batch passes return values between nodes via cloudpickle
            # through DDict.  Under normal conditions (same Python environment
            # on all nodes), cloudpickle preserves dataclass instances faithfully.
            # The dict branch is a safety net for edge cases where cloudpickle
            # fails to reconstruct the TaskResult class (e.g. mismatched
            # environments across nodes) and falls back to a plain dict.
            def _coerce(r: Any) -> TaskResult:
                if is_dataclass(r) and not isinstance(r, type):
                    return r  # already a TaskResult dataclass
                if isinstance(r, dict):
                    r.setdefault("serialized_ddict", serialized_ddict)
                    return TaskResult(**r)  # __post_init__ coerces status str → TaskStatus
                raise TypeError(
                    f"Unexpected upstream result type for agent '{node.agent_id}': "
                    f"{type(r).__name__}: {r!r}"
                )

            upstream_results = tuple(_coerce(r) for r in upstream_results)

            # -- 2. Check upstream statuses ---------------------------------------
            for r in upstream_results:
                if r.status == TaskStatus.ERROR:
                    raise RuntimeError(
                        f"Upstream agent '{r.agent_id}' failed for task "
                        f"'{r.task_id}'."
                    )

            # -- 3. Build header — metadata only, no data -----------------------
            # Agents read global state directly from DDict using task_id.
            # A completion Event is included so the agent can signal us directly
            # — no polling needed.
            completion_event = Event()
            header = DispatchHeader(
                task=node.task_description,
                serialized_ddict=serialized_ddict,
                completion_event=completion_event,
                upstream_agent_ids=list(node.depends_on),
                tracing=config.tracing,
            )
            # dispatch_id is auto-generated inside DispatchHeader; read it back
            # here so the dispatcher can construct the exact same DDict keys that
            # the agent will write to (all per-invocation keys are scoped by it).
            dispatch_id = header.dispatch_id

            # Write dispatch_id to a stable DDict key so the orchestrator can look
            # it up after task.run() to construct the correct RESULT_KEY.
            accessor.put(
                DISPATCH_ID_KEY.format(task_id=task_id, agent_id=node.agent_id),
                dispatch_id,
            )

            # -- Tracing: write this agent's trace key to its own slot ----------
            # Each dispatcher writes to a unique per-agent slot key — no shared
            # list, so no read-modify-write race on fan-out.
            trace_key = TRACE_KEY.format(
                task_id=task_id, agent_id=node.agent_id, dispatch_id=dispatch_id
            )
            slot_key = TRACE_INDEX_SLOT_KEY.format(
                task_id=task_id, agent_id=node.agent_id
            )
            accessor.put(slot_key, trace_key)

            # -- 4. Send header to agent's queue directly -----------------------
            msg = Message(
                task_id=task_id,
                sender_id="dispatcher",
                recipient_id=node.agent_id,
                header=header,
            )

            _up = [r.agent_id for r in upstream_results]
            _log.info(
                "DISPATCH -> %s [%s] dispatch=[%s] upstream=%s",
                node.agent_id, task_id[:8], dispatch_id[:8], _up,
            )

            agent_queue.put(msg)

            # -- 5. Block on Event until agent signals completion ---------------
            status_key = STATUS_KEY.format(
                task_id=task_id, agent_id=node.agent_id, dispatch_id=dispatch_id
            )

            signaled = completion_event.wait(timeout=config.poll_timeout)
            completion_event.destroy()

            if not signaled:
                _log.error(
                    "Agent '%s' did not complete within %ss for task '%s' (dispatch=%s)",
                    node.agent_id, config.poll_timeout, task_id, dispatch_id,
                )
                raise TimeoutError(
                    f"Agent '{node.agent_id}' did not complete within "
                    f"{config.poll_timeout}s for task '{task_id}' "
                    f"(dispatch={dispatch_id})."
                )

            # Read status once to distinguish done vs error.
            status = accessor.get(status_key)
            _log.info(
                "DISPATCH <- %s [%s] dispatch=[%s] status=%s",
                node.agent_id, task_id[:8], dispatch_id[:8], status,
            )
            if status == TaskStatus.ERROR:
                _log.error(
                    "Agent '%s' reported error for task '%s' (dispatch=%s)",
                    node.agent_id, task_id, dispatch_id,
                )
                raise RuntimeError(
                    f"Agent '{node.agent_id}' reported error for task '{task_id}' "
                    f"(dispatch={dispatch_id})."
                )

            return TaskResult(
                task_id=task_id,
                agent_id=node.agent_id,
                status=TaskStatus.DONE,
                serialized_ddict=serialized_ddict,
            )
        finally:
            ddict.detach()

    return dispatcher