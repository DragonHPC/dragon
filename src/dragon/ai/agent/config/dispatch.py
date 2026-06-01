"""DispatchHeader — internal message header between dispatcher and agent."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DispatchHeader:
    """Typed header sent by the batch dispatcher to an agent's input queue.

    Parameters
    ----------
    task:
        Human-readable task description for the agent.
    serialized_ddict:
        Serialized handle of the shared DDict for the current run.
    completion_event:
        A :class:`~dragon.native.event.Event` the agent sets when it finishes.
        The dispatcher blocks on this event instead of polling DDict.
        ``None`` when the agent is invoked outside of Dragon Batch (e.g. direct
        unit tests).
    dispatch_id:
        Universally unique identifier for this specific invocation, generated
        automatically via :func:`uuid.uuid4`.  Scopes all per-invocation DDict
        keys (``RESULT_KEY``, ``STATUS_KEY``,
        ``LLM_EVENT_KEY``, ``TOOL_EVENT_KEY``, ``HITL_EVENT_KEY``) so that
        concurrent dispatches of the same
        ``(task_id, agent_id)`` pair — e.g. retries or parallel pipeline runs —
        never collide in the shared DDict.  The dispatcher reads this value
        back after construction to format the same keys when checking status.
    upstream_agent_ids:
        List of direct-dependency ``agent_id`` values whose results this
        agent should read from DDict.  Populated from ``PipelineNode.depends_on``
        by the batch dispatcher.  Empty list (default) = fall back to reading
        the full global state (backward-compatible).
    tracing:
        Pipeline-level tracing flag propagated from ``OrchestratorConfig``.
        When ``True`` the agent creates a ``DictTracingProcessor`` and writes
        per-event DDict keys in real time.
    """

    task: str
    serialized_ddict: str
    completion_event: Any = None  # dragon.native.event.Event | None
    dispatch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    upstream_agent_ids: list[str] = field(default_factory=list)
    tracing: bool = False
