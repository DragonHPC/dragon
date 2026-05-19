"""Pipeline topology, TaskStatus enum, and TaskResult dataclass."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


# ---------------------------------------------------------------------------
# TaskStatus — canonical status values used by agents and TaskResult tokens
# ---------------------------------------------------------------------------

class TaskStatus(str, Enum):
    """Canonical status values for agents (written to DDict) and TaskResult tokens.

    Inherits from ``str`` so values compare equal to plain strings
    (``TaskStatus.DONE == "done"`` is ``True``) and DDict read-back works
    without special handling.  ``TaskStatus("done")`` reconstructs the enum
    from a plain string, e.g. when Dragon Batch deserializes a TaskResult dict.
    """

    READY      = "ready"       # initial / seed state
    PROCESSING = "processing"  # agent is actively working
    WAITING    = "waiting"     # awaiting human approval (HITL)
    DONE       = "done"        # completed successfully
    ERROR      = "error"       # task failed


# ---------------------------------------------------------------------------
# Pipeline topology (user-defined)
# ---------------------------------------------------------------------------

@dataclass
class PipelineNode:
    """A single node in a user-defined pipeline DAG.

    A node is either **agent-backed** or a **plain function**:

    * **Agent-backed** (``fn=None``): the orchestrator dispatches the task to
      the named persistent agent via its queue.  ``task_description`` is
      required in this case.
    * **Plain function** (``fn=<callable>``): the callable is wired directly
      into the Dragon Batch graph — no LLM, no agent queue.  The callable
      must accept ``(*upstreams: TaskResult) -> TaskResult``.  The shared
      DDict handle is available via ``upstreams[0].serialized_ddict``.
      ``task_description`` is ignored.

    Parameters
    ----------
    agent_id:
        Unique node identifier (also the agent id for agent-backed nodes).
    task_description:
        Human-readable task description sent to the agent.  Ignored for
        plain-function nodes.
    fn:
        Optional callable for plain-function nodes.  When set, the
        orchestrator uses it directly instead of dispatching to an agent.
    depends_on:
        List of ``agent_id`` values whose results must be available
        before this node executes.
    """

    agent_id: str
    task_description: str = ""
    fn: Callable | None = None
    depends_on: list[str] = field(default_factory=list)


@dataclass
class Pipeline:
    """User-configurable DAG topology for a multi-agent workflow.

    The user defines nodes in any order; the orchestrator resolves
    ``depends_on`` references when building the Dragon Batch DAG.
    """

    nodes: list[PipelineNode] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate and topologically sort the pipeline at construction time.

        Nodes may be provided in any order.  After validation (uniqueness
        and dependency existence), Kahn's algorithm sorts them so that
        every node appears after all of its dependencies.  This guarantees
        the orchestrator can iterate ``self.nodes`` in a single pass when
        building the Dragon Batch DAG.

        Raises
        ------
        ValueError
            If any ``agent_id`` appears more than once, if a ``depends_on``
            entry references an undeclared ``agent_id``, or if the dependency
            graph contains a cycle.
        """
        seen: set[str] = set()
        for node in self.nodes:
            if node.agent_id in seen:
                raise ValueError(
                    f"Pipeline contains duplicate agent_id '{node.agent_id}'. "
                    "Each node must have a unique agent_id."
                )
            seen.add(node.agent_id)

        for node in self.nodes:
            for dep in node.depends_on:
                if dep not in seen:
                    raise ValueError(
                        f"Node '{node.agent_id}' depends_on '{dep}', "
                        "but no node with that agent_id is declared in the pipeline."
                    )

        # -- Topological sort (Kahn's algorithm) -----------------------------
        node_map = {n.agent_id: n for n in self.nodes}
        in_degree = {n.agent_id: len(n.depends_on) for n in self.nodes}
        # Build forward adjacency: parent -> list of children
        children: dict[str, list[str]] = {n.agent_id: [] for n in self.nodes}
        for n in self.nodes:
            for dep in n.depends_on:
                children[dep].append(n.agent_id)

        queue: deque[str] = deque(
            aid for aid, deg in in_degree.items() if deg == 0
        )
        sorted_ids: list[str] = []
        while queue:
            aid = queue.popleft()
            sorted_ids.append(aid)
            for child in children[aid]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(sorted_ids) != len(self.nodes):
            raise ValueError(
                "Pipeline dependency graph contains a cycle. "
                "Topological sort could not order all nodes."
            )

        self.nodes = [node_map[aid] for aid in sorted_ids]

    # -- helpers -------------------------------------------------------------

    def terminal_nodes(self) -> list[PipelineNode]:
        """Return nodes that are not depended upon by any other node."""
        all_deps = {dep for n in self.nodes for dep in n.depends_on}
        return [n for n in self.nodes if n.agent_id not in all_deps]


# ---------------------------------------------------------------------------
# TaskResult — lightweight Python object flowing between batch dispatchers
# ---------------------------------------------------------------------------

@dataclass
class TaskResult:
    """Lightweight token passed between Dragon Batch node functions.

    This object is **never** written to the distributed dictionary.  It flows
    purely as a Python return value between dispatcher closures executed by
    Dragon Batch.

    ``serialized_ddict`` carries the shared DDict handle so plain-function
    nodes can attach directly via ``upstream.serialized_ddict`` without
    needing it injected as a separate argument.

    Parameters
    ----------
    task_id:
        Unique identifier for the pipeline task.
    agent_id:
        Identifier of the agent that produced this result.
    status:
        Canonical completion status (coerced to :class:`TaskStatus`).
    serialized_ddict:
        Serialized handle of the shared DDict for the current run.
    metadata:
        Arbitrary extra data attached by the agent or dispatcher.
    """

    task_id: str
    agent_id: str
    status: TaskStatus
    serialized_ddict: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Coerce plain strings back to the enum (Dragon Batch may deserialize
        # TaskResult as a plain dict, reconstructing with status as a str).
        if not isinstance(self.status, TaskStatus):
            self.status = TaskStatus(self.status)
