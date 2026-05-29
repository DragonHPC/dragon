"""DAG Orchestrator — builds and runs ephemeral Dragon Batch workflows."""

from __future__ import annotations

import socket
import uuid
from typing import Any

from ....data.ddict import DDict
from ....native.queue import Queue
from ....workflows.batch import Batch

from ..config import (
    OrchestratorConfig,
    Pipeline,
    TaskResult,
    TaskStatus,
    RESULT_KEY,
    USER_INPUT_KEY,
    GLOBAL_STATE_KEY,
    GLOBAL_STATE_ENTRY_KEY,
    DISPATCH_ID_KEY,
    HITL_QUEUE_KEY,
)
from ..core.batch_dispatch import make_dispatcher_fn
from ..ddict import DDictAccessor
from ..utils.logging import setup_agent_logging, get_agent_logger

_log = get_agent_logger("orchestrator")


class DAGOrchestrator:
    """Build and execute multi-agent DAG workflows on Dragon Batch.

    The orchestrator is the **request-side** entry point.  It assumes all
    agents are already running persistently (started externally via their
    ``listen()`` loops).

    ``DAGOrchestrator`` owns the lifecycle of the :class:`~dragon.data.ddict.DDict`.
    The caller creates a :class:`~dragon.workflows.batch.Batch` instance and
    passes it to :meth:`run`; the orchestrator uses it to build and execute
    the DAG.  The caller is responsible for calling ``batch.join()``
    and ``batch.destroy()``.

    Construction sets up all shared infrastructure (DDict, HITL bridge,
    trace bridge) so that :attr:`hitl_address`, :attr:`trace_address`,
    and :attr:`serialized_ddict` are available immediately — before
    :meth:`run` is called.

    Parameters
    ----------
    config:
        :class:`OrchestratorConfig` with agent declarations, poll interval,
        timeout, and optional ``ddict_kwargs`` for DDict tuning.
    pipeline:
        A :class:`Pipeline` defining the DAG topology — nodes and their
        dependencies.

    Example
    -------
    ::

        orchestrator = DAGOrchestrator(
            config=OrchestratorConfig(
                agents=[...],
                poll_interval=0.5,
                poll_timeout=120.0,
                ddict_kwargs={"managers_per_node": 2, "total_mem": 1 << 30},
            ),
            pipeline=pipeline,
        )

        if orchestrator.hitl_address:
            host, port = orchestrator.hitl_address
            print(f"HITL client:  python -m dragon.ai.agent.hitl --tcp {host}:{port}")
        if orchestrator.trace_address:
            host, port = orchestrator.trace_address
            print(f"Trace viewer: python -m dragon.ai.agent.observability --tcp {host}:{port} -i")

        # External clients can attach to the shared DDict for debugging:
        print(f"DDict descriptor: {orchestrator.serialized_ddict}")

        batch = Batch()
        try:
            result = orchestrator.run("Write a report on quantum computing.", batch=batch)
        finally:
            orchestrator.destroy()
            batch.join()
            batch.destroy()
    """

    def __init__(self, config: OrchestratorConfig, pipeline: Pipeline) -> None:
        self.config = config
        self.pipeline = pipeline
        self._agent_handles: dict[str, Any] = {}  # agent_id -> input Queue

        # Generate the per-pipeline-run ID early so the log filename
        # includes it for easy identification across multiple pipelines.
        self._task_id = str(uuid.uuid4())

        # Configure Dragon-native logging for the orchestrator process.
        # Must happen before any logger usage.
        setup_agent_logging(label=f"orchestrator_{self._task_id[:8]}")

        # Attach to each pre-existing persistent agent
        for agent_cfg in config.agents:
            self._agent_handles[agent_cfg.agent_id] = agent_cfg.input_queue

        # Cross-validate: every agent-backed pipeline node must reference
        # a registered agent in the config.
        registered = set(self._agent_handles)
        for node in pipeline.nodes:
            if node.fn is None and node.agent_id not in registered:
                raise ValueError(
                    f"Pipeline node '{node.agent_id}' references an agent "
                    f"not declared in OrchestratorConfig.agents. "
                    f"Registered agents: {sorted(registered)}"
                )

        # Set up DDict, HITL bridge, trace bridge — makes hitl_address and
        # trace_address available immediately after construction.
        self._init_infrastructure(pipeline)

    # -- infrastructure setup ------------------------------------------------

    def _init_infrastructure(self, pipeline: Pipeline) -> None:
        """Create shared DDict, HITL bridge, and trace bridge for this run."""
        _log.debug(
            "INIT  task_id=%s  nodes=%s",
            self._task_id[:8], [n.agent_id for n in pipeline.nodes],
        )

        # Create shared DDict for this run.  Sensible defaults are applied
        # first, then user overrides from config.ddict_kwargs are merged in.
        # DDict-level ``trace`` defaults to False but can be overridden via
        # ddict_kwargs — it is independent of config.tracing, which controls
        # the TCP trace bridge.
        _ddict_defaults: dict[str, Any] = {
            "managers_per_node": 1,
            "n_nodes": 1,
            "trace": False,
        }
        ddict_kwargs = {**_ddict_defaults, **self.config.ddict_kwargs}
        self._ddict = DDict(**ddict_kwargs)
        self._serialized_ddict = self._ddict.serialize()
        self._accessor = DDictAccessor(
            self._ddict, agent_id="orchestrator", task_id=self._task_id,
        )

        self._init_hitl()
        self._init_tracing(pipeline)

    def _init_hitl(self) -> None:
        """Create HITL queue and TCP bridge if any agent uses approval."""
        needs_hitl = any(
            getattr(a, "approval_filter", None) is not None
            for a in self.config.agents
        )
        self._hitl_queue: Queue | None = None
        self._hitl_bridge: Any = None
        self._hitl_address: tuple[str, int] | None = None

        if not needs_hitl:
            return

        self._hitl_queue = Queue()
        self._accessor.put(HITL_QUEUE_KEY, self._hitl_queue)
        _log.debug("HITL queue created.")

        from ..hitl.tcp_bridge import HitlTcpBridge

        self._hitl_bridge = HitlTcpBridge(self._hitl_queue)
        self._hitl_bridge.start()
        # Prefer FQDN for cross-node reachability; fall back to short
        # hostname if DNS is misconfigured and getfqdn() returns "".
        hostname = socket.getfqdn() or socket.gethostname()
        port = self._hitl_bridge.address[1]
        self._hitl_address = (hostname, port)
        _log.debug("HITL TCP bridge listening on %s:%s", hostname, port)

    def _init_tracing(self, pipeline: Pipeline) -> None:
        """Start trace TCP bridge if tracing is enabled."""
        self._trace_bridge: Any = None
        self._trace_address: tuple[str, int] | None = None

        if not self.config.tracing:
            return

        agent_ids = [n.agent_id for n in pipeline.nodes]
        from ..observability.tcp_bridge import TraceTcpBridge
        self._trace_bridge = TraceTcpBridge(
            self._ddict, self._task_id, agent_ids=agent_ids,
        )
        self._trace_bridge.start()
        # Prefer FQDN for cross-node reachability; fall back to short
        # hostname if DNS is misconfigured and getfqdn() returns "".
        hostname = socket.getfqdn() or socket.gethostname()
        port = self._trace_bridge.address[1]
        self._trace_address = (hostname, port)
        _log.debug("Trace TCP bridge listening on %s:%s", hostname, port)

    # -- public API ----------------------------------------------------------

    @property
    def hitl_address(self) -> tuple[str, int] | None:
        """Return ``(host, port)`` of the HITL TCP bridge, or ``None``.

        Available immediately after construction.  Returns ``None`` if no
        agent in the pipeline uses an ``approval_filter``.
        """
        return self._hitl_address

    @property
    def trace_address(self) -> tuple[str, int] | None:
        """Return ``(host, port)`` of the Trace TCP bridge, or ``None``.

        Available immediately after construction.  Returns ``None`` when
        tracing is disabled (``OrchestratorConfig.tracing=False``).
        """
        return self._trace_address

    @property
    def ddict(self) -> DDict:
        """Return the shared DDict instance for this run."""
        return self._ddict

    @property
    def serialized_ddict(self) -> str:
        """Return the serialized DDict descriptor for this run.

        External clients can use this to attach to the shared working
        memory for debugging or monitoring::

            from dragon.data.ddict import DDict
            ddict = DDict.attach(orchestrator.serialized_ddict)
        """
        return self._serialized_ddict

    def run(self, user_input: str, batch: Batch) -> Any:
        """Execute the pipeline and return the final result.

        Parameters
        ----------
        user_input:
            The end-user's request string.
        batch:
            Caller-owned :class:`~dragon.workflows.batch.Batch` instance used
            to build and execute the DAG.  The caller is responsible for
            ``batch.join()`` and ``batch.destroy()`` after this method
            returns.

        Returns
        -------
        Any
            If the pipeline has a single terminal node, returns that node's
            result dict directly.  If there are multiple terminal nodes,
            returns ``dict[agent_id, result]``.
        """
        task_id = self._task_id
        serialized_ddict = self._serialized_ddict
        accessor = self._accessor
        pipeline = self.pipeline

        _log.debug(
            "PIPELINE Starting run [%s]  user_input=%r  nodes=%s",
            task_id[:8], user_input, [n.agent_id for n in pipeline.nodes],
        )

        user_input_key = USER_INPUT_KEY.format(task_id=task_id)
        accessor.put(user_input_key, user_input)

        global_state_key = GLOBAL_STATE_KEY.format(task_id=task_id)
        # Seed global state with the user prompt as the first entry so all
        # agents can see the original request.
        accessor.put(global_state_key, [{"agent_id": "user", "answer": user_input}])

        # -- 2. Build Dragon Batch DAG ----------------------------------------
        #
        # Register each pipeline node as a Batch task with dependency wiring.
        # Collect the Task handles for the terminal (leaf) nodes and call
        # .get() on each to block until the pipeline completes.
        terminal_tasks = self._build_dag(task_id, pipeline, serialized_ddict, batch)

        # -- 3. Wait for terminal tasks to complete ----------------------------
        for task_handle in terminal_tasks:
            task_handle.get()
        _log.debug("All batch nodes finished [%s]", task_id[:8])

        # -- 5. Collect results: global state + per-terminal-node result ------
        terminal_nodes = pipeline.terminal_nodes()
        results: dict[str, Any] = {}

        # Assemble global_state from per-agent atomic keys.
        # Order follows the pipeline node declaration order.
        global_state: list[dict[str, Any]] = [
            {"agent_id": "user", "answer": user_input}
        ]
        for node in pipeline.nodes:
            entry_key = GLOBAL_STATE_ENTRY_KEY.format(
                task_id=task_id, agent_id=node.agent_id,
            )
            entry = accessor.get_or_default(entry_key, None)
            if entry is not None:
                global_state.append(entry)
        results["global_state"] = global_state

        for node in terminal_nodes:
            # Retrieve the dispatch_id the dispatcher wrote to DDict — needed
            # to reconstruct the correct RESULT_KEY (all per-invocation keys
            # are scoped by dispatch_id to prevent concurrent-run collisions).
            dispatch_id = accessor.get_or_default(
                DISPATCH_ID_KEY.format(task_id=task_id, agent_id=node.agent_id),
                "unknown",
            )
            result_key = RESULT_KEY.format(
                task_id=task_id, agent_id=node.agent_id, dispatch_id=dispatch_id
            )
            results[node.agent_id] = accessor.get_or_default(
                result_key,
                {"error": f"No result found for agent '{node.agent_id}'."},
            )

        _log.debug("Results collected [%s]: %s", task_id[:8], list(results.keys()))

        if len(terminal_nodes) == 1:
            return results[terminal_nodes[0].agent_id]
        return results

    # -- internal ------------------------------------------------------------

    def _build_dag(
        self,
        task_id: str,
        pipeline: Pipeline,
        serialized_ddict: str,
        batch: Batch,
    ) -> list[Any]:
        """Construct a Dragon Batch DAG from the pipeline topology.

        Each ``batch.function()`` call creates a Batch task and enqueues it
        for background execution.  Task-typed positional arguments wire
        arg-passing dependencies automatically — Dragon Batch resolves the
        execution order and fans results to downstream consumers.

        Returns
        -------
        list[Task]
            Batch Task handles for the **terminal** (leaf) pipeline nodes.
            Call ``.get()`` on each to block until the pipeline completes.
        """
        batch_keys: dict[str, Any] = {}   # agent_id -> Task object (for dep wiring)

        # Root result that seeds nodes with no dependencies.
        # serialized_ddict is embedded so plain-function nodes can attach to
        # the shared DDict via upstream.serialized_ddict without needing it
        # passed as a separate argument.
        initial = TaskResult(
            task_id=task_id,
            agent_id="__root__",
            status=TaskStatus.DONE,
            serialized_ddict=serialized_ddict,
        )

        for node in pipeline.nodes:
            if node.fn is not None:
                # Plain-function node — use directly. serialized_ddict is
                # already carried in every upstream TaskResult.
                fn = node.fn
            else:
                # Agent-backed node — build a dispatcher closure.
                agent_handle = self._agent_handles.get(node.agent_id)
                if agent_handle is None:
                    raise ValueError(
                        f"No attached agent handle for agent_id '{node.agent_id}'. "
                        f"Known agents: {list(self._agent_handles.keys())}"
                    )
                fn = make_dispatcher_fn(
                    agent_queue=agent_handle,
                    node=node,
                    task_id=task_id,
                    serialized_ddict=serialized_ddict,
                    config=self.config,
                )

            dep_keys = [batch_keys[dep] for dep in node.depends_on]
            if dep_keys:
                k = batch.function(fn, *dep_keys)
            else:
                k = batch.function(fn, initial)

            batch_keys[node.agent_id] = k  # Task object — wires arg-passing deps

        # Return only the terminal (leaf) task handles — .get() on these
        # blocks until the entire upstream sub-graph has completed.
        terminal_ids = {n.agent_id for n in pipeline.terminal_nodes()}
        return [batch_keys[aid] for aid in batch_keys if aid in terminal_ids]

    def destroy(self) -> None:
        """Tear down all orchestrator-owned resources.

        Stops the HITL and trace TCP bridges, destroys the HITL queue
        and the shared DDict.  Batch teardown
        (``batch.join()`` / ``batch.destroy()``) remains the caller's
        responsibility.

        Safe to call multiple times — each resource is cleaned up at most
        once.
        """
        # Stop the trace TCP bridge thread
        if getattr(self, "_trace_bridge", None) is not None:
            try:
                self._trace_bridge.stop()
            except Exception as exc:
                _log.debug("Trace bridge teardown failed: %s", exc)
            self._trace_bridge = None
        # Stop the HITL TCP bridge thread
        if getattr(self, "_hitl_bridge", None) is not None:
            try:
                self._hitl_bridge.stop()
            except Exception as exc:
                _log.debug("HITL bridge teardown failed: %s", exc)
            self._hitl_bridge = None
        # Destroy the HITL queue if it was created
        if getattr(self, "_hitl_queue", None) is not None:
            try:
                self._hitl_queue.destroy()
            except Exception as exc:
                _log.debug("HITL queue destroy failed: %s", exc)
            self._hitl_queue = None
        # Destroy the shared DDict
        if getattr(self, "_ddict", None) is not None:
            try:
                self._ddict.destroy()
            except Exception as exc:
                _log.debug("DDict destroy failed: %s", exc)
            self._ddict = None
