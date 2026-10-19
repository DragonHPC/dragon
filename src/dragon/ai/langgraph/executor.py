"""DragonExecutor — launches AgentHost processes and exposes node callables
that plug directly into a LangGraph StateGraph.

Placement is this module's job; graph topology, routing, and checkpointing
remain LangGraph's.
"""

from __future__ import annotations

import queue as _queue
from typing import Any, Callable

from ...native.process import Process
from ...native.queue import Queue

from .logging import get_langgraph_logger, setup_langgraph_logging
from .node import DragonAgentNode
from .host import agent_host_entry
from .watcher import DragonWatcher

logger = get_langgraph_logger("executor")

# Default number of agent tasks allowed in flight at once. One knob, one number:
# users raise it for more parallelism, lower it to use less memory.
_DEFAULT_MAX_CONCURRENT_TASKS = 64

# How often the startup handshake re-checks that the host is still alive. Only a
# liveness poll, not a deadline — a healthy host may take as long as it needs.
_HANDSHAKE_POLL_INTERVAL = 1.0


class DragonExecutor:
    """Manages the lifecycle of Dragon AgentHost processes.

    Owns a single DragonWatcher that resolves all pending task futures over one
    or more shared completion Queues (one recv thread per shard).  All agent
    nodes dispatched through this executor share the same watcher — no
    per-node thread.

    Use as a context manager::

        with DragonExecutor() as executor:
            executor.launch_host(agents={...})
            ...

    Or manage lifetime manually::

        executor = DragonExecutor()
        executor.launch_host(agents={...})
        ...
        executor.shutdown()
    """

    def __init__(
        self,
        *,
        max_concurrent_tasks: int = _DEFAULT_MAX_CONCURRENT_TASKS,
        num_shards: int = 1,
    ) -> None:
        """Create the executor and its single shared-transport watcher.

        This executor transports only agent *results* (inline, on Dragon
        Queues).  It does not create or manage any DDict.  Bulk scientific
        data (tensors, arrays, datasets) is the user's responsibility: create
        your own Dragon DDict, pass it into your agent functions, store
        artifacts there, and return only a lightweight handle/key through
        LangGraph state.

        :param max_concurrent_tasks: Cap on agent tasks in flight across the
            whole executor. Dispatching beyond it applies backpressure —
            ``graph.invoke`` blocks its calling thread, ``graph.ainvoke`` awaits
            the slot. Per-shard slots, completion-queue capacity and host
            input-queue size are all derived from this, so it is normally the
            only one you set. Distinct from a host's ``max_threads``, which caps
            *sync* node bodies inside one host; async nodes are bounded only by
            this cap. Defaults to 64.
        :type max_concurrent_tasks: int, optional
        :param num_shards: Independent completion lanes, each a Dragon Queue
            plus a watcher thread, sharing the ``max_concurrent_tasks`` budget
            evenly. Raise only if one watcher thread becomes the bottleneck.
            Defaults to 1.
        :type num_shards: int, optional
        :raises ValueError: If ``max_concurrent_tasks`` or ``num_shards`` is
            less than 1.
        """
        if max_concurrent_tasks < 1:
            raise ValueError("max_concurrent_tasks must be >= 1")
        if num_shards < 1:
            raise ValueError("num_shards must be >= 1")

        # Configure Dragon-native three-tier logging for the coordinator process.
        setup_langgraph_logging()

        self._max_concurrent_tasks = max_concurrent_tasks
        self._num_shards = num_shards

        self._procs: list[Any] = []  # dragon.native.process.Process
        self._nodes: dict[str, DragonAgentNode] = {}
        self._host_queues: list[bytes] = []  # serialized input queues for shutdown
        self._reply_queues: list[Any] = []  # handshake Queues, destroyed on shutdown

        # One shared watcher resolves every pending task future over the
        # completion Queues.  Its queues are sized to max_concurrent_tasks.
        self._watcher: DragonWatcher | None = DragonWatcher(
            max_concurrent_tasks=max_concurrent_tasks,
            num_shards=num_shards,
        )

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    @property
    def max_concurrent_tasks(self) -> int:
        """Maximum number of agent tasks allowed in flight at once."""
        return self._max_concurrent_tasks

    @property
    def num_shards(self) -> int:
        """Number of independent completion lanes in the watcher."""
        return self._num_shards

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def launch_host(
        self,
        agents: dict[str, Callable[..., Any]],
        *,
        policy: Any | None = None,
        event_timeout: float | None = None,
        max_threads: int | None = None,
    ) -> Any:
        """Spawn one AgentHost process containing multiple agent functions.

        :param agents: Mapping of ``node_name`` to ``fn(state) -> dict``. All
            functions in this dict run in a single Dragon process. Both plain
            ``def`` and ``async def`` node functions are supported — sync nodes
            run on a bounded thread pool; async nodes run as concurrent tasks on
            the host's shared event loop, so an existing LangGraph graph with
            async nodes runs unchanged.
        :type agents: dict[str, Callable]
        :param policy: Optional ``dragon.infrastructure.policy.Policy`` to pin
            this host to a specific cluster node, NUMA zone, or GPU. Defaults
            to None.
        :type policy: dragon.infrastructure.policy.Policy, optional
        :param event_timeout: Seconds before a task is considered timed out.
            ``None`` means no limit. Defaults to None.
        :type event_timeout: float, optional
        :param max_threads: Maximum concurrent **sync** node bodies in this host
            — the size of the thread pool that runs plain ``def`` nodes. ``None``
            defaults to ``max(len(agents) * 4, 8)``. Async (``async def``) nodes
            are *not* bounded by this; they run as tasks on the host's shared
            event loop (bounded only by ``max_concurrent_tasks``). See
            :meth:`__init__` (the ``max_concurrent_tasks`` note) for how the two
            relate. Defaults to None.
        :type max_threads: int, optional
        :returns: The Dragon Process running this AgentHost. Useful for
            monitoring, joining, or killing the host externally.
        :rtype: dragon.native.process.Process
        :raises ValueError: If an agent ``node_name`` was already registered by
            a previous ``launch_host()`` call.
        :raises RuntimeError: If called after :meth:`shutdown`, or if the host
            process exits before completing its startup handshake.
        """
        if self._watcher is None:
            raise RuntimeError(
                "DragonExecutor has been shut down; its watcher and hosts are "
                "gone. Create a new DragonExecutor to launch more hosts."
            )

        reply_queue: Any = Queue()
        # Retained so shutdown() can destroy it once the host has exited
        # (it is only used for the one-shot startup handshake below).
        self._reply_queues.append(reply_queue)

        proc = Process(
            target=agent_host_entry,
            kwargs={
                "agents": agents,
                "reply_queue": reply_queue,
                "max_threads": max_threads,
                # Size the host's input queue to the in-flight cap so a
                # dispatched put() never blocks on queue capacity (which would
                # be a second, uncoordinated backpressure point).
                "input_maxsize": self._max_concurrent_tasks,
            },
            policy=policy,
        )
        proc.start()
        self._procs.append(proc)

        # Block until the host sends its input queue handle.
        serialized_input_queue = self._await_host_handshake(proc, reply_queue, agents)
        self._host_queues.append(serialized_input_queue)

        # Register a DragonAgentNode for each agent in this host.
        for node_name in agents:
            if node_name in self._nodes:
                raise ValueError(
                    f"Agent '{node_name}' already registered. "
                    "Each node_name must be unique across all hosts."
                )
            self._nodes[node_name] = DragonAgentNode(
                node_name=node_name,
                serialized_input_queue=serialized_input_queue,
                watcher=self._watcher,
                event_timeout=event_timeout,
            )

        logger.info(
            "[DragonExecutor] host ready — agents: %s",
            list(agents.keys()),
        )
        return proc

    @staticmethod
    def _await_host_handshake(
        proc: Any,
        reply_queue: Any,
        agents: dict[str, Callable[..., Any]],
    ) -> bytes:
        """Wait for the host's input-queue handle, giving up if the host dies.

        A plain blocking ``get()`` would wait forever when the host process
        exits during import or setup — the most common startup failure — with
        nothing at all to say why.  Polling liveness instead of imposing a
        deadline keeps a slow-but-healthy host (large model imports) unaffected.

        :param proc: The freshly started AgentHost process.
        :type proc: dragon.native.process.Process
        :param reply_queue: The one-shot handshake Queue passed to the host.
        :type reply_queue: dragon.native.queue.Queue
        :param agents: The agents this host was launched with, for the error.
        :type agents: dict[str, Callable]
        :returns: The host's serialized input Queue descriptor.
        :rtype: bytes
        :raises RuntimeError: If the host exits before sending its handle.
        """
        while True:
            try:
                return reply_queue.get(timeout=_HANDSHAKE_POLL_INTERVAL)
            except _queue.Empty:
                pass
            # native Process exposes is_alive as a property, not a method.
            if not proc.is_alive:
                # One last look: the handle may have landed as the host exited.
                try:
                    return reply_queue.get(timeout=_HANDSHAKE_POLL_INTERVAL)
                except _queue.Empty:
                    pass
                raise RuntimeError(
                    f"AgentHost for agents {list(agents)} exited before it was "
                    f"ready (exit code {proc.returncode}). Check that host's "
                    f"LANGGRAPH log for the failure."
                )

    def node(self, node_name: str) -> Any:
        """Return the callable for *node_name* to pass to StateGraph.add_node.

        The returned ``RunnableCallable`` carries both a sync and an async
        implementation. The choice of ``graph.invoke`` vs ``graph.ainvoke`` only
        changes how the *coordinator* waits — the node runs on the host the same
        way either way (sync ``def`` on the thread pool, ``async def`` on the host
        loop) and returns the identical result:

        * ``graph.invoke``  → sync path (a BackgroundExecutor thread parks on
          ``result()``): up to one parked thread per in-flight node. Simplest for
          a synchronous driver with a chain or modest fan-out.
        * ``graph.ainvoke`` → async path (awaits an asyncio future): N concurrent
          agents cost 0 extra threads — only the event loop and the watcher
          thread(s). Prefer it as you scale the number of concurrent agents.

        :param node_name: The name of a registered agent node.
        :type node_name: str
        :returns: A LangGraph ``RunnableCallable`` wrapping the agent's sync
            and async entry points.
        :rtype: langgraph.utils.runnable.RunnableCallable
        :raises KeyError: If *node_name* was not included in any
            ``launch_host()`` call.
        """
        if node_name not in self._nodes:
            raise KeyError(
                f"No agent '{node_name}' registered. "
                f"Available: {list(self._nodes.keys())}"
            )

        from langgraph.utils.runnable import RunnableCallable

        agent_node = self._nodes[node_name]
        return RunnableCallable(
            func=agent_node.__call__,
            afunc=agent_node.acall,
            name=node_name,
            trace=False,
        )

    def shutdown(self, *, graceful: bool = True) -> None:
        """Stop all AgentHost processes and the watcher thread.

        :param graceful: If True, send shutdown sentinels and wait for hosts
            to exit. If False, kill immediately. Defaults to True.
        :type graceful: bool, optional
        """
        if graceful:
            for queue_bytes in self._host_queues:
                try:
                    q = Queue.attach(queue_bytes)
                    q.put(None)  # sentinel
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[executor.shutdown] sentinel put failed: %s", exc)

        for proc in self._procs:
            try:
                if graceful:
                    exit_code = proc.join(timeout=10)
                    if exit_code is not None:
                        continue  # exited cleanly, no need to kill
                proc.kill()
            except Exception as exc:  # noqa: BLE001
                logger.error("[executor.shutdown] proc cleanup failed: %s", exc)

        # Hosts are down — destroy each per-host handshake Queue we created.
        for reply_queue in self._reply_queues:
            try:
                reply_queue.destroy()
            except Exception as exc:  # noqa: BLE001
                logger.warning("[executor.shutdown] reply_queue destroy failed: %s", exc)

        # Stop the watcher (guarded so a second shutdown() is a safe no-op).
        if self._watcher is not None:
            self._watcher.stop()

        self._procs.clear()
        self._nodes.clear()
        self._host_queues.clear()
        self._reply_queues.clear()
        self._watcher = None

    def launch_hosts(
        self,
        hosts: list[dict[str, Callable[..., Any]]],
        *,
        policies: list[Any] | None = None,
        event_timeout: float | None = None,
        max_threads: int | None = None,
    ) -> list[Any]:
        """Spawn multiple AgentHost processes, one per entry in *hosts*.

        This is a convenience wrapper around :meth:`launch_host` that mirrors
        the Dragon convention of passing a list of policies (one per item) to
        cover multi-node placement in a single call.

        :param hosts: List of agent dicts. Each dict maps ``node_name`` to
            ``fn(state) -> dict`` and becomes one AgentHost process. Node
            functions may be plain ``def`` or ``async def``.
        :type hosts: list[dict[str, Callable]]
        :param policies: Optional list of
            ``dragon.infrastructure.policy.Policy`` objects, one per host. Must
            be the same length as *hosts* when provided. ``None`` means all
            hosts use default (unspecified) placement. Defaults to None.
        :type policies: list[dragon.infrastructure.policy.Policy], optional
        :param event_timeout: Seconds before a task is considered timed out.
            ``None`` means no limit. Applied to every host. Defaults to None.
        :type event_timeout: float, optional
        :param max_threads: Maximum concurrent **sync** node bodies inside
            *each* host (thread-pool size for ``def`` nodes). ``None`` uses the
            per-host default ``max(len(agents) * 4, 8)``. Async nodes run on the
            host's shared event loop and are not bounded by this (see the
            ``__init__`` note). Defaults to None.
        :type max_threads: int, optional
        :returns: The Dragon Processes running each AgentHost, in the same
            order as *hosts*.
        :rtype: list[dragon.native.process.Process]
        :raises ValueError: If *policies* is provided but its length differs
            from *hosts*.

        Example usage::

            executor.launch_hosts(
                hosts=[
                    {"researcher": fn_research},
                    {"analyzer": fn_analyze, "writer": fn_write},
                ],
                policies=[policy_node0, policy_node1],
            )
        """
        if policies is not None and len(policies) != len(hosts):
            raise ValueError(
                f"len(policies)={len(policies)} must equal "
                f"len(hosts)={len(hosts)}"
            )

        procs = []
        for i, agents in enumerate(hosts):
            policy = policies[i] if policies is not None else None
            proc = self.launch_host(
                agents=agents,
                policy=policy,
                event_timeout=event_timeout,
                max_threads=max_threads,
            )
            procs.append(proc)
        return procs

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "DragonExecutor":
        """Enter the context manager, returning this executor unchanged.

        :returns: This executor instance.
        :rtype: DragonExecutor
        """
        return self

    def __exit__(self, *_: object) -> None:
        """Exit the context manager, shutting down all hosts and the watcher."""
        self.shutdown()
