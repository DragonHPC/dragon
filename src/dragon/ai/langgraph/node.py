"""DragonAgentNode — a LangGraph node callable that dispatches work to a
Dragon AgentHost process via the DragonWatcher's completion queue.

From LangGraph's perspective, ``DragonAgentNode`` is just a callable
``fn(state) -> dict``.  Internally it delegates to the DragonWatcher:

1. Watcher registers a pending task and hands the host the shared
   completion-Queue handle.
2. Dispatches message to the AgentHost's shared Dragon Queue.
3. The host puts its result envelope on the shared completion Queue.
4. Watcher's single recv thread receives the envelope, routes it to the
   matching Future by ``task_id``, and resolves it.
5. Node's __call__ returns future.result() to LangGraph.

No per-node thread is created — all waiting is consolidated in the
watcher's blocking recv loop on the single shared completion Queue.

The AgentHost process is **persistent** and hosts multiple agents as
threads.  Resources (LLM clients, model weights, DB connections) are
shared across agents on the same host.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .logging import get_langgraph_logger

if TYPE_CHECKING:
    from .watcher import DragonWatcher

logger = get_langgraph_logger("node")


class DragonAgentNode:
    """A LangGraph-compatible callable that dispatches execution to an
    agent thread inside a Dragon AgentHost process.

    Users do not instantiate this directly; use
    ``DragonExecutor.node(node_name)`` after calling ``launch_host()``.

    :param node_name: The LangGraph node name.
    :type node_name: str
    :param serialized_input_queue: cloudpickle-serialized Dragon Queue shared by
        all agents on the same host.
    :type serialized_input_queue: bytes
    :param watcher: DragonWatcher that owns the shared completion Queue and
        resolves task Futures from inbound result envelopes.
    :type watcher: DragonWatcher
    :param event_timeout: Maximum seconds to wait for the agent to finish
        before raising ``TimeoutError``. ``None`` means wait indefinitely.
        Defaults to None.
    :type event_timeout: float, optional
    """

    def __init__(
        self,
        node_name: str,
        serialized_input_queue: bytes,
        watcher: "DragonWatcher",
        *,
        event_timeout: float | None = None,
    ) -> None:
        self.node_name = node_name
        self._serialized_input_queue = serialized_input_queue
        self._watcher = watcher
        self._event_timeout = event_timeout

    # ------------------------------------------------------------------
    # LangGraph node interface
    # ------------------------------------------------------------------

    def __call__(self, state: Any) -> Any:
        """Dispatch ``state`` to the AgentHost and return its result.

        Synchronous path (``graph.invoke``).  LangGraph runs this inside
        a BackgroundExecutor thread, which parks on ``future.result()``
        while the watcher resolves it.
        """
        future = self._watcher.submit_task(
            node_name=self.node_name,
            state=state,
            serialized_input_queue=self._serialized_input_queue,
        )
        return future.result(timeout=self._event_timeout)

    async def acall(self, state: Any) -> Any:
        """Async dispatch — the zero-extra-thread path (``graph.ainvoke``).

        Awaits the watcher's ``asyncio.Future``.  Awaiting suspends this
        coroutine on the event loop instead of parking a thread, so N
        concurrent agents in a superstep cost 0 extra threads — they are all
        just asyncio tasks multiplexed by the single event loop, with the
        watcher resolving each future when its host signals done.  Backpressure
        is awaited too: if the concurrency cap is reached, the wait for a slot
        suspends the coroutine rather than blocking the event loop.
        """
        import asyncio

        future = await self._watcher.submit_task_async(
            node_name=self.node_name,
            state=state,
            serialized_input_queue=self._serialized_input_queue,
        )
        if self._event_timeout is not None:
            return await asyncio.wait_for(future, timeout=self._event_timeout)
        return await future
