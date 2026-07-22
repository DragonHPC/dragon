"""DragonAgent — stateless, persistent base agent."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..utils.logging import get_agent_logger
from ..config import AgentConfig, MCPServerConfig, MemoryConfig
from ..communication.protocol import CommunicationProtocol
from ..communication.dragon_comm import DragonQueueProtocol
from ..tools.registry import ToolRegistry

_log = get_agent_logger("base")


class DragonAgent(ABC):
    """Stateless, persistent base agent running on a node.

    Each ``DragonAgent`` instance owns:

    * Its own Dragon input queue (wrapped by *comm*).
    * Its own LLM proxy — a lightweight client handle pointing at a shared
      inference pipeline, reused across all tasks.
    * A :class:`ToolRegistry` of inline tools.

    The agent holds **no per-task state**.  All task context, tool history, and
    results are stored in a shared Dragon Distributed Dictionary (DDict) that
    is created per batch run and passed to the agent via the message header.

    LLM proxy resolution (in priority order):

    1. ``config.inference_queue`` — auto-creates a :class:`DragonQueueLLMProxy`
       per agent process.  This is the recommended path.
    2. Neither — ``self.llm`` is ``None``, agent has no LLM.

    Parameters
    ----------
    config:
        Agent identity, role, model, and inference parameters.
        Set ``config.inference_queue`` to the Dragon Queue feeding the
        inference pipeline so a proxy is auto-created.
    tool_registry:
        Tools available for inline invocation during LLM reasoning.
    protocol:
        Communication protocol class (default: DragonQueueProtocol).
    mcp_servers:
        Optional list of MCP server configurations for remote tools.
    shutdown_event:
        Event signalled by the parent to stop the agent listen loop.
    """

    def __init__(
        self,
        config: AgentConfig,
        tool_registry: ToolRegistry,
        protocol: CommunicationProtocol = DragonQueueProtocol,
        mcp_servers: list[MCPServerConfig] | None = None,
        shutdown_event=None,
    ) -> None:
        self.config = config
        if protocol is not DragonQueueProtocol:
            raise NotImplementedError("Only Dragon Queue Protocol is supported!")
        # Queue is created here — its lifecycle is tied to this agent.
        self.comm = DragonQueueProtocol()
        self.tool_registry = tool_registry
        # Public handle — shared with other processes so they can attach to
        # and send messages through this agent's input queue.
        self.serialized_queue: str = self.comm.serialized_queue
        # -- Shutdown event: signalled by the parent to stop the listen loop --
        self.shutdown_event = shutdown_event  # dragon.native.event.Event | None

        # -- LLM proxy: auto-created from config.inference_queue -------------
        self.llm = None
        if config.inference_queue is not None:
            from ...inference import DragonQueueLLMProxy  # noqa: PLC0415

            pool_kwargs = {}
            if config.max_concurrent_requests is not None:
                pool_kwargs["max_concurrent_requests"] = config.max_concurrent_requests
            self.llm = DragonQueueLLMProxy(
                config.inference_queue, **pool_kwargs,
            )

        # -- Summarizer proxy: same pattern ----------------------------------
        summarizer_llm = None
        if config.summarizer_inference_queue is not None:
            from ...inference import DragonQueueLLMProxy  # noqa: PLC0415

            pool_kwargs = {}
            if config.max_concurrent_requests is not None:
                pool_kwargs["max_concurrent_requests"] = config.max_concurrent_requests
            summarizer_llm = DragonQueueLLMProxy(
                config.summarizer_inference_queue, **pool_kwargs,
            )

        # -- ToolDispatcher: built internally from llm + tool_registry -------
        self.tool_dispatcher = None
        if self.llm is not None:
            from ..reasoning.tool_dispatcher import ToolDispatcher  # noqa: PLC0415
            from ..memory import ContextManager  # noqa: PLC0415

            # Resolve memory config: MemoryConfig | None → validated MemoryConfig | None
            memory_config = MemoryConfig.resolve(config.memory)
            context_manager = (
                ContextManager(memory_config, summarizer_engine=summarizer_llm)
                if memory_config is not None
                else None
            )

            self.tool_dispatcher = ToolDispatcher(
                self.llm, tool_registry,
                approval_filter=config.approval_filter,
                max_tool_call_iterations=config.max_tool_call_iterations,
                context_manager=context_manager,
            )
        # -- MCP servers: deferred to the agent's own event loop -------------
        # Stored here; actual connection happens in listen() so that the
        # fastmcp.Client async context lives on the same event loop as
        # the tool-call code.  asyncio.run() in __init__ would create a
        # separate loop that is destroyed before call_tool() runs.
        self._pending_mcp_servers = mcp_servers or []

    # -- persistent listen loop ----------------------------------------------
    @abstractmethod
    def listen(self) -> None:
        """Listening task from dragon queue or a port. """

    # -- tool introspection --------------------------------------------------

    def list_tools(self) -> dict[str, Any]:
        """Return all tools available to this agent, separated by source.

        Returns a dict with two keys:

        * ``"registry"`` — local Python tools registered in
          :attr:`tool_registry`.  Each entry is an OpenAI-compatible schema
          dict as returned by :meth:`~BaseTool.to_schema`.
        * ``"mcp"`` — remote MCP tools owned by the internal
          :class:`ToolDispatcher`, grouped by server alias.  Each alias maps
          to a list of OpenAI-compatible schema dicts.  Empty ``{}`` when no
          dispatcher or no MCP servers are connected.

        Example output::

            {
                "registry": [
                    {"type": "function", "function": {"name": "web_search", ...}},
                    {"type": "function", "function": {"name": "calculate_word_count", ...}},
                ],
                "mcp": {
                    "jupyter": [
                        {"type": "function", "function": {"name": "jupyter__create_notebook", ...}},
                    ],
                },
            }
        """
        registry_tools = self.tool_registry.list_tools()

        mcp_tools: dict[str, list] = {}
        if self.tool_dispatcher is not None:
            for alias, client in self.tool_dispatcher._mcp_clients.items():
                mcp_tools[alias] = client.tools_schemas

        return {"registry": registry_tools, "mcp": mcp_tools}


    # -- abstract process ----------------------------------------------------

    @abstractmethod
    async def process(self, task_id: str, header: Any) -> dict[str, Any]:
        """Process a single task.

        Parameters
        ----------
        task_id:
            Unique identifier for this batch run / user request.
        header:
            Implementation-defined header object carrying task context.
            For :class:`SubAgent` this is a :class:`DispatchHeader`.

        Returns
        -------
        dict
            Result payload to be written to DDict under the agent's result key.
        """
