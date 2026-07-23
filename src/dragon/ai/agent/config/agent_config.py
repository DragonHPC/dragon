"""User-facing configuration dataclasses: AgentConfig, MCPServerConfig, OrchestratorConfig."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from ....native.queue import Queue

from .memory_config import MemoryConfig


# ---------------------------------------------------------------------------
# Agent configuration
# ---------------------------------------------------------------------------

@dataclass
class AgentConfig:
    """Immutable configuration for a single Dragon agent.

    Parameters
    ----------
    agent_id:
        Unique identifier for this agent within the pipeline.
    name:
        Human-readable display name for the agent.
    role:
        System-prompt role description sent to the LLM.
    input_queue:
        Dragon Queue populated after the agent starts via ``reply_queue``.
        Users should not set this directly.
    inference_queue:
        Dragon Queue feeding the inference pipeline backend.  When set,
        the agent auto-creates a :class:`DragonQueueLLMProxy` internally
        — no pre-built proxy needs to be passed.  Different agents can
        point to different inference pipelines.
    max_concurrent_requests:
        Maximum number of concurrent in-flight LLM requests this agent
        can have at once.  ``None`` (default) uses the proxy's built-in
        default (32).
    summarizer_inference_queue:
        Optional inference queue for a separate summarizer pipeline.
        When set, a summarizer proxy is auto-created and wired into
        the agent's context manager.
    node_affinity:
        Optional node placement hint (e.g. hostname or node index).
    approval_filter:
        HITL gating predicate.  When set, tool calls matching the filter
        are paused for human approval before execution.
        ``None`` (default) = no gating.
    max_tool_call_iterations:
        Maximum number of tool-call iterations in the agentic loop.
        Each iteration = one LLM call that may produce a tool request or
        final answer.  Increase for agents that need many tool calls or
        multiple HITL feedback rounds.  Defaults to ``20``.
    memory:
        Memory management — controls how conversation history is managed
        during the tool-call loop.  ``None`` (default) keeps everything.
        Pass a ``MemoryConfig(...)`` for explicit strategy and tuning
        parameters.
    """

    agent_id: str
    name: str
    role: str
    input_queue: Queue = None
    inference_queue: Any = None
    max_concurrent_requests: int | None = None
    summarizer_inference_queue: Any = None
    node_affinity: str = ""
    approval_filter: Callable[[str, dict], bool] | None = None
    max_tool_call_iterations: int = 20
    memory: MemoryConfig | None = None


# ---------------------------------------------------------------------------
# MCP server configuration
# ---------------------------------------------------------------------------

@dataclass
class MCPServerConfig:
    """Configuration for a single MCP server connection.

    Parameters
    ----------
    url:
        Full HTTP/HTTPS URL of the MCP server endpoint.
    alias:
        Short unique label used to scope tool names from this server.
        Tools will be exposed as ``{alias}__{tool_name}``.
    token:
        Optional bearer token for authentication.
    max_retries:
        Number of connection attempts before raising ``ConnectionError``.
        Defaults to ``3``.
    retry_delay:
        Seconds to wait between retry attempts.  Defaults to ``0.5``.
    timeout:
        Per-attempt connection timeout in seconds.  Defaults to ``5.0``.
    """

    url: str
    alias: str
    token: str | None = None
    max_retries: int = 3
    retry_delay: float = 0.5
    timeout: float = 5.0

    def __post_init__(self):
        if not self.alias or not self.alias.strip():
            raise ValueError(
                f"MCPServerConfig requires a non-empty 'alias' (url={self.url!r})."
            )


# ---------------------------------------------------------------------------
# Orchestrator configuration
# ---------------------------------------------------------------------------

@dataclass
class OrchestratorConfig:
    """Top-level configuration for :class:`DAGOrchestrator`.

    Parameters
    ----------
    agents:
        List of :class:`AgentConfig` instances to register.  Duplicate
        ``agent_id`` values are rejected at construction time.
    poll_interval:
        Seconds between DDict status polls.  Defaults to ``0.5``.
    poll_timeout:
        Maximum seconds to wait per agent before timing out.
        Defaults to ``120.0``.
    tracing:
        Controls whether tracing is enabled pipeline-wide.
        ``False`` (default) = no tracing, zero overhead.
        ``True`` = DDict tracing: per-task ``DictTracingProcessor`` is
        created automatically, per-event keys are written in real time,
        and the orchestrator starts a TCP bridge for live viewing.
        Use the trace viewer's ``--save`` flag to persist traces to disk.
    ddict_kwargs:
        DDict constructor overrides — merged with sensible defaults and
        passed as ``**kwargs`` to ``DDict()``.  Use this to control memory
        size, persistence, working-set size, placement policies, etc.

        Defaults (applied when a key is absent):
        ``managers_per_node=1``, ``n_nodes=1``, ``trace=False``.

        User-supplied values override defaults (standard dict merge).
        ``trace`` controls DDict-level tracing (independent of the
        ``tracing`` flag, which controls the TCP trace bridge).

        Example::

            OrchestratorConfig(
                ...,
                ddict_kwargs={"managers_per_node": 2, "total_mem": 1 << 30},
            )
    """

    agents: list[AgentConfig] = field(default_factory=list)
    poll_interval: float = 0.5
    poll_timeout: float = 120.0
    tracing: bool = False
    ddict_kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate that no duplicate agent_id appears in the agents list."""
        seen: set[str] = set()
        for agent_cfg in self.agents:
            if agent_cfg.agent_id in seen:
                raise ValueError(
                    f"OrchestratorConfig contains duplicate agent_id "
                    f"'{agent_cfg.agent_id}'. Each agent must be registered once."
                )
            seen.add(agent_cfg.agent_id)
