.. _AgentAPI:

Agent Framework
+++++++++++++++

The Dragon AI Agent Framework provides a multi-agent orchestration system for
executing LLM-powered DAG workflows on HPC clusters. It combines Dragon's
distributed memory, process management, and communication objects with a
structured-output agentic loop.

For architecture and design details, see :ref:`developer-guide-agent`. For a
hands-on tutorial, see :ref:`agent_tutorial`.

Python Reference
================

Core Agent Classes
------------------

Abstract base class for persistent agents and the concrete ``SubAgent``
implementation with LLM tool-calling support.

.. currentmodule:: dragon.ai.agent.core.base

.. autosummary::
    :toctree:
    :recursive:

    DragonAgent

.. currentmodule:: dragon.ai.agent.core.sub_agent

.. autosummary::
    :toctree:
    :recursive:

    SubAgent
    create_sub_agent


Configuration
-------------

Dataclasses for agent identity, orchestrator settings, MCP server connections,
pipeline DAG definitions, memory strategies, and dispatch metadata.

.. currentmodule:: dragon.ai.agent.config.agent_config

.. autosummary::
    :toctree:
    :recursive:

    AgentConfig
    MCPServerConfig
    OrchestratorConfig

.. currentmodule:: dragon.ai.agent.config.pipeline

.. autosummary::
    :toctree:
    :recursive:

    Pipeline
    PipelineNode
    TaskResult
    TaskStatus

.. currentmodule:: dragon.ai.agent.config.memory_config

.. autosummary::
    :toctree:
    :recursive:

    MemoryConfig
    MemoryStrategy

.. currentmodule:: dragon.ai.agent.config.dispatch

.. autosummary::
    :toctree:
    :recursive:

    DispatchHeader


Tool System
-----------

Abstract tool interface, automatic callable-to-tool wrapping, a registry with
decorator support, and MCP server integration with scoped tool naming.

.. currentmodule:: dragon.ai.agent.tools.base

.. autosummary::
    :toctree:
    :recursive:

    BaseTool

.. currentmodule:: dragon.ai.agent.tools.function_tool

.. autosummary::
    :toctree:
    :recursive:

    FunctionTool

.. currentmodule:: dragon.ai.agent.tools.registry

.. autosummary::
    :toctree:
    :recursive:

    ToolRegistry

.. currentmodule:: dragon.ai.agent.tools.mcp_tool

.. autosummary::
    :toctree:
    :recursive:

    MCPServerClient


Reasoning
---------

The structured-output agentic loop and Pydantic models for LLM response parsing.

.. currentmodule:: dragon.ai.agent.reasoning.tool_dispatcher

.. autosummary::
    :toctree:
    :recursive:

    ToolDispatcher

.. currentmodule:: dragon.ai.agent.reasoning.response_parser

.. autosummary::
    :toctree:
    :recursive:

    ResponseModel
    ToolCall
    ToolRequest
    FinalResponse


Orchestration
-------------

DAG-based workflow execution using Dragon Batch, with automatic dependency
resolution and lifecycle management.

.. currentmodule:: dragon.ai.agent.orchestrator.orchestrator

.. autosummary::
    :toctree:
    :recursive:

    DAGOrchestrator

.. currentmodule:: dragon.ai.agent.core.batch_dispatch

.. autosummary::
    :toctree:
    :recursive:

    make_dispatcher_fn


Memory
------

Conversation history management with configurable pruning and summarization
strategies.

.. currentmodule:: dragon.ai.agent.memory.context_manager

.. autosummary::
    :toctree:
    :recursive:

    ContextManager


Human-in-the-Loop
-----------------

Approval gate for tool calls requiring human oversight, with a TCP bridge for
external access from outside the Dragon runtime.

.. currentmodule:: dragon.ai.agent.hitl.approval

.. autosummary::
    :toctree:
    :recursive:

    request_human_approval

.. currentmodule:: dragon.ai.agent.hitl.models

.. autosummary::
    :toctree:
    :recursive:

    HumanApprovalRequest
    HumanApprovalResponse

.. currentmodule:: dragon.ai.agent.hitl.tcp_bridge

.. autosummary::
    :toctree:
    :recursive:

    HitlTcpBridge


Observability
-------------

Span-based tracing with DDict-backed storage, TCP streaming to external viewers,
and a rich terminal UI.

.. currentmodule:: dragon.ai.agent.observability.ddict_tracer

.. autosummary::
    :toctree:
    :recursive:

    DictTracingProcessor

.. currentmodule:: dragon.ai.agent.observability.tracer

.. autosummary::
    :toctree:
    :recursive:

    TracingProcessor
    Span
    Trace

.. currentmodule:: dragon.ai.agent.observability.trace_protocol

.. autosummary::
    :toctree:
    :recursive:

    SpanKind
    MsgType

.. currentmodule:: dragon.ai.agent.observability.tcp_bridge

.. autosummary::
    :toctree:
    :recursive:

    TraceTcpBridge


Communication
-------------

Abstract communication protocol and the Dragon Queue-based implementation used
for inter-process messaging.

.. currentmodule:: dragon.ai.agent.communication.protocol

.. autosummary::
    :toctree:
    :recursive:

    CommunicationProtocol

.. currentmodule:: dragon.ai.agent.communication.dragon_comm

.. autosummary::
    :toctree:
    :recursive:

    DragonQueueProtocol

.. currentmodule:: dragon.ai.agent.communication.message

.. autosummary::
    :toctree:
    :recursive:

    Message


DDict Access
------------

Typed wrapper over raw DDict key operations for structured reads and writes.

.. currentmodule:: dragon.ai.agent.ddict.accessor

.. autosummary::
    :toctree:
    :recursive:

    DDictAccessor


Errors
------

Structured exception hierarchy for agent errors, tool failures, and
observability warnings.

.. currentmodule:: dragon.ai.agent.utils.errors

.. autosummary::
    :toctree:
    :recursive:

    AgentError
    ToolExecutionError
    AgentLoopError
    HITLBridgeError
    CompletionSignalError
    AgentObservabilityWarning
