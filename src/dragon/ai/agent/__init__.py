"""Dragon AI Agent — agentic framework for HPC workflows.

Public API
----------
Import directly from the submodule you need::

    from dragon.ai.agent.core import DragonAgent, SubAgent, create_sub_agent
    from dragon.ai.agent.config import AgentConfig, MCPServerConfig
    from dragon.ai.agent.tools import BaseTool, ToolRegistry, FunctionTool
    from dragon.ai.agent.communication import Message, DragonQueueProtocol
    from dragon.ai.agent.observability import TracingProcessor, Span, trace_span
    from dragon.ai.agent.utils.errors import AgentError
    from dragon.ai.agent.utils.logging import setup_agent_logging
"""
