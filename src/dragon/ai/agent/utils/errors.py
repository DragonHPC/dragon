"""Custom exception and warning classes for the Dragon AI Agent framework.

Exception hierarchy
-------------------

::

    AgentError (base)
    ├── ToolExecutionError    — a tool call (MCP or local) raised an exception
    ├── AgentLoopError        — LLM response parsing failure or max iterations
    ├── HITLBridgeError       — HITL TCP bridge communication failure
    └── CompletionSignalError — completion_event.set() failed (dispatcher may hang)

Warning class
-------------

::

    AgentObservabilityWarning (UserWarning)
        — non-fatal DDict/trace write failures that should be visible
          in the execution terminal but should not kill the pipeline.
          Users can promote these to errors via:
              warnings.filterwarnings("error", category=AgentObservabilityWarning)
"""

from __future__ import annotations


class AgentError(Exception):
    """Base exception for all Dragon AI Agent internal errors."""


class ToolExecutionError(AgentError):
    """A tool call (MCP or local) raised an exception.

    Attributes
    ----------
    tool_name : str
        The name of the tool that failed.
    tool_args : dict
        The arguments passed to the tool.
    original : Exception
        The underlying exception from the tool.
    """

    def __init__(self, tool_name: str, tool_args: dict, original: Exception) -> None:
        self.tool_name = tool_name
        self.tool_args = tool_args
        self.original = original
        super().__init__(
            f"Tool '{tool_name}' failed: {type(original).__name__}: {original}"
        )


class AgentLoopError(AgentError):
    """The agentic tool-calling loop failed.

    Covers:
    - LLM produced unparseable / truncated JSON for a tool_request
    - Max tool-call iterations exceeded
    """


class HITLBridgeError(AgentError):
    """HITL TCP bridge communication failure.

    Covers:
    - Queue read errors (corrupted or destroyed queue)
    - Failed to re-queue a request after client disconnect
    """


class CompletionSignalError(AgentError):
    """completion_event.set() failed — the batch dispatcher will hang.

    This is raised in the ``finally`` block of ``_handle_message`` when
    the Dragon IPC completion event cannot be signalled.
    """


class AgentObservabilityWarning(UserWarning):
    """Non-fatal warning for DDict/trace write failures.

    These warnings are emitted via ``warnings.warn()`` and appear in the
    execution terminal by default.  They do NOT kill the pipeline.

    To promote to hard errors::

        import warnings
        from dragon.ai.agent.errors import AgentObservabilityWarning
        warnings.filterwarnings("error", category=AgentObservabilityWarning)
    """
