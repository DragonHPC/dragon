"""Cross-cutting utilities for the Dragon AI Agent framework.

This sub-package collects infrastructure modules that are imported by
virtually every other sub-package (``core/``, ``reasoning/``, ``hitl/``,
``observability/``, ``ddict/``, ``tools/``, ``memory/``,
``orchestrator/``).  Consolidating them here keeps the package root clean.

Modules
-------
ansi
    ANSI terminal colour helpers (``Color``, ``colorize``, ``strip_ansi``, …).
errors
    Custom exception and warning classes (``AgentError`` hierarchy,
    ``AgentObservabilityWarning``).
logging
    Dragon-native three-tier logging setup (``setup_agent_logging``,
    ``get_agent_logger``).
"""

from .ansi import Color, colorize, ANSI_RE, visible_len, strip_ansi
from .errors import (
    AgentError,
    ToolExecutionError,
    AgentLoopError,
    HITLBridgeError,
    CompletionSignalError,
    AgentObservabilityWarning,
)
from .logging import setup_agent_logging, get_agent_logger

__all__ = [
    # ansi
    "Color",
    "colorize",
    "ANSI_RE",
    "visible_len",
    "strip_ansi",
    # errors
    "AgentError",
    "ToolExecutionError",
    "AgentLoopError",
    "HITLBridgeError",
    "CompletionSignalError",
    "AgentObservabilityWarning",
    # logging
    "setup_agent_logging",
    "get_agent_logger",
]
