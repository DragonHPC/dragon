"""Dragon-native logging setup for the LangGraph integration.

Mirrors the three-tier pattern used by DDict, Batch, and the AI Agent framework
(see ``dragon.ai.agent.utils.logging``), so these logs land in the same places
and honour the same ``DRAGON_LOG_DEVICE_*`` env-vars: stderr, the FE-aggregated
dragon-file, and a per-process actor-file
``$DRAGON_LA_LOG_DIR/LANGGRAPH_<label>_<hostname>_<puid>.log``.

Setup happens once per process, in the two roles that start one: the coordinator
(:meth:`DragonExecutor.__init__`) and each AgentHost (:func:`agent_host_entry`).

The service label stays a plain string rather than a new
``DragonLoggingServices`` enum member, so the integration needs no change to
core Dragon; ``setup_BE_logging`` accepts a ``str`` service directly.
"""

from __future__ import annotations

import logging
import socket
from typing import Optional

# ``dragon.dlogging`` is pure-Python and safe to import without a running
# runtime; the actual handler attachment happens inside setup_langgraph_logging.
from ...dlogging.util import setup_BE_logging

# Service label for this integration's logs.
_SERVICE = "LANGGRAPH"
_initialized = False


def setup_langgraph_logging(label: Optional[str] = None) -> None:
    """Configure Dragon-native three-tier logging for the current process.

    Idempotent — only the first call per process has effect (mirrors
    ``DDict.setup_logging`` and ``setup_agent_logging``).  Delegates to
    :func:`dragon.dlogging.util.setup_BE_logging`, which reads the three
    ``DRAGON_LOG_DEVICE_*`` env-vars to decide which handlers to attach to the
    root logger.

    Logging setup must never break the integration: any failure (e.g. no active
    runtime to resolve ``my_puid``) is swallowed and the process falls back to
    whatever logging configuration is already in place.

    :param label: Optional label folded into the per-process log filename
        (``LANGGRAPH_<label>_<hostname>_<puid>.log``) so each process's log is
        easy to tell apart on disk (e.g. an AgentHost's first agent name).
        Defaults to None.
    :type label: str, optional
    """
    global _initialized
    if _initialized:
        return

    try:
        from ...infrastructure.parameters import this_process

        puid = str(this_process.my_puid)
        if label:
            fname = f"{_SERVICE}_{label}_{socket.gethostname()}_{puid}.log"
        else:
            fname = f"{_SERVICE}_{socket.gethostname()}_{puid}.log"
        setup_BE_logging(service=_SERVICE, fname=fname)
    except Exception:  # noqa: BLE001 - logging setup must never crash the caller
        pass
    finally:
        _initialized = True


def get_langgraph_logger(child: Optional[str] = None) -> logging.Logger:
    """Return a logger under the ``LANGGRAPH`` service namespace.

    ``get_langgraph_logger("executor")`` returns
    ``logging.getLogger("LANGGRAPH.executor")``; ``get_langgraph_logger()``
    returns the root ``LANGGRAPH`` logger.

    :param child: Optional child name appended to the ``LANGGRAPH`` namespace.
        ``None`` returns the root service logger. Defaults to None.
    :type child: str, optional
    :returns: A logger scoped to the ``LANGGRAPH`` service namespace.
    :rtype: logging.Logger
    """
    base = logging.getLogger(_SERVICE)
    if child:
        return base.getChild(child)
    return base
