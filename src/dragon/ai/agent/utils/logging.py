"""Dragon-native logging setup for AI Agent processes.

This module follows the standard Dragon logging pattern used by DDict, Batch,
GlobalServices, and every other Dragon backend service.  Each agent process
calls :func:`setup_agent_logging` once at startup (inside
:func:`create_sub_agent`) to configure the three-tier logging infrastructure:

1. **stderr**    — controlled by ``DRAGON_LOG_DEVICE_STDERR``
2. **dragon-file** — aggregated via ``DragonLoggingHandler`` to the FE,
   controlled by ``DRAGON_LOG_DEVICE_DRAGON_FILE``
3. **actor-file** — per-process file at
   ``$DRAGON_LA_LOG_DIR/AI_AGENT_<label>_<hostname>_<puid>.log``,
   controlled by ``DRAGON_LOG_DEVICE_ACTOR_FILE``

Usage (in ``create_sub_agent`` or orchestrator ``__init__``)::

    from ..utils.logging import setup_agent_logging, get_agent_logger

    setup_agent_logging()
    log = get_agent_logger("sub_agent")   # logger named "AI_AGENT.sub_agent"
    log.info("Agent process started")

This module assumes it is always running inside an active Dragon runtime.
If ``my_puid`` is not available, something is fundamentally wrong and the
error will propagate immediately.
"""

from __future__ import annotations

import logging
import socket
from typing import Optional

# These imports are deferred-safe: ``dragon.dlogging`` is a pure-Python
# package that does not require a running Dragon runtime to import.
from ....dlogging.util import (
    DragonLoggingServices as dls,
    setup_BE_logging,
)

_SERVICE = dls.AI_AGENT
_initialized = False


def setup_agent_logging(label: Optional[str] = None) -> None:
    """Configure Dragon-native logging for the current process.

    Safe to call multiple times — only the first invocation has effect,
    mirroring ``DDict.setup_logging()`` behaviour.

    The function builds a per-process filename following Dragon convention::

        AI_AGENT_<label>_<hostname>_<puid>.log   (when label is given)
        AI_AGENT_<hostname>_<puid>.log           (fallback)

    and delegates to :func:`dragon.dlogging.util.setup_BE_logging` which
    reads the three ``DRAGON_LOG_DEVICE_*`` env-vars to decide what
    handlers to attach to the root logger.

    Parameters
    ----------
    label:
        Optional label to include in the log filename so each process's
        log is easy to distinguish on disk.  Typically an agent_id for
        sub-agents or a pipeline run identifier for the orchestrator.
    """
    global _initialized
    if _initialized:
        return

    from ....infrastructure.parameters import this_process
    puid = str(this_process.my_puid)

    if label:
        fname = f"{_SERVICE}_{label}_{socket.gethostname()}_{puid}.log"
    else:
        fname = f"{_SERVICE}_{socket.gethostname()}_{puid}.log"
    setup_BE_logging(service=_SERVICE, fname=fname)
    _initialized = True


def get_agent_logger(child: Optional[str] = None) -> logging.Logger:
    """Return a logger under the ``AI_AGENT`` service namespace.

    Parameters
    ----------
    child:
        Optional sub-component name.  ``get_agent_logger("sub_agent")``
        returns ``logging.getLogger("AI_AGENT.sub_agent")``.
        ``get_agent_logger()`` with no argument returns the root
        ``AI_AGENT`` logger.

    Returns
    -------
    logging.Logger
    """
    base = logging.getLogger(str(_SERVICE))
    if child:
        return base.getChild(child)
    return base
