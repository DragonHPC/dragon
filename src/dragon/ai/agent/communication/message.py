"""Lightweight message dataclass for inter-agent signaling."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from ..config.dispatch import DispatchHeader as _DispatchHeader


@dataclass
class Message:
    """A message sent between dispatchers and agents.

    Routing fields (consumed by the transport layer):
        ``recipient_serialized_queue`` — serialized Dragon queue handle for the
        target agent.  Used by :meth:`DragonQueueProtocol.send` to deserialize
        the destination queue.  Not read by the agent itself.

    Content fields (received and processed by the agent):
        ``header`` — a :class:`~dragon.ai.agent.config.dispatch.DispatchHeader`
        containing the task description, upstream references, serialized DDict
        handle, and completion event.  This is the only field the agent
        inspects inside its ``listen()`` loop.
    """

    # -- content (agent-facing) ----------------------------------------------
    task_id: str
    sender_id: str
    recipient_id: str
    header: _DispatchHeader | None = None

    # -- routing (transport-layer only) --------------------------------------
    recipient_serialized_queue: str = ""

    # -- identity ------------------------------------------------------------
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
