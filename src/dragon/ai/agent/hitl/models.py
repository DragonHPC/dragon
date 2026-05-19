"""Data structures for the Human-in-the-Loop (HITL) approval gate."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class HumanApprovalRequest:
    """Payload sent to the HITL client when an agent requests human approval.

    Also written to DDict for in-runtime logging.
    """
    tool_name: str
    tool_args: dict[str, Any]
    context: str                    # why the agent wants to call this tool
    agent_id: str
    task_id: str
    dispatch_id: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class HumanApprovalResponse:
    """Payload sent back from the HITL client after the operator decides.

    Also written to DDict for in-runtime logging.

    Attributes
    ----------
    approved:
        ``True`` if the operator approved the tool call.
    reason:
        Operator's explanation (rejection reason or feedback text).
    is_feedback:
        ``True`` when the operator chose to provide feedback rather than
        a hard reject.  The LLM receives the feedback and retries.
    """
    approved: bool
    reason: str = ""
    is_feedback: bool = False
    timestamp: float = field(default_factory=time.time)
