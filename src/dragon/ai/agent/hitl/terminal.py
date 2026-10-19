"""Operator-side terminal UI for HITL approval decisions.

Provides coloured, formatted output for approval requests and operator
prompts — the HITL equivalent of the trace viewer's tree renderer.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field

from ..utils.ansi import Color as _Color, colorize as _colorize
from .models import HumanApprovalRequest


# ---------------------------------------------------------------------------
# Session statistics (parallel to TraceState span counters)
# ---------------------------------------------------------------------------

@dataclass
class HitlSessionStats:
    """Tracks operator decisions during an HITL session."""

    approved: int = 0
    rejected: int = 0
    feedback: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def total(self) -> int:
        return self.approved + self.rejected + self.feedback

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    def record(self, approved: bool, is_feedback: bool) -> None:
        """Update counters after an operator decision."""
        if approved:
            self.approved += 1
        elif is_feedback:
            self.feedback += 1
        else:
            self.rejected += 1


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_request(request: HumanApprovalRequest) -> str:
    """Pretty-print an approval request with ANSI colours."""
    header = _colorize("  APPROVAL REQUESTED", _Color.BOLD)
    tool = _colorize(request.tool_name, _Color.CYAN)
    agent = _colorize(request.agent_id, _Color.CYAN)
    ts = time.strftime("%H:%M:%S", time.localtime(request.timestamp))

    lines = [
        "",
        _colorize("=" * 60, _Color.DIM),
        header,
        _colorize("=" * 60, _Color.DIM),
        f"  Agent      : {agent}",
        f"  Task ID    : {_colorize(request.task_id[:8] + '...', _Color.DIM)}",
        f"  Dispatch   : {_colorize(request.dispatch_id[:8] + '...', _Color.DIM)}",
        f"  Tool       : {tool}",
        f"  Timestamp  : {ts}",
        _colorize("-" * 60, _Color.DIM),
        f"  {_colorize('Arguments:', _Color.BOLD)}",
    ]
    try:
        args_str = json.dumps(request.tool_args, indent=4)
    except (TypeError, ValueError):
        args_str = str(request.tool_args)
    for line in args_str.splitlines():
        lines.append(f"    {line}")
    lines.append(_colorize("-" * 60, _Color.DIM))
    if request.context:
        lines.append(f"  {_colorize('Context:', _Color.BOLD)} {request.context}")
        lines.append(_colorize("-" * 60, _Color.DIM))
    return "\n".join(lines)


def format_decision(approved: bool, reason: str, is_feedback: bool) -> str:
    """Format the operator's decision with colour."""
    if approved:
        return _colorize("  --> APPROVED", _Color.GREEN)
    elif is_feedback:
        return _colorize(f"  --> FEEDBACK", _Color.YELLOW) + f" ({reason})"
    else:
        return _colorize(f"  --> REJECTED", _Color.RED) + f" ({reason})"


def format_banner(host: str | None = None, port: int | None = None) -> str:
    """Render the startup banner (parallel to trace viewer's connection msg)."""
    lines = [
        "",
        _colorize("=" * 60, _Color.DIM),
        _colorize("  HITL Approval Handler active (TCP)", _Color.BOLD),
        _colorize("=" * 60, _Color.DIM),
    ]
    if host and port:
        lines.insert(3, _colorize(f"  Connected to {host}:{port}", _Color.DIM))
    lines.append(_colorize("  Waiting for approval requests...", _Color.DIM))
    return "\n".join(lines)


def format_session_summary(stats: HitlSessionStats) -> str:
    """Render end-of-session statistics (parallel to trace viewer's span summary)."""
    elapsed = f"{stats.elapsed:.1f}s"

    parts = [f"  Decisions: {stats.total}"]
    if stats.approved:
        parts.append(_colorize(f"✓ {stats.approved} approved", _Color.GREEN))
    if stats.rejected:
        parts.append(_colorize(f"✗ {stats.rejected} rejected", _Color.RED))
    if stats.feedback:
        parts.append(_colorize(f"↩ {stats.feedback} feedback", _Color.YELLOW))
    parts.append(_colorize(f"({elapsed})", _Color.DIM))

    lines = [
        "",
        _colorize("=" * 60, _Color.DIM),
        _colorize("  HITL Session Summary", _Color.BOLD),
        _colorize("=" * 60, _Color.DIM),
        "  ".join(parts),
        "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Operator prompt
# ---------------------------------------------------------------------------

def prompt_operator() -> tuple[bool, str, bool]:
    """Ask the operator to approve, reject, or give feedback.

    Returns ``(approved, reason, is_feedback)``.

    .. note::

        Uses plain ``input()`` which reads from stdin.  This must be called
        from a process with a real terminal.
    """
    hint = (
        f"\n  {_colorize('[a]pprove', _Color.GREEN)}  "
        f"{_colorize('[r]eject', _Color.RED)}  "
        f"{_colorize('[f]eedback', _Color.YELLOW)} > "
    )
    while True:
        choice = input(hint).strip().lower()
        if choice in ("a", "approve"):
            return True, "", False
        elif choice in ("r", "reject"):
            reason = input("  Reason (optional): ").strip()
            return False, reason, False
        elif choice in ("f", "feedback"):
            feedback = input("  Feedback: ").strip()
            return False, feedback, True
        else:
            print("  Please enter 'a' to approve, 'r' to reject, or 'f' for feedback.")



