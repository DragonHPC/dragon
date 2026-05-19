"""Client-side trace state — accumulates span events for rendering.

The ``observability/`` package layout:

* ``trace_state.py``       — data model  (parallel to ``hitl/models.py``)
* ``trace_renderer.py``    — terminal rendering  (parallel to ``hitl/terminal.py``)
* ``trace_interactive.py`` — curses TUI
* ``trace_report.py``      — report writer
* ``viewer.py``            — argparse CLI entry point
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .trace_protocol import MsgType

__all__ = ["ViewerSpan", "TraceState"]


# ---------------------------------------------------------------------------
# Internal span model
# ---------------------------------------------------------------------------

@dataclass
class ViewerSpan:
    """Lightweight span representation for the viewer."""

    span_id: str = ""
    trace_id: str = ""
    parent_id: str | None = None
    name: str = ""
    kind: str = "TASK"
    start_time: float = 0.0
    end_time: float | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    children: list[ViewerSpan] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> ViewerSpan:
        return cls(
            span_id=d.get("span_id", ""),
            trace_id=d.get("trace_id", ""),
            parent_id=d.get("parent_id"),
            name=d.get("name", ""),
            kind=d.get("kind", "TASK"),
            start_time=d.get("start_time", 0.0),
            end_time=d.get("end_time"),
            attributes=d.get("attributes", {}),
            error=d.get("error"),
        )

    @property
    def is_complete(self) -> bool:
        return self.end_time is not None

    @property
    def duration(self) -> float | None:
        if self.end_time is None:
            return None
        return self.end_time - self.start_time


# ---------------------------------------------------------------------------
# Trace state — accumulates spans for rendering
# ---------------------------------------------------------------------------

class TraceState:
    """Accumulates span events and builds a renderable tree."""

    def __init__(self) -> None:
        self.spans: dict[str, ViewerSpan] = {}  # span_id → ViewerSpan
        self.trace_start_time: float | None = None
        self.trace_end_time: float | None = None
        self.trace_name: str = ""
        self.shutdown: bool = False

        # Per-event content keyed by (agent_id, event_type, index)
        self.events: dict[tuple[str, str, int], dict[str, Any]] = {}

    def handle_message(self, msg: dict) -> None:
        """Process a single TCP/JSONL message."""
        msg_type = msg.get("type") or msg.get("event", "")

        if msg_type in (MsgType.SPAN_START, MsgType.SPAN_END, MsgType.SPAN_UPDATE):
            span_dict = msg.get("span", msg)
            span_id = span_dict.get("span_id", "")
            if span_id in self.spans:
                # Update existing span
                s = self.spans[span_id]
                s.end_time = span_dict.get("end_time", s.end_time)
                s.error = span_dict.get("error", s.error)
                s.attributes.update(span_dict.get("attributes", {}))
            else:
                # New span
                s = ViewerSpan.from_dict(span_dict)
                self.spans[span_id] = s

            # Track pipeline start time from the earliest span
            if self.trace_start_time is None or s.start_time < self.trace_start_time:
                self.trace_start_time = s.start_time

        elif msg_type == MsgType.TRACE_START:
            self.trace_start_time = msg.get("start_time", self.trace_start_time)
            self.trace_name = msg.get("name", self.trace_name)

        elif msg_type == MsgType.TRACE_END:
            self.trace_end_time = msg.get("end_time", time.time())
            self.trace_name = msg.get("name", self.trace_name)

        elif msg_type in (MsgType.LLM_EVENT, MsgType.TOOL_EVENT,
                          MsgType.HITL_EVENT, MsgType.MEMORY_EVENT):
            agent_id = msg.get("agent_id", "")
            idx = msg.get("index", 0)
            event_data = msg.get("event", {})
            self.events[(agent_id, msg_type, idx)] = event_data

        elif msg_type == MsgType.SHUTDOWN:
            self.shutdown = True

    def build_tree(self) -> list[ViewerSpan]:
        """Build a tree of spans, returning root-level spans."""
        # Reset children
        for s in self.spans.values():
            s.children = []

        roots: list[ViewerSpan] = []
        for s in self.spans.values():
            if s.parent_id and s.parent_id in self.spans:
                self.spans[s.parent_id].children.append(s)
            else:
                roots.append(s)

        # Sort children by start_time
        def _sort_children(span: ViewerSpan) -> None:
            span.children.sort(key=lambda c: c.start_time)
            for child in span.children:
                _sort_children(child)

        roots.sort(key=lambda s: s.start_time)
        for r in roots:
            _sort_children(r)

        return roots

    @property
    def pipeline_end(self) -> float:
        """The end of the time axis — trace end or current time."""
        if self.trace_end_time:
            return self.trace_end_time
        # Use the latest known timestamp
        now = time.time()
        latest = now
        for s in self.spans.values():
            if s.end_time and s.end_time > latest:
                latest = s.end_time
        return latest

    @property
    def pipeline_start(self) -> float:
        """Start of the time axis."""
        return self.trace_start_time or 0.0
