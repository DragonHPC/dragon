"""Core tracing abstractions for Dragon agent observability.

This module provides the foundational types and protocol for structured
tracing across the Dragon agent framework: a minimal 2-method processor
protocol with automatic parent tracking via :mod:`contextvars`.

Key types:

* :class:`SpanKind` — classification of trace spans.
* :class:`Span` — a single timed operation (task, LLM call, tool call, etc.).
* :class:`Trace` — top-level container for a pipeline run.
* :class:`TracingProcessor` — abstract interface for trace backends.
* :class:`NoOpProcessor` — default no-op implementation (zero overhead).
* :func:`trace_span` — async context manager for instrumentation.
"""

from __future__ import annotations

import time
import uuid
from contextvars import ContextVar
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from typing import Any, AsyncIterator

from .trace_protocol import SpanKind


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Span:
    """A single timed operation in the trace tree.

    Attributes
    ----------
    span_id:
        Unique identifier for this span (auto-generated uuid4).
    trace_id:
        Shared identifier across all spans in a pipeline run.
    parent_id:
        ``span_id`` of the parent span, or ``None`` for root spans.
        Automatically set by :func:`trace_span` via ``_current_span`` ContextVar.
    name:
        Human-readable label (e.g. agent name, model name, tool name).
    kind:
        Classification of this span (:class:`SpanKind`).
    start_time:
        Unix timestamp when the span started.
    end_time:
        Unix timestamp when the span ended, or ``None`` while in-progress.
    attributes:
        Structured data captured during the span.  Uses namespaced keys:
        ``llm.model``, ``tool.name``, ``hitl.approved``, ``agent.id``, etc.
    error:
        Exception string if the span failed, or ``None``.
    """

    span_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    trace_id: str = ""
    parent_id: str | None = None
    name: str = ""
    kind: SpanKind = SpanKind.TASK
    start_time: float = field(default_factory=time.time)
    end_time: float | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict (JSON-safe)."""
        d = asdict(self)
        d["kind"] = self.kind.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Span:
        """Reconstruct a Span from a dict (e.g. from JSON)."""
        d = dict(d)  # shallow copy
        if "kind" in d and isinstance(d["kind"], str):
            d["kind"] = SpanKind(d["kind"])
        return cls(**d)


@dataclass
class Trace:
    """Top-level container for a pipeline run trace.

    Attributes
    ----------
    trace_id:
        Shared identifier across all spans in this run.
    name:
        Human-readable label (e.g. pipeline name, task description).
    start_time:
        Unix timestamp when the pipeline started.
    end_time:
        Unix timestamp when the pipeline ended, or ``None`` while running.
    """

    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    start_time: float = field(default_factory=time.time)
    end_time: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict (JSON-safe)."""
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Trace:
        """Reconstruct a Trace from a dict."""
        return cls(**d)


# ---------------------------------------------------------------------------
# Tracing processor protocol
# ---------------------------------------------------------------------------

class TracingProcessor:
    """Abstract base for trace backends.

    Subclasses must implement both methods.  Each method receives a
    :class:`Span` instance and should handle it synchronously (the caller
    awaits if needed).
    """

    def on_span_start(self, span: Span) -> None:
        """Called when a span starts (``end_time`` is ``None``)."""
        raise NotImplementedError

    def on_span_end(self, span: Span) -> None:
        """Called when a span ends (``end_time`` is set)."""
        raise NotImplementedError


class NoOpProcessor(TracingProcessor):
    """Default no-op processor.  All methods are pass — zero overhead."""

    def on_span_start(self, span: Span) -> None:
        pass

    def on_span_end(self, span: Span) -> None:
        pass


# ---------------------------------------------------------------------------
# ContextVar for automatic parent span tracking
# ---------------------------------------------------------------------------

_current_span: ContextVar[Span | None] = ContextVar("_current_span", default=None)

# Also expose trace_id at ContextVar level so non-span code can read it
_current_trace_id: ContextVar[str] = ContextVar("_current_trace_id", default="")


# Task-local processor — set once per async task in _handle_message().
# trace_span() auto-resolves the processor from this ContextVar when the
# explicit processor argument is None.  This avoids threading tracers
# through every call site and ensures concurrent tasks in the same agent
# each use the correct backend (e.g. per-task DictTracingProcessor).
_current_processor: ContextVar[TracingProcessor | None] = ContextVar(
    "_current_processor", default=None
)


def get_current_span() -> Span | None:
    """Return the currently active span, or ``None``."""
    return _current_span.get()


def get_current_trace_id() -> str:
    """Return the current trace_id, or empty string."""
    return _current_trace_id.get()


def get_current_processor() -> TracingProcessor | None:
    """Return the processor set for the current async task, or ``None``."""
    return _current_processor.get()


def set_current_processor(processor: TracingProcessor | None) -> Any:
    """Set the processor for the current async task.

    Returns a token that can be passed to ``_current_processor.reset()``
    to restore the previous value.
    """
    return _current_processor.set(processor)


# ---------------------------------------------------------------------------
# trace_span — async context manager for instrumentation
# ---------------------------------------------------------------------------

@asynccontextmanager
async def trace_span(
    processor: TracingProcessor | None,
    name: str,
    kind: SpanKind,
    attributes: dict[str, Any] | None = None,
    trace_id: str | None = None,
) -> AsyncIterator[Span]:
    """Async context manager that creates, starts, and ends a trace span.

    Usage::

        async with trace_span(tracer, "gpt-4o", SpanKind.LLM_CALL, {"llm.model": "gpt-4o"}) as span:
            result = await llm.chat(messages)
            span.attributes["llm.output"] = result

    Lifecycle — what happens on enter and exit::

        ENTER (before ``yield``):
          1. Resolve the active processor from ``_current_processor`` ContextVar
             (falls back to ``_noop`` if tracing is off).
          2. Read the current parent span from ``_current_span`` ContextVar.
          3. Create a new ``Span`` with ``parent_id = parent.span_id``
             (this is how parent-child linking works — see below).
          4. Call ``processor.on_span_start(span)``.
          5. Push this span as the new ``_current_span`` (so any nested
             ``trace_span`` calls will see it as their parent).
          6. ``yield span`` — the caller's code runs.

        EXIT (after ``yield``):
          7. Record ``span.end_time``.
          8. Call ``processor.on_span_end(span)``.
          9. Reset ``_current_span`` back to the previous value (the parent).

    Parent-child linking uses the ``_current_span`` ContextVar as an
    implicit call stack.  Nesting ``trace_span`` calls automatically
    builds the tree::

        trace_span("task")            → _current_span = task_span  (parent_id=None)
          └─ trace_span("llm")        → _current_span = llm_span   (parent_id=task_span.id)
               └─ trace_span("tool")  → _current_span = tool_span  (parent_id=llm_span.id)

    This is the same pattern used by distributed tracing systems.  No explicit wiring
    is needed — the ContextVar acts as the span stack.

    Parameters
    ----------
    processor:
        The tracing processor to notify.  If ``None``, the processor is
        resolved from the ``_current_processor`` ContextVar; if that is
        also ``None``, a :class:`NoOpProcessor` is used (the span object
        is still created for structural correctness but no backend I/O
        occurs).
    name:
        Human-readable label for this span.
    kind:
        Classification of this span.
    attributes:
        Initial attributes dict.  The caller can add more attributes on the
        yielded ``span`` object during execution.
    trace_id:
        Override trace_id.  If ``None``, inherits from the parent span or
        the ``_current_trace_id`` ContextVar.
    """
    # 1. Resolve processor: explicit arg → ContextVar → no-op fallback
    if processor is None:
        processor = _current_processor.get() or _noop

    # 2. Read parent span from ContextVar (None if this is a root span)
    parent = _current_span.get()
    resolved_trace_id = trace_id or (parent.trace_id if parent else _current_trace_id.get()) or str(uuid.uuid4())

    # 3. Create span, linking to parent via parent_id
    span = Span(
        trace_id=resolved_trace_id,
        parent_id=parent.span_id if parent else None,
        name=name,
        kind=kind,
        attributes=dict(attributes) if attributes else {},
    )

    # 4–5. Notify processor and push this span as _current_span
    processor.on_span_start(span)
    token = _current_span.set(span)
    try:
        # 6. Yield — caller's code executes here
        yield span
        # 7–8. Record end time and notify processor
        span.end_time = time.time()
        processor.on_span_end(span)
    except Exception as exc:
        span.error = f"{type(exc).__name__}: {exc}"
        span.end_time = time.time()
        processor.on_span_end(span)
        raise
    finally:
        # 9. Pop back to parent span
        _current_span.reset(token)


# Module-level singleton
_noop = NoOpProcessor()


# ---------------------------------------------------------------------------
# Content truncation helper
# ---------------------------------------------------------------------------

DEFAULT_MAX_CONTENT_SIZE = 2048  # 2KB


def truncate_content(value: Any, max_size: int = DEFAULT_MAX_CONTENT_SIZE) -> str:
    """Convert *value* to a string, truncating to *max_size* characters."""
    s = str(value) if not isinstance(value, str) else value
    if len(s) <= max_size:
        return s
    return s[:max_size] + f"... [truncated, {len(s)} total chars]"
