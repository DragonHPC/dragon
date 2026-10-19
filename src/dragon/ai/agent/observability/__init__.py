"""Dragon Agent Observability — structured tracing for multi-agent pipelines.

Public API::

    from dragon.ai.agent.observability import (
        # Core types
        Span,
        SpanKind,
        Trace,
        TracingProcessor,
        NoOpProcessor,
        trace_span,
        truncate_content,
        get_current_span,
        get_current_trace_id,
        get_current_processor,
        set_current_processor,
        # Backends
        DictTracingProcessor,
    )
"""

from .trace_protocol import (
    SpanKind,
    MsgType,
    ATTR_LLM_ITERATION,
    ATTR_LLM_EVENT_INDEX,
    ATTR_TOOL_NAME,
    ATTR_TOOL_SOURCE,
    ATTR_TOOL_EVENT_INDEX,
    ATTR_HITL_TOOL_GATED,
    ATTR_HITL_APPROVED,
    ATTR_HITL_EVENT_INDEX,
    ATTR_AGENT_ID,
    EVT_INPUT_MESSAGES,
    EVT_OUTPUT,
    EVT_ARGUMENTS,
    EVT_RESULT,
    EVT_APPROVED,
    EVT_REASON,
)
from .tracer import (
    Span,
    Trace,
    TracingProcessor,
    NoOpProcessor,
    trace_span,
    truncate_content,
    get_current_span,
    get_current_trace_id,
    get_current_processor,
    set_current_processor,
)
from .ddict_tracer import DictTracingProcessor
from .trace_state import ViewerSpan, TraceState
from .trace_renderer import render_tree, redraw
from .trace_report import write_readable_report

__all__ = [
    "Span",
    "SpanKind",
    "MsgType",
    "Trace",
    "TracingProcessor",
    "NoOpProcessor",
    "trace_span",
    "truncate_content",
    "get_current_span",
    "get_current_trace_id",
    "get_current_processor",
    "set_current_processor",
    "DictTracingProcessor",
    # Viewer (client-side)
    "ViewerSpan",
    "TraceState",
    "render_tree",
    "redraw",
    "write_readable_report",
    # Span attribute key constants
    "ATTR_LLM_ITERATION",
    "ATTR_LLM_EVENT_INDEX",
    "ATTR_TOOL_NAME",
    "ATTR_TOOL_SOURCE",
    "ATTR_TOOL_EVENT_INDEX",
    "ATTR_HITL_TOOL_GATED",
    "ATTR_HITL_APPROVED",
    "ATTR_HITL_EVENT_INDEX",
    "ATTR_AGENT_ID",
    # Event payload field names
    "EVT_INPUT_MESSAGES",
    "EVT_OUTPUT",
    "EVT_ARGUMENTS",
    "EVT_RESULT",
    "EVT_APPROVED",
    "EVT_REASON",
]
