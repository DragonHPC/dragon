"""Wire-protocol constants shared between the Dragon runtime and trace viewer.

This module contains **only** stdlib imports (``enum``) and string constants.
It is the single source of truth for every identifier that crosses the
TCP bridge between the Dragon runtime (writers like ``TraceTcpBridge``,
``ToolDispatcher``, ``DictTracingProcessor``) and the standalone
``trace_viewer`` (reader).

**No Dragon dependencies** — safe to import from any environment.

Contents:

* :class:`SpanKind`  — classification of trace spans.
* :class:`MsgType`   — TCP/JSONL message type verbs.
* ``ATTR_*``         — span attribute key constants.
* ``EVT_*``          — per-event payload field name constants.
"""

from __future__ import annotations

from enum import Enum


# ---------------------------------------------------------------------------
# Span classification
# ---------------------------------------------------------------------------

class SpanKind(str, Enum):
    """Classification of trace span types.

    Inherits from ``str`` so values serialise directly to JSON strings.
    """

    PIPELINE      = "PIPELINE"
    TASK          = "TASK"
    LLM_CALL      = "LLM_CALL"
    TOOL_CALL     = "TOOL_CALL"
    HITL_APPROVAL = "HITL_APPROVAL"
    MEMORY_PRUNE     = "MEMORY_PRUNE"
    MEMORY_SUMMARIZE = "MEMORY_SUMMARIZE"


# ---------------------------------------------------------------------------
# Wire-protocol message types (TCP bridge ↔ trace viewer)
# ---------------------------------------------------------------------------

class MsgType(str, Enum):
    """Message type verbs for the TCP bridge ↔ trace viewer protocol.

    Inherits from ``str`` so values serialise directly to JSON strings.
    """

    SPAN_START  = "span_start"
    SPAN_END    = "span_end"
    SPAN_UPDATE = "span_update"
    TRACE_START = "trace_start"
    TRACE_END   = "trace_end"
    LLM_EVENT   = "llm_event"
    TOOL_EVENT  = "tool_event"
    HITL_EVENT   = "hitl_event"
    MEMORY_EVENT = "memory_event"
    SHUTDOWN     = "shutdown"


# ---------------------------------------------------------------------------
# Span attribute keys — used in Span.attributes dicts.
# Writers: ToolDispatcher, SubAgent.  Readers: trace_viewer.
# ---------------------------------------------------------------------------

# LLM call spans
ATTR_LLM_ITERATION   = "llm.iteration"
ATTR_LLM_EVENT_INDEX = "llm.event_index"

# Tool call spans
ATTR_TOOL_NAME        = "tool.name"
ATTR_TOOL_SOURCE      = "tool.source"
ATTR_TOOL_EVENT_INDEX = "tool.event_index"

# HITL approval spans
ATTR_HITL_TOOL_GATED  = "hitl.tool_gated"
ATTR_HITL_APPROVED    = "hitl.approved"
ATTR_HITL_EVENT_INDEX = "hitl.event_index"

# Agent / task spans
ATTR_AGENT_ID    = "agent.id"
ATTR_DISPATCH_ID = "dispatch.id"

# Memory management spans
ATTR_MEMORY_EVENT_INDEX      = "memory.event_index"
ATTR_MEMORY_TURNS_PRUNED     = "memory.turns_pruned"
ATTR_MEMORY_TURNS_BEFORE     = "memory.turns_before"
ATTR_MEMORY_TURNS_KEPT       = "memory.turns_kept"
ATTR_MEMORY_TURNS_SUMMARIZED = "memory.turns_summarized"
ATTR_MEMORY_SUMMARY_CHARS    = "memory.summary_chars"
ATTR_MEMORY_STRATEGY         = "memory.strategy"


# ---------------------------------------------------------------------------
# Event payload field names — keys inside the per-event dicts written to
# DDict by ToolDispatcher and read by trace_viewer.
# ---------------------------------------------------------------------------

# LLM event fields
EVT_INPUT_MESSAGES = "input_messages"
EVT_OUTPUT         = "output"

# Tool event fields
EVT_ARGUMENTS = "arguments"
EVT_RESULT    = "result"

# HITL event fields
EVT_APPROVED = "approved"
EVT_REASON   = "reason"

# Memory event fields (stored in DDict event, keyed by MEMORY_EVENT_KEY)
EVT_MEMORY_INPUT  = "input"     # messages fed to the summarizer
EVT_MEMORY_OUTPUT = "output"    # resulting summary text
