"""DDict event-write helpers for the reasoning loop.

Each function guards on ``tracing`` + ``accessor is not None`` so the call
sites in :mod:`tool_dispatcher` stay clean — callers just fire-and-forget.
"""

from __future__ import annotations

from typing import Any

from ..config import (
    LLM_EVENT_KEY, LLM_EVENT_COUNT_KEY,
    TOOL_EVENT_KEY, TOOL_EVENT_COUNT_KEY,
    HITL_EVENT_KEY, HITL_EVENT_COUNT_KEY,
    MEMORY_EVENT_KEY, MEMORY_EVENT_COUNT_KEY,
)
from ..observability.trace_protocol import (
    EVT_INPUT_MESSAGES, EVT_OUTPUT,
    EVT_ARGUMENTS, EVT_RESULT,
    EVT_APPROVED, EVT_REASON,
    EVT_MEMORY_INPUT, EVT_MEMORY_OUTPUT,
)
from ..ddict import DDictAccessor


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def write_llm_event(
    accessor: DDictAccessor | None,
    tracing: bool,
    fmt: dict[str, str],
    iteration: int,
    copy_prompts: list[dict[str, Any]],
    output_text: str,
    event_idx: int,
) -> None:
    """Write an LLM call event to DDict (last 2 tool messages + raw output).

    Filters out non-tool messages so trace viewers see only tool results.
    The omitted count reflects tool-role messages not shown but fed to the LLM.
    """
    if not tracing or accessor is None:
        return
    _max_shown = 2
    # Collect only tool-role messages for display
    _tool_msgs = [m for m in copy_prompts if m.get("role") == "tool"]
    _total_tool = len(_tool_msgs)
    _shown = _tool_msgs[-_max_shown:] if _total_tool > _max_shown else _tool_msgs
    _omitted_tool = max(0, _total_tool - _max_shown)
    llm_event = {
        "iteration": iteration,
        EVT_INPUT_MESSAGES: [
            {"role": m.get("role", "?"), "content": m.get("content", "")}
            for m in _shown
        ],
        "total_messages": len(copy_prompts),
        "total_tool_messages": _total_tool,
        "omitted_tool_messages": _omitted_tool,
        EVT_OUTPUT: output_text,
    }
    accessor.write_event(
        LLM_EVENT_KEY, LLM_EVENT_COUNT_KEY,
        llm_event, event_idx, **fmt,
    )


def write_tool_event(
    accessor: DDictAccessor | None,
    tracing: bool,
    fmt: dict[str, str],
    name: str,
    args: dict,
    tool_answer: Any,
    tool_source: str,
    tool_call_id: str,
    event_idx: int,
) -> None:
    """Write a tool-execution event to DDict."""
    if not tracing or accessor is None:
        return
    tool_event = {
        "name": name,
        EVT_ARGUMENTS: args,
        EVT_RESULT: tool_answer,
        "source": tool_source,
        "tool_call_id": tool_call_id,
    }
    accessor.write_event(
        TOOL_EVENT_KEY, TOOL_EVENT_COUNT_KEY,
        tool_event, event_idx, **fmt,
    )


def write_hitl_event(
    accessor: DDictAccessor | None,
    tracing: bool,
    fmt: dict[str, str],
    name: str,
    args: dict,
    response: Any,
    event_idx: int,
) -> None:
    """Write a HITL approval event to DDict."""
    if not tracing or accessor is None:
        return
    hitl_event = {
        "tool_name": name,
        "tool_args": args,
        EVT_APPROVED: response.approved,
        EVT_REASON: getattr(response, "reason", ""),
        "is_feedback": getattr(response, "is_feedback", False),
    }
    accessor.write_event(
        HITL_EVENT_KEY, HITL_EVENT_COUNT_KEY,
        hitl_event, event_idx, **fmt,
    )


def write_memory_event(
    accessor: DDictAccessor | None,
    tracing: bool,
    fmt: dict[str, str],
    summ_result: dict[str, Any],
    event_idx: int,
) -> None:
    """Write a memory-summarization event to DDict."""
    if not tracing or accessor is None:
        return
    memory_event = {
        EVT_MEMORY_INPUT: summ_result["input"],
        EVT_MEMORY_OUTPUT: summ_result["output"],
        "truncated": summ_result.get("truncated", False),
        "zone_c_messages": summ_result.get("zone_c_messages", 0),
        "summarizer_max_tool_chars": summ_result.get("summarizer_max_tool_chars"),
        "summarizer_max_content_chars": summ_result.get("summarizer_max_content_chars"),
    }
    accessor.write_event(
        MEMORY_EVENT_KEY, MEMORY_EVENT_COUNT_KEY,
        memory_event, event_idx, **fmt,
    )
