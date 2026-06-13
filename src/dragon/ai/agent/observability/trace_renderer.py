"""Trace tree renderer — Gantt bars, timing columns, and content display.

Pure data→string transformation functions.  No I/O — callers handle
printing or passing the rendered output to curses.
"""

from __future__ import annotations

import json
import time
from statistics import median
from typing import Any

from ..utils.ansi import Color as _Color, colorize as _colorize, visible_len as _visible_len
from .trace_protocol import (
    SpanKind,
    MsgType,
    ATTR_LLM_EVENT_INDEX, ATTR_TOOL_EVENT_INDEX, ATTR_HITL_EVENT_INDEX,
    ATTR_MEMORY_EVENT_INDEX,
    ATTR_HITL_APPROVED, ATTR_HITL_TOOL_GATED, ATTR_AGENT_ID, ATTR_DISPATCH_ID,
    ATTR_MEMORY_TURNS_PRUNED, ATTR_MEMORY_TURNS_BEFORE, ATTR_MEMORY_TURNS_KEPT,
    ATTR_MEMORY_TURNS_SUMMARIZED, ATTR_MEMORY_SUMMARY_CHARS,
    ATTR_MEMORY_STRATEGY,
    EVT_INPUT_MESSAGES, EVT_OUTPUT,
    EVT_ARGUMENTS, EVT_RESULT,
    EVT_APPROVED, EVT_REASON,
    EVT_MEMORY_INPUT, EVT_MEMORY_OUTPUT,
)
from .trace_state import TraceState, ViewerSpan

__all__ = [
    "GANTT_WIDTH",
    "render_tree",
    "redraw",
]


# ---------------------------------------------------------------------------
# Renderer constants & helpers
# ---------------------------------------------------------------------------

GANTT_WIDTH = 20


def _fmt_time(t: float | None, base: float) -> str:
    """Format a timestamp as relative seconds from base."""
    if t is None:
        return "—"
    return f"{t - base:.1f}s"


def _fmt_dur(dur: float | None) -> str:
    """Format a duration."""
    if dur is None:
        return "—"
    return f"{dur:.1f}s"


def _gantt_bar(
    start: float, end: float | None, pipeline_start: float, pipeline_end: float,
    is_error: bool = False,
) -> str:
    """Render a fixed-width Gantt bar."""
    total = pipeline_end - pipeline_start
    if total <= 0:
        return "█" * GANTT_WIDTH

    rel_start = (start - pipeline_start) / total
    if end is not None:
        rel_end = (end - pipeline_start) / total
        in_progress = False
    else:
        rel_end = (time.time() - pipeline_start) / total
        in_progress = True

    # Clamp
    rel_start = max(0.0, min(1.0, rel_start))
    rel_end = max(0.0, min(1.0, rel_end))

    bar_start = int(rel_start * GANTT_WIDTH)
    bar_end = int(rel_end * GANTT_WIDTH)
    bar_end = max(bar_end, bar_start + 1)  # at least 1 char wide
    bar_end = min(bar_end, GANTT_WIDTH)

    chars = []
    for i in range(GANTT_WIDTH):
        if i < bar_start:
            chars.append("░")
        elif i < bar_end:
            if in_progress and i >= bar_end - 1:
                chars.append("▸")
            else:
                chars.append("█")
        else:
            chars.append("░")

    bar_str = "".join(chars)

    # Apply colour
    if is_error:
        return _colorize(bar_str, _Color.BOLD_RED)
    elif in_progress:
        return _colorize(bar_str, _Color.CYAN)
    return bar_str


def _color_duration(dur: float | None, sibling_durs: list[float], error: str | None) -> str:
    """Colour-code a duration string based on sibling percentiles."""
    if error:
        return _colorize(_fmt_dur(dur), _Color.BOLD_RED) + " " + _colorize("✗", _Color.BOLD_RED)
    if dur is None:
        return _colorize("—", _Color.CYAN) + " " + _colorize("⏳", _Color.CYAN)

    text = _fmt_dur(dur)

    if not sibling_durs or len(sibling_durs) < 2:
        return text + " ✓"

    sorted_durs = sorted(sibling_durs)
    n = len(sorted_durs)
    p25 = sorted_durs[max(0, n // 4)]
    p75 = sorted_durs[min(n - 1, 3 * n // 4)]
    med = median(sorted_durs)

    if dur > 2 * med:
        return _colorize(text, _Color.RED) + " ✓"
    elif dur > p75:
        return _colorize(text, _Color.YELLOW) + " ✓"
    elif dur < p25:
        return _colorize(text, _Color.GREEN) + " ✓"
    else:
        return text + " ✓"


def _collect_sibling_durations(children: list[ViewerSpan]) -> list[float]:
    """Collect durations of completed sibling spans."""
    return [c.duration for c in children if c.duration is not None]


# ---------------------------------------------------------------------------
# Tree rendering
# ---------------------------------------------------------------------------

def render_tree(
    state: TraceState,
    verbose: bool = False,
    full: bool = False,
    term_width: int = 120,
) -> str:
    """Render the trace tree as a formatted string."""
    roots = state.build_tree()
    if not roots:
        return _colorize("  (no spans received yet)", _Color.DIM)

    pstart = state.pipeline_start
    pend = state.pipeline_end

    # Collect all rows first to determine max label width
    rows: list[dict] = []
    _flatten_tree(roots, rows, pstart, pend, depth=0, prefix="", is_last=True,
                  verbose=verbose, full=full, term_width=term_width,
                  events=state.events, all_spans=state.spans)

    if not rows:
        return ""

    # Determine max label width from SPAN rows only (not content lines).
    span_labels = [r["label"] for r in rows if not r.get("is_content_line")]
    max_label_len = max(
        (_visible_len(lbl) for lbl in span_labels),
        default=20,
    )
    max_label_len = max(max_label_len, 20)

    # Header
    header_label = "TREE"
    header = f"{header_label:<{max_label_len}s}  {'START':>6s} {'END':>6s} {'DUR':>8s}  {'GANTT'}"
    sep = "─" * len(header)

    lines = [_colorize(header, _Color.DIM), _colorize(sep, _Color.DIM)]

    for r in rows:
        if r.get("is_content_line"):
            lines.append(r["label"])
        else:
            visible = _visible_len(r["label"])
            padding = max_label_len - visible
            line = (
                f"{r['label']}{' ' * padding}  "
                f"{r['start']:>6s} "
                f"{r['end']:>6s} "
                f"{r['dur']:>8s}  "
                f"{r['gantt']}"
            )
            lines.append(line)

    return "\n".join(lines)


def _flatten_tree(
    spans: list[ViewerSpan],
    rows: list[dict],
    pstart: float,
    pend: float,
    depth: int,
    prefix: str,
    is_last: bool,
    verbose: bool,
    full: bool,
    term_width: int,
    parent_children: list[ViewerSpan] | None = None,
    events: dict[tuple[str, str, int], dict[str, Any]] | None = None,
    all_spans: dict[str, ViewerSpan] | None = None,
) -> None:
    """Recursively flatten the span tree into rows."""
    sibling_durs = (_collect_sibling_durations(spans) if parent_children is None
                    else _collect_sibling_durations(parent_children or spans))

    for i, span in enumerate(spans):
        is_span_last = (i == len(spans) - 1)

        # Build tree prefix
        if depth == 0:
            if len(spans) == 1:
                connector = "┌ "
            elif i == 0:
                connector = "┌ "
            elif is_span_last:
                connector = "└─ "
            else:
                connector = "├─ "
            child_prefix = "│  " if not is_span_last else "   "
        else:
            if is_span_last:
                connector = prefix + "└─ "
                child_prefix = prefix + "   "
            else:
                connector = prefix + "├─ "
                child_prefix = prefix + "│  "

        label = f"{connector}{span.kind} {span.name}"
        # Append short dispatch_id for TASK spans
        did = span.attributes.get(ATTR_DISPATCH_ID, "")
        if did:
            label += f" [{did[:8]}]"

        # Time columns
        start_str = _fmt_time(span.start_time, pstart)
        end_str = _fmt_time(span.end_time, pstart)
        dur_str = _color_duration(span.duration, sibling_durs, span.error)
        gantt = _gantt_bar(span.start_time, span.end_time, pstart, pend,
                           is_error=bool(span.error))

        rows.append({
            "label": label,
            "start": start_str,
            "end": end_str,
            "dur": dur_str,
            "gantt": gantt,
        })

        # Verbose content lines
        if verbose and (span.attributes or events):
            content_prefix = child_prefix + "┊ "
            _append_content_lines(rows, span, content_prefix, full, term_width,
                                  events=events, all_spans=all_spans)

        # Recurse into children
        if span.children:
            _flatten_tree(
                span.children, rows, pstart, pend,
                depth=depth + 1,
                prefix=child_prefix,
                is_last=is_span_last,
                verbose=verbose,
                full=full,
                term_width=term_width,
                parent_children=span.children,
                events=events,
                all_spans=all_spans,
            )


# ---------------------------------------------------------------------------
# Verbose content lines per span kind
# ---------------------------------------------------------------------------

def _append_content_lines(
    rows: list[dict],
    span: ViewerSpan,
    prefix: str,
    full: bool,
    term_width: int,
    events: dict[tuple[str, str, int], dict[str, Any]] | None = None,
    all_spans: dict[str, ViewerSpan] | None = None,
) -> None:
    """Append verbose content lines for a span."""
    attrs = span.attributes
    wrap_width = max(30, term_width - _visible_len(prefix) - 20)
    events = events or {}
    all_spans = all_spans or {}

    def _format_content(text: str) -> str:
        raw = str(text)
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        if not lines:
            return ""

        result: list[str] = []
        for ln in lines:
            if len(ln) <= wrap_width:
                result.append(ln)
            elif full:
                while len(ln) > wrap_width:
                    result.append(ln[:wrap_width])
                    ln = ln[wrap_width:]
                if ln:
                    result.append(ln)
            else:
                result.append(ln[:wrap_width] + "…")

        continuation = "\n" + prefix + "      "
        return continuation.join(result)

    def _resolve_agent_id() -> str:
        aid = attrs.get(ATTR_AGENT_ID, "")
        if aid:
            return aid
        parent_id = span.parent_id
        while parent_id and parent_id in all_spans:
            parent = all_spans[parent_id]
            aid = parent.attributes.get(ATTR_AGENT_ID, "")
            if aid:
                return aid
            parent_id = parent.parent_id
        return ""

    def _get_event(event_type: str, index_attr: str) -> dict[str, Any] | None:
        agent_id = _resolve_agent_id()
        idx = attrs.get(index_attr)
        if idx is not None and agent_id:
            return events.get((agent_id, event_type, int(idx)))
        return None

    if span.kind == SpanKind.LLM_CALL:
        ev = _get_event(MsgType.LLM_EVENT, ATTR_LLM_EVENT_INDEX)
        messages = (ev or {}).get(EVT_INPUT_MESSAGES, [])
        output = (ev or {}).get(EVT_OUTPUT, "")

        if messages:
            omitted_tool = (ev or {}).get("omitted_tool_messages", 0)
            total_tool = (ev or {}).get("total_tool_messages")
            if omitted_tool > 0 and total_tool is not None:
                rows.append({"label": f"{prefix}{_colorize('IN:', _Color.DIM)}  "
                                      f"[... {omitted_tool} earlier tool result(s) not shown, "
                                      f"{total_tool} total tool result(s) fed to LLM]",
                             "is_content_line": True})
            for msg in messages:
                role = msg.get("role", "?")
                content = _format_content(msg.get("content", ""))
                rows.append({"label": f"{prefix}{_colorize('IN:', _Color.DIM)}  [{role}] {content}",
                             "is_content_line": True})

        if output:
            rows.append({"label": f"{prefix}{_colorize('OUT:', _Color.DIM)} {_format_content(output)}",
                         "is_content_line": True})

    elif span.kind == SpanKind.TOOL_CALL:
        ev = _get_event(MsgType.TOOL_EVENT, ATTR_TOOL_EVENT_INDEX)
        args = (ev or {}).get(EVT_ARGUMENTS, {})
        result = (ev or {}).get(EVT_RESULT, "")

        if args:
            args_str = _format_content(json.dumps(args, default=str))
            rows.append({"label": f"{prefix}{_colorize('ARG:', _Color.DIM)} {args_str}",
                         "is_content_line": True})

        if result:
            rows.append({"label": f"{prefix}{_colorize('RES:', _Color.DIM)} {_format_content(result)}",
                         "is_content_line": True})

    elif span.kind == SpanKind.HITL_APPROVAL:
        ev = _get_event(MsgType.HITL_EVENT, ATTR_HITL_EVENT_INDEX)
        approved = (ev or {}).get(EVT_APPROVED)
        reason = (ev or {}).get(EVT_REASON, "")
        tool = attrs.get(ATTR_HITL_TOOL_GATED, "?")
        if approved is None:
            status = _colorize("PENDING", _Color.CYAN)
        elif approved:
            status = _colorize("APPROVED", _Color.GREEN)
        else:
            status = _colorize("REJECTED", _Color.RED)
        rows.append({"label": f"{prefix}Tool: {tool} → {status}", "is_content_line": True})
        if reason:
            rows.append({"label": f"{prefix}Reason: {_format_content(reason)}",
                         "is_content_line": True})

    elif span.kind == SpanKind.TASK:
        pass

    elif span.kind == SpanKind.MEMORY_SUMMARIZE:
        evt = _get_event(MsgType.MEMORY_EVENT, ATTR_MEMORY_EVENT_INDEX)
        turns_n = attrs.get(ATTR_MEMORY_TURNS_SUMMARIZED, "?")
        summary_chars = attrs.get(ATTR_MEMORY_SUMMARY_CHARS, "?")
        rows.append({"label": f"{prefix}{_colorize('SUMMARIZED:', _Color.DIM)} "
                              f"{turns_n} turn(s) → {summary_chars} chars",
                     "is_content_line": True})
        if evt:
            if evt.get("truncated"):
                zone_n = evt.get("zone_c_messages", "?")
                tool_lim = evt.get("summarizer_max_tool_chars", "?")
                cont_lim = evt.get("summarizer_max_content_chars", "?")
                rows.append({"label": f"{prefix}{_colorize('⚠ INPUT TRUNCATED:', _Color.YELLOW)} "
                                      f"some of {zone_n} messages were clipped "
                                      f"(tool results >{tool_lim} chars, content >{cont_lim} chars)",
                             "is_content_line": True})
            mem_in = evt.get(EVT_MEMORY_INPUT, "")
            mem_out = evt.get(EVT_MEMORY_OUTPUT, "")
            if mem_in:
                rows.append({"label": f"{prefix}{_colorize('IN:', _Color.DIM)}  {_format_content(mem_in)}",
                             "is_content_line": True})
            if mem_out:
                rows.append({"label": f"{prefix}{_colorize('OUT:', _Color.DIM)} {_format_content(mem_out)}",
                             "is_content_line": True})

    elif span.kind == SpanKind.MEMORY_PRUNE:
        strategy = attrs.get(ATTR_MEMORY_STRATEGY, "?")
        turns_n = attrs.get(ATTR_MEMORY_TURNS_PRUNED, "?")
        turns_before = attrs.get(ATTR_MEMORY_TURNS_BEFORE)
        kept = attrs.get(ATTR_MEMORY_TURNS_KEPT, "?")
        if turns_before is not None:
            rows.append({"label": f"{prefix}{_colorize('PRUNED:', _Color.DIM)} "
                                  f"{turns_n} turn(s) removed ({turns_before} → {kept}), {kept} kept",
                         "is_content_line": True})
        else:
            rows.append({"label": f"{prefix}{_colorize('PRUNED:', _Color.DIM)} "
                                  f"{turns_n} turn(s) removed, {kept} kept",
                         "is_content_line": True})

    # Error
    if span.error:
        rows.append({"label": f"{prefix}{_colorize('ERR:', _Color.BOLD_RED)} {_format_content(span.error)}",
                     "is_content_line": True})


# ---------------------------------------------------------------------------
# Screen redraw helper (non-interactive streaming mode)
# ---------------------------------------------------------------------------

def redraw(state: TraceState, verbose: bool, full: bool, term_width: int) -> None:
    """Clear screen and render the current trace tree."""
    print("\033[2J\033[H", end="")

    title = state.trace_name or "Dragon Agent Trace"
    print(_colorize(f"  {title}", _Color.BOLD))
    print()

    output = render_tree(state, verbose=verbose, full=full, term_width=term_width)
    print(output)
    print()

    # Summary line
    total = len(state.spans)
    completed = sum(1 for s in state.spans.values() if s.is_complete)
    errors = sum(1 for s in state.spans.values() if s.error)
    in_progress = total - completed

    parts = [f"  Spans: {total}"]
    if completed:
        parts.append(_colorize(f"✓ {completed}", _Color.GREEN))
    if in_progress:
        parts.append(_colorize(f"⏳ {in_progress}", _Color.CYAN))
    if errors:
        parts.append(_colorize(f"✗ {errors}", _Color.RED))

    print("  ".join(parts))
