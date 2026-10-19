"""Interactive curses-based trace viewer TUI.

Provides the keyboard-navigable span tree with expand/collapse and
scrolling.
"""

from __future__ import annotations

import curses
import threading
from dataclasses import dataclass
from typing import Any

from ..utils.ansi import Color as _Color, visible_len as _visible_len, strip_ansi as _strip_ansi
from .trace_renderer import (
    _fmt_time,
    _fmt_dur,
    _gantt_bar,
    _color_duration,
    _collect_sibling_durations,
    _append_content_lines,
)
from .trace_protocol import ATTR_DISPATCH_ID
from .trace_state import TraceState, ViewerSpan

__all__ = ["curses_main", "build_interactive_rows", "InteractiveRow"]


# ---------------------------------------------------------------------------
# Row model for interactive display
# ---------------------------------------------------------------------------

@dataclass
class InteractiveRow:
    """One row in the interactive display — either a span or content line."""

    text: str            # rendered text (may contain ANSI)
    is_span: bool        # True for span rows, False for content lines
    span_id: str = ""    # the owning span's id (for expand/collapse)
    depth: int = 0       # tree depth (for indentation tracking)


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------

def build_interactive_rows(
    state: TraceState,
    expanded: set[str],
    term_width: int,
) -> list[InteractiveRow]:
    """Build the flat row list for curses, expanding only selected spans."""
    roots = state.build_tree()
    if not roots:
        return [InteractiveRow(text="  (no spans received yet)", is_span=False)]

    pstart = state.pipeline_start
    pend = state.pipeline_end
    rows: list[InteractiveRow] = []

    def _walk(
        spans: list[ViewerSpan],
        depth: int,
        prefix: str,
        parent_children: list[ViewerSpan] | None = None,
    ) -> None:
        sibling_durs = _collect_sibling_durations(
            parent_children if parent_children is not None else spans
        )
        for i, span in enumerate(spans):
            is_last = (i == len(spans) - 1)

            if depth == 0:
                if len(spans) == 1 or i == 0:
                    connector = "┌ "
                elif is_last:
                    connector = "└─ "
                else:
                    connector = "├─ "
                child_prefix = "│  " if not is_last else "   "
            else:
                if is_last:
                    connector = prefix + "└─ "
                    child_prefix = prefix + "   "
                else:
                    connector = prefix + "├─ "
                    child_prefix = prefix + "│  "

            label = f"{connector}{span.kind} {span.name}"
            did = span.attributes.get(ATTR_DISPATCH_ID, "")
            if did:
                label += f" [{did[:8]}]"
            start_str = _fmt_time(span.start_time, pstart)
            end_str = _fmt_time(span.end_time, pstart)
            dur_str = _color_duration(span.duration, sibling_durs, span.error)
            gantt = _gantt_bar(span.start_time, span.end_time, pstart, pend,
                               is_error=bool(span.error))

            row = InteractiveRow(
                text="",  # filled during render pass
                is_span=True,
                span_id=span.span_id,
                depth=depth,
            )
            # Stash rendering components for column alignment
            row._label = label           # type: ignore[attr-defined]
            row._start = start_str       # type: ignore[attr-defined]
            row._end = end_str           # type: ignore[attr-defined]
            row._dur = dur_str           # type: ignore[attr-defined]
            row._gantt = gantt           # type: ignore[attr-defined]
            rows.append(row)

            # Expanded content
            if span.span_id in expanded:
                content_prefix = child_prefix + "┊ "
                content_rows: list[dict] = []
                _append_content_lines(
                    content_rows, span, content_prefix,
                    full=True, term_width=999999,
                    events=state.events, all_spans=state.spans,
                )
                for cr in content_rows:
                    rows.append(InteractiveRow(
                        text=cr["label"],
                        is_span=False,
                        span_id=span.span_id,
                        depth=depth,
                    ))

            if span.children:
                _walk(span.children, depth + 1, child_prefix, span.children)

    _walk(roots, 0, "")

    # Column alignment pass
    span_rows = [r for r in rows if r.is_span]
    max_label_len = max(
        (_visible_len(r._label) for r in span_rows),   # type: ignore[attr-defined]
        default=20,
    )
    max_label_len = max(max_label_len, 20)

    for r in rows:
        if r.is_span:
            visible = _visible_len(r._label)             # type: ignore[attr-defined]
            padding = max_label_len - visible
            r.text = (
                f"{r._label}{' ' * padding}  "           # type: ignore[attr-defined]
                f"{r._start:>6s} "                       # type: ignore[attr-defined]
                f"{r._end:>6s} "                         # type: ignore[attr-defined]
                f"{r._dur:>8s}  "                        # type: ignore[attr-defined]
                f"{r._gantt}"                            # type: ignore[attr-defined]
            )

    return rows


# ---------------------------------------------------------------------------
# Curses main loop
# ---------------------------------------------------------------------------

def curses_main(stdscr: curses.window, state: TraceState, stop_event: threading.Event) -> None:
    """Main curses loop for interactive mode."""
    curses.curs_set(0)
    curses.use_default_colors()

    curses.init_pair(1, curses.COLOR_GREEN, -1)
    curses.init_pair(2, curses.COLOR_YELLOW, -1)
    curses.init_pair(3, curses.COLOR_RED, -1)
    curses.init_pair(4, curses.COLOR_CYAN, -1)
    curses.init_pair(5, curses.COLOR_WHITE, curses.COLOR_BLUE)   # selected row
    curses.init_pair(6, curses.COLOR_WHITE, -1)                  # dim

    stdscr.timeout(500)

    cursor_pos = 0
    scroll_offset = 0
    expanded: set[str] = set()
    pipeline_done = False

    while True:
        height, width = stdscr.getmaxyx()
        stdscr.erase()

        all_rows = build_interactive_rows(state, expanded, width)

        span_indices = [i for i, r in enumerate(all_rows) if r.is_span]
        if not span_indices:
            stdscr.addnstr(0, 0, "  (no spans received yet — waiting...)", width - 1)
            stdscr.refresh()
            key = stdscr.getch()
            if key == ord("q"):
                break
            continue

        cursor_pos = max(0, min(cursor_pos, len(span_indices) - 1))
        selected_row_idx = span_indices[cursor_pos]

        # Header (2 lines)
        header_lines = 2
        title = state.trace_name or "Dragon Agent Trace"
        total = len(state.spans)
        completed = sum(1 for s in state.spans.values() if s.is_complete)
        errors = sum(1 for s in state.spans.values() if s.error)
        in_prog = total - completed

        title_str = f"  {title}"
        status_parts = [f"Spans: {total}"]
        if completed:
            status_parts.append(f"✓ {completed}")
        if in_prog:
            status_parts.append(f"⏳ {in_prog}")
        if errors:
            status_parts.append(f"✗ {errors}")

        if stop_event.is_set():
            pipeline_done = True
        if pipeline_done:
            status_parts.append("DONE")
        help_hint = "[↑/↓ navigate | Enter expand | a/c all | q quit]"
        status_str = "  " + "  ".join(status_parts) + "    " + help_hint

        try:
            stdscr.addnstr(0, 0, title_str, width - 1, curses.A_BOLD)
            stdscr.addnstr(1, 0, status_str, width - 1, curses.A_DIM)
        except curses.error:
            pass

        content_height = height - header_lines
        if content_height <= 0:
            stdscr.refresh()
            key = stdscr.getch()
            if key == ord("q"):
                break
            continue

        # Flatten rows into screen lines
        screen_lines: list[tuple[int, str, bool, bool]] = []
        for row_idx, row in enumerate(all_rows):
            plain = _strip_ansi(row.text)
            is_sel = row.is_span and row_idx == selected_row_idx
            if row.is_span:
                screen_lines.append((row_idx, plain, True, is_sel))
            else:
                for sub_line in plain.split("\n"):
                    if len(sub_line) <= width - 1:
                        screen_lines.append((row_idx, sub_line, False, False))
                    else:
                        while len(sub_line) > width - 1:
                            screen_lines.append((row_idx, sub_line[: width - 1], False, False))
                            sub_line = "      " + sub_line[width - 1:]
                        if sub_line:
                            screen_lines.append((row_idx, sub_line, False, False))

        selected_screen_line = 0
        for sl_idx, (ri, _, _, is_sel) in enumerate(screen_lines):
            if is_sel:
                selected_screen_line = sl_idx
                break

        if selected_screen_line < scroll_offset:
            scroll_offset = selected_screen_line
        elif selected_screen_line >= scroll_offset + content_height:
            scroll_offset = selected_screen_line - content_height + 1
        scroll_offset = max(0, min(scroll_offset, max(0, len(screen_lines) - content_height)))

        # Render visible screen lines
        for screen_line in range(content_height):
            sl_idx = scroll_offset + screen_line
            if sl_idx >= len(screen_lines):
                break

            row_idx, display_text, is_span, is_sel = screen_lines[sl_idx]
            row = all_rows[row_idx]
            y = header_lines + screen_line

            try:
                if is_sel:
                    padded = display_text.ljust(width - 1)
                    stdscr.addnstr(y, 0, padded, width - 1,
                                   curses.color_pair(5) | curses.A_BOLD)
                elif is_span:
                    attr = curses.A_NORMAL
                    raw = row.text
                    if _Color.BOLD_RED and _Color.BOLD_RED in raw:
                        attr = curses.color_pair(3) | curses.A_BOLD
                    elif _Color.CYAN and _Color.CYAN in raw:
                        attr = curses.color_pair(4)
                    elif _Color.RED and _Color.RED in raw:
                        attr = curses.color_pair(3)
                    elif _Color.YELLOW and _Color.YELLOW in raw:
                        attr = curses.color_pair(2)
                    elif _Color.GREEN and _Color.GREEN in raw:
                        attr = curses.color_pair(1)
                    stdscr.addnstr(y, 0, display_text, width - 1, attr)
                else:
                    stdscr.addnstr(y, 0, display_text, width - 1, curses.A_DIM)
            except curses.error:
                pass

        stdscr.refresh()

        # Handle input
        key = stdscr.getch()
        if key == ord("q"):
            break
        elif key == curses.KEY_UP or key == ord("k"):
            cursor_pos = max(0, cursor_pos - 1)
        elif key == curses.KEY_DOWN or key == ord("j"):
            cursor_pos = min(len(span_indices) - 1, cursor_pos + 1)
        elif key == curses.KEY_PPAGE:
            cursor_pos = max(0, cursor_pos - content_height)
        elif key == curses.KEY_NPAGE:
            cursor_pos = min(len(span_indices) - 1, cursor_pos + content_height)
        elif key == curses.KEY_HOME or key == ord("g"):
            cursor_pos = 0
        elif key == curses.KEY_END or key == ord("G"):
            cursor_pos = len(span_indices) - 1
        elif key in (curses.KEY_ENTER, 10, 13, ord(" ")):
            sel_row = all_rows[selected_row_idx]
            if sel_row.span_id in expanded:
                expanded.discard(sel_row.span_id)
            else:
                expanded.add(sel_row.span_id)
        elif key == ord("a"):
            expanded.update(s.span_id for s in state.spans.values())
        elif key == ord("c"):
            expanded.clear()
        elif key == curses.KEY_LEFT or key == ord("h"):
            pass  # future: horizontal scroll
        elif key == curses.KEY_RIGHT or key == ord("l"):
            pass  # future: horizontal scroll
