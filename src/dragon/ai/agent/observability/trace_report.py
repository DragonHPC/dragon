"""Human-readable ``.txt`` trace report writer.

Generates a plain-text report alongside a JSONL trace file with:
* Pipeline metadata (name, timestamps, duration)
* Full trace tree with timing columns and Gantt bars
* All LLM/tool/HITL content fully expanded (no truncation)
* Per-agent summary table
"""

from __future__ import annotations

import os
from typing import Any

from ..utils.ansi import strip_ansi
from .trace_protocol import SpanKind
from .trace_renderer import render_tree
from .trace_state import TraceState

__all__ = ["write_readable_report"]


def write_readable_report(
    state: TraceState,
    jsonl_path: str,
    report_path: str | None = None,
) -> str | None:
    """Write a human-readable ``.txt`` report alongside the JSONL file.

    If *report_path* is ``None``, the path is derived from *jsonl_path*
    by replacing its extension with ``.txt``.

    Returns the path of the written report, or ``None`` if nothing to write.
    """
    if not state.spans:
        return None

    if not report_path:
        base, _ = os.path.splitext(jsonl_path)
        report_path = base + ".txt"

    # Strip ANSI for the report since it's a plain text file
    tree_output = render_tree(state, verbose=True, full=True, term_width=200)
    plain_tree = strip_ansi(tree_output)

    total = len(state.spans)
    completed = sum(1 for s in state.spans.values() if s.is_complete)
    errors = sum(1 for s in state.spans.values() if s.error)
    in_progress = total - completed

    # Per-agent stats
    agent_stats: dict[str, dict] = {}
    for s in state.spans.values():
        aid = s.attributes.get("agent.id", "")
        if not aid and s.parent_id and s.parent_id in state.spans:
            pid = s.parent_id
            while pid and pid in state.spans:
                p = state.spans[pid]
                aid = p.attributes.get("agent.id", "")
                if aid:
                    break
                pid = p.parent_id
        if not aid:
            continue
        if aid not in agent_stats:
            agent_stats[aid] = {"spans": 0, "errors": 0, "llm_calls": 0,
                                "tool_calls": 0, "duration": None}
        agent_stats[aid]["spans"] += 1
        if s.error:
            agent_stats[aid]["errors"] += 1
        if s.kind == SpanKind.LLM_CALL:
            agent_stats[aid]["llm_calls"] += 1
        elif s.kind == SpanKind.TOOL_CALL:
            agent_stats[aid]["tool_calls"] += 1
        if s.kind == SpanKind.TASK and s.duration is not None:
            agent_stats[aid]["duration"] = s.duration

    from datetime import datetime as _dt

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 72 + "\n")
        f.write("  Dragon Agent Trace Report\n")
        f.write("=" * 72 + "\n\n")

        name = state.trace_name or "(unnamed)"
        f.write(f"  Pipeline : {name}\n")
        if state.trace_start_time:
            f.write(f"  Started  : {_dt.fromtimestamp(state.trace_start_time).isoformat(sep=' ', timespec='seconds')}\n")
        if state.trace_end_time:
            f.write(f"  Ended    : {_dt.fromtimestamp(state.trace_end_time).isoformat(sep=' ', timespec='seconds')}\n")
            if state.trace_start_time:
                dur = state.trace_end_time - state.trace_start_time
                f.write(f"  Duration : {dur:.1f}s\n")
        f.write(f"  Spans    : {total} total, {completed} completed, "
                f"{in_progress} in-progress, {errors} errors\n")
        f.write(f"  Source   : {os.path.abspath(jsonl_path)}\n")
        f.write("\n")

        if agent_stats:
            f.write("-" * 72 + "\n")
            f.write("  Agent Summary\n")
            f.write("-" * 72 + "\n\n")
            f.write(f"  {'Agent':<30s}  {'Duration':>10s}  {'LLM':>4s}  {'Tool':>5s}  {'Err':>4s}\n")
            f.write(f"  {'─' * 30}  {'─' * 10}  {'─' * 4}  {'─' * 5}  {'─' * 4}\n")
            for aid, st in sorted(agent_stats.items()):
                dur_s = f"{st['duration']:.1f}s" if st['duration'] is not None else "—"
                f.write(f"  {aid:<30s}  {dur_s:>10s}  {st['llm_calls']:>4d}  "
                        f"{st['tool_calls']:>5d}  {st['errors']:>4d}\n")
            f.write("\n")

        f.write("-" * 72 + "\n")
        f.write("  Trace Tree (verbose, full content)\n")
        f.write("-" * 72 + "\n\n")
        f.write(plain_tree)
        f.write("\n\n")

        f.write("=" * 72 + "\n")
        f.write(f"  Replay: python -m dragon.ai.agent.observability "
                f"--file {jsonl_path} -i\n")
        f.write("=" * 72 + "\n")

    return report_path
