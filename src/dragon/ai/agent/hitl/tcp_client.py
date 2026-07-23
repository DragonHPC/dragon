"""TCP client loop for the HITL approval client.

Handles TCP connection to the :class:`HitlTcpBridge`, JSON request/response
protocol, ANSI-coloured terminal output, decision audit logging (JSONL),
auto-saved ``.txt`` reports, and session statistics.

**No Dragon runtime required.**
"""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
from datetime import datetime as _dt

from ..utils.ansi import Color as _Color, colorize as _colorize
from .models import HumanApprovalRequest, HumanApprovalResponse
from .terminal import (
    HitlSessionStats,
    format_request,
    format_decision,
    format_banner,
    format_session_summary,
    prompt_operator,
)

__all__ = ["hitl_tcp_client_loop", "replay_audit_log"]


# ---------------------------------------------------------------------------
# .txt report writer (parallel to trace_report.write_readable_report)
# ---------------------------------------------------------------------------

def _write_hitl_report(
    stats: HitlSessionStats,
    jsonl_path: str,
    report_path: str | None = None,
) -> str | None:
    """Write a human-readable ``.txt`` report alongside the JSONL audit log.

    Returns the path of the written report, or ``None`` if nothing to write.
    """
    if stats.total == 0:
        return None

    if not report_path:
        base, _ = os.path.splitext(jsonl_path)
        report_path = base + ".txt"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("  Dragon Agent HITL Session Report\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"  JSONL log  : {os.path.abspath(jsonl_path)}\n")
        start = _dt.fromtimestamp(stats.start_time)
        f.write(f"  Started    : {start.isoformat(sep=' ', timespec='seconds')}\n")
        f.write(f"  Duration   : {stats.elapsed:.1f}s\n\n")

        f.write(f"  Decisions  : {stats.total}\n")
        f.write(f"    Approved : {stats.approved}\n")
        f.write(f"    Rejected : {stats.rejected}\n")
        f.write(f"    Feedback : {stats.feedback}\n\n")

        # Replay each decision from the JSONL file
        f.write("-" * 60 + "\n")
        f.write("  Decision Log\n")
        f.write("-" * 60 + "\n\n")

        try:
            with open(jsonl_path, "r", encoding="utf-8") as jf:
                for line_num, line in enumerate(jf, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if entry.get("type") == "session_end":
                        continue
                    req = entry.get("request", {})
                    resp = entry.get("response", {})
                    if not req:
                        continue

                    ts = _dt.fromtimestamp(entry.get("timestamp", 0))
                    approved = resp.get("approved", False)
                    reason = resp.get("reason", "")
                    is_feedback = resp.get("is_feedback", False)
                    decision = "APPROVED" if approved else ("FEEDBACK" if is_feedback else "REJECTED")

                    f.write(f"  [{ts.strftime('%H:%M:%S')}] {req.get('tool_name', '?')} "
                            f"({req.get('agent_id', '?')}) -> {decision}")
                    if reason:
                        f.write(f" ({reason})")
                    f.write("\n")

                    args_str = json.dumps(req.get("tool_args", {}), indent=4)
                    for a_line in args_str.splitlines():
                        f.write(f"    {a_line}\n")
                    f.write("\n")
        except OSError:
            f.write("  (could not read JSONL file for details)\n\n")

        f.write("=" * 60 + "\n")
        f.write(f"  Replay: python -m dragon.ai.agent.hitl --file {jsonl_path}\n")
        f.write("=" * 60 + "\n")

    return report_path


def hitl_tcp_client_loop(
    host: str,
    port: int,
    stop_event: threading.Event,
    *,
    save_path: str | None = None,
    report_path: str | None = None,
) -> HitlSessionStats:
    """Connect to the HITL TCP bridge and handle approval requests via stdin.

    Auto-saves a timestamped JSONL audit log + ``.txt`` report when the
    session ends.  Pass *save_path* / *report_path* to override defaults.

    Args:

    host:
        Hostname or IP of the HITL TCP bridge.
    port:
        Port of the HITL TCP bridge.
    stop_event:
        A :class:`threading.Event` checked each iteration; set to exit early.
    save_path:
        Path for the JSONL audit log.  When ``None`` (default), an
        auto-generated ``hitl_YYYYMMDD_HHMMSS.jsonl`` is used.
    report_path:
        Path for the human-readable ``.txt`` report.  When ``None``
        (default), derived from the JSONL filename.

    Returns:

    HitlSessionStats
        Summary of operator decisions made during the session.
    """
    stats = HitlSessionStats()

    # -- Connect -----------------------------------------------------------
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
    except OSError as exc:
        sock.close()
        print(_colorize(f"\n  Failed to connect to {host}:{port}: {exc}",
                        _Color.RED), flush=True)
        return stats
    rfile = sock.makefile("r")
    wfile = sock.makefile("w")

    print(format_banner(host, port), flush=True)

    # -- Audit log (auto-saved, mirroring trace viewer) --------------------
    if save_path is None:
        save_path = f"hitl_{_dt.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        _auto_saved = True
    else:
        _auto_saved = False

    save_file = open(save_path, "a", encoding="utf-8")
    print(_colorize(f"  Saving audit log to {save_path}", _Color.DIM), flush=True)

    try:
        while not stop_event.is_set():
            line = rfile.readline()
            if not line:
                print(_colorize("\n  Connection closed by server.",
                                _Color.DIM), flush=True)
                break

            data = json.loads(line)
            if data.get("type") == "shutdown":
                print(_colorize("\n  Pipeline finished — shutting down.",
                                _Color.DIM), flush=True)
                break

            request = HumanApprovalRequest(
                tool_name=data["tool_name"],
                tool_args=data["tool_args"],
                context=data.get("context", ""),
                agent_id=data["agent_id"],
                task_id=data["task_id"],
                dispatch_id=data["dispatch_id"],
                timestamp=data.get("timestamp", time.time()),
            )

            print(format_request(request), flush=True)
            approved, reason, is_feedback = prompt_operator()
            stats.record(approved, is_feedback)

            # -- Send response to bridge -----------------------------------
            response_msg = json.dumps({
                "approved": approved,
                "reason": reason,
                "is_feedback": is_feedback,
            })
            wfile.write(response_msg + "\n")
            wfile.flush()

            print(format_decision(approved, reason, is_feedback), flush=True)

            # -- Audit log entry -------------------------------------------
            log_entry = json.dumps({
                "timestamp": time.time(),
                "request": {
                    "tool_name": request.tool_name,
                    "tool_args": request.tool_args,
                    "context": request.context,
                    "agent_id": request.agent_id,
                    "task_id": request.task_id,
                    "dispatch_id": request.dispatch_id,
                },
                "response": {
                    "approved": approved,
                    "reason": reason,
                    "is_feedback": is_feedback,
                },
            })
            save_file.write(log_entry + "\n")
            save_file.flush()

    finally:
        sock.close()
        # -- Session-end footer --------------------------------------------
        save_file.write(json.dumps({
            "type": "session_end",
            "timestamp": time.time(),
            "stats": {
                "approved": stats.approved,
                "rejected": stats.rejected,
                "feedback": stats.feedback,
                "total": stats.total,
                "elapsed": round(stats.elapsed, 2),
            },
        }) + "\n")
        save_file.close()

    # -- Session summary ---------------------------------------------------
    print(format_session_summary(stats), flush=True)

    if stats.total > 0:
        rpt = _write_hitl_report(stats, save_path, report_path=report_path)
        print(f"\n  Session saved:")
        print(f"    JSONL  : {os.path.abspath(save_path)}  ({stats.total} decisions)")
        if rpt:
            print(f"    Report : {os.path.abspath(rpt)}")
        print(f"  Replay:  python -m dragon.ai.agent.hitl "
              f"--file {save_path}\n")
    elif _auto_saved and os.path.exists(save_path):
        os.remove(save_path)
        print(_colorize("  (no decisions — auto-saved log removed)", _Color.DIM),
              flush=True)

    return stats


# ---------------------------------------------------------------------------
# Audit log replay (parallel to trace viewer's --file mode)
# ---------------------------------------------------------------------------

def replay_audit_log(filepath: str) -> None:
    """Read a JSONL audit log and display past decisions."""
    if not os.path.exists(filepath):
        print(f"Error: file not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    stats = HitlSessionStats()

    print(_colorize(f"\n  Replaying HITL audit log: {filepath}\n", _Color.BOLD),
          flush=True)

    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                print(_colorize(f"  (skipping malformed line {line_num})",
                                _Color.DIM), flush=True)
                continue

            # Skip metadata entries (session_end footer, etc.)
            if entry.get("type") in ("session_end",):
                continue

            req = entry.get("request", {})
            resp = entry.get("response", {})

            request = HumanApprovalRequest(
                tool_name=req.get("tool_name", "?"),
                tool_args=req.get("tool_args", {}),
                context=req.get("context", ""),
                agent_id=req.get("agent_id", "?"),
                task_id=req.get("task_id", "?"),
                dispatch_id=req.get("dispatch_id", "?"),
                timestamp=entry.get("timestamp", 0),
            )
            approved = resp.get("approved", False)
            reason = resp.get("reason", "")
            is_feedback = resp.get("is_feedback", False)
            stats.record(approved, is_feedback)

            print(format_request(request), flush=True)
            print(format_decision(approved, reason, is_feedback), flush=True)

    print(format_session_summary(stats), flush=True)
