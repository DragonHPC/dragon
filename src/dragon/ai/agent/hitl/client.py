"""HITL Approval CLI — TCP client for human-in-the-loop decisions.

A standalone script that connects to the HITL TCP bridge running inside
a Dragon AI pipeline and handles approval requests interactively.
**No Dragon runtime required** — connects via plain TCP.

Usage::

    # Live session (auto-saves JSONL + .txt report on exit):
    python -m dragon.ai.agent.hitl --tcp HOST:PORT

    # Custom save path:
    python -m dragon.ai.agent.hitl --tcp HOST:PORT --jsonl decisions.jsonl
    python -m dragon.ai.agent.hitl --tcp HOST:PORT --report report.txt

    # Replay past decisions from an audit log:
    python -m dragon.ai.agent.hitl --file decisions.jsonl

    # Disable colours:
    python -m dragon.ai.agent.hitl --tcp HOST:PORT --no-color

Architecture::

    The HITL client internals are split across ``hitl/``:

    * ``models.py``      — HumanApprovalRequest / Response  (data model)
    * ``terminal.py``    — coloured formatting, prompts, session stats
    * ``tcp_client.py``  — TCP loop, audit logging, replay, .txt report
    * ``tcp_bridge.py``  — server-side bridge (inside pipeline)

    This entry point wires those together with an argparse CLI.
"""

from __future__ import annotations

import argparse
import sys
import threading

from ..utils.ansi import Color as _Color
from .tcp_client import hitl_tcp_client_loop, replay_audit_log


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Parse arguments and launch the HITL approval client."""
    parser = argparse.ArgumentParser(
        prog="hitl_client",
        description="Dragon Agent HITL Approval Client — connects to a live "
                     "pipeline via TCP.",
    )

    # -- Source selection (mutually exclusive) ------------------------------
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--tcp", metavar="HOST:PORT",
        help="Connect directly to the HITL TCP bridge (e.g. localhost:9876).",
    )
    source.add_argument(
        "--file", metavar="PATH",
        help="Replay a JSONL audit log from a previous session "
             "(same pattern as trace_viewer --file).",
    )

    # -- Options -----------------------------------------------------------
    parser.add_argument(
        "--jsonl", metavar="PATH",
        help="Custom path for the JSONL audit log (default: auto-generated "
             "hitl_YYYYMMDD_HHMMSS.jsonl).",
    )
    parser.add_argument(
        "--report", metavar="PATH",
        help="Custom path for the human-readable .txt report "
             "(default: derived from JSONL filename).",
    )
    parser.add_argument(
        "--no-color", action="store_true",
        help="Disable ANSI colour output.",
    )

    args = parser.parse_args()

    # -- Colour setup ------------------------------------------------------
    if args.no_color or not sys.stdout.isatty():
        _Color.disable()

    # -- File replay mode --------------------------------------------------
    if args.file:
        replay_audit_log(args.file)
        return

    # -- Resolve host and port ---------------------------------------------
    parts = args.tcp.rsplit(":", 1)
    if len(parts) != 2:
        print("Error: --tcp must be HOST:PORT (e.g. localhost:9876)",
              file=sys.stderr)
        sys.exit(1)
    host = parts[0]
    try:
        port = int(parts[1])
    except ValueError:
        print(f"Error: invalid port: {parts[1]}", file=sys.stderr)
        sys.exit(1)

    # -- Run the interactive approval loop ---------------------------------
    stop_event = threading.Event()
    try:
        hitl_tcp_client_loop(host, port, stop_event, save_path=args.jsonl,
                             report_path=args.report)
    except ConnectionRefusedError:
        print(
            f"[HITL Client] ERROR: Connection refused at {host}:{port}.\n"
            f"              Is the pipeline running?",
            flush=True,
        )
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n  HITL CLI shutting down.", flush=True)
    except Exception as exc:
        print(f"\n[HITL Client] Approval loop ended: {exc}", flush=True)


if __name__ == "__main__":
    main()
