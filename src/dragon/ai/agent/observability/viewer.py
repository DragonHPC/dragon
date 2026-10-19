"""Terminal trace viewer for Dragon agent pipelines.

A standalone script that renders a live or offline trace tree with Gantt
bars, timing columns, and content display.  **No Dragon runtime
required** — connects via plain TCP or reads JSONL files.

Usage::

    # Interactive mode (recommended — auto-saves JSONL + .txt report on quit):
    python -m dragon.ai.agent.observability --tcp HOST:PORT -i

    # Non-interactive (auto-saves JSONL + .txt report; full content shown):
    python -m dragon.ai.agent.observability --tcp HOST:PORT

    # Custom save paths:
    python -m dragon.ai.agent.observability --tcp HOST:PORT --jsonl run.jsonl
    python -m dragon.ai.agent.observability --tcp HOST:PORT --report report.txt

    # Offline replay:
    python -m dragon.ai.agent.observability --file traces/abc123.jsonl -i
    python -m dragon.ai.agent.observability --file traces/abc123.jsonl

Architecture::

    The viewer internals are split across ``observability/``:

    * ``trace_state.py``       — ViewerSpan + TraceState  (data model)
    * ``trace_renderer.py``    — Gantt bars, tree rendering  (terminal output)
    * ``trace_interactive.py`` — curses TUI  (interactive mode)
    * ``trace_report.py``      — .txt report writer

    This entry point wires those together with TCP / file sources and
    an argparse CLI.
"""

from __future__ import annotations

import argparse
import curses
import json
import os
import shutil
import socket
import sys
import threading
import time

from ..utils.ansi import Color as _Color, colorize as _colorize
from .trace_interactive import curses_main as _curses_main
from .trace_renderer import redraw as _redraw
from .trace_report import write_readable_report as _write_readable_report
from .trace_state import TraceState


# ---------------------------------------------------------------------------
# TCP source — reads from TraceTcpBridge
# ---------------------------------------------------------------------------

def _run_tcp_viewer(
    host: str,
    port: int,
    save_path: str | None = None,
    report_path: str | None = None,
) -> None:
    """Connect to TraceTcpBridge via TCP and render live updates.

    Auto-saves a timestamped JSONL + .txt report.  Pass ``--jsonl``
    or ``--report`` to override the default filenames.
    """
    from datetime import datetime as _dt

    if save_path is None:
        save_path = f"trace_{_dt.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        _auto_saved = True
    else:
        _auto_saved = False

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
    except OSError as exc:
        sock.close()
        print(f"Error: cannot connect to {host}:{port}: {exc}",
              file=sys.stderr)
        sys.exit(1)

    rfile = sock.makefile("r")
    state = TraceState()
    term_width = shutil.get_terminal_size(fallback=(120, 40)).columns
    save_file = open(save_path, "a", encoding="utf-8")
    _save_count = 0

    print(_colorize(f"  Saving trace to {save_path}", _Color.DIM))
    print(_colorize(f"  Connected to {host}:{port} — waiting for spans...\n", _Color.DIM))

    try:
        while True:
            line = rfile.readline()
            if not line:
                break
            save_file.write(line)
            save_file.flush()
            _save_count += 1
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue

            state.handle_message(msg)
            if state.shutdown:
                break
            _redraw(state, True, True, term_width)

    except KeyboardInterrupt:
        pass
    finally:
        sock.close()
        save_file.close()

    _redraw(state, True, True, term_width)
    print(_colorize("\n  Pipeline completed.\n", _Color.DIM))

    if _save_count > 0:
        rpt = _write_readable_report(state, save_path, report_path=report_path)
        print(f"\n  Trace saved:")
        print(f"    JSONL  : {os.path.abspath(save_path)}  ({_save_count} messages)")
        if rpt:
            print(f"    Report : {os.path.abspath(rpt)}")
        print(f"  Replay:  python -m dragon.ai.agent.observability "
              f"--file {save_path} -i\n")
    elif _auto_saved and os.path.exists(save_path):
        os.remove(save_path)


# ---------------------------------------------------------------------------
# File source — reads from JSONL
# ---------------------------------------------------------------------------

def _run_file_viewer(filepath: str) -> None:
    """Read a JSONL trace file and render the tree (full content, no tailing)."""
    if not os.path.exists(filepath):
        print(f"Error: file not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    state = TraceState()
    term_width = shutil.get_terminal_size(fallback=(120, 40)).columns

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                state.handle_message(msg)
            except json.JSONDecodeError:
                continue

    _redraw(state, True, True, term_width)


# ---------------------------------------------------------------------------
# Interactive launchers
# ---------------------------------------------------------------------------

def _run_interactive_tcp(
    host: str,
    port: int,
    save_path: str | None = None,
    report_path: str | None = None,
) -> None:
    """Interactive curses viewer with TCP source.

    Auto-saves a timestamped JSONL + .txt report unless ``--jsonl``
    overrides the path.
    """
    from datetime import datetime as _dt

    if save_path is None:
        save_path = f"trace_{_dt.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        _auto_saved = True
    else:
        _auto_saved = False

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
    except OSError as exc:
        sock.close()
        print(f"Error: cannot connect to {host}:{port}: {exc}",
              file=sys.stderr)
        sys.exit(1)

    state = TraceState()
    stop_event = threading.Event()
    _save_count = [0]

    def _reader() -> None:
        rfile = sock.makefile("r")
        save_file = open(save_path, "a", encoding="utf-8")
        try:
            while not stop_event.is_set():
                line = rfile.readline()
                if not line:
                    break
                save_file.write(line)
                save_file.flush()
                _save_count[0] += 1
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                state.handle_message(msg)
                if state.shutdown:
                    break
        except OSError:
            pass
        finally:
            save_file.close()

    reader_thread = threading.Thread(target=_reader, daemon=True)
    reader_thread.start()

    try:
        curses.wrapper(lambda stdscr: _curses_main(stdscr, state, stop_event))
    finally:
        stop_event.set()
        sock.close()
        reader_thread.join(timeout=2)

    if _save_count[0] > 0:
        rpt = _write_readable_report(state, save_path, report_path=report_path)
        print(f"\n  Trace saved:")
        print(f"    JSONL  : {os.path.abspath(save_path)}  ({_save_count[0]} messages)")
        if rpt:
            print(f"    Report : {os.path.abspath(rpt)}")
        print(f"  Replay:  python -m dragon.ai.agent.observability "
              f"--file {save_path} -i\n")
    elif _auto_saved and os.path.exists(save_path):
        os.remove(save_path)


def _run_interactive_file(filepath: str) -> None:
    """Interactive curses viewer with file source."""
    if not os.path.exists(filepath):
        print(f"Error: file not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    state = TraceState()
    stop_event = threading.Event()

    def _reader() -> None:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    if stop_event.is_set():
                        return
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        state.handle_message(msg)
                    except json.JSONDecodeError:
                        continue
                while not stop_event.is_set() and not state.shutdown:
                    line = f.readline()
                    if not line:
                        time.sleep(0.5)
                        continue
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        state.handle_message(msg)
                    except json.JSONDecodeError:
                        continue
        except OSError:
            pass

    reader_thread = threading.Thread(target=_reader, daemon=True)
    reader_thread.start()

    try:
        curses.wrapper(lambda stdscr: _curses_main(stdscr, state, stop_event))
    finally:
        stop_event.set()
        reader_thread.join(timeout=2)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Parse arguments and launch the appropriate viewer mode."""
    parser = argparse.ArgumentParser(
        prog="trace_viewer",
        description="Dragon Agent Trace Viewer — terminal-based trace visualisation.",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--tcp", metavar="HOST:PORT",
        help="Connect to a live TraceTcpBridge (e.g. localhost:9876).",
    )
    source.add_argument(
        "--file", metavar="PATH",
        help="Read from a JSONL trace file.",
    )
    parser.add_argument(
        "-i", "--interactive", action="store_true",
        help="Curses-based interactive mode: arrow keys navigate, Enter expands detail.",
    )
    parser.add_argument(
        "--no-color", action="store_true",
        help="Disable ANSI colour output.",
    )
    parser.add_argument(
        "--jsonl", metavar="PATH",
        help="Custom path for the JSONL trace file "
             "(default: auto-generated trace_YYYYMMDD_HHMMSS.jsonl).",
    )
    parser.add_argument(
        "--report", metavar="PATH",
        help="Custom path for the human-readable .txt report "
             "(default: derived from JSONL filename).",
    )

    args = parser.parse_args()

    if args.no_color or not sys.stdout.isatty():
        _Color.disable()

    if args.tcp:
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

        if args.interactive:
            _run_interactive_tcp(host, port, save_path=args.jsonl,
                                 report_path=args.report)
        else:
            _run_tcp_viewer(host, port, save_path=args.jsonl,
                            report_path=args.report)

    elif args.file:
        if args.interactive:
            _run_interactive_file(args.file)
        else:
            _run_file_viewer(args.file)


if __name__ == "__main__":
    main()
