"""
client.py — External process that exercises a Dragon runtime via dragon.attach().

Run as plain Python (NOT under dragon):
    python client.py [--start | --attach | --proxy]

Three modes are supported, selected by a command-line argument:

**--start** (Path A — client starts the runtime itself):
    Calls :py:func:`dragon.runtime.start_runtime` to launch a fresh Dragon
    runtime, attaches to it, runs the demo workload, detaches, then tears the
    runtime down via :py:meth:`DragonRuntime.stop`.

**--attach** (Path B — attach to an already-running runtime; DEFAULT):
    Reads control coordinates from a JSON file published by the running
    ``_runtime_head.py`` (path given by ``DRAGON_ATTACH_INFO_PATH`` env var,
    or ``/tmp/dragon_<uid>/attach_ctrl.json`` by default).  Attaches to the
    runtime, runs the demo workload, detaches, reconstructs a
    :py:class:`DragonRuntime` handle from the ``_ctrl`` block, and shuts the
    runtime down via the SAME :py:meth:`DragonRuntime.stop` control-token TCP
    sequence used in ``--start`` mode.

**--proxy** (Path C - attach to a remote runtime via Proxy API):
    Looks up a published runtime descriptor with
    :py:func:`dragon.workflows.runtime.lookup`, attaches with
    :py:func:`dragon.workflows.runtime.attach`, enables the Proxy, runs the
    demo workload, then disables/stops the Proxy. Optionally writes an exit
    marker file for the remote proxy server shutdown flow.

Demo workload (both modes):
    - Creates a managed :py:class:`dragon.native.queue.Queue`.
    - Puts the first line of ``OPENING_SCROLL`` into the queue.
    - Spawns a managed :py:class:`dragon.native.process.Process` running
      ``worker``, which reads the first line, prints it, and puts the second
      line back.
    - The main process reads the second line and asserts it matches.
"""

import argparse
import os
import sys

import dragon
from dragon.native.process import Process
from dragon.native.queue import Queue

from scroll import OPENING_SCROLL

# ---------------------------------------------------------------------------
# Worker — must be a top-level function for cloudpickle to serialise it.
# Scroll lines are passed in as an argument so the worker needs no extra
# imports beyond what is available inside a Dragon-managed process.
# ---------------------------------------------------------------------------


def worker(out_q, scroll_lines):
    """Managed worker process: inspect the scroll and report back.

    Reads the first scroll line from *out_q*, prints it, then puts the second
    scroll line back onto *out_q* for the main process to verify.

    :param out_q: Shared managed queue used to exchange data.
    :type out_q: dragon.native.queue.Queue
    :param scroll_lines: The list of scroll lines passed from the client.
    :type scroll_lines: list[str]
    """
    first_line = out_q.get()
    second_line = scroll_lines[1]
    print(f"Worker: got the first line of the scroll: {first_line!r}", flush=True)
    out_q.put(second_line)
    print("Worker: put the second line into the queue.", flush=True)


# ---------------------------------------------------------------------------
# Shared demo workload
# ---------------------------------------------------------------------------


def _run_demo_workload():
    """Run the queue/worker demo workload against the attached Dragon runtime.

    Creates a managed Queue, enqueues the first scroll line, spawns a worker
    process, waits for it to finish, reads the second scroll line back from
    the queue, and asserts it matches the expected value.

    Must be called while a Dragon session is active (i.e. between
    :py:func:`dragon.attach` and :py:func:`dragon.detach`).

    :raises AssertionError: If the value returned by the worker does not match
        ``OPENING_SCROLL[1]``.
    """
    q = Queue(maxsize=10)
    q.put(OPENING_SCROLL[0])
    p = Process(target=worker, args=(q, OPENING_SCROLL))
    p.start()
    p.join()

    second_line = q.get(timeout=30)
    print(f"Client (main): got the second line from the queue: {second_line!r}", flush=True)

    assert (
        second_line == OPENING_SCROLL[1]
    ), f"Queue value mismatch!\n  got:      {second_line!r}\n  expected: {OPENING_SCROLL[1]!r}"
    print("Client: assertion passed — queue round-trip verified.", flush=True)


# ---------------------------------------------------------------------------
# Mode A — client starts and owns the runtime
# ---------------------------------------------------------------------------


def _mode_start():
    """Run the demo by starting a fresh Dragon runtime.

    Calls :py:func:`dragon.runtime.start_runtime` to launch the runtime,
    attaches to it, runs the shared demo workload, detaches, and stops the
    runtime via :py:meth:`DragonRuntime.stop` (control-token TCP shutdown).

    Uses a try/finally so :py:meth:`DragonRuntime.stop` always executes even
    if the workload raises.

    :raises FileNotFoundError: If the ``dragon`` command cannot be found.
    :raises RuntimeError: If the runtime does not start within the timeout.
    """
    from dragon.workflows.runtime import LocalRuntime

    print("Client [--start]: launching Dragon runtime via start_runtime()...", flush=True)
    try:
        attached_runtime = LocalRuntime.start()
        print("Client [--start]: attached.", flush=True)
        _run_demo_workload()
    finally:
        print("Client [--start]: stopping runtime via attached_runtime.stop()...", flush=True)
        attached_runtime.stop()
        print("Client [--start]: runtime stopped.", flush=True)


# ---------------------------------------------------------------------------
# Mode B — attach to an existing runtime, then shut it down via the same path
# ---------------------------------------------------------------------------


def _mode_attach():
    """Run the demo by attaching to an already-running Dragon runtime.

    Reads control coordinates from the published ctrl-info file (path from
    ``DRAGON_ATTACH_INFO_PATH`` env var or the default location), attaches to
    the runtime via :py:func:`dragon.attach`, runs the shared demo workload,
    detaches, reconstructs a :py:class:`DragonRuntime` handle from the
    control coordinates, and shuts the runtime down via the SAME
    :py:meth:`DragonRuntime.stop` control-token TCP sequence used by
    ``--start`` mode.

    :raises SystemExit: If the ctrl-info file is missing or malformed.
    """
    from dragon.workflows.runtime import LocalRuntime, attach

    # ctrl = _read_ctrl_info()

    print("Client [--attach]: attaching to running Dragon runtime...", flush=True)
    attached_runtime = attach()
    print(f"Client [--attach]: attached — assigned p_uid={attached_runtime._puid}", flush=True)

    _run_demo_workload()

    # Reconstruct a handle and shut the runtime down via the unified path.
    print("Client [--attach]: sending shutdown token via DragonRuntime.stop()...", flush=True)
    attached_runtime.stop()
    print("Client [--attach]: runtime stopped.", flush=True)

# ---------------------------------------------------------------------------
# Mode C - attach to a remote runtime via Proxy/OOB and run the same workload
# ---------------------------------------------------------------------------

def _mode_proxy(args):
    """Run the demo against a remote runtime using the Proxy workflow API."""
    import dragon.workflows.runtime as runtime

    print("Client [--proxy]: looking up remote runtime descriptor...", flush=True)
    runtime_sdesc = runtime.lookup(
        args.system,
        args.runtime_name,
        30,
        publish_dir=args.remote_working_dir,
    )

    print("Client [--proxy]: attaching to remote runtime via Proxy...", flush=True)
    proxy = runtime.attach(sdesc_str=runtime_sdesc, remote_cwd=args.remote_working_dir)

    try:
        print("Client [--proxy]: enabling Proxy runtime...", flush=True)
        proxy.enable()
        print("Client [--proxy]: proxy enabled.", flush=True)
        _run_demo_workload()
    finally:
        proxy.stop(exit_path=args.exit_path, remote_working_dir=args.remote_working_dir)
        print("Client [--proxy]: proxy stopped.", flush=True)

# ---------------------------------------------------------------------------
# Mode D - try to run the workload outside of a Dragon runtime. The workload explicitly uses Dragon so this will not work but demonstrates the dependency of the workload on Dragon.
# ---------------------------------------------------------------------------

def _mode_outside(args):
    """Run the demo workload outside of a Dragon runtime."""
    print("Client [--outside]: running workload outside of Dragon runtime...", flush=True)
    _run_demo_workload()
    print("Client [--outside]: workload completed.", flush=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    """Parse arguments and dispatch to the selected demo mode.

    :raises SystemExit: Propagated from the selected mode on error.
    """
    parser = argparse.ArgumentParser(
        description="Dragon outerspace-proc demo client. Supports start, attach, and proxy modes."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--start",
        action="store_true",
        help="Start a fresh Dragon runtime, run the demo, then stop it (Path A).",
    )
    group.add_argument(
        "--attach",
        action="store_true",
        help="Attach to an existing runtime, run the demo, then stop it (Path B, default).",
    )
    group.add_argument(
        "--proxy",
        action="store_true",
        help="Attach to a remote runtime via Proxy, run the demo, then disable/stop the proxy (Path C).",
    )
    parser.add_argument(
        "--outside",
        action="store_true",
        help="Run the demo workload outside of a Dragon runtime (Path D).",
    )
    parser.add_argument(
        "--system",
        type=str,
        help="Remote system to connect to (required with --proxy).",
    )
    parser.add_argument(
        "--runtime-name",
        type=str,
        default="my-runtime",
        help="Published runtime descriptor name for --proxy mode.",
    )
    parser.add_argument(
        "--remote-working-dir",
        "-rwd",
        type=str,
        default=os.getcwd(),
        help="Remote publish/working directory for --proxy mode.",
    )
    parser.add_argument(
        "--exit-path",
        type=str,
        default=os.path.join(os.getcwd(), "client_exit"),
        help="Exit marker path used for proxy server shutdown signaling.",
    )
    args = parser.parse_args()

    if args.start:
        _mode_start()
    elif args.proxy:
        if not args.system:
            parser.error("--system is required when using --proxy")
        _mode_proxy(args)
    elif args.outside:
        _mode_outside(args)
    else:
        # Default to --attach when neither --start, --proxy, nor --outside is given.
        _mode_attach()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        print(f"Client ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
