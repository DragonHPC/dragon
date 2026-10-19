#!/usr/bin/env python3
"""Client driver for the cross-runtime relay symmetry matrix.

Mirrors proxy_remote_queue_client.py: attach to the published remote runtime,
run the relay matrix over the proxy, then signal the remote runtime to shut
down. Kept as a separate driver so the matrix is a dedicated, named test.
"""

import argparse
import multiprocessing as mp
import os
import pathlib
import sys
import time

import dragon.workflows.runtime as runtime

THIS_DIR = pathlib.Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from proxy_relay_matrix import run_relay_matrix
from proxy_object_suite import shutdown_remote_runtime


def read_runtime_sdesc(publish_dir, runtime_name, timeout):
    """Read the descriptor after the shared publish directory makes it visible.

    The server and client may run on different nodes and the publish directory
    may be backed by a parallel filesystem, so a short polling loop is more
    reliable than assuming the write is immediately visible everywhere.
    """
    sdesc_path = os.path.join(publish_dir, runtime_name)
    start = time.time()

    while time.time() - start < timeout:
        if os.path.exists(sdesc_path) and os.path.getsize(sdesc_path) > 0:
            with open(sdesc_path, "r", encoding="ascii") as sdesc_file:
                return sdesc_file.read()
        time.sleep(1)

    raise RuntimeError(f"timed out waiting for published runtime descriptor at {sdesc_path}")


def parse_args():
    """Parse shared-runner paths and matrix timeouts."""
    parser = argparse.ArgumentParser(description="Relay matrix client for cross-runtime transport tests")
    parser.add_argument("--runtime-name", type=str, required=True, help="Published runtime name")
    parser.add_argument("--publish-dir", type=str, required=True, help="Shared directory containing runtime descriptor")
    parser.add_argument(
        "--remote-working-dir", type=str, required=True, help="Working directory to use in remote runtime"
    )
    parser.add_argument(
        "--exit-path", type=str, required=True, help="Shared file used to signal remote runtime shutdown"
    )
    parser.add_argument("--lookup-timeout", type=int, default=120, help="Timeout waiting for published descriptor")
    parser.add_argument("--process-timeout", type=int, default=120, help="Per-case timeout (remote processes / queues)")
    # Accepted for drop-in compatibility with the shared launcher; the relay
    # matrix does not verify remote placement, so these are ignored.
    parser.add_argument("--allocation-hosts", type=str, default="", help=argparse.SUPPRESS)
    parser.add_argument("--expected-remote-hosts", type=str, default="", help=argparse.SUPPRESS)
    return parser.parse_args()


def main():
    """Attach once, run every relay cell, and signal the server to exit."""
    args = parse_args()

    # The workers in the matrix use Dragon-native processes. Set the start
    # method before attaching so every queue/process created by the matrix uses
    # the same runtime-aware multiprocessing implementation.
    mp.set_start_method("dragon", force=True)

    runtime_sdesc = read_runtime_sdesc(args.publish_dir, args.runtime_name, args.lookup_timeout)
    proxy = runtime.attach(sdesc_str=runtime_sdesc, remote_cwd=args.remote_working_dir)

    shutdown_sent = False

    try:
        # One Proxy object is reused across all cells. Each runner enters and
        # leaves its proxy context, which exercises repeated runtime switching
        # without rebuilding the remote attachment for every case.
        run_relay_matrix(proxy, args.remote_working_dir, args.process_timeout)

        shutdown_remote_runtime(proxy, args.exit_path, args.remote_working_dir, args.process_timeout)
        shutdown_sent = True
        print("PROXY_RELAY_MATRIX_OK", flush=True)
    finally:
        # A failed matrix must not strand the remote runtime in the allocation.
        if not shutdown_sent:
            try:
                shutdown_remote_runtime(proxy, args.exit_path, args.remote_working_dir, args.process_timeout)
            except Exception:
                pass


if __name__ == "__main__":
    main()
