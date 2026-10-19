#!/usr/bin/env python3
"""Client driver for the broader proxy object integration suite.

The driver discovers a descriptor published by the remote server, attaches one
proxy, runs queue, DDict, ProcessGroup, and round-trip coverage, and finally
signals the server through a shared exit file. It is intentionally separate
from the relay matrix: this suite checks higher-level object lifecycles while
the matrix isolates directional transport behavior.
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

from proxy_object_suite import run_proxy_object_suite, shutdown_remote_runtime


def read_runtime_sdesc(publish_dir, runtime_name, timeout):
    """Poll for and read a non-empty runtime descriptor from shared storage."""
    sdesc_path = os.path.join(publish_dir, runtime_name)
    start = time.time()

    while time.time() - start < timeout:
        if os.path.exists(sdesc_path) and os.path.getsize(sdesc_path) > 0:
            with open(sdesc_path, "r", encoding="ascii") as sdesc_file:
                return sdesc_file.read()
        time.sleep(1)

    raise RuntimeError(f"timed out waiting for published runtime descriptor at {sdesc_path}")


def parse_args():
    """Parse launcher-compatible options for the proxy object client."""
    parser = argparse.ArgumentParser(description="Proxy client for remote Dragon object integration tests")
    parser.add_argument("--runtime-name", type=str, required=True, help="Published runtime name")
    parser.add_argument("--publish-dir", type=str, required=True, help="Shared directory containing runtime descriptor")
    parser.add_argument(
        "--remote-working-dir", type=str, required=True, help="Working directory to use in remote runtime"
    )
    parser.add_argument(
        "--exit-path", type=str, required=True, help="Shared file used to signal remote runtime shutdown"
    )
    parser.add_argument("--lookup-timeout", type=int, default=120, help="Timeout waiting for published descriptor")
    parser.add_argument("--process-timeout", type=int, default=120, help="Timeout waiting for remote processes")
    parser.add_argument("--queue-timeout", type=int, default=120, help="Timeout waiting for queue data")
    parser.add_argument("--payload", type=str, default="proxy-remote-queue-payload", help="Payload to send")
    parser.add_argument("--sequence", type=int, default=17, help="Sequence marker to verify")
    parser.add_argument(
        "--allocation-hosts",
        type=str,
        default="",
        help="Comma-separated allocation hosts used to verify the remote writer ran on an allocated node",
    )
    parser.add_argument(
        "--expected-remote-hosts",
        type=str,
        default="",
        help="Comma-separated hostnames where the remote writer is expected to run",
    )
    return parser.parse_args()


def main():
    """Run the object suite and make a best-effort remote shutdown attempt."""
    args = parse_args()
    allocation_hosts = {host for host in args.allocation_hosts.split(",") if host}
    expected_remote_hosts = {host for host in args.expected_remote_hosts.split(",") if host} or allocation_hosts

    # The suite creates Dragon-native queues and processes, so the client must
    # select Dragon's multiprocessing implementation before creating any of
    # those objects.
    mp.set_start_method("dragon", force=True)

    runtime_sdesc = read_runtime_sdesc(args.publish_dir, args.runtime_name, args.lookup_timeout)
    proxy = runtime.attach(sdesc_str=runtime_sdesc, remote_cwd=args.remote_working_dir)

    shutdown_sent = False

    try:
        run_proxy_object_suite(
            proxy,
            args.remote_working_dir,
            args.process_timeout,
            args.queue_timeout,
            args.payload,
            args.sequence,
            expected_remote_hosts,
        )

        shutdown_remote_runtime(proxy, args.exit_path, args.remote_working_dir, args.process_timeout)
        shutdown_sent = True
        print("PROXY_REMOTE_OBJECT_SUITE_OK", flush=True)
        print("PROXY_REMOTE_QUEUE_TEST_OK", flush=True)
    finally:
        # Preserve the allocation even when an assertion or timeout interrupts
        # the functional checks before the normal shutdown signal is sent.
        if not shutdown_sent:
            try:
                shutdown_remote_runtime(proxy, args.exit_path, args.remote_working_dir, args.process_timeout)
            except Exception:
                pass


if __name__ == "__main__":
    main()
