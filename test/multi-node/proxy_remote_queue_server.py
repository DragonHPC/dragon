#!/usr/bin/env python3
"""Publish and hold a remote runtime for proxy integration tests.

The server creates the runtime descriptor before waiting on the shared exit
sentinel. Keeping publication and shutdown in this small process lets the
client suites concentrate on proxy behavior instead of embedding launcher
lifecycle code in every test.
"""

import argparse
import os
import time

import dragon.infrastructure.parameters as dparm
import dragon.workflows.runtime as runtime


def _write_exit_state(path, state):
    """Write a complete exit state before another node can observe it."""
    with open(path, "w", encoding="ascii") as exit_file:
        exit_file.write(state)
        exit_file.flush()
        os.fsync(exit_file.fileno())


def initialize_exit_signal(path):
    """Reset a stale shutdown marker before publishing the new runtime."""
    _write_exit_state(path, "waiting\n")


def wait_for_exit(path):
    """Wait for the client to request shutdown, then remove the shared marker.

    All runtime ranks observe the same file, but only rank zero removes it so
    teardown does not turn into a multi-writer race.
    """
    while True:
        try:
            with open(path, "r", encoding="ascii") as exit_file:
                if exit_file.read().strip() == "exit":
                    break
        except FileNotFoundError:
            pass
        time.sleep(0.2)

    if dparm.this_process.index == 0 and os.path.exists(path):
        os.remove(path)


def parse_args():
    """Parse the shared paths used to publish and stop the remote runtime."""
    parser = argparse.ArgumentParser(description="Publish a remote Dragon runtime for proxy queue tests")
    parser.add_argument("--runtime-name", type=str, required=True, help="Published runtime name")
    parser.add_argument("--publish-dir", type=str, required=True, help="Shared directory for runtime descriptor")
    parser.add_argument("--exit-path", type=str, required=True, help="Shared file used to signal runtime shutdown")
    return parser.parse_args()


def main():
    """Publish the runtime, wait for the client, and report clean shutdown."""
    args = parse_args()
    initialize_exit_signal(args.exit_path)
    # The descriptor and exit sentinel must live where both the launcher ranks
    # and the client can access them; the test harness supplies that shared
    # directory explicitly rather than relying on each node's home cache.
    sdesc = runtime.publish(args.runtime_name, publish_dir=args.publish_dir)
    print(
        f"REMOTE_RUNTIME_PUBLISHED name={args.runtime_name} publish_dir={args.publish_dir} bytes={len(sdesc)}",
        flush=True,
    )
    wait_for_exit(args.exit_path)
    print("REMOTE_RUNTIME_EXIT_OK", flush=True)


if __name__ == "__main__":
    main()
