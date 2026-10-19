#!/usr/bin/env python3
"""Cross-runtime relay symmetry matrix.

This suite exercises the transport relay between two runtimes across the full
matrix that the ad-hoc proxy tests only touched by accident:

    direction  x  size regime  x  operation shape

- direction:  payload flows remote->local AND local->remote
- size regime: eager (< HSTA rendezvous threshold) AND rendezvous (>> threshold)
- operation shape: one-way push (send + get) AND request->response round trip

Rationale: the cross-cluster relay had several directional / message-kind gaps
(most painfully, a server->local rendezvous payload that stranded at the relay
head). A one-way test in a single direction with small messages could not have
caught them. This matrix makes every combination an explicit, named case so a
regression fails fast and points at the exact cell.

The functional cases run both with the single-cluster relay mock and against a
genuine two-cluster deployment. A real deployment must still be validated with
deployment-specific observability before adding a test for physical network
unreachability; this matrix intentionally contains only executable cases.

The launcher and client driver own runtime discovery and teardown. This module
stays focused on channel behavior so the same matrix can be reused by a local
relay test and by the Slurm two-runtime harness.
"""

import cloudpickle
import enum
import hashlib
import multiprocessing as mp
import os
import sys

from dragon.native.process import Process

# The worker callables live in this test module. Pickling the module by value
# lets remote Dragon workers execute the same definitions without depending on
# an importable path on the remote runtime's filesystem.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


# HSTA switches from inline "eager" delivery to "rendezvous" (separate,
# specially-allocated landing buffer) above HSTA_RNDV_THRESHOLD, which is
# HSTA_EAGER_BUF_MAX_BYTES (0x800 = 2048) minus a small header reserve (~1984 B).
# Keep the eager payload comfortably under that once serialized, and make the
# rendezvous payload unambiguously over it.
_EAGER_PAYLOAD_BYTES = 32
_RNDV_PAYLOAD_BYTES = 128 * 1024


class Direction(enum.Enum):
    """The direction of the first payload crossing the runtime boundary."""

    REMOTE_TO_LOCAL = "remote->local"
    LOCAL_TO_REMOTE = "local->remote"
    ROUND_TRIP = "round-trip"


class SizeRegime(enum.Enum):
    """The HSTA delivery path selected by the payload size."""

    EAGER = "eager"
    RNDV = "rndv"


def _make_payload(nbytes):
    """A deterministic, verifiable byte payload of exactly ``nbytes``."""
    base = b"relay-matrix-payload-"
    reps = (nbytes // len(base)) + 1
    return (base * reps)[:nbytes]


def _checksum(data):
    return hashlib.sha256(data).hexdigest()


# --- remote-side workers (run inside the remote runtime) ---------------------


def _remote_push(out_queue, nbytes, expected_checksum):
    """Remote runtime: push a payload so it crosses remote->local to the client."""
    payload = _make_payload(nbytes)
    assert _checksum(payload) == expected_checksum, "remote payload generation mismatch"
    out_queue.put({"payload": payload, "checksum": expected_checksum, "nbytes": nbytes})


def _remote_pull_verify(in_queue, result_queue, expected_checksum, timeout):
    """Remote runtime: pull a client payload (local->remote) and verify it."""
    msg = in_queue.get(timeout=timeout)
    payload = msg.get("payload", b"")
    ok = _checksum(payload) == expected_checksum and msg.get("checksum") == expected_checksum
    result_queue.put({"ok": ok, "got_bytes": len(payload), "reported_checksum": msg.get("checksum")})


def _remote_echo(in_queue, out_queue, expected_checksum, reply_nbytes, reply_checksum, timeout):
    """Remote runtime: request->response. Verify the inbound request payload
    (local->remote), then echo a reply payload back (remote->local)."""
    msg = in_queue.get(timeout=timeout)
    payload = msg.get("payload", b"")
    request_ok = _checksum(payload) == expected_checksum and msg.get("checksum") == expected_checksum

    reply_payload = _make_payload(reply_nbytes)
    out_queue.put(
        {
            "request_ok": request_ok,
            "got_bytes": len(payload),
            "payload": reply_payload,
            "checksum": reply_checksum,
            "nbytes": reply_nbytes,
        }
    )


# --- case runners ------------------------------------------------------------


def _join(proc, timeout, label):
    """Join a worker before its proxy context is restored to the local runtime."""
    proc.join(timeout)
    if proc.exitcode is None:
        raise RuntimeError(f"timed out waiting for {label}")
    if proc.exitcode != 0:
        raise RuntimeError(f"{label} exited with code {proc.exitcode}")


def _run_remote_to_local(proxy, remote_working_dir, nbytes, timeout):
    """Send a payload from a remote worker and verify it locally."""
    payload = _make_payload(nbytes)
    checksum = _checksum(payload)

    with proxy:
        queue = mp.Queue(maxsize=1)
        proc = Process(
            target=_remote_push,
            args=(queue, nbytes, checksum),
            cwd=remote_working_dir,
        )
        proc.start()
        _join(proc, timeout, "remote push worker")

    msg = queue.get(timeout=timeout)
    got = msg.get("payload", b"")
    if len(got) != nbytes:
        raise AssertionError(f"remote->local length mismatch: expected {nbytes}, got {len(got)}")
    if _checksum(got) != checksum or msg.get("checksum") != checksum:
        raise AssertionError("remote->local payload checksum mismatch (corruption across relay)")


def _run_local_to_remote(proxy, remote_working_dir, nbytes, timeout):
    """Send a payload locally and verify it in a remote worker."""
    payload = _make_payload(nbytes)
    checksum = _checksum(payload)

    with proxy:
        in_queue = mp.Queue(maxsize=1)
        result_queue = mp.Queue(maxsize=1)
        proc = Process(
            target=_remote_pull_verify,
            args=(in_queue, result_queue, checksum, timeout),
            cwd=remote_working_dir,
        )
        proc.start()
        # local -> remote: the client sends the payload to the remote-owned queue.
        in_queue.put({"payload": payload, "checksum": checksum, "nbytes": nbytes})
        _join(proc, timeout, "remote pull/verify worker")

    result = result_queue.get(timeout=timeout)
    if not result.get("ok"):
        raise AssertionError(
            f"local->remote verification failed: got_bytes={result.get('got_bytes')} "
            f"reported_checksum={result.get('reported_checksum')} expected={checksum}"
        )
    if result.get("got_bytes") != nbytes:
        raise AssertionError(f"local->remote length mismatch: expected {nbytes}, got {result.get('got_bytes')}")


def _run_round_trip(proxy, remote_working_dir, nbytes, timeout):
    """Send a request to a remote worker and verify its reply locally."""
    request_payload = _make_payload(nbytes)
    request_checksum = _checksum(request_payload)
    reply_checksum = request_checksum  # same size/content both ways for this cell

    with proxy:
        in_queue = mp.Queue(maxsize=1)
        out_queue = mp.Queue(maxsize=1)
        proc = Process(
            target=_remote_echo,
            args=(in_queue, out_queue, request_checksum, nbytes, reply_checksum, timeout),
            cwd=remote_working_dir,
        )
        proc.start()
        in_queue.put({"payload": request_payload, "checksum": request_checksum, "nbytes": nbytes})
        _join(proc, timeout, "remote echo worker")

    reply = out_queue.get(timeout=timeout)
    if not reply.get("request_ok"):
        raise AssertionError(
            f"round-trip request (local->remote) failed verification: got_bytes={reply.get('got_bytes')}"
        )
    got = reply.get("payload", b"")
    if len(got) != nbytes or _checksum(got) != reply_checksum or reply.get("checksum") != reply_checksum:
        raise AssertionError("round-trip reply (remote->local) payload checksum mismatch")


_SIZE_BYTES = {
    SizeRegime.EAGER: _EAGER_PAYLOAD_BYTES,
    SizeRegime.RNDV: _RNDV_PAYLOAD_BYTES,
}

_DIRECTION_RUNNERS = {
    Direction.REMOTE_TO_LOCAL: _run_remote_to_local,
    Direction.LOCAL_TO_REMOTE: _run_local_to_remote,
    Direction.ROUND_TRIP: _run_round_trip,
}


class RelayCase:
    """One named cell in the direction x size matrix.

    Keeping the matrix declarative makes it difficult to add a new direction or
    size regime without also adding an explicit, reported test case.
    """

    def __init__(self, direction, size, name=None, runner=None):
        self.direction = direction
        self.size = size
        self.name = name or f"{direction.value}__{size.value}"
        self._runner = runner

    def run(self, proxy, remote_working_dir, timeout):
        """Dispatch this case's direction-specific runner."""
        runner = self._runner or _DIRECTION_RUNNERS[self.direction]
        nbytes = _SIZE_BYTES[self.size] if self.size is not None else 0
        runner(proxy, remote_working_dir, nbytes, timeout)


# The matrix. Every functional cell runs under the relay mock, a real two-
# cluster deployment, and a same-cluster direct proxy deployment. The runners
# enter the proxy separately for each cell so enable/disable transitions and
# reuse of one Proxy instance are covered as well as the payload direction.
RELAY_MATRIX = [
    RelayCase(Direction.REMOTE_TO_LOCAL, SizeRegime.EAGER),
    RelayCase(Direction.REMOTE_TO_LOCAL, SizeRegime.RNDV),
    RelayCase(Direction.LOCAL_TO_REMOTE, SizeRegime.EAGER),
    RelayCase(Direction.LOCAL_TO_REMOTE, SizeRegime.RNDV),
    RelayCase(Direction.ROUND_TRIP, SizeRegime.EAGER),
    RelayCase(Direction.ROUND_TRIP, SizeRegime.RNDV),
]


def run_relay_matrix(proxy, remote_working_dir, timeout):
    """Run every relay matrix case and return the passed-case count.

    Raises on the first failing case so the launcher exits non-zero and the log
    names the exact cell that regressed.
    """
    print("Relay matrix:", flush=True)

    passed = 0
    for case in RELAY_MATRIX:
        print(f"  RUN   {case.name} ...", flush=True)
        case.run(proxy, remote_working_dir, timeout)
        passed += 1
        print(f"  PASS  {case.name}", flush=True)

    print(f"Relay matrix complete: passed={passed}", flush=True)
    return passed
