"""
ext_attach_publisher.py — Python "head" side of the dragon_ext_attach() demo.

This file is itself the process launched BY ``dragon -a`` (the ``--attachable``
launcher flag; see ``ATTACHABLE_HELP`` in ``src/dragon/launcher/launchargs.py``
for the exact semantics). It is NOT analogous to ``client.py``, which is an
external process that attaches to a runtime started some other way. It is
analogous to how ``server.py``/``_runtime_head.py`` are the process that IS
the runtime's head in the other demos in this directory.

This demo shows a standalone, native C++ executable
(``ext_attach_client``, built from ``ext_attach_client.cpp``) attaching to a
running Dragon runtime via the C ``dragon_ext_attach()``/``dragon_ext_detach()``
bootstrap API (``src/include/dragon/ext_attach.h``), and interoperating with
``dragon.native.queue.Queue`` objects created by this Python process, entirely
through the Local Services key-value store (``dragon.utils.set_local_kv()`` /
``get_local_kv()`` on the Python side; ``dragon_ls_set_kv()`` /
``dragon_ls_get_kv()`` on the C++ side). No Global Services connection or
gateway channels are needed by the C++ side for this -- it only ever talks to
two ``Queue`` FLIs it attaches to directly from their serialized descriptors.

Usage (two-terminal / manual):

    # Terminal 1:
    export DRAGON_ATTACH_TOKEN=<some-secret-string>
    dragon -a ext_attach_publisher.py

    # Terminal 2 (once Terminal 1 prints that it is blocked on queue2.get()):
    DRAGON_ATTACH_TOKEN=<same-secret-string> ./ext_attach_client

Or, scripted end-to-end, see ``run_ext_attach_demo.sh`` in this directory,
which generates the token, launches this file via ``dragon -a`` in the
background, waits for the runtime to come up, then runs the compiled
``ext_attach_client`` binary against it.

Workload:

1. Create two managed ``Queue`` objects, ``queue1`` and ``queue2``.
2. Put the first line of ``OPENING_SCROLL`` onto ``queue1`` as raw ``bytes``
   (NOT ``str``) so that ``Queue.put()`` takes its direct-bytes fast path
   (see ``_direct_payload()`` / ``_DIRECT_BYTES_HINT`` in
   ``src/dragon/native/queue.py``) -- this is the only Queue wire format
   simple enough for a plain C++ client (which has no cloudpickle/pickle
   decoder) to consume directly via ``dragon_fli_recv_bytes()``.
3. Serialize both queues (``Queue.serialize()``, already documented there as
   intended for C++ attach) and publish the two base64 strings to the LS
   key-value store under fixed, namespaced keys.
4. Write a small readiness sentinel file in the launch CWD (see ``READY_FILE``
    below) so that a driver script knows it is now safe to run the C++ client.
5. Block (no timeout) on ``queue2.get()``, waiting for the external C++
   process to read ``queue1`` and reply on ``queue2``.
6. Decode and assert the reply equals ``OPENING_SCROLL[1]``.
"""

import argparse
import dragon.utils as dutils
import socket
import statistics
import time
import os

import dragon  # noqa: F401 -- required to be running under the Dragon runtime
from dragon.native.machine import System
from dragon.native.queue import Queue
from dragon.native.pool import Pool
from dragon.native.process import Process
import dragon.utils as dutils

from scroll import OPENING_SCROLL

# Namespaced LS key-value store keys used to publish the serialized queues.
# These MUST match, verbatim, the key strings hardcoded in
# ext_attach_client.cpp.
QUEUE1_KV_KEY = "outerspace_proc.ext_attach.queue1"
QUEUE2_KV_KEY = "outerspace_proc.ext_attach.queue2"

# Readiness sentinel file in launch CWD: written only after both Queues are
# published to the LS key-value store. A driver script can safely watch this
# path from the launch node even when runtime-local /tmp paths are remote.
READY_FILE = os.path.join(os.getcwd(), "ext_attach_publisher_ready")
SRUN_PROCS_PER_NODE = 128


def _parse_args():
    parser = argparse.ArgumentParser(description="Publish Dragon queues for external attach demo")
    parser.add_argument(
        "--srun",
        action="store_true",
        help=("Enable multi-rank mode for srun: queue1 writes and queue2 reads are " "scaled to 128 * System().nnodes"),
    )
    return parser.parse_args()


def deposit_queues(args):

    queue1, queue2 = args

    dutils.set_local_kv(QUEUE1_KV_KEY, queue1.serialize())
    dutils.set_local_kv(QUEUE2_KV_KEY, queue2.serialize())
    print(f"[ext_attach_publisher] {socket.gethostname()}: Published queue1 -> key {QUEUE1_KV_KEY!r}", flush=True)
    print(f"[ext_attach_publisher] {socket.gethostname()}: Published queue2 -> key {QUEUE2_KV_KEY!r}", flush=True)
    time.sleep(5)


def disseminate_queues(queue1, queue2):
    """Disseminate the given queues across the Dragon runtime using a pool. Should probably work on a way to make this easier. Or just use a DDict, access that and then access keys in that."""
    hostname_policies = System().hostname_policies()
    pool = Pool(policy=hostname_policies, processes_per_policy=1, maxtasksperchild=1)
    _ = pool.map_async(deposit_queues, [(queue1, queue2)] * len(hostname_policies)).get()
    pool.close()
    pool.join()


def writer(queue1, n_msgs):
    # Must be bytes (not str) so Queue.put() takes the direct-payload fast
    # path (_direct_payload() in queue.py only recognizes bytes/bytearray
    # with the default cloudpickle pickler) -- this is the only wire format
    # a plain C++ client can decode without a pickle implementation.
    payload = OPENING_SCROLL[0].encode("utf-8")
    for _ in range(n_msgs):
        queue1.put(payload)


def _parse_attach_ms(msg):
    """Extract the ``attach_ms=<float>`` value from a SRUN_ATTACH_TIMING message."""
    for token in msg.split():
        if token.startswith("attach_ms="):
            return float(token.split("=", 1)[1])
    raise ValueError(f"No attach_ms field found in message: {msg!r}")


def write_timing_stats(nnodes, attach_times):
    """Compute summary stats over attach_times (ms) and write them to a
    per-node-count txt file consumed by attach_delay_analysis.py."""
    sorted_times = sorted(attach_times)
    if len(sorted_times) > 1:
        q1, _, q3 = statistics.quantiles(sorted_times, n=4)
    else:
        q1 = q3 = sorted_times[0]
    stats = {
        "nodes": nnodes,
        "count": len(sorted_times),
        "min": min(sorted_times),
        "q1": q1,
        "median": statistics.median(sorted_times),
        "mean": statistics.mean(sorted_times),
        "q3": q3,
        "max": max(sorted_times),
    }
    out_path = os.path.join(os.getcwd(), f"attach_delay_nnodes_{nnodes}.txt")
    with open(out_path, "w") as f:
        for key, value in stats.items():
            f.write(f"{key}={value}\n")
        f.write("values=" + ",".join(repr(t) for t in sorted_times) + "\n")
    print(f"[ext_attach_publisher] Wrote timing stats -> {out_path!r}", flush=True)
    return out_path


def main():
    args = _parse_args()

    n_msgs = 1
    if args.srun:
        nnodes = System().nnodes
        n_msgs = nnodes * SRUN_PROCS_PER_NODE
        print(
            "[ext_attach_publisher] SRUN mode enabled: "
            f"nnodes={nnodes}, ntasks_per_node={SRUN_PROCS_PER_NODE}, total_msgs={n_msgs}",
            flush=True,
        )

    queue1 = Queue()
    queue2 = Queue()

    disseminate_queues(queue1, queue2)
    # Both KV keys are published now, so it is safe for a driver script to
    # run the C++ client. Write the readiness sentinel only now, after both
    # set_local_kv() calls above have returned.
    with open(READY_FILE, "w") as f:
        f.write("ready\n")
    print(f"[ext_attach_publisher] READY: wrote sentinel file {READY_FILE!r}", flush=True)

    # Launch a writer process to populate queue1
    writer_proc = Process(target=writer, args=(queue1, n_msgs))
    writer_proc.start()

    if args.srun:
        print(
            "[ext_attach_publisher] Blocking on queue2.get() for " f"{n_msgs} timing payloads from srun ranks...",
            flush=True,
        )
        n_timing_msgs = 0
        attach_times = []
        for idx in range(n_msgs):
            msg = queue2.get().decode("utf-8")
            if msg.startswith("SRUN_ATTACH_TIMING"):
                n_timing_msgs += 1
                attach_times.append(_parse_attach_ms(msg))
            if idx < 3:
                print(f"[ext_attach_publisher] sample[{idx}]={msg}", flush=True)
        assert n_timing_msgs == n_msgs, (
            "Unexpected queue2 payload count in --srun mode: " f"timing_msgs={n_timing_msgs}, expected={n_msgs}"
        )
        print(
            "[ext_attach_publisher] Received all timing payloads " f"({n_timing_msgs}/{n_msgs}).",
            flush=True,
        )
        write_timing_stats(nnodes, attach_times)
    else:
        print(
            "[ext_attach_publisher] Blocking on queue2.get() (no timeout) waiting for the external C++ process...",
            flush=True,
        )
        second_part = queue2.get()

        second_part = second_part.decode("utf-8")
        print(f"[ext_attach_publisher] Received from C++ client via queue2: {second_part!r}", flush=True)

        assert (
            second_part == OPENING_SCROLL[1]
        ), f"Queue value mismatch!\n  got:      {second_part!r}\n  expected: {OPENING_SCROLL[1]!r}"
        print("[ext_attach_publisher] Assertion passed -- C++ <-> Python queue round-trip verified.", flush=True)
    writer_proc.join()


if __name__ == "__main__":
    main()
