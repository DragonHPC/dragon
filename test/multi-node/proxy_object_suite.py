#!/usr/bin/env python3
"""Reusable higher-level object coverage for a remote Dragon proxy.

The suite complements the directional relay matrix. It checks that common
objects can be created while the proxy selects the remote runtime, that remote
workers really run on the expected hosts, and that results remain consumable
after the client restores its local runtime context.
"""

import cloudpickle
import multiprocessing as mp
import os
import socket
import sys

from dragon.data.ddict import DDict
from dragon.globalservices.node import get_list as node_get_list
from dragon.infrastructure.policy import Policy
from dragon.native.process import Process, ProcessTemplate
from dragon.native.process_group import ProcessGroup

# The worker functions are serialized by value so the remote runtime can run
# this suite even when the client module is not importable from the same path.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def signal_exit(path):
    """Request remote-runtime teardown through the shared sentinel file."""
    with open(path, "w", encoding="ascii") as exit_file:
        exit_file.write("exit\n")
        exit_file.flush()
        os.fsync(exit_file.fileno())


def build_report(payload, sequence):
    """Return a payload plus enough provenance to verify remote execution."""
    return {
        "payload": payload,
        "sequence": sequence,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
    }


def _join_remote_process(remote_proc, timeout, label):
    """Require a remote worker to finish before leaving its proxy context."""
    remote_proc.join(timeout)
    if remote_proc.exitcode is None:
        raise RuntimeError(f"timed out waiting for {label}")
    if remote_proc.exitcode != 0:
        raise RuntimeError(f"{label} exited with code {remote_proc.exitcode}")


def shutdown_remote_runtime(proxy, exit_path, remote_working_dir, timeout):
    """Signal shutdown without performing another Dragon control-plane call.

    The unused arguments are kept for a common call signature across client
    drivers. A plain shared-file write is deliberate: teardown must still work
    when the proxy is already being unwound after a test failure.
    """
    del proxy
    del remote_working_dir
    del timeout
    signal_exit(exit_path)


def remote_writer(queue, payload, sequence):
    """Run in the remote runtime and report where the worker executed."""
    queue.put(build_report(payload, sequence))


def remote_ddict_writer(ddict, key, payload, sequence):
    """Write one provenance-bearing value into a remote DDict and detach."""
    ddict[key] = build_report(payload, sequence)
    ddict.detach()


def remote_echo_worker(request_queue, reply_queue, expected_payload, expected_sequence, queue_timeout):
    """Verify a local-to-remote request and send a remote-to-local reply."""
    # Runs in the remote runtime: read the request the local client sent over the
    # mesh (local -> remote), verify it, then echo a report back (remote -> local).
    request = request_queue.get(timeout=queue_timeout)
    if request.get("payload") != expected_payload or request.get("sequence") != expected_sequence:
        raise AssertionError(f"remote echo worker received unexpected request: {request!r}")

    reply_queue.put(build_report(expected_payload, expected_sequence))


def verify_message(msg, expected_payload, expected_sequence, expected_remote_hosts):
    """Validate payload integrity and, when requested, remote host placement."""
    if msg.get("payload") != expected_payload:
        raise AssertionError(f"unexpected payload: {msg.get('payload')!r}")

    if msg.get("sequence") != expected_sequence:
        raise AssertionError(f"unexpected sequence: {msg.get('sequence')!r}")

    remote_hostname = msg.get("hostname")
    if not remote_hostname:
        raise AssertionError("remote hostname was not reported by the writer")

    if expected_remote_hosts and remote_hostname not in expected_remote_hosts:
        raise AssertionError(
            f"remote writer hostname {remote_hostname!r} was not found in expected remote hosts {sorted(expected_remote_hosts)!r}"
        )


def exercise_queue_process(
    proxy, remote_working_dir, process_timeout, queue_timeout, payload, sequence, expected_hosts
):
    """Exercise a remote process and consume its queue after proxy restoration."""
    with proxy:
        # The queue is constructed while the proxy selects the remote runtime;
        # its local backing pool is an intentional node-local exception. The get
        # below happens after context restoration and tests the proxy object
        # boundary rather than only a remote worker.
        queue = mp.Queue(maxsize=1)
        remote_proc = Process(
            target=remote_writer,
            args=(queue, payload, sequence),
            cwd=remote_working_dir,
        )
        remote_proc.start()
        _join_remote_process(remote_proc, process_timeout, "remote writer process")

    msg = queue.get(timeout=queue_timeout)
    verify_message(msg, payload, sequence, expected_hosts)
    return msg


def _ddict_node_count(expected_hosts):
    """Choose a small, allocation-independent DDict footprint for the test."""
    try:
        available_nodes = len(node_get_list())
    except Exception:
        available_nodes = 1

    if expected_hosts:
        available_nodes = min(available_nodes, len(expected_hosts))

    return max(1, min(2, available_nodes))


def exercise_ddict(proxy, remote_working_dir, process_timeout, payload, sequence, expected_hosts):
    """Create, write, read, and destroy a DDict through the remote runtime."""
    ddict = None
    remote_proc = None
    key = f"proxy-ddict-key-{sequence}"
    ddict_payload = f"{payload}-ddict"
    ddict_sequence = sequence + 100

    with proxy:
        # Limit the DDict to two nodes so the test checks distributed placement
        # without making its memory footprint depend on allocation size.
        ddict_nodes = _ddict_node_count(expected_hosts)
        ddict = DDict(
            1,
            ddict_nodes,
            ddict_nodes * 64 * 1024 * 1024,
            wait_for_keys=True,
            working_set_size=2,
        )

        try:
            remote_proc = Process(
                target=remote_ddict_writer,
                args=(ddict, key, ddict_payload, ddict_sequence),
                cwd=remote_working_dir,
            )
            remote_proc.start()
            _join_remote_process(remote_proc, process_timeout, "remote ddict writer")

            msg = ddict[key]
            verify_message(msg, ddict_payload, ddict_sequence, expected_hosts)
        finally:
            if ddict is not None:
                ddict.destroy()


def _process_group_expectations(result_queue, payload, sequence, remote_working_dir, expected_hosts):
    """Build host-pinned or allocation-portable ProcessGroup expectations."""
    selected_hosts = sorted(expected_hosts)[:2]
    expectations = []

    if selected_hosts:
        # When the launcher tells us the remote host set, pin one worker to each
        # of two hosts so a passing result proves placement as well as routing.
        for offset, host in enumerate(selected_hosts):
            worker_payload = f"{payload}-process-group-{offset}"
            worker_sequence = sequence + 200 + offset
            worker_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=host)
            template = ProcessTemplate(
                target=remote_writer,
                args=(result_queue, worker_payload, worker_sequence),
                cwd=remote_working_dir,
                policy=worker_policy,
            )
            expectations.append((1, template, worker_payload, worker_sequence, {host}))
    else:
        # The fallback keeps the suite useful outside the Slurm wrapper, where
        # host names may not be available to the client.
        worker_payload = f"{payload}-process-group"
        worker_sequence = sequence + 200
        template = ProcessTemplate(
            target=remote_writer,
            args=(result_queue, worker_payload, worker_sequence),
            cwd=remote_working_dir,
        )
        expectations.append((2, template, worker_payload, worker_sequence, expected_hosts))

    return expectations


def exercise_process_group(
    proxy, remote_working_dir, process_timeout, queue_timeout, payload, sequence, expected_hosts
):
    """Run a remote ProcessGroup and verify every worker result locally."""
    result_queue = None
    pg = None

    with proxy:
        result_queue = mp.Queue(maxsize=max(2, len(expected_hosts) or 2))
        expectations = _process_group_expectations(result_queue, payload, sequence, remote_working_dir, expected_hosts)
        worker_count = sum(nproc for nproc, *_ in expectations)
        expected_by_payload = {}
        repeated_expectations = []

        for nproc, template, worker_payload, worker_sequence, worker_hosts in expectations:
            expected_by_payload[worker_payload] = (worker_sequence, worker_hosts)
            repeated_expectations.extend([(worker_payload, worker_sequence, worker_hosts)] * nproc)

        pg = ProcessGroup(restart=False)
        for nproc, template, *_ in expectations:
            pg.add_process(nproc, template)

        try:
            pg.init()
            pg.start()
            pg.join(timeout=process_timeout)
        finally:
            # Close first so normal completion does not leave process-group
            # resources behind; stop is the bounded fallback for a failed join.
            if pg is not None:
                try:
                    pg.close()
                except Exception:
                    try:
                        pg.stop(patience=1.0)
                    except Exception:
                        pass

    # Collect and verify results after leaving the proxy context, mirroring the
    # queue/process phase: the result queue is drained by the local client, not
    # inside the remote proxy context.
    msgs = [result_queue.get(timeout=queue_timeout) for _ in range(worker_count)]

    if len(expected_by_payload) == worker_count:
        for msg in msgs:
            worker_payload = msg.get("payload")
            if worker_payload not in expected_by_payload:
                raise AssertionError(f"unexpected process-group payload: {worker_payload!r}")
            worker_sequence, worker_hosts = expected_by_payload.pop(worker_payload)
            verify_message(msg, worker_payload, worker_sequence, worker_hosts)
        if expected_by_payload:
            raise AssertionError(f"missing process-group payloads: {sorted(expected_by_payload)!r}")
    else:
        for msg, (worker_payload, worker_sequence, worker_hosts) in zip(msgs, repeated_expectations):
            verify_message(msg, worker_payload, worker_sequence, worker_hosts)


def exercise_roundtrip_echo(
    proxy, remote_working_dir, process_timeout, queue_timeout, payload, sequence, expected_hosts
):
    """Verify a request/reply crossing in both directions of the mesh."""
    echo_payload = f"{payload}-roundtrip"
    echo_sequence = sequence + 300

    with proxy:
        request_queue = mp.Queue(maxsize=1)
        reply_queue = mp.Queue(maxsize=1)

        remote_proc = Process(
            target=remote_echo_worker,
            args=(request_queue, reply_queue, echo_payload, echo_sequence, queue_timeout),
            cwd=remote_working_dir,
        )
        remote_proc.start()

        # Drive the local -> remote direction: the local client puts the request
        # into the remote-owned queue, which reaches the remote worker over the
        # mesh. The worker verifies it and echoes a report back (remote -> local).
        request_queue.put({"payload": echo_payload, "sequence": echo_sequence})
        _join_remote_process(remote_proc, process_timeout, "remote echo worker")

    msg = reply_queue.get(timeout=queue_timeout)
    verify_message(msg, echo_payload, echo_sequence, expected_hosts)
    return msg


def run_proxy_object_suite(
    proxy, remote_working_dir, process_timeout, queue_timeout, payload, sequence, expected_hosts
):
    """Run the queue, DDict, ProcessGroup, and round-trip object checks."""
    print("Running proxy queue/process coverage...", flush=True)
    queue_msg = exercise_queue_process(
        proxy,
        remote_working_dir,
        process_timeout,
        queue_timeout,
        payload,
        sequence,
        expected_hosts,
    )
    print(
        f"Verified queue/process payload from remote writer host={queue_msg['hostname']} pid={queue_msg['pid']} sequence={queue_msg['sequence']}",
        flush=True,
    )

    print("Running proxy DDict coverage...", flush=True)
    exercise_ddict(proxy, remote_working_dir, process_timeout, payload, sequence, expected_hosts)
    print("Verified proxy DDict coverage", flush=True)

    print("Running proxy ProcessGroup coverage...", flush=True)
    exercise_process_group(proxy, remote_working_dir, process_timeout, queue_timeout, payload, sequence, expected_hosts)
    print("Verified proxy ProcessGroup coverage", flush=True)

    print("Running proxy round-trip echo coverage...", flush=True)
    exercise_roundtrip_echo(
        proxy,
        remote_working_dir,
        process_timeout,
        queue_timeout,
        payload,
        sequence,
        expected_hosts,
    )
    print("Verified proxy round-trip echo coverage", flush=True)
