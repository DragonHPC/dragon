#!/usr/bin/env python3

import time
import argparse

from dragon import System, Policy, current
from dragon.native.process import Process as DragonProcess
from dragon.native.queue import Queue as DragonQueue

BURN_ITERS = 1


def _make_payload(msg_size, payload_mode):
    # bytes uses Queue's byte-like path; object exercises normal object
    # serialization while carrying the same-size bytearray payload.
    if payload_mode == "bytes":
        return bytes(msg_size)
    return {"payload": bytearray(msg_size)}


def worker(id, send_queue, recv_queue, result_queue, msg_size, total_iterations, payload_mode):
    my_msg = _make_payload(msg_size, payload_mode)

    start = 0
    if id == 0:
        for i in range(total_iterations + BURN_ITERS):
            if i == BURN_ITERS:
                # Exclude startup and initial transport setup from the result.
                start = time.perf_counter()

            # One forward and one reverse Queue operation make one round trip.
            send_queue.put(my_msg)
            recv_queue.get()

    else:
        for i in range(total_iterations + BURN_ITERS):
            if i == BURN_ITERS:
                start = time.perf_counter()

            # Return the message immediately so the peer measures round-trip time.
            recv_queue.get()
            send_queue.put(my_msg)

    avg_time = (time.perf_counter() - start) / total_iterations
    result_queue.put(avg_time)


def run_p2p_lat(iterations=100, max_msg_sz=1024, payload_mode="object"):
    system = System()
    sender_host = current().hostname

    sender_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=sender_host)

    if system.nnodes < 2:
        receiver_policy = sender_policy
        print("Warning: Less than two Dragon nodes available. Using the same host for sender and receiver.", flush=True)

    else:
        # Use a different host so every measured round trip crosses the network.
        receiver_policy = next((policy for policy in system.hostname_policies() if policy.host_name != sender_host), None)
        if receiver_policy is None:
            raise RuntimeError("The multi-node latency benchmark requires at least two Dragon nodes.")

    # Place every Queue on its consumer's host: worker 1 consumes forward_queue
    # on the receiver, worker 0 consumes reverse_queue on the sender, and this
    # parent process consumes result_queue on the sender.
    forward_queue = DragonQueue(maxsize=2, policy=receiver_policy)
    reverse_queue = DragonQueue(maxsize=2, policy=sender_policy)
    result_queue = DragonQueue(maxsize=2, policy=sender_policy)

    print(
        f"sender={sender_policy.host_name} receiver={receiver_policy.host_name} "
        f"forward_queue={receiver_policy.host_name} reverse_queue={sender_policy.host_name} "
        f"payload={payload_mode}",
        flush=True,
    )

    try:
        msg_sz = 2
        print("Msglen [B]   Lat [usec]", flush=True)
        while msg_sz <= max_msg_sz:
            # New workers per size keep payload allocation and warm-up behavior
            # independent across the size sweep.
            proc0 = DragonProcess(
                target=worker,
                args=(0, forward_queue, reverse_queue, result_queue, msg_sz, iterations, payload_mode),
                policy=sender_policy,
            )
            proc1 = DragonProcess(
                target=worker,
                args=(1, reverse_queue, forward_queue, result_queue, msg_sz, iterations, payload_mode),
                policy=receiver_policy,
            )

            proc0.start()
            proc1.start()

            # Both endpoints report a round-trip average; average them in usec.
            time_avg = (result_queue.get() + result_queue.get()) * 1e6 / 2

            proc0.join()
            proc1.join()

            print(f"{msg_sz}  {time_avg}", flush=True)

            msg_sz *= 2
    finally:
        # Destroy the benchmark-owned queues deterministically so repeated runs
        # do not wait for reference-count or garbage-collection cleanup.
        forward_queue.destroy()
        reverse_queue.destroy()
        result_queue.destroy()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="P2P latency test")

    parser.add_argument("--iterations", type=int, default=1000, help="number of iterations to do")

    parser.add_argument("--lg_max_message_size", type=int, default=4, help="log base 2 of size of message to pass in")

    parser.add_argument(
        "--payload",
        choices=("bytes", "object"),
        default="object",
        help="send raw bytes or an object that requires serialization",
    )

    my_args = parser.parse_args()

    run_p2p_lat(
        iterations=my_args.iterations,
        max_msg_sz=2**my_args.lg_max_message_size,
        payload_mode=my_args.payload,
    )
