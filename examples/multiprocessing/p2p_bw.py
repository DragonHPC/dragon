#!/usr/bin/env python3

import time
import argparse

from dragon import Policy, System, current
from dragon.native.process import Process as DragonProcess
from dragon.native.queue import Queue as DragonQueue

BURN_ITERS = 2
WINDOW = 32


def _make_payload(msg_size, payload_mode):
    # bytes uses Queue's byte-like path; object exercises normal object
    # serialization while carrying the same-size bytearray payload.
    if payload_mode == "bytes":
        return bytes(msg_size)
    return {"payload": bytearray(msg_size)}


def worker(id, send_queue, recv_queue, result_queue, msg_size, total_iterations, payload_mode):
    start = 0
    if id == 0:
        my_msg = _make_payload(msg_size, payload_mode)
        for i in range(total_iterations + BURN_ITERS):
            if i == BURN_ITERS:
                # Exclude startup and initial transport setup from the result.
                start = time.perf_counter()

            # Fill a bounded in-flight window, then wait for one acknowledgement.
            for _ in range(WINDOW):
                send_queue.put(my_msg)
            recv_queue.get()

    else:
        my_msg = bytes(8)
        for i in range(total_iterations + BURN_ITERS):
            if i == BURN_ITERS:
                start = time.perf_counter()

            # Drain the sender's full window before releasing the next window.
            for _ in range(WINDOW):
                recv_queue.get()
            send_queue.put(my_msg)

    avg_time = (time.perf_counter() - start) / total_iterations
    bw = msg_size * WINDOW / (2**20 * avg_time)
    if id == 0:
        result_queue.put(bw)


def run_p2p_bw(iterations=100, max_msg_sz=1024, payload_mode="object"):
    sender_host = current().hostname
    # Use a different host so every measured payload crosses the network.
    receiver_policy = next((policy for policy in System().hostname_policies() if policy.host_name != sender_host), None)
    if receiver_policy is None:
        raise RuntimeError("The bandwidth benchmark requires at least two Dragon nodes.")

    sender_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=sender_host)
    # Place every Queue on its consumer's host: worker 1 consumes data_queue on
    # the receiver, worker 0 consumes ack_queue on the sender, and this parent
    # process consumes result_queue on the sender.
    data_queue = DragonQueue(maxsize=WINDOW, policy=receiver_policy)
    ack_queue = DragonQueue(maxsize=2, policy=sender_policy)
    result_queue = DragonQueue(maxsize=2, policy=sender_policy)

    print(
        f"sender={sender_policy.host_name} receiver={receiver_policy.host_name} "
        f"data_queue={receiver_policy.host_name} ack_queue={sender_policy.host_name} "
        f"payload={payload_mode}",
        flush=True,
    )

    try:
        msg_sz = 2
        print("Msglen [B]   BW [MiB/s]")
        while msg_sz <= max_msg_sz:
            # New workers per size keep payload allocation and warm-up behavior
            # independent across the size sweep.
            proc0 = DragonProcess(
                target=worker,
                args=(0, data_queue, ack_queue, result_queue, msg_sz, iterations, payload_mode),
                policy=sender_policy,
            )
            proc1 = DragonProcess(
                target=worker,
                args=(1, ack_queue, data_queue, result_queue, msg_sz, iterations, payload_mode),
                policy=receiver_policy,
            )

            proc0.start()
            proc1.start()

            bw = result_queue.get()

            proc0.join()
            proc1.join()

            print(f"{msg_sz}  {bw}")

            msg_sz *= 2
    finally:
        # Destroy the benchmark-owned queues deterministically so repeated runs
        # do not wait for reference-count or garbage-collection cleanup.
        data_queue.destroy()
        ack_queue.destroy()
        result_queue.destroy()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="P2P bandwidth test")

    parser.add_argument("--iterations", type=int, default=100, help="number of iterations to do")

    parser.add_argument("--lg_max_message_size", type=int, default=4, help="log base 2 of size of message to pass in")

    parser.add_argument(
        "--payload",
        choices=("bytes", "object"),
        default="object",
        help="send raw bytes or an object that requires serialization",
    )

    my_args = parser.parse_args()

    run_p2p_bw(
        iterations=my_args.iterations,
        max_msg_sz=2**my_args.lg_max_message_size,
        payload_mode=my_args.payload,
    )
