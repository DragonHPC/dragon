#!/usr/bin/env python3

import dragon
import multiprocessing as mp
import time
import os
import json

from dragon.native.machine import System, Node

BURN_ITERS = 1

DATA_TXT_FILE = "tests_hsta_reboot.txt"

def worker_conn(id, send_link, recv_link, result_link, msg_size, total_iterations, use_bytes):

    my_msg = bytearray(msg_size)

    start = 0
    if id == 0:
        for i in range(total_iterations + BURN_ITERS):
            if i == (BURN_ITERS + 1):
                start = time.perf_counter()

            if use_bytes:
                send_link.send_bytes(my_msg)
                msg = recv_link.recv_bytes()
            else:
                send_link.send(my_msg)
                msg = recv_link.recv()

    else:
        for i in range(total_iterations + BURN_ITERS):
            if i == (BURN_ITERS + 1):
                start = time.perf_counter()

            if use_bytes:
                msg = recv_link.recv_bytes()
                send_link.send_bytes(my_msg)
            else:
                msg = recv_link.recv()
                send_link.send(my_msg)

    avg_time = (time.perf_counter() - start) / total_iterations
    result_link.send(avg_time)


def worker_queue(id, send_link, recv_link, result_link, msg_size, total_iterations):

    my_msg = bytearray(msg_size)

    start = 0
    if id == 0:
        for i in range(total_iterations + BURN_ITERS):
            if i == (BURN_ITERS + 1):
                start = time.perf_counter()

            send_link.put(my_msg)
            msg = recv_link.get()

    else:
        for i in range(total_iterations + BURN_ITERS):
            if i == (BURN_ITERS + 1):
                start = time.perf_counter()

            msg = recv_link.get()
            send_link.put(my_msg)

    avg_time = (time.perf_counter() - start) / total_iterations
    result_link.send(avg_time)


def run_p2p_lat(iterations=100, max_msg_sz=1024, use_bytes=False, with_queues=False):

    result_links = [mp.Pipe(duplex=False), mp.Pipe(duplex=False)]

    if with_queues:
        q0 = mp.Queue(maxsize=2)
        q1 = mp.Queue(maxsize=2)
    else:
        left_right_link = mp.Pipe(duplex=False)
        right_left_link = mp.Pipe(duplex=False)

    msg_sz = 2
    print(f"Msglen [B]   Lat [usec]", flush=True)
    while msg_sz <= max_msg_sz:
        if with_queues:
            proc0 = mp.Process(target=worker_queue, args=(0, q1, q0, result_links[0][1], msg_sz, iterations))
            proc1 = mp.Process(target=worker_queue, args=(1, q0, q1, result_links[1][1], msg_sz, iterations))
        else:
            proc0 = mp.Process(
                target=worker_conn,
                args=(
                    0,
                    left_right_link[1],
                    right_left_link[0],
                    result_links[0][1],
                    msg_sz,
                    iterations,
                    use_bytes,
                ),
            )
            proc1 = mp.Process(
                target=worker_conn,
                args=(
                    1,
                    right_left_link[1],
                    left_right_link[0],
                    result_links[1][1],
                    msg_sz,
                    iterations,
                    use_bytes,
                ),
            )

        proc0.start()
        proc1.start()

        time_avg = 0
        time_avg += result_links[0][0].recv()
        time_avg += result_links[1][0].recv()
        time_avg = 1e6 * time_avg / 2

        proc0.join()
        proc1.join()

        print(f"{msg_sz}  {time_avg}", flush=True)

        msg_sz *= 2


if __name__ == "__main__":

    mp.set_start_method("dragon")

    out_json = {}

    out_json["is_restart"] = bool(os.environ.get("DRAGON_RESILIENT_RESTART", False))
    my_alloc = System()
    node_list = my_alloc.nodes
    node_hostnames = []
    for node_id in node_list:
        node = Node(node_id)
        node_hostnames.append(node.hostname)
    out_json["node_hostnames"] = node_hostnames

    print("UNITTEST_DATA_START",out_json,"UNITTEST_DATA_END", flush=True)
        
    run_p2p_lat(
        iterations=1000,
        max_msg_sz=2**4,
        use_bytes=True,
        with_queues=True,
    )
