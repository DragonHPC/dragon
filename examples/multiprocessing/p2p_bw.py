#!/usr/bin/env python3

import dragon
import multiprocessing as mp
import time
import argparse

BURN_ITERS = 2
WINDOW = 32


def worker_conn(id, send_link, recv_link, result_link, msg_size, total_iterations, use_bytes):
    start = 0
    if id == 0:
        my_msg = bytearray(msg_size)
        for i in range(total_iterations + BURN_ITERS):
            if i == (BURN_ITERS + 1):
                start = time.perf_counter()

            for j in range(WINDOW):
                if use_bytes:
                    send_link.send_bytes(my_msg)
                else:
                    send_link.send(my_msg)
            msg = recv_link.recv()

    else:
        my_msg = bytearray(8)
        for i in range(total_iterations + BURN_ITERS):
            if i == (BURN_ITERS + 1):
                start = time.perf_counter()

            for j in range(WINDOW):
                if use_bytes:
                    msg = recv_link.recv_bytes()
                else:
                    msg = recv_link.recv()
            send_link.send(my_msg)

    avg_time = (time.perf_counter() - start) / total_iterations
    bw = 1e-6 * msg_size * WINDOW / avg_time
    result_link.send(bw)


def worker_queue(id, q, q_ack, result_link, msg_size, total_iterations):
    start = 0
    if id == 0:
        my_msg = bytearray(msg_size)
        for i in range(total_iterations + BURN_ITERS):
            if i == (BURN_ITERS + 1):
                start = time.perf_counter()

            for j in range(WINDOW):
                q.put(my_msg)
            msg = q_ack.get()

    else:
        my_msg = bytearray(8)
        for i in range(total_iterations + BURN_ITERS):
            if i == (BURN_ITERS + 1):
                start = time.perf_counter()

            for j in range(WINDOW):
                msg = q.get()
            q_ack.put(my_msg)

    avg_time = (time.perf_counter() - start) / total_iterations
    bw = 1e-6 * msg_size * WINDOW / avg_time
    result_link.send(bw)


def run_p2p_bw(iterations=100, max_msg_sz=1024, use_bytes=False, with_queues=False):

    result_links = [mp.Pipe(duplex=False), mp.Pipe(duplex=False)]

    if with_queues:
        q = mp.Queue(maxsize=WINDOW)
        q_ack = mp.Queue(maxsize=WINDOW)
    else:
        left_right_link = mp.Pipe(duplex=False)
        right_left_link = mp.Pipe(duplex=False)

    msg_sz = 2
    print(f"Msglen [B]   BW [MiB/s]")
    while msg_sz <= max_msg_sz:
        if with_queues:
            proc0 = mp.Process(
                target=worker_queue, args=(0, q, q_ack, result_links[0][1], msg_sz, iterations)
            )
            proc1 = mp.Process(
                target=worker_queue, args=(1, q, q_ack, result_links[1][1], msg_sz, iterations)
            )
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

        bw = result_links[0][0].recv()

        proc0.join()
        proc1.join()

        print(f"{msg_sz}  {bw}")

        msg_sz *= 2


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="P2P bandwidth test")

    parser.add_argument("--iterations", type=int, default=100, help="number of iterations to do")

    parser.add_argument(
        "--lg_max_message_size", type=int, default=4, help="log base 2 of size of message to pass in"
    )

    parser.add_argument("--dragon", action="store_true", help="run using dragon Connection")

    parser.add_argument(
        "--with_bytes", action="store_true", help="use send_bytes/recv_bytes instead of send/recv"
    )

    parser.add_argument("--queues", action="store_true", help="use queues for the communication")

    my_args = parser.parse_args()

    if my_args.dragon:
        print("using Dragon")
        mp.set_start_method("dragon")

    else:
        print("using multiprocessing")
        mp.set_start_method("spawn")

    run_p2p_bw(
        iterations=my_args.iterations,
        max_msg_sz=2**my_args.lg_max_message_size,
        use_bytes=my_args.with_bytes,
        with_queues=my_args.queues,
    )
