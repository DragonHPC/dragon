#!/usr/bin/env python3

from mpi4py import MPI
import time
import argparse
import sys


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="All-to-all communication test")

    parser.add_argument("--iterations", type=int, default=100, help="number of iterations to do")

    parser.add_argument(
        "--burn_iterations", type=int, default=5, help="number of iterations to burn first time"
    )

    parser.add_argument(
        "--lg_message_size", type=int, default=21, help="log base 2 of msg size between each pair"
    )

    parser.add_argument("--with_bytes", action="store_true", help="creates a payload using bytearray")

    my_args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    burns = my_args.burn_iterations
    its = my_args.iterations
    workers = comm.Get_size()

    msg_size = 2**my_args.lg_message_size

    if my_args.with_bytes:
        my_msg = bytearray(msg_size)
    else:
        my_msg = []
        my_size = sys.getsizeof(my_msg)
        while my_size < msg_size:
            my_msg.append("a")
            my_size = sys.getsizeof(my_msg)

    senddata = workers * [my_msg]

    timer = 0.0
    for i in range(its + burns):
        start = time.perf_counter()
        recvdata = comm.alltoall(senddata)
        stop = time.perf_counter()

        if i >= burns:
            timer += stop - start

    timing = timer / its
    avg_time = comm.reduce(timing, op=MPI.SUM, root=0)

    if rank == 0:
        completion_time = avg_time / workers
        completion_time_in_ms = 1000 * completion_time
        msg_size_in_k = msg_size / (2**10)
        bandwidth = msg_size * workers / completion_time  # bytes/second
        bw_in_mb_sec = bandwidth / (2**20)
        print(f"{workers=} {its=} {msg_size_in_k=} {completion_time_in_ms=:.3f} {bw_in_mb_sec=:.2f}")
