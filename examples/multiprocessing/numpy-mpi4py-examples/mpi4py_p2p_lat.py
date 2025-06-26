#!/usr/bin/env python3

from mpi4py import MPI
import time
import argparse
import sys

BURN_ITERS = 2

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="P2P latency test")

    parser.add_argument("--iterations", type=int, default=100, help="number of iterations to do")

    parser.add_argument(
        "--lg_max_message_size", type=int, default=4, help="log base 2 of size of message to pass in"
    )

    parser.add_argument("--with_bytes", action="store_true", help="creates a payload using bytearray")

    my_args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    msg_size = 2
    if rank == 0:
        print(f"Msglen [B]   Lat [usec]")

    while msg_size <= 2**my_args.lg_max_message_size:
        start = 0

        if my_args.with_bytes:
            my_msg = bytearray(msg_size)
        else:
            my_msg = []
            my_size = sys.getsizeof(my_msg)
            while my_size < msg_size:
                my_msg.append("a")
                my_size = sys.getsizeof(my_msg)

        if rank == 0:
            for i in range(my_args.iterations + BURN_ITERS):
                if i == (BURN_ITERS + 1):
                    start = time.perf_counter()

                comm.send(my_msg, dest=1, tag=11)
                data = comm.recv(source=1, tag=12)

            avg_time = 1e6 * (time.perf_counter() - start) / my_args.iterations
            print(f"{msg_size}  {avg_time}")
        elif rank == 1:
            for i in range(my_args.iterations + BURN_ITERS):
                data = comm.recv(source=0, tag=11)
                comm.send(data, dest=0, tag=12)

        msg_size *= 2
