""" Calculate pi in parallel using the standard Monte Carlo technique 
    with the Dragon native interface. 

    NOTE: due to the missing Process object in Dragon native for version v0.3
    we are use the Multiprocessing process API here. This will change with
    v1.0
"""

import sys
import os
import random
import pickle
import math

import dragon
from dragon.native.queue import Queue

from multiprocessing import Process as Process  # only for Dragon v0.3


def pi_helper(q_in: Queue, q_out: Queue) -> None:

    while True:  # keep working

        x, y = q_in.get(timeout=None)

        if x == -1 and y == -1:  # we're done here
            break

        r = x * x + y * y

        if r > 1.0:
            q_out.put(False)
        else:
            q_out.put(True)


def main(num_workers):

    # create Dragon queues
    q_send = Queue()
    q_recv = Queue()

    # create & start processes
    processes = []
    for _ in range(num_workers):
        p = Process(target=pi_helper, args=(q_send, q_recv))
        p.start()
        processes.append(p)

    # start calculation
    random.seed(a=42)

    num_true = 0
    num_send = 0
    delta = 1
    count = 0

    while abs(delta) > 1e-6:

        # send new values to everyone
        for _ in range(num_workers):
            x = random.uniform(0.0, 1.0)
            y = random.uniform(0.0, 1.0)

            q_send.put((x, y), timeout=0)
            num_send += 1

        # receive results from everyone
        for _ in range(num_workers):

            is_inside = q_recv.get(timeout=None)

            if is_inside:
                num_true += 1

        # calculate result of this iteration
        value = 4.0 * float(num_true) / float(num_send)
        delta = (value - math.pi) / math.pi

        if count % 512 == 0:
            print(f"{count:04}: pi={value:10.7f}, error={delta:8.1e}", flush=True)

        count += 1

    print(f"Final value after {count} iterations: pi={value}, error={delta}")

    # shut down all managed processes
    for _ in range(num_workers):
        q_send.put((-1.0, -1.0), timeout=0)  # termination message

    # wait for all processes to be finished
    for p in processes:
        p.join(timeout=None)


def usage():
    print(f"USAGE: dragon pi_demo.py $N")
    print(f"  N : number of worker puids to start")


if __name__ == "__main__":

    print(f"\npi-demo: Calculate π = 3.1415 ... in parallel using the Dragon native API.\n")

    try:
        num_workers = int(sys.argv[1])
    except:
        usage()
        sys.exit(f"Wrong argument '{sys.argv[1]}'")

    print(f"Got num_workers = {num_workers}", flush=True)
    main(num_workers)
