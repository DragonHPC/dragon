#!/usr/bin/env python3

import multiprocessing as mp
import dragon.infrastructure.standalone_conn as dconn
import time
import argparse
import threading

# with the very naive approach to an all-all,
# using base multiprocessing Connection only works with very small message sizes
# without involving threading in the all-all, owing to short buffer depth in an
# OS-supplied socket.
# The dragon Connection version is much more forgiving and offers the option
# to get things flowing simply by making the underlying Channels deep enough
# and the underlying Pool big enough.
THREADED = True


def test_worker(links, result_link, msg_size, total_iterations):
    result_timings = []

    my_msg = bytearray(msg_size)

    def send_stuff():
        for link in links:
            link.send(my_msg)

    def recv_stuff():
        for link in links:
            msg = link.recv()
            assert len(msg) == msg_size

    # Each of these measures the time each worker
    # takes to send and recv a message of msg_size
    # through each link, which goes to each other worker.
    for _ in range(total_iterations):
        start = time.perf_counter()

        if THREADED:  # a slightly less dumb all-all
            send_th = threading.Thread(target=send_stuff)
            recv_th = threading.Thread(target=recv_stuff)

            send_th.start()
            recv_th.start()
            send_th.join()
            recv_th.join()
        else:  # the simplest all-all
            send_stuff()
            recv_stuff()

        result_timings.append(time.perf_counter() - start)

    result_link.send(result_timings)


def aa_test(nworkers, burn, iterations, msg_sz, my_pipe):
    total_iterations = burn + iterations

    # half duplex return links
    result_links = [my_pipe(duplex=False) for _ in range(nworkers)]

    all_links = {}

    # full duplex links; nworkers * (nworkers-1)/2 of them
    for i in range(nworkers):
        for j in range(i, nworkers):
            all_links[i, j] = my_pipe()

    all_workers = []
    for i in range(nworkers):
        # first end if other's index less than yours,
        # second end if other's index greater than yours
        links = []
        for k in range(nworkers):
            if k != i:
                links.append(all_links[min(i, k), max(i, k)][int(k < i)])

        proc = mp.Process(target=test_worker,
                          args=(links, result_links[i][1], msg_sz, total_iterations))

        all_workers.append(proc)

    for worker in all_workers:
        worker.start()

    avg_times = []
    total_times = []
    for i in range(nworkers):
        result = result_links[i][0].recv()[burn:]
        total_times.append(sum(result))
        avg_times.append(total_times[i] / iterations)

    avg_aa_completion_time = sum(avg_times) / nworkers  # seconds
    bandwidth = msg_sz * nworkers / avg_aa_completion_time  # bytes/second

    msg_size_in_k = msg_sz / (2 ** 10)
    bw_in_mb_sec = bandwidth / (2 ** 20)
    completion_time_in_ms = avg_aa_completion_time * 1000

    for worker in all_workers:
        worker.join()

    print(f'{nworkers=} {iterations=} {msg_size_in_k=} {completion_time_in_ms=:.3f} {bw_in_mb_sec=:.2f}')


if __name__ == "__main__":
    mp.set_start_method('spawn')

    parser = argparse.ArgumentParser(description='All-all channels/connection test')
    parser.add_argument('--num_workers', type=int, default=4,
                        help='number of workers to spawn at a time')

    parser.add_argument('--iterations', type=int, default=100,
                        help='number of iterations to do')

    parser.add_argument('--burn_iterations', type=int, default=5,
                        help='number of iterations to burn first time')

    parser.add_argument('--lg_message_size', type=int, default=4,
                        help='log base 2 of size of message to pass in')

    parser.add_argument('--dragon', action='store_true', help='run using dragon Connection')

    my_args = parser.parse_args()

    assert my_args.lg_message_size <= 25, 'limit: 32MB per worker to each other worker'

    if my_args.dragon:
        print('using dragon Connection')
        dconn.get_going(pool_size=2 ** 31)  # setup for standalone channels
        pipe_call = dconn.Pipe
    else:
        print('using multiprocessing Connection')
        pipe_call = mp.Pipe

    aa_test(nworkers=my_args.num_workers, burn=my_args.burn_iterations,
            iterations=my_args.iterations, msg_sz=2 ** my_args.lg_message_size,
            my_pipe=pipe_call)
