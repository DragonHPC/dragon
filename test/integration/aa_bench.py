import dragon

import argparse
import multiprocessing
import time
import threading


def get_args():
    parser = argparse.ArgumentParser(description='All-all connection/queue test')

    parser.add_argument('--num_workers', type=int, default=4,
                        help='number of workers in all-all')

    parser.add_argument('--iterations', type=int, default=100,
                        help='number of iterations to do')

    parser.add_argument('--burn_iterations', type=int, default=5,
                        help='number of iterations to burn first time')

    parser.add_argument('--lg_message_size', type=int, default=4,
                        help='log base 2 of msg size between each pair')

    parser.add_argument('--dragon', action='store_true', help='run with dragon objs')
    parser.add_argument('--queues', action='store_true', help='measure aa with per-worker queues')
    my_args = parser.parse_args()

    assert my_args.lg_message_size <= 24, 'arbitrary limit: 16MB message size'

    return my_args


def test_worker_conn(links, result_link, msg_size, total_iterations):
    result_timings = []

    my_msg = bytearray(msg_size)

    def send_stuff():
        for link in links:
            link.send(my_msg)

    def recv_stuff():
        for link in links:
            _ = link.recv()

    for _ in range(total_iterations):
        start = time.perf_counter()

        send_th = threading.Thread(target=send_stuff)
        recv_th = threading.Thread(target=recv_stuff)

        send_th.start()
        recv_th.start()
        send_th.join()
        recv_th.join()

        result_timings.append(time.perf_counter() - start)

    result_link.send(result_timings)


# messages can mix between workers between iterations - there really
# ought to be a barrier - but the books balance at the end and this
# is meant to be more of a stress/comparison test anyway than
# a serious all-all.
def test_worker_queue(queues, my_idx, result_link, msg_size, total_iterations):
    result_timings = []
    my_msg = bytearray(msg_size)
    others = set(range(len(queues)))
    others.discard(my_idx)

    def send_stuff():
        for idx, q in enumerate(queues):
            if idx != my_idx:
                q.put(my_msg)

    def recv_stuff():
        for _ in range(len(others)):
            msg = queues[my_idx].get()
            assert msg_size == len(msg)

    for it_cnt in range(total_iterations):
        start = time.perf_counter()

        send_th = threading.Thread(target=send_stuff)
        recv_th = threading.Thread(target=recv_stuff)

        send_th.start()
        recv_th.start()
        send_th.join()
        recv_th.join()

        result_timings.append(time.perf_counter() - start)

    result_link.send(result_timings)


def run_test(with_queues, nworkers, its, burns, msg_sz):
    total_iterations = burns + its

    # half duplex return links
    result_links = [multiprocessing.Pipe(duplex=False) for _ in range(nworkers)]

    if with_queues:
        print(f'using {nworkers} Queues')
        queues = [multiprocessing.Queue(maxsize=nworkers) for _ in range(nworkers)]

        all_workers = [multiprocessing.Process(target=test_worker_queue,
                                               args=(queues, i,
                                                     result_links[i][1], msg_sz,
                                                     total_iterations))
                       for i in range(nworkers)]
    else:
        print(f'using {nworkers * (nworkers - 1) // 2} Pipes')
        all_links = {}

        # full duplex links; nworkers * (nworkers-1)/2 of them
        for i in range(nworkers):
            for j in range(i, nworkers):
                all_links[i, j] = multiprocessing.Pipe()

        all_workers = []
        for i in range(nworkers):
            # first end if other's index less than yours,
            # second end if other's index greater than yours
            links = []
            for k in range(nworkers):
                if k != i:
                    links.append(all_links[min(i, k), max(i, k)][int(k < i)])

            proc = multiprocessing.Process(target=test_worker_conn,
                                           args=(links, result_links[i][1],
                                                 msg_sz, total_iterations))

            all_workers.append(proc)

    for worker in all_workers:
        worker.start()

    avg_times = []
    total_times = []
    for i in range(nworkers):
        result = result_links[i][0].recv()[burns:]
        total_times.append(sum(result))
        avg_times.append(total_times[i] / its)

    avg_aa_completion_time = sum(avg_times) / nworkers  # seconds
    bandwidth = msg_sz * nworkers / avg_aa_completion_time  # bytes/second

    msg_size_in_k = msg_sz / (2 ** 10)
    bw_in_mb_sec = bandwidth / (2 ** 20)
    completion_time_in_ms = avg_aa_completion_time * 1000

    for worker in all_workers:
        worker.join()

    print(f'{nworkers=} {its=} {msg_size_in_k=} {completion_time_in_ms=:.3f} {bw_in_mb_sec=:.2f}')


def main():
    args = get_args()

    if args.dragon:
        import dragon.infrastructure.parameters as dparms

        if dparms.this_process.my_puid <= 1:
            print('not running under dragon infrastructure, exiting')
            exit(-3)

        print('using dragon runtime')
        multiprocessing.set_start_method('dragon')
    else:
        print('using regular mp libs/calls')
        multiprocessing.set_start_method('spawn')

    run_test(with_queues=args.queues, nworkers=args.num_workers,
             msg_sz=2 ** args.lg_message_size,
             its=args.iterations, burns=args.burn_iterations)


if __name__ == '__main__':
    exit(main())
