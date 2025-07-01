import argparse
import cloudpickle
import random
import time

from dragon.native.machine import cpu_count
from dragon.native.pool import Pool
from dragon.native.queue import Queue
from dragon.native.process import ProcessTemplate
from dragon.native.process_group import ProcessGroup

import mpi4py

mpi4py.rc.initialize = False
from mpi4py import MPI


num_workers = int((cpu_count() / 2) - 8)


def do_work(imbalance_factor) -> int:
    min_time = 0.1
    max_time = imbalance_factor * min_time
    time.sleep(random.uniform(min_time, max_time))
    return 0


def do_mpi_test(num_iters: int, items_per_worker: int, imbalance_factor: int, q: Queue) -> None:
    comm = MPI.COMM_WORLD
    my_rank = comm.Get_rank()
    num_ranks = comm.Get_size()

    root = 0
    tag = 0
    anysrc = MPI.ANY_SOURCE

    begin = time.time()

    if my_rank == root:
        # send work
        results = []
        for _ in range(num_iters):
            for _ in range(items_per_worker):
                for rank in range(1, num_ranks):
                    func_and_arg = cloudpickle.dumps((do_work, imbalance_factor))
                    comm.send(func_and_arg, dest=rank, tag=tag)

                for _ in range(1, num_ranks):
                    rc = comm.recv(source=anysrc, tag=tag)
                    results.append(rc)

        # send done msg
        for rank in range(1, num_ranks):
            comm.send(0, dest=rank, tag=tag)
    else:
        # receive work until done msg
        while True:
            item = comm.recv(source=root, tag=tag)
            if isinstance(item, int):
                break

            func, imbalance_factor = cloudpickle.loads(item)
            rc = func(imbalance_factor)
            comm.send(rc, dest=root, tag=tag)

    runtime = time.time() - begin
    if my_rank == 0:
        print(f"runtime for MPI-based pool: {runtime}", flush=True)
        q.put(runtime)


def mpi_based_pool_test(num_iters: int, items_per_worker: int, imbalance_factor: int) -> float:
    q = Queue()
    pg = ProcessGroup(restart=False, pmi_enabled=True)
    template = ProcessTemplate(target=do_mpi_test, args=(num_iters, items_per_worker, imbalance_factor, q))

    nproc = 1 + num_workers
    pg.add_process(nproc=nproc, template=template)
    pg.init()
    pg.start()
    pg.join()
    pg.close()

    runtime = q.get()
    return runtime


def dragon_pool_test(num_iters: int, items_per_worker: int, imbalance_factor: int) -> float:
    pool = Pool(processes=num_workers)

    args_list = []
    for _ in range(0, items_per_worker * num_workers):
        args_list.append(imbalance_factor)

    begin = time.time()

    for _ in range(num_iters):
        result = pool.map_async(do_work, args_list, 1)
        result.get()

    runtime = time.time() - begin
    print(f"runtime for Dragon pool: {runtime}", flush=True)

    pool.close()
    pool.join()

    return runtime


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dragon Pool vs MPI Benchmark")
    parser.add_argument(
        "--num_iters",
        type=int,
        default=1,
        help="number of iterations",
    )
    parser.add_argument(
        "--items_per_worker",
        type=int,
        default=4,
        help="number of work items per worker per iteration",
    )
    parser.add_argument(
        "--imbalance_factor",
        type=int,
        default=10,
        help="tunes how imbalanced the workload is--specifically, (max work item time) / (min work item time)",
    )

    args = parser.parse_args()

    dragon_runtime = dragon_pool_test(args.num_iters, args.items_per_worker, args.imbalance_factor)
    mpi_runtime = mpi_based_pool_test(args.num_iters, args.items_per_worker, args.imbalance_factor)

    print(f"speedup: {mpi_runtime / dragon_runtime}", flush=True)
