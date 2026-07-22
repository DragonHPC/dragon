import sys
import os
import dragon
from dragon.data.ddict import DDict
from dragon.native.machine import System
from dragon.infrastructure.policy import Policy
import multiprocessing as mp
import numpy as np
import functools
import socket


def bench_block_size(q, dd_sz_mb, array_size_kb, id):
    print(f"running on node {socket.gethostname()}, pid={os.getpid()}", flush=True)
    dd_sz = int(dd_sz_mb * (1024 * 1024))
    print(f"starting ddict", flush=True)
    dd = DDict(1, 1, dd_sz)

    print(f"started ddict", flush=True)
    array_ideal_size = int(array_size_kb * 1024 * 0.25)

    print(f"set array", flush=True)
    arr = np.ones(array_ideal_size, dtype=np.int32)
    arr_sz = sys.getsizeof(arr)
    c = 1000
    print(f"trying to write", flush=True)
    while True:
        try:
            dd[str(c)] = arr
            c += 1
        except Exception as err:
            print(f"Got exception {err} at {c=}", flush=True)
            break

    print(f"done writing", flush=True)
    num_expected = dd_sz // arr_sz
    print(f"destroying dict", flush=True)
    dd.destroy()
    print(f"putting results in queue", flush=True)
    if q is not None:
        q.put((arr_sz, c, num_expected))
    else:
        return (arr_sz, c, num_expected)


def use_serial():
    dd_sz_mb = 1 * 1024
    num_samples = 2
    upper_bound = 11
    lower_bound = 9
    step_size = (upper_bound - lower_bound) / num_samples

    array_sizes = [lower_bound + i * step_size for i in range(num_samples)]

    sizes_tested = []
    successful_writes = []
    expected_writes = []
    results_queue = mp.Queue()

    for i, array_size in enumerate(array_sizes):
        print(f"Starting test {i} with data size {array_size}", flush=True)
        bench_block_size(results_queue, dd_sz_mb, array_size, i)
        result = results_queue.get()
        size_tested, num_writes, num_expected_writes = result
        sizes_tested.append(size_tested)
        successful_writes.append(num_writes)
        expected_writes.append(num_expected_writes)

    print(f"{sizes_tested=}")
    print(f"{successful_writes=}")
    print(f"{expected_writes=}")
    print(f"{(np.array(successful_writes) - np.array(expected_writes))=}")


def use_pool():
    my_alloc = System()
    nnodes = my_alloc.nnodes

    dd_sz_mb = 1 * 1024
    num_samples = 10
    upper_bound = 11
    lower_bound = 1
    step_size = (upper_bound - lower_bound) / num_samples

    array_sizes = [lower_bound + i * step_size for i in range(num_samples)]

    sizes_tested = []
    successful_writes = []
    expected_writes = []

    pool = mp.Pool(nnodes)
    bench_block_partial = functools.partial(bench_block_size, None, dd_sz_mb)

    print("launching pool", flush=True)
    results = pool.map(bench_block_partial, array_sizes)

    print("parsing results", flush=True)
    for result in results:
        size_tested, num_writes, num_expected_writes = result
        sizes_tested.append(size_tested)
        successful_writes.append(num_writes)
        expected_writes.append(num_expected_writes)

    print(f"{sizes_tested=}")
    print(f"{successful_writes=}")
    print(f"{expected_writes=}")
    print(f"{(np.array(successful_writes) - np.array(expected_writes))=}")


if __name__ == "__main__":

    mp.set_start_method("dragon")
    use_serial()
