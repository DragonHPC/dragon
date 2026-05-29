"""
Joblib benchmark written by Olivier Grisel. Addition of Dragon libraries.

Benchmark batching="auto" on high number of fast tasks

The goal of this script is to study the behavior of the batch_size='auto'
and in particular the impact of the default value of the
joblib.parallel.MIN_IDEAL_BATCH_DURATION constant.

Noop function to emulate real computation. Induce overhead by accepting (and ignoring) any amount of data as input and allocating a requested amount of data. The data is provided in bytes.

First pair of benchmarks to check that the auto-batching strategy is stable (do not change the batch size too often) in the presence of large variance while still be comparable to the equivalent load without variance.

Second pair of benchmarks: one has a cycling task duration pattern that the auto batching feature should be able to roughly track. The shuffle variant should not oscillate too much and still approximately have the same total run time.
"""

import dragon
import multiprocessing as mp
import numpy as np
import time
import tempfile
from pprint import pprint
from joblib import Parallel, delayed
from joblib._parallel_backends import AutoBatchingMixin


def sleep_noop(duration, input_data, output_data_size):
    time.sleep(duration)
    if output_data_size:
        return np.ones(output_data_size, dtype=np.byte)


def bench_short_tasks(
    task_times,
    n_jobs=2,
    batch_size="auto",
    pre_dispatch="2*n_jobs",
    verbose=True,
    input_data_size=0,
    output_data_size=0,
    backend=None,
    memmap_input=False,
):

    with tempfile.NamedTemporaryFile() as temp_file:
        if input_data_size:
            if memmap_input:
                temp_file.close()
                input_data = np.memmap(temp_file.name, shape=input_data_size, dtype=np.byte, mode="w+")
                input_data[:] = 1
            else:
                input_data = np.ones(input_data_size, dtype=np.byte)
        else:
            input_data = None

        t0 = time.monotonic()
        p = Parallel(
            n_jobs=n_jobs, verbose=verbose, pre_dispatch=pre_dispatch, batch_size=batch_size, backend=backend
        )
        p(delayed(sleep_noop)(max(t, 0), input_data, output_data_size) for t in task_times)
        duration = time.monotonic() - t0
        effective_batch_size = getattr(p._backend, "_effective_batch_size", p.batch_size)
    print(
        "Completed {} tasks in {:3f}s, final batch_size={}\n".format(
            len(task_times), duration, effective_batch_size
        )
    )
    return duration, effective_batch_size


if __name__ == "__main__":
    mp.set_start_method("dragon")

    bench_parameters = dict(
        backend="multiprocessing", input_data_size=int(2e7), output_data_size=int(1e5), n_jobs=2, verbose=10,
    )
    print("Common benchmark parameters:")
    pprint(bench_parameters)

    AutoBatchingMixin.MIN_IDEAL_BATCH_DURATION = 0.2
    AutoBatchingMixin.MAX_IDEAL_BATCH_DURATION = 2

    print("# high variance, no trend")
    high_variance = np.random.normal(loc=0.000001, scale=0.001, size=5000)
    high_variance[high_variance < 0] = 0

    bench_short_tasks(high_variance, **bench_parameters)
    print("# low variance, no trend")
    low_variance = np.empty_like(high_variance)
    low_variance[:] = np.mean(high_variance)
    bench_short_tasks(low_variance, **bench_parameters)

    print("# cyclic trend")
    slow_time = 0.1
    positive_wave = np.cos(np.linspace(1, 4 * np.pi, 300)) ** 8
    cyclic = positive_wave * slow_time
    bench_short_tasks(cyclic, **bench_parameters)

    print("shuffling of the previous benchmark: same mean and variance")
    np.random.shuffle(cyclic)
    bench_short_tasks(cyclic, **bench_parameters)
