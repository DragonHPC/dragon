"""
This example from the joblib package illustrates how to cache intermediate computing results using
:class:`joblib.Memory` within :class:`joblib.Parallel`. Processing is executed in parallel with caching for the deterministic data. 

"""

import dragon
import multiprocessing as mp
import time
from joblib import Memory, Parallel, delayed
import numpy as np
import time

if __name__ == "__main__":
    mp.set_start_method("dragon")

    def costly_compute(data, column):
        time.sleep(2)
        return data[column]

    def data_processing_mean(data, column):
        return costly_compute(data, column).mean()

    rng = np.random.RandomState(42)
    data = rng.randn(int(1e4), 4)

    start = time.monotonic()
    results = [data_processing_mean(data, col) for col in range(data.shape[1])]
    stop = time.monotonic()

    print("\nSequential processing")
    print("Elapsed time for the entire processing: {:.2f} s".format(stop - start))

    location = "./cachedir"
    memory = Memory(location, verbose=0)
    costly_compute_cached = memory.cache(costly_compute)

    def data_processing_mean_using_cache(data, column):
        """Compute the mean of a column."""
        return costly_compute_cached(data, column).mean()

    start = time.monotonic()
    results = Parallel(n_jobs=2, backend="multiprocessing")(
        delayed(data_processing_mean_using_cache)(data, col) for col in range(data.shape[1])
    )
    stop = time.monotonic()

    print("\nFirst round - caching the data")
    print("Elapsed time for the entire processing: {:.2f} s".format(stop - start))

    start = time.monotonic()
    results = Parallel(n_jobs=2, backend="multiprocessing")(
        delayed(data_processing_mean_using_cache)(data, col) for col in range(data.shape[1])
    )
    stop = time.monotonic()

    print("\nSecond round - reloading from the cache")
    print("Elapsed time for the entire processing: {:.2f} s".format(stop - start))

    def data_processing_max_using_cache(data, column):
        """Compute the max of a column."""
        return costly_compute_cached(data, column).max()

    start = time.monotonic()
    results = Parallel(n_jobs=2, backend="multiprocessing")(
        delayed(data_processing_max_using_cache)(data, col) for col in range(data.shape[1])
    )
    stop = time.monotonic()

    print("\nReusing intermediate checkpoints")
    print("Elapsed time for the entire processing: {:.2f} s".format(stop - start))

    memory.clear(warn=False)

    def costly_compute(data, column):
        """Emulate a costly function by sleeping and returning a column."""
        time.sleep(2)
        return data[column]

    def data_processing_mean(data, column):
        """Compute the mean of a column."""
        return costly_compute(data, column).mean()

    rng = np.random.RandomState(42)
    data = rng.randn(int(1e4), 4)

    start = time.monotonic()
    results = [data_processing_mean(data, col) for col in range(data.shape[1])]
    stop = time.monotonic()

    print("\nSequential processing")
    print("Elapsed time for the entire processing: {:.2f} s".format(stop - start))

    location = "./cachedir"
    memory = Memory(location, verbose=0)
    costly_compute_cached = memory.cache(costly_compute)

    def data_processing_mean_using_cache(data, column):
        """Compute the mean of a column."""
        return costly_compute_cached(data, column).mean()

    start = time.monotonic()
    results = Parallel(n_jobs=2, backend="multiprocessing")(
        delayed(data_processing_mean_using_cache)(data, col) for col in range(data.shape[1])
    )
    stop = time.monotonic()

    print("\nFirst round - caching the data")
    print("Elapsed time for the entire processing: {:.2f} s".format(stop - start))

    start = time.monotonic()
    results = Parallel(n_jobs=2, backend="multiprocessing")(
        delayed(data_processing_mean_using_cache)(data, col) for col in range(data.shape[1])
    )
    stop = time.monotonic()

    print("\nSecond round - reloading from the cache")
    print("Elapsed time for the entire processing: {:.2f} s".format(stop - start))

    def data_processing_max_using_cache(data, column):
        """Compute the max of a column."""
        return costly_compute_cached(data, column).max()

    start = time.monotonic()
    results = Parallel(n_jobs=2, backend="multiprocessing")(
        delayed(data_processing_max_using_cache)(data, col) for col in range(data.shape[1])
    )
    stop = time.monotonic()

    print("\nReusing intermediate checkpoints")
    print("Elapsed time for the entire processing: {:.2f} s".format(stop - start))

    memory.clear(warn=False)
