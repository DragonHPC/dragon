"""
Adapted from joblib memory basic usage example.
This example illustrates the usage of :class:`joblib.Memory` with both functions and methods.
Be sure to set the random seed to generate deterministic data. Indeed, if the data is not deterministic, the :class:`joblib.Memory` instance will not be able to reuse the cache from one run to another.
Therefore, the computation time corresponds to the time to compute the results plus the time to dump the disk.
At the second call, the computation time is largely reduced since the results are obtained by loading the data previously dumped to the disk instead of recomputing the results.
"""

import dragon
import multiprocessing as mp
import time
import numpy as np

if __name__ == "__main__":

    mp.set_start_method("dragon")

    def costly_compute(data, column_index=0):
        time.sleep(5)
        return data[column_index]

    rng = np.random.RandomState(42)
    data = rng.randn(int(1e5), 10)
    start = time.monotonic()
    data_trans = costly_compute(data)
    end = time.monotonic()

    print("\nThe function took {:.2f} s to compute.".format(end - start))
    print("\nThe transformed data are:\n {}".format(data_trans))

    from joblib import Memory

    location = "./cachedir"
    memory = Memory(location, verbose=0)

    def costly_compute_cached(data, column_index=0):
        """Simulate an expensive computation"""
        time.sleep(5)
        return data[column_index]

    costly_compute_cached = memory.cache(costly_compute_cached)
    start = time.monotonic()
    data_trans = costly_compute_cached(data)
    end = time.monotonic()

    print("\nThe function took {:.2f} s to compute.".format(end - start))
    print("\nThe transformed data are:\n {}".format(data_trans))

    start = time.monotonic()
    data_trans = costly_compute_cached(data)
    end = time.monotonic()

    print("\nThe function took {:.2f} s to compute.".format(end - start))
    print("\nThe transformed data are:\n {}".format(data_trans))

    def _costly_compute_cached(data, column):
        time.sleep(5)
        return data[column]

    class Algorithm(object):
        """A class which is using the previous function."""

        def __init__(self, column=0):
            self.column = column

        def transform(self, data):
            costly_compute = memory.cache(_costly_compute_cached)
            return costly_compute(data, self.column)

    transformer = Algorithm()
    start = time.monotonic()
    data_trans = transformer.transform(data)
    end = time.monotonic()

    print("\nThe function took {:.2f} s to compute.".format(end - start))
    print("\nThe transformed data are:\n {}".format(data_trans))

    start = time.monotonic()
    data_trans = transformer.transform(data)
    end = time.monotonic()

    print("\nThe function took {:.2f} s to compute.".format(end - start))
    print("\nThe transformed data are:\n {}".format(data_trans))

    memory.clear(warn=False)
