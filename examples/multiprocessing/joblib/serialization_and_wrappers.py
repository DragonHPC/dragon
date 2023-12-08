"""
This example by Thomas Moreau highlights the options for tempering with joblib serialization
process.

"""

import dragon
import multiprocessing as mp

import sys
import time
import traceback
from joblib.externals.loky import set_loky_pickler
from joblib import parallel_config
from joblib import Parallel, delayed
from joblib import wrap_non_picklable_objects

if __name__ == "__main__":
    mp.set_start_method("dragon")

    def func_async(i, *args):
        return 2 * i

    print(Parallel(n_jobs=2, backend="multiprocessing")(delayed(func_async)(21) for _ in range(1))[0])

    large_list = list(range(1000000))

    t_start = time.monotonic()
    Parallel(n_jobs=2, backend="multiprocessing")(delayed(func_async)(21, large_list) for _ in range(1))
    print("With loky backend and cloudpickle serialization: {:.3f}s".format(time.monotonic() - t_start))

    with parallel_config("multiprocessing"):
        t_start = time.monotonic()
        Parallel(n_jobs=2, backend="multiprocessing")(delayed(func_async)(21, large_list) for _ in range(1))
        print(
            "With multiprocessing backend and pickle serialization: {:.3f}s".format(
                time.monotonic() - t_start
            )
        )

    set_loky_pickler("pickle")
    t_start = time.monotonic()
    Parallel(n_jobs=2, backend="multiprocessing")(delayed(id)(large_list) for _ in range(1))
    print("With pickle serialization: {:.3f}s".format(time.monotonic() - t_start))
