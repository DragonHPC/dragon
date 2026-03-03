"""
Delayed is important for allowing optimization of the Python code in conjunction with 
joblib's Parallel. Delayed is used to create a lazy or deferred function call which allows the
computations to be parallelized across multiple CPU cores or machines.
"""

import dragon
import multiprocessing as mp
from joblib import Parallel, delayed
import time

if __name__ == "__main__":
    mp.set_start_method("dragon")

    def cube(x):
        return x ** 3

    def sleep_cube(x):
        time.sleep(0.001)
        return x ** 3

    numbers = [*range(0, 10001, 1)]

    start = time.monotonic()
    results_no_delayed = [sleep_cube(number) for number in numbers]
    end = time.monotonic()
    time_no_delayed = end - start

    delayed_calls = [delayed(cube)(number) for number in numbers]
    start = time.monotonic()
    results_delayed = Parallel(n_jobs=1, backend="multiprocessing")(delayed_calls)
    end = time.monotonic()
    time_delayed = end - start

    print("Results without delayed:", results_no_delayed)
    print("\n")
    print("Results with delayed:   ", results_delayed)
    print("\n")
    print("Time without delayed:   ", time_no_delayed)
    print("\n")
    print("Time with delayed:      ", time_delayed)
