"""
Randomness is affected by parallel execution differently by the different
backends.

In particular, when using multiple processes, the random sequence can be
the same in all processes. This example from joblib illustrates the problem and shows
how to work around it.
"""

import dragon
import multiprocessing as mp

import time
import numpy as np
from joblib import Parallel, delayed

if __name__ == "__main__":

    mp.set_start_method("dragon")

    def print_vector(vector, backend):
        """Helper function to print the generated vector with a given backend."""
        print(
            "\nThe different generated vectors using the {} backend are:\n {}".format(
                backend, np.array(vector)
            )
        )

    def stochastic_function(max_value):
        """Randomly generate integer up to a maximum value."""
        return np.random.randint(max_value, size=5)

    n_vectors = 5
    random_vector = [stochastic_function(10) for _ in range(n_vectors)]
    print(
        "\nThe different generated vectors in a sequential manner are:\n {}".format(np.array(random_vector))
    )

    start = time.monotonic()
    random_vector = Parallel(n_jobs=2, backend="multiprocessing")(
        delayed(stochastic_function)(10) for _ in range(n_vectors)
    )
    stop = time.monotonic()
    print(stop - start)
    print_vector(random_vector, "multiprocessing")

    def stochastic_function_seeded(max_value, random_state):
        rng = np.random.RandomState(random_state)
        return rng.randint(max_value, size=5)

    start = time.monotonic()
    random_vector = Parallel(n_jobs=2, backend="multiprocessing")(
        delayed(stochastic_function_seeded)(10, None) for _ in range(n_vectors)
    )
    stop = time.monotonic()
    print(stop - start)
    print_vector(random_vector, "multiprocessing")

    random_state = np.random.randint(np.iinfo(np.int32).max, size=n_vectors)
    start = time.monotonic()
    random_vector = Parallel(n_jobs=2, backend="multiprocessing")(
        delayed(stochastic_function_seeded)(10, rng) for rng in random_state
    )
    stop = time.monotonic()
    print(stop - start)
    print_vector(random_vector, "multiprocessing")
