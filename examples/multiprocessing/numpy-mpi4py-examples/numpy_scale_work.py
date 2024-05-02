import dragon
import argparse
import multiprocessing
import numpy as np
import time


def get_args():
    parser = argparse.ArgumentParser(description="Basic NumPy test")

    parser.add_argument("--num_workers", type=int, default=4, help="number of workers")

    parser.add_argument("--iterations", type=int, default=100, help="number of iterations to do")

    parser.add_argument(
        "--burns", type=int, default=5, help="number of iterations to burn/ignore in order to warm up"
    )

    parser.add_argument("--dragon", action="store_true", help="run with dragon objs")

    parser.add_argument("--work_size", type=int, default=1, help="size of the array in thousands")

    parser.add_argument(
        "--work_items",
        type=int,
        default=100,
        help="number of work items, i.e., number of numpy arrays that the workers will work on",
    )

    parser.add_argument("--debug", action="store_true", default=False)

    my_args = parser.parse_args()

    return my_args


def g(size):
    y = np.ones(size)
    return np.exp(y)


if __name__ == "__main__":
    import sys

    args = get_args()

    if args.dragon:
        multiprocessing.set_start_method("dragon")
    else:
        multiprocessing.set_start_method("spawn")

    if args.debug:
        import logging

        multiprocessing.util.log_to_stderr(logging.DEBUG)

    print(f"Multiprocessing start method: {multiprocessing.get_start_method()}", flush=True)

    num_cpus = args.num_workers
    ar_size = int(1e3 * args.work_size)

    with multiprocessing.Pool(num_cpus) as pool:
        print(f"Created pool: {type(pool)}", file=sys.stderr)

        all_iter = args.iterations + args.burns
        results = np.zeros(all_iter)
        for i in range(all_iter):
            print(f"trial {i}: ", end="", file=sys.stderr)
            start = time.perf_counter()
            pool.map(g, [ar_size] * args.work_items)
            finish = time.perf_counter()
            results[i] = finish - start
            print(results[i], file=sys.stderr)

        print(f"Average time: {round(np.mean(results[args.burns:]), 4)} second(s)", file=sys.stderr)
        print(f"Standard deviation: {round(np.std(results[args.burns:]), 4)} second(s)", file=sys.stderr)
