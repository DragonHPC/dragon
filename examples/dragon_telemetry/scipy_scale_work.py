import dragon
import argparse
import multiprocessing
import numpy as np
import scipy.signal
import time
import itertools
from dragon.telemetry.telemetry import Telemetry as dragon_telem

def get_args():
    parser = argparse.ArgumentParser(description="Basic SciPy test")

    parser.add_argument("--num_workers", type=int, default=4,
                        help="number of workers")

    parser.add_argument("--iterations", type=int, default=10,
                        help="number of iterations to do")

    parser.add_argument("--burns", type=int, default=2,
                        help="number of iterations to burn/ignore in order to warm up")

    parser.add_argument("--dragon", action="store_true",
                        help="run with dragon objs")

    parser.add_argument("--size", type=int, default=1000,
                        help="size of the array")

    parser.add_argument("--mem", type=int, default=(512 * 1024 * 1024),
                        help="overall footprint of image dataset to process")

    parser.add_argument('--work_time', type=float, default=0.0,
                        help='how many seconds of compute per image')

    my_args = parser.parse_args()

    return my_args


def f(args):
    # Do some image processing
    image, random_filter, work_time = args
    elapsed = 0.
    start = time.perf_counter()
    last = None
    # Explicitly control compute time per image
    while elapsed < work_time:
        last = scipy.signal.convolve2d(image, random_filter)[::5, ::5]
        elapsed = time.perf_counter() - start

    return last


if __name__ == "__main__":
    args = get_args()
    # Initializes local telemetry object. This has to be done for each process that adds data
    dt = dragon_telem()
    # Start Dragon or base Multiprocessing
    if args.dragon:
        print("using dragon runtime")
        multiprocessing.set_start_method("dragon")
    else:
        print("using regular mp libs/calls")
        multiprocessing.set_start_method("spawn")

    # Image and filter generation
    image = np.zeros((args.size, args.size))
    nimages = int(float(args.mem) / float(image.size))
    print(f"Number of images: {nimages}", flush=True)
    images = []
    images.append(image)
    for j in range(nimages-1):
        images.append(np.zeros((args.size, args.size)))
    filters = [np.random.normal(size=(4, 4)) for _ in range(nimages)]

    num_cpus = args.num_workers
    print(f"Number of workers: {num_cpus}", flush=True)

    # Initialize the pool of workers
    start = time.perf_counter()
    pool = multiprocessing.Pool(num_cpus)
    pool_start_time = time.perf_counter() - start
    dt.add_data("pool_start_time", pool_start_time)

    # Main body of the computation
    times = []
    for i in range(args.iterations + args.burns):
        start = time.perf_counter()
        res = pool.map(f, zip(images, filters, itertools.repeat(float(args.work_time))))
        del res
        times.append(time.perf_counter() - start)
        # this will only be collected if the telemetry level is >= 3
        dt.add_data("iteration", i, telemetry_level=3)
        # this will be collected any time telemetry data is being collected
        dt.add_data("iteration_time", times[i])
        print(f"Time for iteration {i} is {times[i]} seconds", flush=True)

    pool.close()
    pool.join()
    # Print out the mean and standard deviation, excluding the burns iterations
    print(f"Average time: {round(np.mean(times[args.burns:]), 2)} second(s)")
    print(f"Standard deviation: {round(np.std(times[args.burns:]), 2)} second(s)")
    # This shuts down the telemetry collection and cleans up the workers and the node local telemetry databases.
    # This only needs to be called by one process.
    dt.finalize()
