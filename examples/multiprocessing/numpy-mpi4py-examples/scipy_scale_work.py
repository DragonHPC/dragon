import dragon
import argparse
import multiprocessing
import numpy as np
import scipy.signal
import time
import itertools


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

    # Explicitly control compute time per image
    while elapsed < work_time:
        scipy.signal.convolve2d(image, random_filter)[::5, ::5]
        elapsed = time.perf_counter() - start
    return scipy.signal.convolve2d(image, random_filter)[::5, ::5]


if __name__ == "__main__":
    args = get_args()

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
    for j in range(nimages):
        images.append(np.zeros((args.size, args.size)))
    filters = [np.random.normal(size=(4, 4)) for _ in range(nimages)]

    num_cpus = args.num_workers
    print(f"Number of workers: {num_cpus}", flush=True)

    # Initialize the pool of workers
    pool = multiprocessing.Pool(num_cpus)

    # Main body of the computation
    times = []
    for i in range(args.iterations + args.burns):
        start = time.perf_counter()
        res = pool.map(f, zip(images, filters, itertools.repeat(float(args.work_time))))
        del res
        times.append(time.perf_counter() - start)
        print(f"Time for iteration {i} is {times[i]} seconds", flush=True)

    pool.close()
    pool.join()
    # Print out the mean and standard deviation, excluding the burns iterations
    print(f"Average time: {round(np.mean(times[args.burns:]), 2)} second(s)")
    print(f"Standard deviation: {round(np.std(times[args.burns:]), 2)} second(s)")
