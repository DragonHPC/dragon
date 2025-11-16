#!/usr/bin/env python3
import dragon
import multiprocessing as mp
import time
import argparse


def numAcq(nlock, acquisitions):
    for _ in range(acquisitions):
        is_locked = nlock.acquire(block=True)
        if is_locked:
            nlock.release()


class LockPerformance:
    def workerAcquisition(self, min_workers, max_workers, worker_inc, acquisitions):

        avg_acq_time_num_workers = {}
        lock = mp.Lock()

        print(type(lock))
        print("Number of processes   Average acquire time(in usecs)")
        for num_workers in range(min_workers, max_workers + 1, worker_inc):

            processes = []

            start = time.monotonic()

            for _ in range(0, num_workers):
                p = mp.Process(target=numAcq, args=(lock, acquisitions))
                processes.append(p)
                p.start()

            for index in range(len(processes)):
                processes[index].join()

            end = time.monotonic()
            total_time = end - start
            avg_time_per_acquisition = (total_time / (num_workers * acquisitions)) * 1000000
            avg_acq_time_num_workers[num_workers] = avg_time_per_acquisition
            print(f"%19d%23f" % (num_workers, avg_time_per_acquisition))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Dragon Lock Performance Tests")

    parser.add_argument("--dragon", action="store_true", help="run using dragon lock")
    parser.add_argument("--min_workers", type=int, default=300, help="workers for processes to do")
    parser.add_argument("--max_workers", type=int, default=400, help="workers for processes to do")
    parser.add_argument("--worker_inc", type=int, default=10, help="workers for processes to do")
    parser.add_argument("--acquisitions", type=int, default=1000, help="acquisitions per worker")

    args = parser.parse_args()

    if args.dragon:
        mp.set_start_method("dragon")

    else:
        mp.set_start_method("spawn")

    test = LockPerformance()
    test.workerAcquisition(
        min_workers=args.min_workers,
        max_workers=args.max_workers,
        worker_inc=args.worker_inc,
        acquisitions=args.acquisitions,
    )
