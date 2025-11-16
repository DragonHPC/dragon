"""A simple recursive merge sort implementation using Python Multiprocessing.
It starts a process for every sublist until the sublist contains less than
CUTOFF elements.
`__main__` actually executes (MAX_SIZE-MIN_SIZE)/INCREMENT merge sorts of
increasingly large sublists, measures the time and prints the timing results.

See e.g. Knuth, The Art of Computer Programming, 1998, Vol. 3, section 5.2.4.
"""

import random
import sys
import time
import math
import gc

import dragon
import multiprocessing as mp
from dragon.telemetry import AnalysisClient, Telemetry

# values are chosen so memory usage fits into the default Dragon memory pool of 4GB
CUTOFF = 20000
MIN_SIZE = 100000
MAX_SIZE = 1000000
INCREMENT = MIN_SIZE


def merge(left: list, right: list) -> list:
    """This function merges two lists.

    :param left: First list containing data
    :type left: list
    :param right: Second list containing data
    :type right: list
    :return: Merged data
    :rtype: list
    """

    merged_list = [None] * (len(left) + len(right))

    i = 0
    j = 0
    k = 0

    while i < len(left) and j < len(right):
        if left[i] < right[j]:
            merged_list[k] = left[i]
            i = i + 1
        else:
            merged_list[k] = right[j]
            j = j + 1
        k = k + 1

    # When we are done with the while loop above
    # it is either the case that i > midpoint or
    # that j > end but not both.

    # finish up copying over the 1st list if needed
    while i < len(left):
        merged_list[k] = left[i]
        i = i + 1
        k = k + 1

    # finish up copying over the 2nd list if needed
    while j < len(right):
        merged_list[k] = right[j]
        j = j + 1
        k = k + 1

    return merged_list


def parallel_merge_sort(chunk: list, sorted_chunk_queue: object, stop_recursion: object) -> None:
    """Recursive merge sort function. Below `cutoff` items, sort immediately.
    Otherwise, divide the input list `chunk` into two, start two processes
    executing the same function on one of the sublist. Wait for their result in
    the `result_queue`. Merge the two results and put the resulting list into
    `sorted_chunk_queue`.

    :param chunk: sub-list to recursively sort
    :type chunk: list
    :param sorted_chunk_queue: Queue to put the merged list into
    :type sorted_chunk_queue: mp.Queue object
    :param stop_recursion: event that is set when we have too many recursed processes
    :type stop_recursion: mp.Event
    """

    if stop_recursion.is_set() or len(chunk) < 2:
        dt = Telemetry()
        dt.add_data("merge_cutoff", len(chunk))
        chunk.sort()
        sorted_chunk_queue.put(chunk)

    else:
        midpoint = len(chunk) // 2

        left_chunk = chunk[:midpoint]
        right_chunk = chunk[midpoint:]

        result_queue = mp.Queue()

        # do some hard work so we don't recurse too quickly
        time.sleep(1)

        left_proc = mp.Process(target=parallel_merge_sort, args=(left_chunk, result_queue, stop_recursion))
        right_proc = mp.Process(target=parallel_merge_sort, args=(right_chunk, result_queue, stop_recursion))

        left_proc.start()
        right_proc.start()

        result_a = result_queue.get(timeout=None)  # blocking
        right_b = result_queue.get(timeout=None)

        result = merge(result_a, right_b)

        sorted_chunk_queue.put(result)
        gc.collect()


def merge_sort(data: list, size: int, stop_recursion) -> int:
    """Kick off merge sort on a slice of `data` of size `size`,
    measure the runtime and return it.

    :param data: The whole input data
    :type data: list
    :param size: the size of the slice to sort
    :type size: int
    :param cutoff: when to stop recursing
    :type cutoff: int
    :return: runtime in seconds
    :rtype: int
    """

    the_lst = data[:size]

    start = time.perf_counter()

    result_queue = mp.Queue()
    parallel_merge_sort(the_lst, result_queue, stop_recursion)
    result = result_queue.get()

    the_lst.clear()
    the_lst.extend(result)

    end = time.perf_counter()
    delta = end - start

    return delta


def procs_watcher(stop_recursion, start_time, node_recursion_limit=64):
    tac = AnalysisClient()
    tac.connect(timeout=60)
    while not stop_recursion.is_set():
        num_python_procs_data = tac.get_data("num_running_processes", start_time=start_time)
        for data in num_python_procs_data:
            if any([num_procs > node_recursion_limit for num_procs in data["dps"].values()]):
                stop_recursion.set()
                break


def final_max_num_procs(start_time):
    tac = AnalysisClient()
    tac.connect()
    nproc_merge_data = tac.get_data(["num_running_processes", "merge_cutoff"], start_time=start_time)
    results = []
    for data_nproc in nproc_merge_data:
        if data_nproc["metric"] == "num_running_processes":
            host = data_nproc["tags"]["host"]
            max_nproc = max([num_procs for num_procs in data_nproc["dps"].values()])
            for data_cutoff in nproc_merge_data:
                if data_cutoff["metric"] == "merge_cutoff":
                    if data_cutoff["tags"]["host"] == host:
                        last_write = max([int(t) for t in data_cutoff["dps"].keys()])
                        cutoff = data_cutoff["dps"][str(last_write)]
                        break
            results.append((host, max_nproc, cutoff))

    return results


if __name__ == "__main__":

    mp.set_start_method("dragon")
    dt = Telemetry()

    # ensures that collectors are collecting the number of python procs
    assert (
        dt.level > 1
    ), "This example requires a telemetry level of 2 or greater. Re-run with dragon --telemetry-level=2 merge_sort.py."

    data: list = [random.randrange(MAX_SIZE) for i in range(MAX_SIZE)]

    print(
        f"    List Size    Time (seconds)      Hostname     Processes     Cutoff",
        flush=True,
    )

    for size in range(MIN_SIZE, MAX_SIZE + 1, INCREMENT):
        stop_recursion = mp.Event()
        start_time = int(time.time())
        # process that watches the number of python processes on a node
        # and sets an event to prevent any more recursion.
        watcher = mp.Process(target=procs_watcher, args=(stop_recursion, start_time, 32))
        watcher.start()
        delta = merge_sort(data, size, stop_recursion)
        watcher.join()
        # gets the final cutoff and
        node_proc_data = final_max_num_procs(start_time)
        for host, proc_count, cutoff in node_proc_data:
            print(f"{size:13d}    {delta:14.6f}    {host}{proc_count:12}{cutoff:12}")

    tac = AnalysisClient()
    tac.connect()
    metrics = tac.get_metrics()
    print(f"All available telmetery metrics: {metrics}", flush=True)

    dt.finalize()
