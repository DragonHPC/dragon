"""A simple recursive merge sort implementation using Python Multiprocessing.
It starts a process for every sublist until the sublist contains less than
CUTOFF elements.
`__main__` actually executes (MAX_SIZE-MIN_SIZE)/INCREMENT merge sorts of
increasingly large sublists, measures the time and prints the timing results.

See e.g. Knuth, The Art of Computer Programming, 1998, Vol. 3, section 5.2.4.
"""

import gc
import random
import sys
import time
import math

import dragon
import multiprocessing as mp

# values are chosen so memory usage fits into the default Dragon memory pool of 4GB
CUTOFF = 20000
MIN_SIZE = 100000
MAX_SIZE = 500000
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


def parallel_merge_sort(chunk: list, cutoff: int, sorted_chunk_queue: object) -> None:
    """Recursive merge sort function. Below `cutoff` items, sort immediately.
    Otherwise, divide the input list `chunk` into two, start two processes
    executing the same function on one of the sublist. Wait for their result in
    the `result_queue`. Merge the two results and put the resulting list into
    `sorted_chunk_queue`.

    :param chunk: sub-list to recursively sort
    :type chunk: list
    :param cutoff: number of items below which the list will be sorted immediately
    :type cutoff: int
    :param sorted_chunk_queue: Queue to put the merged list into
    :type sorted_chunk_queue: mp.Queue object
    """

    if len(chunk) <= cutoff:
        chunk.sort()
        sorted_chunk_queue.put(chunk)

    else:
        midpoint = len(chunk) // 2

        left_chunk = chunk[:midpoint]
        right_chunk = chunk[midpoint:]

        result_queue = mp.Queue()

        left_proc = mp.Process(target=parallel_merge_sort, args=(left_chunk, cutoff, result_queue))
        right_proc = mp.Process(target=parallel_merge_sort, args=(right_chunk, cutoff, result_queue))

        left_proc.start()
        right_proc.start()

        result_a = result_queue.get(timeout=None)  # blocking
        result_b = result_queue.get(timeout=None)

        result = merge(result_a, result_b)

        del result_a
        del result_b
        del result_queue
        gc.collect()

        sorted_chunk_queue.put(result)
        left_proc.join()
        right_proc.join()


def merge_sort(data: list, size: int, cutoff: int) -> int:
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
    parallel_merge_sort(the_lst, cutoff, result_queue)
    result = result_queue.get()

    the_lst.clear()
    the_lst.extend(result)

    end = time.perf_counter()
    delta = end - start

    return delta


def find_number_of_processes(n: int, cutoff: int) -> int:
    """Return the number of processes started by effectively
    replaying the recursion.

    :param n: number of elements
    :type n: int
    :param cutoff: umber of items below which no additional process is started
    :type cutoff: int
    :return: number of started processes
    :rtype: int
    """

    procs = 1

    while True:
        left = n // 2
        right = n - left
        procs = 2 * procs + 1

        if left <= cutoff:
            if right > cutoff:
                procs = procs + 2 * left

            return procs

        n = n // 2


if __name__ == "__main__":

    if "dragon" in sys.argv:
        mp.set_start_method("dragon")

    data: list = [random.randrange(MAX_SIZE) for i in range(MAX_SIZE)]

    print(
        f"    List Size    Time (seconds)    Processes    Channels (or Queues) with cutoff={CUTOFF}",
        flush=True,
    )

    for size in range(MIN_SIZE, MAX_SIZE + 1, INCREMENT):
        delta = merge_sort(data, size, CUTOFF)
        proc_count = find_number_of_processes(size, CUTOFF)
        channel_count = proc_count // 2
        print(f"{size:13d}    {delta:14.6f}{proc_count:12}{channel_count:12}")

