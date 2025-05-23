"""This file contains Dragon multi-node acceptance test for the
`dragon.native.Array` object.  The test scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.

The test is run with `dragon test_array.py -f -v`
"""

import unittest
import numpy as np
import random
import sys

import dragon
import multiprocessing as mp

from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate


def process_function(
    event,
    array,
    queue_in,
    queue_out,
    max_val,
    barrier,
):
    p = mp.current_process()
    mypuid = p.pid

    # each child process waits on parent process
    event.wait()

    idx = queue_in.get()
    array[idx] = 1
    barrier.wait()

    # while the value is less than max_value
    while array[idx] < max_val:
        with array.get_lock():
            array[idx] += 1
            array_list = array[:]
            array[:] = array_list
            queue_out.put((mypuid, array[idx]))


class TestArrayMultiNode(unittest.TestCase):
    def test_lock_fairness(
        self,
    ):
        """Tests that array can be incremented by multiple processes in a
        multinode environment.

        1. The Test uses `nproc = mp.cpu_count()` processes.
        2. Each child process gets handed the same `Array("i", range(nproc)), lock=True)`, a
           Queue, an Event and the max_val.
        3. Each child process first waits in the Event.
        4. Once all processes have been started by the parent, the parent sets
           the event, so that all processes start working at the same time. This
           means the runtime sees a very sudden increase in activity, which is a
           test in itself.
        5. Each child process tries to acquire the lock and add elements to the array
           associated with the `Array`. This step is repeated until the element at the
           index of the Array is equal to max_val.
        6. The parent process counts how often it received an array from any of
           the puids. It prints the minimum, maximum, mean and median of the number
           of times.
        7. Finally the parent joins on all processes and check that their
           exit_code is 0, i.e. that they exited cleanly without an exception. The size of the array is checked,
           and the the first nproc values in the array are checked.
        """

        puid_dict = {}

        # number of processes for the test -- 16 should be set to a smaller number after improvements to lock contention
        nproc = max(1, mp.cpu_count() // 16)
        max_val = 4

        # each child process is handed the same array, queue, and event
        array = mp.Array("i", nproc * [0], lock=True)
        queue_in = mp.Queue(maxsize=nproc * 100)
        queue_out = mp.Queue(maxsize=nproc * 100)
        event = mp.Event()
        barrier = mp.Barrier(nproc)

        # Fill the queue with indices
        for idx in range(nproc):
            queue_in.put(idx)

        pg = ProcessGroup()
        pg.add_process(
            nproc=nproc,
            template=ProcessTemplate(
                target=process_function, args=(event, array, queue_in, queue_out, max_val, barrier)
            ),
        )
        pg.init()
        pg.start()

        # parent sets event for children processes to work at same time
        event.set()

        # Grab some values off the queue
        for _ in range(1, max_val):
            for pidx in range(nproc):
                if not queue_out.empty():
                    (
                        puid,
                        val,
                    ) = queue_out.get()
                    # increment each time child puid returns array
                    if puid in puid_dict:
                        puid_dict[puid] += 1
                    else:
                        puid_dict[puid] = 1

        pg.join()
        pg.close()

        # Finish getting all the values out of the queue:
        while not queue_out.empty():
            puid, val = queue_out.get(timeout=1)
            # increment each time child puid returns array
            if puid in puid_dict:
                puid_dict[puid] += 1
            else:
                puid_dict[puid] = 1

        numpy_puids = np.array(list(puid_dict.values()))

        # print minimum, maximum, mean, and median from puids_dict
        (puids_max, puids_min, puids_mean, puids_median) = (
            np.max(numpy_puids),
            np.min(numpy_puids),
            np.mean(numpy_puids),
            np.median(numpy_puids),
        )

        print(f"Lock acquisition, {sum(numpy_puids)/nproc} tries / proc, {nproc} processes:", flush=True)
        print(
            f"    Maximum: {puids_max}, Minimum: {puids_min}, Mean: {puids_mean}, Median: {puids_median}",
            flush=True,
        )

        # check that the child process exited cleanly in parent process

        # checks the length of the array
        self.assertLessEqual(len(array), (128 + (max_val + 1) * nproc))

        # Confirm everyone got to the max val
        for index in range(0, nproc):
            self.assertTrue(array[index] == max_val)


if __name__ == "__main__":
    mp.set_start_method(
        "dragon",
        force=True,
    )
    unittest.main()
