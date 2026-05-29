"""This file contains Dragon multi-node acceptance test for the
`dragon.native.Value` object.  The test scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.

The test is run with `dragon test_value.py -f -v`
"""

import unittest
import numpy as np

import dragon
import multiprocessing as mp
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate


def process_function(
    event,
    value,
    queue,
    max_val,
):
    p = mp.current_process()
    mypuid = p.pid

    # each child process waits on parent process
    event.wait()

    val = -1

    while val < max_val:
        with value.get_lock():
            val = value.value
            if val <= max_val:
                value.value += 1
                queue.put((mypuid, val))


class TestValueMultiNode(unittest.TestCase):
    def test_lock_fairness(
        self,
    ):
        """Tests that value can be incremented by multiple processes in a
        multinode environment.

        1. The Test uses `nproc = mp.cpu_count()` processes.
        2. Each child process gets handed the same `Value(int(0), lock=True)`, a
           Queue, an Event and the max_val.
        3. Each child process first waits in the Event.
        4. Once all processes have been started by the parent, the parent sets
           the event, so that all processes start working at the same time. This
           means the runtime sees a very sudden increase in activity, which is a
           test in itself.
        5. Each child process tries to acquire the lock and increase the value
           associated with the `Value`. This (step 6) is repeated until Value is equal to max_val.
        6. When the lock was acquired, the child process saves the current value
           from the Value object in a variable, and then increments the value in
           the Value object by 1. It then puts the saved value and its PUID into
           the queue and releases the lock.
        7. The parent process `get`s from the Queue `max_val` times without a
           timeout and checks that the values it is receiving from the Queue are
           monotonically increasing by 1 each time.
        8. The parent process counts how often it received a value from any of
           the puids. It prints the minimum, maximum, mean and median of the number
           of times.
        9. Finally the parent joins on all processes and check that their
           exit_code is 0, i.e. that they exited cleanly without an exception
        """

        puid_dict = {}

        # number of processes for the test
        nproc = max(2, mp.cpu_count() // 32)
        max_val = nproc * 8

        # each child process is handed the same value, queue, and event
        value = mp.Value("i", 0, lock=True)
        queue = mp.Queue(maxsize=nproc * 100)
        event = mp.Event()

        pg = ProcessGroup()
        pg.add_process(
            nproc=nproc, template=ProcessTemplate(target=process_function, args=(event, value, queue, max_val))
        )
        pg.init()
        pg.start()

        # parent sets event for children processes to work at same time
        event.set()

        cnt = 0
        for _ in range(0, max_val):
            (puid, val) = queue.get()

            # check that the value from child process is valid
            self.assertTrue(val == cnt)
            cnt += 1

            # increment each time child puid returns value
            if puid in puid_dict:
                puid_dict[puid] += 1
            else:
                puid_dict[puid] = 1

        numpy_puids = np.array(list(puid_dict.values()))

        # pring minimum, maximum, mean, and median from puids_dict
        (puids_max, puids_min, puids_mean, puids_median) = (
            np.max(numpy_puids),
            np.min(numpy_puids),
            np.mean(numpy_puids),
            np.median(numpy_puids),
        )

        print(f"Lock acquisition, {max_val/nproc} tries / proc, {nproc} processes:")
        print(f"    Maximum: {puids_max}, Minimum: {puids_min}, Mean: {puids_mean}, Median: {puids_median}")

        # check that the child process exited cleanly in parent process
        pg.join()
        pg.close()


if __name__ == "__main__":
    mp.set_start_method(
        "dragon",
        force=True,
    )
    unittest.main()
