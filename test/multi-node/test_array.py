""" This file contains Dragon multi-node acceptance test for the
`dragon.native.Array` object.  The test scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.

The test is run with `dragon test_array.py -f -v`
"""
import unittest
import os
import sys
import numpy as np

import dragon
import multiprocessing as mp
import random


def process_function(event, array, queue, max_val, idx, barrier,):

    p = mp.current_process()
    mypuid = p.pid

    # each child process waits on parent process
    event.wait()

    array[idx] = 1
    barrier.wait()

    #while the value is less than max_value
    while array[idx] < max_val:
        with array.get_lock():
            array[idx] += 1
            array_list = array[:]
            array_list.append(random.randint(-sys.maxsize - 1, sys.maxsize))
            array[:] = array_list
            queue.put((mypuid, array[idx],))


class TestArrayMultiNode(unittest.TestCase):
    @unittest.skip('fix forthcoming')
    def test_lock_fairness(self,):
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
           associated with the `Array`. This (step 6) is repeated until the element at the
           index of the Array is equal to max_val.
        6. When the lock was acquired, also adds a random element to the array while the element at the selected
            index of the array is being incremented to max_val.
        7. The parent process counts how often it received a array from any of
           the puids. It prints the minimum, maximum, mean and median of the number
           of times.
        8. Finally the parent joins on all processes and check that their
           exit_code is 0, i.e. that they exited cleanly without an exception. The size of the array is checked,
           and the the first nproc values in the array are checked.
        """

        procs = []
        puid_dict = {}

        # number of processes for the test
        nproc = mp.cpu_count() // 16
        max_val = 2

        # each child process is handed the same array, queue, and event
        array = mp.Array("i", range(128+nproc), lock=True,)
        queue = mp.Queue()
        event = mp.Event()
        barrier = mp.Barrier(nproc)

        for idx in range(nproc):
            p = mp.Process(target=process_function,args=(event, array, queue, max_val, idx, barrier,),)
            p.start()
            procs.append(p)
            puid_dict[p.pid] = 0

        # parent sets event for children processes to work at same time
        event.set()

        for _ in range(1, max_val):
            for pidx in range(nproc):
                puid, val, = (queue.get(timeout=10))
                # increment each time child puid returns array
                puid_dict[puid] += 1

        numpy_puids = np.array(list(puid_dict.values()))

        # print minimum, maximum, mean, and median from puids_dict
        (puids_max,puids_min,puids_mean,puids_median,) = (np.max(numpy_puids),np.min(numpy_puids),np.mean(numpy_puids),np.median(numpy_puids),)

        print(f"Lock acquisition, {max_val/nproc} tries / proc, {nproc} processes:", flush=True)
        print(f"    Maximum: {puids_max}, Minimum: {puids_min}, Mean: {puids_mean}, Median: {puids_median}", flush=True)

        # check that the child process exited cleanly in parent process
        for p in procs:
            p.join()
            self.assertEqual(p.exitcode,0,)

        #checks the length of the array
        self.assertLessEqual(len(array), (128 + (max_val+1) * nproc))

        #checks that all the integers in the array fall in the range of ints possible
        for index in range(0, nproc):
            self.assertTrue((-sys.maxsize - 1) <= array[index] <= (sys.maxsize))



if (__name__== "__main__"):
    mp.set_start_method("dragon",force=True,)
    unittest.main()
