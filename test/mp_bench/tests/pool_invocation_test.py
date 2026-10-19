#!/usr/bin/env python3

"""
Basic performance test of multiprocessing.Pool
"""

import multiprocessing
import time
import test_util


class PoolInvocationTest:

    def __init__(self):
        self.args = None
        self.results = None

    def __setup(self):
        parser = test_util.add_default_args("Does a basic speed test of dispatching a task in an existingPool")
        self.args = test_util.process_args(parser)
        multiprocessing.set_start_method("spawn")

    def run(self):
        self.__setup()
        times = test_util.iterations(self.do_spawn_gather, self.args)
        self.results = test_util.Results(self.args, times, "pool_invocation_test")
        if self.args.json:
            return self.dump()
        return self.results

    def dump(self):
        return self.results.dump()

    def dummy_func(self, x):
        return 42

    def do_spawn_gather(self):
        """Spawns a bunch of processes in a pool that do nothing and return.

        :return: Time it took to do this in ns
        """

        num_workers = self.args.num_workers

        assert num_workers > 0

        completions = []
        completions2 = []
        with multiprocessing.Pool(num_workers) as the_pool:
            for idx in range(num_workers):
                completions.append(the_pool.apply_async(self.dummy_func, (idx,)))

            for k in completions:
                k.get()

            start_time = time.time_ns()
            for idy in range(num_workers):
                completions2.append(the_pool.apply_async(self.dummy_func, (idy,)))

            for j in completions2:
                j.get()

        return time.time_ns() - start_time


if __name__ == "__main__":
    test = PoolInvocationTest()
    print(test.run())
