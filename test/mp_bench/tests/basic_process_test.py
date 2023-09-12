#!/usr/bin/env python3

"""
Basic performance test of multiprocessing.Process
"""

import multiprocessing
import time
import test_util

class BasicProcessTest:
    
    def __init__(self):
        self.args = None
        self.results = None

    def __setup(self):
        parser = test_util.add_default_args('Does a basic speed test of spawning processes')
        self.args = test_util.process_args(parser)
        multiprocessing.set_start_method('spawn')

    def run(self):
        self.__setup()
        times = test_util.iterations(self.do_spawn_gather, self.args)
        self.results = test_util.Results(self.args, times, "basic_process_test")
        if self.args.json:
            return self.dump()
        return self.results

    def dump(self):
        return self.results.dump()


    def dummy_func(self, x):
        return x

    def do_spawn_gather(self):
        """Spawns a bunch of processes that do nothing and return.

        :return: Time it took to do this in ns
        """
        num_workers = self.args.num_workers

        assert num_workers > 0

        process_handles = []

        start_time = time.time_ns()

        for _ in range(num_workers):
            handle = multiprocessing.Process()
            process_handles.append(handle)

        for handle in process_handles:
            handle.start()

        for handle in process_handles:
            handle.join()

        return time.time_ns() - start_time


if __name__ == '__main__':
    #f = open("basic_pool_test.py", "r")
    test = BasicProcessTest()
    results = test.run()
    print(results)
    #f.close()
