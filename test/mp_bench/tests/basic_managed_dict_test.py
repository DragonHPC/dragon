#!/usr/bin/env python3

"""
Basic performance test of multiprocessing managed dictionary
"""
import argparse
import multiprocessing
import time
import test_util

class BasicManagedDictTest:

    def __init__(self):
        self.args = None
        self.results = None
        self.test = 0

    def __setup(self):
        parser = test_util.add_default_args('Does a basic speed test of managed dicts')
        parser.add_argument('--spins', type=int, default=4,
                             help='number of times for each worker to touch the dictionary')
        parser = test_util.add_connections_args(parser)
        self.args = test_util.process_args(parser)
        multiprocessing.set_start_method('spawn')

    def run(self):
        self.__setup()
        times = test_util.iterations(self.do_managed_dict_test, self.args)
        self.results = test_util.Results(self.args, times, "basic_managed_dict_test")
        if self.args.json:
            return self.dump()
        return self.results

    def dump(self):
        return self.results.dump()

    def worker_entry(self, x, key_to_set, spins, message):
        for idx in range(spins):
            x[key_to_set] = message + bytes(idx)

    def do_managed_dict_test(self):
        """Spawns a bunch of processes that set a key in a dictionary and return.

        They all set a different key.

        :return: Time it took to do this in ns
        """

        num_workers = self.args.num_workers
        spins = self.args.spins
        assert self.args.message_size > 0
        message = bytes(self.args.message_size)
        assert num_workers > 0

        process_handles = []

        start_time = time.time_ns()

        with multiprocessing.Manager() as mgr:

            d = mgr.dict()
            for idx in range(num_workers):
                handle = multiprocessing.Process(target=self.worker_entry, args=(d, idx, spins, message))
                process_handles.append(handle)

            for handle in process_handles:
                handle.start()

            for handle in process_handles:
                handle.join()


        return time.time_ns() - start_time



if __name__ == '__main__':
    test = BasicManagedDictTest()
    print(test.run())
