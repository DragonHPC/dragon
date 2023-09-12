#!/usr/bin/env python3
"""
Basic test of multiprocessing message passing through queues.
"""

import argparse
import multiprocessing
import time
import test_util

class ChainTokenTest:

    def __init__(self):
        self.args = None
        self.results = None

    def __setup(self):
        parser = test_util.add_default_args('Basic token passing test, passing around a chain using Queue')
        parser = test_util.add_token_args(parser)
        self.args = test_util.process_args(parser)
        multiprocessing.set_start_method('spawn')

    def run(self):
        self.__setup()
        times = test_util.iterations(self.do_queue_token_ring, self.args)
        self.results = test_util.Results(self.args, times, 'chain_token_test')
        if self.args.json:
            return self.dump()
        return self.results

    def dump(self):
        return self.results.dump()


    def pass_it(self, is_first, spins, in_queue, out_queue, message, num_tokens):
        """Worker function to pass a cookie around.

        The is_first one generates the cookie and absorbs it.
        All the other wait and pass the cookie.
        """

        if is_first:
            for _ in range(num_tokens):
                out_queue.put(message)
            for _ in range((spins*num_tokens) - 1):
                item = in_queue.get()
                out_queue.put(item)

            last = in_queue.get()
            #assert (message == last)
        else:
            for _ in range(spins*num_tokens):
                item = in_queue.get()
                out_queue.put(item)


    def do_queue_token_ring(self):

        num_workers = self.args.num_workers
        spins = self.args.spins
        message = bytes(self.args.message_size)
        num_tokens = self.args.num_tokens
        assert num_workers > 0
        assert num_tokens > 0

        start_time = time.time_ns()

        queues = [multiprocessing.Queue() for _ in range(num_workers)]
        processes = []
        for idx in range(num_workers):
            processes.append(multiprocessing.Process(target=self.pass_it,
                                                     args=((idx == 0), spins,
                                                           queues[idx], 
                                                           queues[(idx + 1) % num_workers],
                                                           message, num_tokens)
                                                     ))
        for handle in processes:
            handle.start()

        for handle in processes:
            handle.join()

        return time.time_ns() - start_time


if __name__ == '__main__':
    test = ChainTokenTest()
    print(test.run())
