#!/usr/bin/env python3
"""
Basic test of multiprocessing message passing through multiprocessing.Pipe.
"""

import argparse
import multiprocessing
import time
import test_util


class ChainTokenPipeTest:

    def __init__(self):
        self.args = None
        self.results = None

    def __setup(self):
        parser = test_util.add_default_args("Basic token passing test, passing around a chain using Pipe")
        parser = test_util.add_token_args(parser)
        self.args = test_util.process_args(parser)
        multiprocessing.set_start_method("spawn")

    def run(self):
        self.__setup()
        times = test_util.iterations(self.do_pipe_token_ring, self.args)
        self.results = test_util.Results(self.args, times, "chain_token_pipe_test")
        if self.args.json:
            return self.dump()
        return self.results

    def dump(self):
        return self.results.dump()

    def pass_it(self, is_first, spins, in_conn, out_conn, message, num_tokens):
        """Worker function to pass a cookie around.

        The is_first one generates the cookie and absorbs it.
        All the other wait and pass the cookie.
        """

        if is_first:
            for _ in range(num_tokens):
                out_conn.send(message)
            for _ in range((spins * num_tokens) - 1):
                item = in_conn.recv()
                out_conn.send(item)

            last = in_conn.recv()
            assert message == last
        else:
            for _ in range(spins * num_tokens):
                item = in_conn.recv()
                out_conn.send(item)

    def do_pipe_token_ring(self):

        num_workers = self.args.num_workers
        spins = self.args.spins
        num_tokens = self.args.num_tokens
        assert num_workers > 0
        assert self.args.message_size >= 0
        message = bytes(self.args.message_size)

        start_time = time.time_ns()

        pipeconns = [multiprocessing.Pipe(duplex=False) for _ in range(num_workers)]
        processes = []
        for idx in range(num_workers):
            processes.append(
                multiprocessing.Process(
                    target=self.pass_it,
                    args=(
                        (idx == 0),
                        spins,
                        pipeconns[(idx - 1) % num_workers][0],
                        pipeconns[idx][1],
                        message,
                        num_tokens,
                    ),
                )
            )
        for handle in processes:
            handle.start()

        for handle in processes:
            handle.join()

        return time.time_ns() - start_time


if __name__ == "__main__":
    test = ChainTokenPipeTest()
    print(test.run())
