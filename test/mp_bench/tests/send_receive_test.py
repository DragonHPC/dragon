#!/usr/bin/env python3
"""
Basic test of multiprocessing message passing through multiprocessing.Pipe.
"""

import argparse
import multiprocessing
import time
import test_util


class SendReceiveTest:

    def __init__(self):
        self.args = None
        self.results = None

    def __setup(self):
        parser = test_util.add_default_args("Send and receive a message of varying size using a Pipe")
        parser = test_util.add_connections_args(parser)
        parser.add_argument("--response_size", type=int, default=4, help="size of message pass back")
        self.args = test_util.process_args(parser)
        multiprocessing.set_start_method("spawn")

    def run(self):
        self.__setup()
        times = test_util.iterations(self.do_send_receive, self.args)
        self.results = test_util.Results(self.args, times, "send_receive_test")
        if self.args.json:
            return self.dump()
        return self.results

    def dump(self):
        return self.results.dump()

    def dummy_func(self, out_conn, response):
        out_conn.recv()
        out_conn.send(response)

    def do_send_receive(self):

        num_workers = self.args.num_workers
        # assert self.args.message_size > 0
        message = bytes(self.args.message_size)
        response = bytes(self.args.response_size)
        assert num_workers > 0

        pipeconns = [multiprocessing.Pipe() for _ in range(num_workers)]

        processes = []
        for idx in range(num_workers):
            processes.append(
                multiprocessing.Process(
                    target=self.dummy_func,
                    args=(
                        pipeconns[idx][1],
                        response,
                    ),
                )
            )

        start_time = time.time_ns()
        for handle in processes:
            handle.start()

        for i in range(len(pipeconns)):
            pipeconns[i][0].send(message)
        for i in range(len(pipeconns)):
            pipeconns[i][0].recv()

        for handle in processes:
            handle.join()

        return time.time_ns() - start_time


if __name__ == "__main__":
    test = SendReceiveTest()
    print(test.run())
