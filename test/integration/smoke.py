#!/usr/bin/env python3
"""
Basic test of multiprocessing message passing through multiprocessing.Pipe.

"""
import sys

import dragon.mpbridge
import dragon.infrastructure.connection
import dragon.infrastructure.log_setup as dlog

import multiprocessing
import time
import dragon.infrastructure.util as dutil
import logging
import mp_bench.util.test_util as test_util


# could this be a deadly embrace sort of thing
# given that open() happens and something
# is being asked for out of order in full duplex world?
# I think that could have to do with this...


def external_single_echo(conn, resp_size, idx):
    conn.recv()
    conn.send(bytes(resp_size))


class SendReceiveTest:

    def __init__(self):
        parser = test_util.add_default_args("Send and receive a message of varying size using a Pipe")
        parser = test_util.add_connections_args(parser)
        parser.add_argument("--response_size", type=int, default=4, help="size of message pass back")
        parser.add_argument("--dragon", action="store_true", help="run a la single node dragon")
        self.args = test_util.process_args(parser)
        self.results = None

        if self.args.dragon:
            multiprocessing.set_start_method("dragon")  # could call connect to infrastructure here if we wanted in
            # check avail I guess.
            self.Pipe = dragon.infrastructure.connection.Pipe
            self.Process = dragon.mpbridge.DragonProcess
        else:
            multiprocessing.set_start_method("spawn")
            self.Pipe = multiprocessing.Pipe
            self.Process = multiprocessing.Process

    def run(self):

        times = test_util.iterations(self.do_send_receive, self.args)
        self.results = test_util.Results(self.args, times, "send_receive_test")
        if self.args.json:
            return self.dump()
        return self.results

    def dump(self):
        return self.results.dump()

    def do_send_receive(self):

        num_workers = self.args.num_workers

        message = bytes(self.args.message_size)

        pipeconns = [self.Pipe() for _ in range(num_workers)]

        processes = []
        for idx in range(num_workers):
            processes.append(
                self.Process(target=external_single_echo, args=(pipeconns[idx][1], self.args.response_size, idx))
            )

        start_time = time.time_ns()

        for idx, handle in enumerate(processes):
            handle.start()

        for i in range(num_workers):
            pipeconns[i][0].send(message)

        for i in range(num_workers):
            pipeconns[i][0].recv()

        for idx, handle in enumerate(processes):
            handle.join()

        return time.time_ns() - start_time


if __name__ == "__main__":
    dlog.setup_logging(basename="head", the_level=logging.DEBUG)
    test = SendReceiveTest()
    print(test.run())
