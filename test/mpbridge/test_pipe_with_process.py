"""
Basic test of multiprocessing message passing through multiprocessing.Pipe.

Unit test adapted from original 'fun_srt.py' code.
"""

import dragon.mpbridge
import dragon.infrastructure.connection
import dragon.infrastructure.messages as dmsg

import dragon.dlogging.util as dlog
from dragon.utils import B64

import multiprocessing
import time
import logging
import mp_bench.util.test_util as test_util

from collections import namedtuple
import os
import unittest
from unittest.mock import patch

POOL_SIZE = 4194304
DEFAULT_UID = 123456
POOL_NAME = f"log_test_{os.getpid()}"


def setUpModule():
    multiprocessing.set_start_method("dragon", force=True)


# could this be a deadly embrace sort of thing
# given that open() happens and something
# is being asked for out of order in full duplex world?
# I think that could have to do with this...


@patch.dict(os.environ, {"DRAGON_LOG_DEVICE_STDERR": "INFO"})
def external_single_echo(conn, resp_size, serialized_dlogging, idx):
    dlog.setup_BE_logging(service=dlog.DragonLoggingServices.TEST, logger_sdesc=B64(serialized_dlogging))
    logger = logging.getLogger(dlog.DragonLoggingServices.TEST)

    logger.info(f"{idx}: external_single_echo")
    conn.recv()
    conn.send(bytes(resp_size))


class SendReceiveTest(unittest.TestCase):
    def setUp(self):
        self.assertEqual(multiprocessing.get_start_method(), "dragon")

        # parser = test_util.add_default_args('Send and receive a message of varying size using a Pipe')
        # parser = test_util.add_connections_args(parser)
        # parser.add_argument('--response_size', type=int, default=4, help='size of message pass back')
        # parser.add_argument('--dragon', action='store_true', help='run a la single node dragon')
        # self.args = test_util.process_args(parser)
        FakeArgs = namedtuple(
            "FakeArgs",
            ["burn_iterations", "json", "message_size", "num_workers", "response_size", "num_iterations"],
        )
        self.response_size = 4
        self.args = FakeArgs(10, True, 100, 2, self.response_size, 3)
        self.results = None

        # check avail I guess.
        self.Pipe = dragon.infrastructure.connection.Pipe
        self.Process = dragon.mpbridge.DragonProcess

    @unittest.skipUnless("DRAGON_MODE" in os.environ, "Must be launched as a dragon process")
    def test_run(self):

        times = test_util.iterations(self.do_send_receive, self.args)
        self.results = test_util.Results(self.args, times, "send_receive_test")
        self.assertEqual(self.response_size, self.results.response_size)

    def do_send_receive(self):

        num_workers = self.args.num_workers

        message = bytes(self.args.message_size)

        pipeconns = [self.Pipe() for _ in range(num_workers)]

        dragon_logger = dlog.setup_dragon_logging(0)
        serialized_dlogging = dragon_logger.serialize()

        processes = []
        for idx in range(num_workers):

            processes.append(
                self.Process(
                    target=external_single_echo,
                    args=(pipeconns[idx][1], self.args.response_size, serialized_dlogging, idx),
                )
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

        num_logs = dragon_logger.num_logs()
        self.assertEqual(num_logs, num_workers, f"expected {num_workers} log entries, but there's only {num_logs}")
        found_worker_log_msg = [False for _ in range(num_workers)]
        for i in range(num_workers):
            msg = dmsg.parse(dragon_logger.get(i))
            worker = int(msg.msg.split(":")[0])
            self.assertFalse(found_worker_log_msg[worker - 1], f"duplicate log message received for node {worker}")
            found_worker_log_msg[worker - 1] = True
        self.assertTrue(all(found_worker_log_msg), "One or more log messages not found in DragonLogging chanel")
        dragon_logger.destroy()

        return time.time_ns() - start_time


if __name__ == "__main__":
    dlog.setup_logging(basename="head", the_level=logging.DEBUG)

    unittest.main()
