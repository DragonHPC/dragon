#!/usr/bin/env python3
import os
import sys
import time
import unittest
import random
import string
import subprocess
import logging
import queue
from contextlib import redirect_stderr
from unittest.mock import MagicMock, patch
from io import StringIO

import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.facts as dfacts
import dragon.dlogging.util as dlog
import dragon.launcher.util as dlutil
import dragon.infrastructure.util as dutil
import dragon.utils as du

from dragon.dlogging.logger import DragonLogger, DragonLoggingError
from dragon.channels import ChannelEmpty
from dragon.utils import B64


def generate_msg(length):
    return "".join(random.choice(string.ascii_letters + string.digits + " ") for i in range(length))


def run_logger(info_msg, debug_msg):

    # set up my stdin for listening to my channel descriptor
    logger_sdesc = B64.from_str(os.environ.get(dfacts.DRAGON_LOGGER_SDESC))

    # Log stuff
    level, full_fname = dlog.setup_BE_logging(service=dlog.DragonLoggingServices.TEST, logger_sdesc=logger_sdesc)

    if level <= logging.DEBUG:
        l_stdout = dutil.NewlineStreamWrapper(sys.stdout, read_intent=False)
        l_stdout.send(full_fname)

    log = logging.getLogger(dlog.DragonLoggingServices.TEST)
    log.info(info_msg)
    log.debug(debug_msg)


def run_logging_subprocesses(nproc, level, sdesc):

    # Update environment
    the_env = os.environ
    the_env["DRAGON_LOGGER_SDESC"] = du.B64.bytes_to_str(sdesc)

    info_msgs = []
    debug_msgs = []
    log_procs = []
    log_stdins = []
    fnames = []
    for i in range(nproc):
        info_msgs.append(generate_msg(random.randrange(20, 100)))
        debug_msgs.append(generate_msg(random.randrange(20, 100)))
        run_string = f"from utils.test_logging import run_logger;  run_logger('{info_msgs[-1]}', '{debug_msgs[-1]}')"
        log_procs.append(
            subprocess.Popen(["python3", "-c", run_string], env=the_env, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        )
        log_stdins.append(dutil.NewlineStreamWrapper(log_procs[-1].stdout, write_intent=False))

    if level <= logging.DEBUG:
        fnames = [log_stdin.recv().rstrip() for log_stdin in log_stdins]
    else:
        fnames = None

    for p in log_procs:
        p.stdin.close()
        p.stdout.close()
        p.wait()

    return info_msgs, debug_msgs, fnames


def close_handlers(log):

    for handler in log.handlers:
        log.removeHandler(handler)
        handler.close()


class LoggingTest(unittest.TestCase):

    def setUp(self):
        self.mpool = dlog._get_logging_mpool(0)
        self.logger = None

    def tearDown(self):
        if self.logger is None:
            self.mpool.destroy()
        else:
            self.logger.destroy()  # This also nukes the underlying pool, change that behavior?

    def test_create(self):
        self.logger = DragonLogger(self.mpool)

    def test_put(self):
        self.logger = DragonLogger(self.mpool)
        self.logger.put("A")

    def test_put_get(self):
        self.logger = DragonLogger(self.mpool)
        self.logger.put("aaaa")
        a = self.logger.get()
        self.assertEqual(a, "aaaa")

    def test_put_get_many(self):
        self.logger = DragonLogger(self.mpool)
        abcs = "abcdefghijklmnopqrstuvwxyz"
        for i in range(0, len(abcs)):
            self.logger.put(abcs[i])

        for i in range(0, len(abcs)):
            msg = self.logger.get()
            self.assertEqual(msg, abcs[i])

    def test_get_timeout(self):
        # What else would be good test cases for timeout besides "it times out"?
        self.logger = DragonLogger(self.mpool)
        with self.assertRaises(ChannelEmpty):
            self.logger.get(logging.WARNING, timeout=1)

    def test_put_get_priority(self):
        self.logger = DragonLogger(self.mpool)
        self.logger.put("debug", logging.DEBUG)
        self.logger.put("info", logging.INFO)
        self.logger.put("warning", logging.WARNING)
        self.logger.put("error", logging.ERROR)

        # Only get messages at warning or above
        msg_debug = self.logger.get(logging.WARNING)
        msg_info = self.logger.get(logging.WARNING)
        msg_warning = self.logger.get(logging.WARNING)
        msg_error = self.logger.get(logging.WARNING)

        self.assertEqual(msg_debug, None)
        self.assertEqual(msg_info, None)
        self.assertEqual(msg_warning, "warning")
        self.assertEqual(msg_error, "error")


class TestLogHandler(unittest.TestCase):

    def setUp(self) -> None:
        self.info_log_message = self.get_random_string(72)
        self.debug_log_message = self.get_random_string(72)

    def get_random_string(self, length: int) -> str:
        # choose from all lowercase letter
        return "".join(random.choice(string.ascii_lowercase) for i in range(length))

    @patch.dict(os.environ, {"DRAGON_LOG_DEVICE_STDERR": "INFO"})
    @patch("dragon.dlogging.util.DragonLogger", autospec=True)
    def test_dragon_loghandler_mock(self, mock_dragon_logger) -> None:

        instance_mock = MagicMock()
        mock_dragon_logger.attach.return_value = instance_mock
        serialized_log_descr = B64.from_str(self.get_random_string(1024))

        level, _ = dlog.setup_BE_logging(service=dlog.DragonLoggingServices.TEST, logger_sdesc=serialized_log_descr)
        py_logger = logging.getLogger(dlog.DragonLoggingServices.TEST)
        py_logger.info(self.info_log_message)
        py_logger.debug(self.debug_log_message)

        mock_dragon_logger.attach.assert_called_with(serialized_log_descr.decode(), mpool=None)
        self.assertEqual(instance_mock.put.call_count, 1)

        serial_msg, level = instance_mock.mock_calls[-1].args
        msg = dmsg.parse(serial_msg)
        self.assertEqual(msg.msg, self.info_log_message)
        self.assertEqual(level, logging.INFO)

    @patch.dict(os.environ, {"DRAGON_LOG_DEVICE_STDERR": "INFO"})
    def test_dragon_loghandler_to_dragon_logging_layer(self) -> None:
        from logging import INFO

        dragon_logger = dlog.setup_dragon_logging(0)
        dlog.setup_BE_logging(service=dlog.DragonLoggingServices.TEST, logger_sdesc=B64(dragon_logger.serialize()))
        py_logger = logging.getLogger(dlog.DragonLoggingServices.TEST)
        py_logger.info(self.info_log_message)
        msg = dmsg.parse(dragon_logger.get(INFO))
        self.assertEqual(msg.msg, self.info_log_message)

        dragon_logger.destroy()  # This also nukes the underlying pool


class TestLoggingSubprocesses(unittest.TestCase):

    def setUp(self):

        self.dragon_logger = None

    def create_logger(self):

        # Create the logger children will attach to
        self.dragon_logger = dlog.setup_dragon_logging(0)

    def destroy_logger(self):

        self.dragon_logger.destroy()
        self.dragon_logger = None

    def get_messages(self, nmsg, level):

        total = 0
        iter_max = 500
        niter = 0  # set a break point
        msgs = []
        while total != nmsg or niter > iter_max:
            try:
                msgs.append(dmsg.parse(self.dragon_logger.get(level)))
                total += 1
            except (DragonLoggingError, ChannelEmpty):
                pass
            time.sleep(0.01)
            niter += 1

        # Make sure we didn't time out
        self.assertLessEqual(niter, iter_max)
        return msgs

    def run_subprocesses(self, nproc, level):

        # Create logger
        self.create_logger()

        # Run logging subprocesses
        info_msgs, debug_msgs, fnames = run_logging_subprocesses(nproc, level, self.dragon_logger.serialize())

        if level is logging.DEBUG:
            self.nmsgs = 2 * nproc
        else:
            self.nmsgs = nproc

        log_msgs = self.get_messages(self.nmsgs, level)

        # Destroy my logger
        self.destroy_logger()

        return info_msgs, debug_msgs, log_msgs

    def check_msgs(self, info_msgs, debug_msgs, log_msgs, level):

        # Get the actual log message out of the object
        log_msgs = [msg.msg for msg in log_msgs]
        self.assertEqual(len(log_msgs), self.nmsgs)

        if level is logging.DEBUG:
            input_msgs = debug_msgs + info_msgs
        else:
            input_msgs = info_msgs

        for msg in input_msgs:
            self.assertTrue(msg in log_msgs)
            self.assertEqual(log_msgs.count(msg), 1)
            log_msgs.remove(msg)

        self.assertEqual(len(log_msgs), 0)

    @patch.dict(os.environ, {"DRAGON_LOG_DEVICE_STDERR": "INFO"})
    def test_one_to_one_logging_info(self):
        nproc = 1
        level = logging.INFO
        info_msgs, debug_msgs, log_msgs = self.run_subprocesses(nproc, level)
        self.check_msgs(info_msgs, debug_msgs, log_msgs, level)

    @patch.dict(os.environ, {"DRAGON_LOG_DEVICE_STDERR": "DEBUG"})
    def test_one_to_one_logging_debug(self):
        nproc = 1
        level = logging.DEBUG
        info_msgs, debug_msgs, log_msgs = self.run_subprocesses(nproc, level)
        self.check_msgs(info_msgs, debug_msgs, log_msgs, level)

    @patch.dict(os.environ, {"DRAGON_LOG_DEVICE_STDERR": "INFO"})
    def test_many_to_one_logging_info(self):
        nproc = 2
        level = logging.INFO
        info_msgs, debug_msgs, log_msgs = self.run_subprocesses(nproc, level)
        self.check_msgs(info_msgs, debug_msgs, log_msgs, level)

    @patch.dict(os.environ, {"DRAGON_LOG_DEVICE_STDERR": "DEBUG"})
    def test_many_to_one_logging_debug(self):
        nproc = 2
        level = logging.DEBUG
        info_msgs, debug_msgs, log_msgs = self.run_subprocesses(nproc, level)
        self.check_msgs(info_msgs, debug_msgs, log_msgs, level)


class TestLoggingInfrastructure(unittest.TestCase):

    def setUp(self):
        # Create a dragon logger
        self.dragon_logger = dlog.setup_dragon_logging(0)
        self.destroyed_dragon = False
        self.log_levels = [
            logging.CRITICAL,
            logging.ERROR,
            logging.WARNING,
            logging.INFO,
            logging.DEBUG,
            logging.NOTSET,
        ]
        self.dummy_msgs = [dmsg.GSIsUp, dmsg.GSTeardown, dmsg.SHTeardown]
        self._tag = 0

    def tearDown(self):
        # Destroy dragon logger
        if not self.destroyed_dragon:
            self.dragon_logger.destroy()
            self.destroyed_dragon = True

        # Make sure the root logger has closed any handlers since the atexit()
        # function isn't going to get called by logging
        for handler in logging.root.handlers:
            logging.root.removeHandler(handler)
            handler.close()
        logging.shutdown()

    def get_tag(self):
        tag = self._tag
        self._tag += 1
        return tag

    def write_logs(self, log, n_entries):

        messages = [(random.choice(self.log_levels), generate_msg(random.randrange(20, 100))) for _ in range(n_entries)]
        for msg in messages:
            log.log(*msg)

        return messages

    def flush_logs(self, level, test_queue):

        for _ in range(self.dragon_logger.num_logs()):
            msg = self.dragon_logger.get(level)
            if msg is not None:
                test_queue.send(dmsg.parse(msg).serialize())

    def send_dummy_msgs(self, n_msgs, test_queue):

        for _ in range(n_msgs):
            msg_class = random.choice(self.dummy_msgs)
            test_queue.send(msg_class(tag=self.get_tag()).serialize())

    def check_log_entries(self, input_msgs, level, log_queue):

        # Grab messages from the logging queue. This will return a list of records
        logged_msgs = []
        while not log_queue.empty():
            logged_msgs.append(log_queue.get())
        logged_msgs = [(msg.level, msg.msg) for msg in logged_msgs]

        # First remove input_msgs that are < level from our input messages since
        # those wouldn't have been logged
        actually_logged = [(msg_level, msg) for msg_level, msg in input_msgs if msg_level >= level]

        self.assertEqual(len(logged_msgs), len(actually_logged))
        for msg in actually_logged:
            self.assertTrue(msg in logged_msgs)
            self.assertEqual(logged_msgs.count(msg), 1)
            logged_msgs.remove(msg)
        self.assertEqual(len(logged_msgs), 0)

    def clear_test_queue(self, test_queue, log_queue):
        # Get log entries by clearing the queue. Decorator should
        # pull off log messages
        unwrapped_get = dlutil.get_with_timeout.__wrapped__

        def get_with_timeout(test_queue, timeout=0.01):
            return unwrapped_get(test_queue, timeout=timeout)

        wrapped_get = dlutil.logging_queue_monitor(get_with_timeout, log_test_queue=log_queue)
        while not test_queue.empty():
            try:
                wrapped_get(test_queue, timeout=0.01)
            except TimeoutError:
                pass

    def test_logging_FE_queue_simple(self):
        # Set up basic logging for a given service
        level, _ = dlog.setup_BE_logging(
            service=dlog.DragonLoggingServices.TEST, logger_sdesc=B64(self.dragon_logger.serialize())
        )
        log = logging.getLogger(dlog.DragonLoggingServices.TEST)
        test_queue = dlutil.SRQueue()
        log_queue = queue.SimpleQueue()

        # write a slew of messages to the queue
        group_1 = self.write_logs(log, 5)
        self.flush_logs(level, test_queue)
        self.send_dummy_msgs(5, test_queue)
        group_2 = self.write_logs(log, 4)
        self.flush_logs(level, test_queue)
        self.send_dummy_msgs(3, test_queue)

        # Check the logging queue got the messages correctly
        self.clear_test_queue(test_queue, log_queue)
        self.check_log_entries(group_1 + group_2, level, log_queue)

    def test_log_to_wrong_parent_log(self):
        level, _ = dlog.setup_BE_logging(
            service=dlog.DragonLoggingServices.TEST, logger_sdesc=B64(self.dragon_logger.serialize())
        )

        fmter = logging.Formatter(dlog.default_services_fmt)
        not_a_service = dlog.DragonLoggingServices.TEST
        log = logging.getLogger(not_a_service)
        test_queue = dlutil.SRQueue()
        log_queue = queue.SimpleQueue()

        self.write_logs(log, 5)
        self.flush_logs(level, test_queue)
        self.clear_test_queue(test_queue, log_queue)

        logged_msgs = []
        while not log_queue.empty():
            logged_msgs.append(log_queue.get())
        logged_msgs = [fmter.format(logging.makeLogRecord(msg.get_sdict())) for msg in logged_msgs]

        # Make sure formatted entries have service and name info:
        for msg in logged_msgs:
            self.assertIn(dlog.DragonLoggingServices.TEST, msg)
            self.assertIn(not_a_service, msg)

    @patch.dict(os.environ, {"DRAGON_LOG_DEVICE_STDERR": "DEBUG"})
    def test_debug_level_logging(self):
        level, _ = dlog.setup_FE_logging(basedir=os.getcwd())
        log = logging.getLogger(dlog.DragonLoggingServices.LA_FE)
        log.info("FE logger up")

        nproc = 1
        info_msgs, debug_msgs, fnames = run_logging_subprocesses(nproc, level, self.dragon_logger.serialize())

        written_msgs = info_msgs + debug_msgs
        log.info("fe done")

        # Check file exists
        for fname in fnames:
            self.assertTrue(os.path.exists(fname))

        # Open file, parse logs and make sure all the messages are in there
        for file in fnames:
            with open(file, "r") as f:
                log_msgs = f.readlines()
            os.remove(file)

        self.assertEqual(len(written_msgs), len(log_msgs))

        for msg in written_msgs:
            match = [m for m in log_msgs if msg in m]
            self.assertEqual(len(match), 1)
            log_msgs.remove(match[-1])

        self.assertEqual(len(log_msgs), 0)

        # Make sure all handlers associated with FE logging
        # are closed beofre starting next tests
        dlog.close_FE_logging()

    @patch.dict(os.environ, {"DRAGON_LOG_DEVICE_STDERR": "DEBUG", "DRAGON_LA_LOG_DIR": "/not/a/real/dir"})
    def test_undefined_dragon_la_log_dir(self):
        level, fn = dlog.setup_BE_logging(
            service=dlog.DragonLoggingServices.TEST, logger_sdesc=B64(self.dragon_logger.serialize())
        )
        log = logging.getLogger(dlog.DragonLoggingServices.TEST)
        info_msg = [generate_msg(random.randrange(20, 100))]
        debug_msg = [generate_msg(random.randrange(20, 100))]
        log.info(info_msg[-1])
        log.debug(debug_msg[-1])
        written_msgs = info_msg + debug_msg

        # Open file, parse logs and make sure all the messages are in there
        with open(fn, "r") as f:
            log_msgs = f.readlines()
        os.remove(fn)

        self.assertEqual(len(written_msgs), len(log_msgs))
        for msg in written_msgs:
            match = [m for m in log_msgs if msg in m]
            self.assertEqual(len(match), 1)
            log_msgs.remove(match[-1])
        self.assertEqual(len(log_msgs), 0)

    def test_logging_to_terminal(self):
        with redirect_stderr(StringIO()) as f:
            level, fname = dlog.setup_FE_logging(basedir=os.getcwd(), add_console=True)
            log = logging.getLogger(dlog.DragonLoggingServices.LA_FE)
            msgs = ["FE logger up"]
            log.info(msgs[-1])
            msgs.append("test warning")
            log.warning(msgs[-1])

        # get data off of stderr and check for accuracy
        stderr_msgs = f.getvalue().rstrip().split("\n")
        for msg in msgs:
            match = [m for m in stderr_msgs if msg in m]
            self.assertEqual(len(match), 1)
            stderr_msgs.remove(match[-1])
        self.assertEqual(len(stderr_msgs), 0)

        os.remove(fname)
        dlog.close_FE_logging()

    def test_write_to_destroyed_dragon_logger(self):
        # Set up logging for BE services
        level, fn = dlog.setup_BE_logging(
            service=dlog.DragonLoggingServices.TEST, logger_sdesc=B64(self.dragon_logger.serialize())
        )
        log = logging.getLogger(dlog.DragonLoggingServices.TEST)

        # Destroy dragon logger
        self.dragon_logger.destroy()
        self.destroyed_dragon = True
        self.assertRaises(DragonLoggingError, log.info, generate_msg(random.randrange(20, 100)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
