import asyncio
import traceback
import sys
import queue
import logging
import shutil
import subprocess
import os
from functools import wraps, partial
from typing import Callable, Optional

from ..infrastructure.messages import AbnormalTerminationError
from ..infrastructure import messages as dmsg
from ..infrastructure import facts as dfacts
from ..infrastructure import process_desc as pdesc
from ..utils import b64decode, ExceptionalThread

from .wlm import WLM, wlm_cls_dict
from .wlm.base import BaseWLM

logger = logging.getLogger(__name__)

# general amount of patience we have for an expected message
# in startup or teardown before we assume something has gone wrong
TIMEOUT_PATIENCE = 10  # seconds
LA_TAG = 0
LOGBASE = "launcher"

THREAD_JOIN_TIMEOUT = 10.0
"""Seconds to wait for a service thread to exit on its own before escalating."""

THREAD_KILL_TIMEOUT = 5.0
"""Seconds to wait after asking a service thread to raise out of its work."""


class ThreadInterrupt(BaseException):
    """Raised inside a service thread to unwind it during an abnormal exit.

    Derives from BaseException, like KeyboardInterrupt, so the broad
    ``except Exception`` handlers in the service loops do not swallow it and
    keep looping.
    """


class ServiceThread(ExceptionalThread):
    """Launcher service thread that can be interrupted by `join_thread`."""

    def run(self):
        try:
            super().run()
        except ThreadInterrupt:
            # Expected during an abnormal exit; no traceback to report.
            logger.debug("%s interrupted during abnormal exit", self.name)


def join_thread(thread, log=None, timeout: float = THREAD_JOIN_TIMEOUT, abnormal: bool = False) -> bool:
    """Join a launcher service thread, escalating to an exception if it won't exit.

    On an abnormal exit a thread may be parked on a queue or channel that is
    never going to produce anything. `ExceptionalThread.kill_by_exception`
    raises inside it so it can unwind, but that only takes effect once the
    thread runs Python bytecode again; a thread stuck in native code cannot be
    forced and is left behind.

    :return: True if the thread is no longer running.
    """
    if thread is None:
        return True
    if log is None:
        log = logger

    name = thread.name

    thread.join(timeout=timeout)
    if not thread.is_alive():
        log.info("%s joined", name)
        return True

    if not abnormal:
        log.warning("%s did not exit within %ss", name, timeout)
        return False

    kill_by_exception = getattr(thread, "kill_by_exception", None)
    if kill_by_exception is None:
        log.warning("%s did not exit and cannot be interrupted", name)
        return False

    log.warning("%s did not exit within %ss, raising an exception in it", name, timeout)
    try:
        kill_by_exception(ThreadInterrupt)
    except Exception:
        log.exception("failed to raise an exception in %s", name)
        return False

    thread.join(timeout=THREAD_KILL_TIMEOUT)
    if thread.is_alive():
        log.warning("%s is wedged in native code and is being abandoned", name)
        return False

    log.info("%s exited after being interrupted", name)
    return True


class SRQueue(queue.SimpleQueue):
    def send(self, msg):
        super().put(msg)

    def recv(self, timeout=None):
        try:
            return super().get(timeout=timeout)
        except queue.Empty:
            return None


def exec_dragon_cleanup():

    # Modify PATH so our script finds the bash script for dragon-cleanup
    bin_path = os.path.join(dfacts.DRAGON_BASE_DIR, "bin")
    _env = dict(os.environ)
    _env["PATH"] = bin_path + ":" + _env["PATH"]

    # Get the args that were passed in
    if len(sys.argv) > 1:
        args = sys.argv[1:]
    else:
        args = []

    # Execute our cleanup
    cleanup = [os.path.join(bin_path, dfacts.PROCNAME_GLOBAL_CLEANUP)] + args
    proc = subprocess.run(cleanup, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=_env)
    print(proc.stdout.decode("utf-8"), flush=True)


def next_tag():
    global LA_TAG
    tmp = LA_TAG
    LA_TAG += 1
    return tmp


def detect_wlm(wlm: Optional[str] = None) -> Optional[type[BaseWLM]]:
    """
    Generate a priority list of available workload managers (WLMs) on the system.
    From that list, return a single WLM class type object with the highest priority and
    that has an allocation (if required). If no WLMs are detected, return None to
    indicate single-node mode.
    """

    # if the user specified a workload manager, we will use that
    retval = wlm_cls_dict[WLM.from_str(wlm)] if wlm else None

    if not retval:
        detected_wlm_priority_map = {}

        # 1. Detect which WLMs are supported on this system. check_for_wlm_support()
        #    returns an integer priority value. 0 means not supported, 1 means supported,
        #    and higher values indicate higher priority.
        for wlm_cls in wlm_cls_dict.values():
            detected_wlm_priority_map[wlm_cls] = wlm_cls.check_for_wlm_support()

        # 2. Find the maximum value in the dictionary
        max_priority = max(detected_wlm_priority_map.values(), default=0)
        if max_priority == 0:
            logger.info("Max priority of detected workload managers is 0. Assuming single-node mode.")
            return None  # No supported WLMs detected, so we assume single-node mode

        # 3. Get all keys that match this maximum value
        priority_wlms = [k for k, v in detected_wlm_priority_map.items() if v == max_priority]

        # If we found no WLMs, then we are in single-node mode.
        if len(priority_wlms) == 0:
            logger.info("No supported workload managers detected. Assuming single-node mode.")
            return None

        # Filter out any WLMs that require an allocation but don't have one.
        priority_wlms_with_allocation: list[type[BaseWLM]] = [
            wlm_cls for wlm_cls in priority_wlms if not wlm_cls.requires_allocation() or wlm_cls.has_allocation()
        ]

        # We found one or more WLMS, but none of them have an allocation.
        if len(priority_wlms_with_allocation) == 0:
            msg = (
                "Dragon detected one or more workload managers, however, no active job allocations were found. "
                "Please submit a job allocation to the workload manager and try again. "
                "If you are not using a workload manager, please specify the single-node launcher with the '--single-node-override' flag. "
                f"Detected workload managers: {', '.join(wlm_cls.name for wlm_cls in priority_wlms)}."
            )
            raise RuntimeError(msg)

        # If there are multiple WLMs with the same priority and allocation, we raise an error.
        elif len(priority_wlms_with_allocation) > 1:
            msg = (
                "Dragon cannot determine the correct Workload Manager (WLM) to use. "
                "Multiple supported WLMs were detected with the same priority: "
                f"{', '.join(wlm_cls.name for wlm_cls in priority_wlms_with_allocation)}. "
                "Please specify the workload manager to use with --wlm"
            )
            raise RuntimeError(msg)

        # We are left with a single WLM, so lets do Multi-Node
        retval = priority_wlms_with_allocation[0]

    return retval


def queue_monitor(func: Callable, *, log_test_queue=None):
    """Decorator to pull logging or abnormal messages out launch communications queues

    Take messages out of callback queue and puts into a separate logging queue to be dealt
    with later, thereby allowing us to immediately handle infrastructure

    Args:
        func (Callable): Callable that emits messages

        log_test_queue (queue, optional): Queue to drop log messages into. Used for internal
            unit testing. Defaults to None.

    Returns:
        function: The decorator wrapper function
    """

    if func is None:
        return partial(queue_monitor, log_test_queue=log_test_queue)

    @wraps(func)
    def wrapper(*args, **kwargs):
        while True:
            msg = func(*args, **kwargs)
            if isinstance(msg, dmsg.CpLoggingMessage):
                if log_test_queue:
                    log_test_queue.put(msg)
                else:
                    logger_name = f"{msg.hostname}.{msg.name}" if msg.hostname else msg.name
                    log = logging.getLogger(logger_name)
                    log.log(msg.level, msg.msg, extra=msg.get_logging_dict())
            elif isinstance(msg, dmsg.AbnormalTermination):
                raise AbnormalTerminationError("Abnormal exit detected")
            else:
                return msg

    return wrapper


def no_error_queue_monitor(func: Callable, *, log_test_queue=None):
    """Decorator to pull logging or abnormal messages out launch communications queues

    Take messages out of callback queue and puts into a separate logging queue to be dealt
    with later, thereby allowing us to immediately handle infrastructure

    Args:
        func (Callable): Callable that emits messages

        log_test_queue (queue, optional): Queue to drop log messages into. Used for internal
            unit testing. Defaults to None.

    Returns:
        function: The decorator wrapper function
    """

    if func is None:
        return partial(queue_monitor, log_test_queue=log_test_queue)

    @wraps(func)
    def wrapper(*args, **kwargs):
        while True:
            msg = func(*args, **kwargs)
            if isinstance(msg, dmsg.CpLoggingMessage):
                if log_test_queue:
                    log_test_queue.put(msg)
                else:
                    logger_name = f"{msg.hostname}.{msg.name}" if msg.hostname else msg.name
                    log = logging.getLogger(logger_name)
                    log.log(msg.level, msg.msg, extra=msg.get_logging_dict())
            else:
                return msg

    return wrapper


@queue_monitor
def get_with_timeout(handle, timeout=TIMEOUT_PATIENCE):
    """Function for getting messages from a queue given a timeout"""

    # Handle queues we pass in either have a poll or
    # a timeout recv. Use the appropriate one
    if hasattr(handle, "poll"):
        if handle.poll(timeout=timeout):
            return dmsg.parse(handle.recv())
        else:
            raise TimeoutError("get_with_timeout poll operation timed out")
    else:
        msg = handle.recv(timeout=timeout)
        if isinstance(msg, tuple):
            return msg
        elif msg is not None:
            return dmsg.parse(msg)
        else:
            raise TimeoutError("get_with_timeout recv operation timed out")


@queue_monitor
def q_get_with_timeout(handle, timeout=TIMEOUT_PATIENCE):
    """Function for getting messages from a queue given a timeout"""

    # Handle queues we pass in either have a poll or
    if hasattr(handle, "get"):
        try:
            msg = handle.get(timeout=timeout)
            if isinstance(msg, bytes):
                return dmsg.parse(msg)
            else:
                return msg
        except queue.Empty:
            raise TimeoutError("get_with_timeout poll operation timed out")
    # a timeout recv. Use the appropriate one
    elif hasattr(handle, "poll"):
        if handle.poll(timeout=timeout):
            msg = handle.get()
            if isinstance(msg, bytes):
                return dmsg.parse(msg)
            else:
                return msg
        else:
            raise TimeoutError("get_with_timeout poll operation timed out")
    else:
        msg = handle.recv(timeout=timeout)
        if isinstance(msg, tuple):
            return msg
        elif msg is not None:
            return dmsg.parse(msg)
        else:
            raise TimeoutError("get_with_timeout recv operation timed out")


@queue_monitor
def get_with_blocking(handle):
    """Function for getting messages from the queue while blocking"""
    try:
        msg = handle.recv()

        if isinstance(msg, tuple):
            return msg
        else:
            return dmsg.parse(msg)
    except Exception:
        raise


@queue_monitor
def q_get_with_blocking(handle):
    """Function for getting messages from the queue while blocking"""
    msg = handle.get()
    if isinstance(msg, tuple):
        return msg
    elif isinstance(msg, bytes):
        return dmsg.parse(msg)
    else:
        return msg


@no_error_queue_monitor
def get_with_blocking_frontend_server(handle):
    """Function for getting messages from the queue while blocking"""
    try:
        msg = handle.get()
        if isinstance(msg, tuple):
            return msg
        elif isinstance(msg, bytes):
            return dmsg.parse(msg)
        else:
            return msg
    except Exception as ex:
        raise RuntimeError("Got an error while trying to handle a message in the frontend launcher.") from ex


class LaOverlayNetFEQueue(queue.SimpleQueue):
    """Class for sending messages between launcher and OverlayNet threads"""

    def send(self, target, msg):
        super().put(target)
        super().put(msg)

    def recv(self, timeout=None):
        try:
            target = super().get(timeout=timeout)
            msg = dmsg.parse(super().get())
            return target, msg
        except queue.Empty:
            return None


class OverlayNetLaFEQueue(queue.SimpleQueue):
    """Class for sending messages between launcher and OverlayNet threads"""

    def send(self, msg):
        super().put(msg)

    def recv(self, timeout=None):
        try:
            msg = super().get(timeout=timeout)
            return msg
        except queue.Empty:
            return None


class AsyncQueueMonitor:
    def __init__(self, description, queue, dispatch, log=None, queue_wh=None):
        self.queue = queue

        # The write handle is needed to cleanly close the Queue Monitor when
        # it is waiting on a recv. We need to give it something to receive
        # because the run_in_executor is holding a lock that needs to be released.
        if queue_wh is None:
            self.queue_wh = self.queue
        else:
            self.queue_wh = queue_wh

        self.dispatch = dispatch
        self.description = description
        self.log = log
        self.running = True
        self.task = asyncio.create_task(self._receive_dispatch())

    def __str__(self):
        rv = f"""{self.description} AsyncQueueMonitor:
    task = {self.task}
    queue = {self.queue}"""
        return rv

    def get_task(self):
        # This creates a new task if one needs creating. Otherwise
        # it will continue to return the
        if self.task.done():
            self.task = asyncio.create_task(self._receive_dispatch())

        return self.task

    def close(self):
        self.running = False
        try:
            self.queue_wh.send("EXIT\n")
        except:
            pass

    async def _receive_dispatch(self):
        try:
            loop = asyncio.get_running_loop()
            while self.running:
                # Get the serialized message.
                json_string = await loop.run_in_executor(None, self.queue.recv)

                if self.running:
                    if json_string == "":
                        # Seems to happen at times during shutdown.
                        self.log.info("Got an empty string for a message in AsyncQueueMonitor for " + self.description)
                        raise EOFError("Got an empty string for a message in AsyncQueueMonitor for " + self.description)

                    # Parse it into a dictionary
                    msg = dmsg.parse(json_string)

                    # reset json_string in case a bad message comes in
                    # next time around. We don't want to get the previous
                    # json_string if an exception occurs.
                    del json_string

                    # Call dispatch on the server. The dispatch
                    # method will route this message to the
                    # appropriate message handler. By calling
                    # dispatch directly (and not through the object on which
                    # dispatch is defined) multiple dispatch functions
                    # are possible per server and potentially this queue
                    # could have its own custom dispatch function.
                    self.dispatch(msg)

            self.log.info("AsyncQueueMonitor exiting for queue " + self.description)
            self.queue.close()

        except EOFError:
            # The underlying queue has closed.
            self.log.info("AsyncQueueMonitor exiting due to EOF of queue for " + self.description)

        except Exception as ex:

            if not self.running:
                # If this is done, then the exception was likely a closed
                # queue. In that case, just return
                return

            if self.log is not None:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                try:
                    try:
                        text = (
                            "Got an unexpected message in AsyncQueueMonitor _receive_dispatch for "
                            + self.description
                            + ". Message is '"
                            + repr(msg)
                            + "'"
                        )
                    except Exception as ex:
                        text = (
                            "Got an invalid message in AsyncQueueMonitor _receive_dispatch for "
                            + self.description
                            + ". Message is '"
                            + json_string
                            + "'"
                        )
                except:
                    text = "Things are really bad in AysncQueueMonitor _receive_dispatch for " + self.description

                ex_message = text + "\n" + "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                self.log.warning(ex_message)

            raise ex


def get_process_exe_args(args_map=None):
    """Parse sys.argv to get the user executable and arguments"""

    from .launchargs import head_proc_args as get_head_proc_args

    head_proc, head_proc_args = get_head_proc_args(args_map)

    if 1 == len(sys.argv):
        exe = sys.executable
        args = []
    else:
        target_fp = shutil.which(head_proc)
        if target_fp is not None:
            perm = os.stat(target_fp)
            # This checks the permission bits as an extra check.
            # In a Docker container which was not returning
            # executable correctly when headproc was not in the
            # current working directory. This gets the user
            # permission value (rwx) and the remainder of 2
            # division (odd or even). If even, then executable
            # bit is not set and remainder is 0. bool(0) is False.
            # bool(1) is True.
            executable = bool(int(oct(perm.st_mode)[-3]) % 2)
            if not executable:
                target_fp = None
        if target_fp is not None:
            exe = target_fp
            args = head_proc_args[1:]  # Exclude the executable
        else:
            exe = sys.executable
            args = head_proc_args

    return exe, args


def mk_head_proc_start_msg(logbase="launcher", make_inf_channels=True, args_map=None, restart=False, resilient=False):
    """Look at script arguments and return the head process launch message.

    If the first argument looks executable via 'which' then that will
    be what is launched as a managed process.  Otherwise sys.executable
    will be started with all the other arguments tacked on.

    :return: dmsg.GSProcessCreate message
    """
    log = logging.getLogger(logbase).getChild("mk_head_proc_start_msg")

    exe, args = get_process_exe_args(args_map=args_map)

    log.info(f"head process will be {exe} {args}")

    options = None
    if make_inf_channels:
        options = pdesc.ProcessOptions(make_inf_channels=True)

    return dmsg.GSProcessCreate(
        tag=next_tag(),
        p_uid=dfacts.LAUNCHER_PUID,
        r_c_uid=dfacts.BASE_BE_CUID,
        exe=exe,
        args=args,
        options=options,
        head_proc=True,
        restart=restart,
        resilient=resilient,
    )


def mk_shproc_echo_msg(logbase="launcher", stdin_str="", node_index=0):
    """Look at script arguments and return a LSProcessCreate message
    to be used when using the transport test environment mode.

    If the first argument looks executable via 'which' then that will
    be what is launched as a managed process.  Otherwise sys.executable
    will be started with all the other arguments tacked on.

    :return: dmsg.LSProcessCreate message
    """
    log = logging.getLogger(logbase).getChild("mk_shproc_echo_msg")

    exe, args = get_process_exe_args()
    args = ["-c", "import socket; print(f'hello from {socket.gethostname()}')"]

    log.info(f"The shprocesscreate message will include {exe} {args}")

    return dmsg.LSProcessCreate(
        tag=next_tag(),
        p_uid=dfacts.LAUNCHER_PUID,
        r_c_uid=dfacts.launcher_cuid_from_index(node_index),
        t_p_uid=dfacts.FIRST_PUID,
        exe=exe,
        args=args,
        initial_stdin=stdin_str,
    )


def mk_shproc_start_msg(logbase="launcher", stdin_str=""):
    """Look at script arguments and return a LSProcessCreate message
    to be used when using the transport test environment mode.

    If the first argument looks executable via 'which' then that will
    be what is launched as a managed process.  Otherwise sys.executable
    will be started with all the other arguments tacked on.

    :return: dmsg.GSProcessCreate message
    """
    log = logging.getLogger(logbase).getChild("mk_head_shproc_create_msg")

    exe, args = get_process_exe_args()
    log.info(f"The shprocesscreate message will include {exe} {args}")

    return dmsg.LSProcessCreate(
        tag=next_tag(),
        p_uid=dfacts.LAUNCHER_PUID,
        r_c_uid=dfacts.BASE_BE_CUID,
        t_p_uid=dfacts.FIRST_PUID,
        exe=exe,
        args=args,
        initial_stdin=stdin_str,
    )
