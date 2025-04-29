import asyncio
import traceback
import sys
import queue
import logging
import shutil
import subprocess
import os
from functools import wraps, partial
from typing import Callable

from ..infrastructure.messages import AbnormalTerminationError
from ..infrastructure import messages as dmsg
from ..infrastructure import facts as dfacts
from ..infrastructure import process_desc as pdesc

from .wlm import wlm_launch_dict, WLM, wlm_cls_dict


# general amount of patience we have for an expected message
# in startup or teardown before we assume something has gone wrong
TIMEOUT_PATIENCE = 10  # seconds
LA_TAG = 0
LOGBASE = "launcher"


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


def detect_wlm():
    """Detect a supported WLM"""
    from .network_config import NetworkConfig

    wlm = None
    try:
        for wlm, cls in wlm_cls_dict.items():
            if cls.check_for_wlm_support():
                break

    except Exception:
        raise RuntimeError("Error searching for supported WLM")

    if wlm is None:
        raise RuntimeError("No supported WLM found")
    elif wlm is WLM.SSH:
        msg = """
SSH was only supported launcher found. To use it, specify `--wlm ssh` as input to the dragon launcher.
It requires passwordless SSH to all backend compute nodes and a list of hosts. Please see documentation
and `dragon --help` for more information.
"""
        raise RuntimeError(msg)

    return wlm


def get_wlm_launch_args(
    args_map: dict = None,
    launch_args: dict = None,
    nodes: tuple[int, list[str]] = None,
    hostname: str = None,
    wlm: WLM = None,
):
    """Get arguments for WLM to launch the backend"""

    try:
        if wlm is None:
            wlm = detect_wlm()

        if hostname is not None:
            args_map["hostname"] = hostname

        if nodes is not None:
            args_map["nnodes"] = nodes[0]
            args_map["nodelist"] = ",".join(nodes[1])

        wlm_args = wlm_launch_dict[wlm](args_map=args_map, launch_args=launch_args)

    except Exception:
        raise RuntimeError("Unable to generate WLM backend launch args")

    return wlm_args


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
            if isinstance(msg, dmsg.LoggingMsgList):
                if log_test_queue:
                    log_test_queue.put(msg)
                else:
                    for record in msg.records:
                        log = logging.getLogger(record.name)
                        log.log(record.level, record.msg, extra=record.get_logging_dict())
            elif isinstance(msg, dmsg.LoggingMsg):
                if log_test_queue:
                    log_test_queue.put(msg)
                else:
                    log = logging.getLogger(msg.name)
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
            if isinstance(msg, dmsg.LoggingMsgList):
                if log_test_queue:
                    log_test_queue.put(msg)
                else:
                    for record in msg.records:
                        log = logging.getLogger(record.name)
                        log.log(record.level, record.msg, extra=record.get_logging_dict())
            elif isinstance(msg, dmsg.LoggingMsg):
                if log_test_queue:
                    log_test_queue.put(msg)
                else:
                    log = logging.getLogger(msg.name)
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


@no_error_queue_monitor
def get_with_blocking_frontend_server(handle):
    """Function for getting messages from the queue while blocking"""
    try:
        msg = handle.recv()
        if isinstance(msg, tuple):
            return msg
        else:
            return dmsg.parse(msg)
    except Exception:
        raise


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
    """Look at script arguments and return a SHProcessCreate message
    to be used when using the transport test environment mode.

    If the first argument looks executable via 'which' then that will
    be what is launched as a managed process.  Otherwise sys.executable
    will be started with all the other arguments tacked on.

    :return: dmsg.SHProcessCreate message
    """
    log = logging.getLogger(logbase).getChild("mk_shproc_echo_msg")

    exe, args = get_process_exe_args()
    args = ["-c", "import socket; print(f'hello from {socket.gethostname()}')"]

    log.info(f"The shprocesscreate message will include {exe} {args}")

    return dmsg.SHProcessCreate(
        tag=next_tag(),
        p_uid=dfacts.LAUNCHER_PUID,
        r_c_uid=dfacts.launcher_cuid_from_index(node_index),
        t_p_uid=dfacts.FIRST_PUID,
        exe=exe,
        args=args,
        initial_stdin=stdin_str,
    )


def mk_shproc_start_msg(logbase="launcher", stdin_str=""):
    """Look at script arguments and return a SHProcessCreate message
    to be used when using the transport test environment mode.

    If the first argument looks executable via 'which' then that will
    be what is launched as a managed process.  Otherwise sys.executable
    will be started with all the other arguments tacked on.

    :return: dmsg.GSProcessCreate message
    """
    log = logging.getLogger(logbase).getChild("mk_head_shproc_create_msg")

    exe, args = get_process_exe_args()
    log.info(f"The shprocesscreate message will include {exe} {args}")

    return dmsg.SHProcessCreate(
        tag=next_tag(),
        p_uid=dfacts.LAUNCHER_PUID,
        r_c_uid=dfacts.BASE_BE_CUID,
        t_p_uid=dfacts.FIRST_PUID,
        exe=exe,
        args=args,
        initial_stdin=stdin_str,
    )
