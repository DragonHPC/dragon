import os
import selectors
import socket
import subprocess

from . import util
from . import messages
from . import facts

from queue import Queue
from typing import List

import logging

logger = logging.getLogger(__name__)


def handle_stdout(hostname, stream, send_msg_q):
    # logger.debug('++handle_stdout')
    data = stream.read()
    if data:
        logger.debug("handle_stdout data=%s", data)
        while len(data) > facts.FWD_OUTPUT_CHUNK_SIZE:
            chunk = data[:facts.FWD_OUTPUT_CHUNK_SIZE]
            send_msg_q.put(
                messages.FwdOutput(
                    util.next_tag(),
                    fd_num=messages.FwdOutput.FDNum.STDOUT.value,
                    hostname=hostname,
                    data=chunk.decode(),
                )
            )
            data = data[facts.FWD_OUTPUT_CHUNK_SIZE:]
        if len(data) > 0:
            send_msg_q.put(
                messages.FwdOutput(
                    util.next_tag(),
                    fd_num=messages.FwdOutput.FDNum.STDOUT.value,
                    hostname=hostname,
                    data=data.decode(),
                )
            )
    # logger.debug('--handle_stdout')


def handle_stderr(hostname, stream, send_msg_q):
    # logger.debug('++handle_stderr')
    data = stream.read()
    if data:
        logger.debug("handle_stderr data=%s", data)
        while len(data) > facts.FWD_OUTPUT_CHUNK_SIZE:
            chunk = data[:facts.FWD_OUTPUT_CHUNK_SIZE]
            send_msg_q.put(
                messages.FwdOutput(
                    util.next_tag(),
                    fd_num=messages.FwdOutput.FDNum.STDERR.value,
                    hostname=hostname,
                    data=chunk.decode(),
                )
            )
            data = data[facts.FWD_OUTPUT_CHUNK_SIZE:]
        if len(data) > 0:
            send_msg_q.put(
                messages.FwdOutput(
                    util.next_tag(),
                    fd_num=messages.FwdOutput.FDNum.STDERR.value,
                    hostname=hostname,
                    data=data.decode(),
                )
            )
    # logger.debug('--handle_stderr')


def local_executor(user_command: List[str], env: dict, cwd: str, ref: int, my_rank: int, num_ranks: int, send_msg_q: Queue):
    logger.debug("++local_executor(%d, %s, %d)", ref, user_command, my_rank)

    hostname = socket.gethostname()

    _env = {}
    if env:
        _env = env.copy()
    _env["DRAGON_RUN_RANK"] = str(my_rank)
    _env["DRAGON_RUN_NUM_RANKS"] = str(num_ranks)

    try:
        user_process = subprocess.Popen(
            user_command,
            env=_env,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        os.set_blocking(user_process.stdout.fileno(), False) # type: ignore
        os.set_blocking(user_process.stderr.fileno(), False) # type: ignore

        selector = selectors.DefaultSelector()
        selector.register(user_process.stdout, selectors.EVENT_READ, handle_stdout) # type: ignore
        selector.register(user_process.stderr, selectors.EVENT_READ, handle_stderr) # type: ignore

        # Event loop
        while True:
            events = selector.select()
            for key, _ in events:
                callback = key.data
                callback(hostname, key.fileobj, send_msg_q)

            if user_process.poll() is not None:  # Check if the process has finished
                break

        send_msg_q.put(messages.UserAppExit(util.next_tag(), ref=ref, hostname=hostname, exit_code=user_process.returncode))
    except FileNotFoundError as exc:
        send_msg_q.put(messages.UserAppExit(util.next_tag(), ref=ref, hostname=hostname, exit_code=exc.errno))

    logger.debug("--local_executor")
