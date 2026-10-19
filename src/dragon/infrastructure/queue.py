import os
import traceback
import socket
from datetime import datetime

import dragon.infrastructure.messages as dmsg
from dragon.native.queue import Queue as DragonQueue
from dragon.fli import DragonFLIError


class InfraQueue(DragonQueue):
    def __init__(self, *args, **kwargs):
        # override the pickler to be the one that can handle our messages
        try:
            kwargs["pickler"] = dmsg.MessagePickler()
            super().__init__(*args, **kwargs)
        except Exception as e:
            raise ConnectionError(f"Failed to initialize InfraQueue: {e}") from e
        self._pid = os.getpid()
        self._hostname = socket.gethostname()

    @classmethod
    def attach(cls, *args, **kwargs):
        kwargs["pickler"] = dmsg.MessagePickler()
        try:
            obj = super().attach(*args, **kwargs)
            obj._pid = None  # these are attaching procs so we set these to none to indicate that
            obj._hostname = (
                socket.gethostname()
            )  # we set the hostname though so we can see whether these are local or remote operations
            return obj
        except Exception as e:
            raise ConnectionError(f"Failed to attach InfraQueue: {e}") from e

    def put(self, obj, *args, **kwargs):
        # override put to be able to handle the fact that we might be putting messages into a queue that has other types of messages in it
        if not isinstance(obj, dmsg.InfraMsg | dmsg.CapNProtoMsg):
            raise ValueError("InfraQueue only accepts InfraMsg or CapNProtoMsg objects")
        try:
            super().put(obj, *args, **kwargs)
        except DragonFLIError as e:
            raise ConnectionError(f"Failed to put message into {self}: {e}") from e

    def get(self, *args, **kwargs):
        try:
            obj = super().get(*args, **kwargs)
            if not isinstance(obj, dmsg.InfraMsg | dmsg.CapNProtoMsg):
                raise ValueError("InfraQueue only returns InfraMsg or CapNProtoMsg objects")
            return obj
        except DragonFLIError as e:
            raise ConnectionError(f"Failed to get message from {self}: {e}") from e

    def __str__(self):
        return f"InfraQueue(creating pid={self._pid}, hostname={self._hostname}, main_ch_cuid={self._fli.main_channel_cuid})"

    @property
    def cuid(self):
        """Return the CUID of the queue's main channel.

        This is the channel unique identifier (CUID) for the main channel
        backing this InfraQueue.
        """
        return self._fli.main_channel_cuid

    def _log_connection_error(self, operation: str, err: Exception) -> None:
        """Write connection-related queue errors to a local log file."""
        timestamp = datetime.now().isoformat(timespec="seconds")
        tb = traceback.format_exc()
        if not tb or tb.strip() == "NoneType: None":
            tb = "<traceback unavailable>"
        try:
            with open(f"{os.getpid()}_queue_error.log", "a", encoding="utf-8") as log_file:
                log_file.write(f"{timestamp} InfraQueue {operation} failed: {err}\n")
                log_file.write("Traceback:\n")
                log_file.write(tb)
                if not tb.endswith("\n"):
                    log_file.write("\n")
        except OSError:
            # Never let logging failure mask the original queue error.
            pass
