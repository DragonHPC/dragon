import logging
import threading
import subprocess
from ..launcher import util as dlutil
from dragon.channels import Channel
from .messages import InfraMsg


class CriticalPopen(subprocess.Popen):
    """This class is used to monitor the death of a critical process. It inherits from subprocess.Popen and adds a thread that watches the process. The thread will send a notification if the process exits with a non-zero exit code. The notification is sent using a provided channel and infrastructure message. At the moment it does not work for arbitrary send and receive methods (namely stdin/stdout) or arbitrary message types."""

    def __init__(self, *args, notify_channel: Channel = None, notify_msg: InfraMsg = None, name: str = "", **kwargs):

        super().__init__(*args, **kwargs)
        if notify_channel is None:
            raise RuntimeError("notify_channel must be provided")
        self._notify_channel = notify_channel
        if notify_msg is None:
            raise RuntimeError("notify_msg must be provided")
        self._notify_msg = notify_msg
        self._critical_process_name = name
        self.th = threading.Thread(
            name=f"watch death {self._critical_process_name}", target=self._watch_death, daemon=True
        )
        self.log = logging.getLogger(f"{self._critical_process_name} watch_death")

        try:
            self.log.info("critical watcher service thread")
            self.th.start()
            self.log.info("critical watcher service thread started")
        except Exception as ex:
            self._send_notification(f"critical watcher starting thread exception: {ex}")

    def _send_notification(self, msg: str = None):
        self.log.debug(f"Sending {self._notify_msg} with {msg}")
        try:
            self._notify_channel.send(self._notify_msg(tag=dlutil.next_tag(), err_info=msg).serialize())
        except Exception as e:
            self.log.debug(f"Failed to send notification: {e}")

    def _watch_death(self):
        self.log.info("starting")

        ecode = self.wait()
        self.log.info(f"pid: {self.pid} ecode={ecode}")

        # We don't explicitly watch stderr but instead rely on the fact that if an error is raised the process will exit with a non-zero exit code
        if ecode != 0:
            # Signal abnormal termination and notify Launcher BE
            err_msg = f"Critical process exited - {self._critical_process_name} with pid {self.pid} exited with non-zero exit code {ecode}"
            self._send_notification(err_msg)

        self.log.info("exit")
