import os
import logging
import paramiko

from abc import ABC, abstractmethod
from shlex import quote

from . import messages
from .exceptions import RemoteProcessError
from typing import Union, Optional


logger = logging.getLogger(__name__)


class RemoteHost(ABC):
    def __init__(self, hostname: str, rank: int, ssh_config_path: Optional[str] = None, private_key: Optional[str] = None, passphrase: Optional[str] = None, log_level: int = logging.NOTSET):
        self.hostname: str = hostname
        self.rank: int = rank
        self.log_level: int = log_level
        self.ssh_config_path: Optional[str] = ssh_config_path
        self.private_key: Optional[str] = private_key
        self.passphrase: Optional[str] = passphrase

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    @abstractmethod
    def send_message(self, msg) -> bool:
        return NotImplemented

    @abstractmethod
    def recv_message(self, restrict=None) -> messages.InfraMsg:
        return NotImplemented

    @abstractmethod
    def connect(self) -> bool:
        return NotImplemented

    @abstractmethod
    def execute_command(self, command) -> bool:
        return NotImplemented

    @abstractmethod
    def disconnect(self) -> bool:
        return NotImplemented


class SSHHost(RemoteHost):

    SSH_ENV_VARNAMES = (
        "PYTHONPATH",
        "PATH",
    )

    def __init__(self, hostname: str, rank: int, ssh_config_path: Optional[str] = None, private_key: Optional[str] = None, passphrase: Optional[str] = None, log_level: int = logging.NOTSET):
        super().__init__(hostname, rank, ssh_config_path, private_key, passphrase, log_level)
        self.stdin: Optional[paramiko.ChannelFile] = None
        self.stdout: Optional[paramiko.ChannelFile] = None
        self.stderr: Optional[paramiko.ChannelFile] = None

    def connect(self):
        # 1. Initialize the SSH Client
        self.ssh_client = paramiko.SSHClient()

        # 2. Automatically add unknown host keys (use AutoAddPolicy with caution in production)
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # 3. Initialize and parse the custom SSH config file
        host_config = {}
        config_path = os.path.expanduser(self.ssh_config_path) if self.ssh_config_path else os.path.expanduser("~/.ssh/config")
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config = paramiko.SSHConfig()
                config.parse(f)
                host_config = config.lookup(self.hostname)

        # 4. Map SSH config keys to client.connect parameters
        connect_kwargs = {
            "hostname": host_config.get("hostname", self.hostname),
            "port": int(host_config.get("port", 22)),
        }

        if "user" in host_config:
            connect_kwargs["username"] = host_config.get("user")

        if self.private_key:
            connect_kwargs["key_filename"] = self.private_key
        elif "identityfile" in host_config:
            key_filename = os.path.expanduser(host_config["identityfile"][0])
            connect_kwargs["key_filename"] = key_filename

        if self.passphrase:
            connect_kwargs["passphrase"] = self.passphrase

        # 7. Connect using the extracted configurations
        self.ssh_client.connect(**connect_kwargs)
        return True

    def execute_command(self, command):
        logger.debug("++run ssh_host=%s rank=%d cmd=%s", self.hostname, self.rank, command)
        try:
            environment = {key: os.environ.get(key, "") for key in self.SSH_ENV_VARNAMES}

            # Set the rank of the remote process
            environment["DRAGON_RUN_RANK"] = str(self.rank)

            if self.log_level:
                # Set the log level for the remote process
                environment["DRAGON_RUN_LOG_LEVEL"] = str(self.log_level)

            # cd <dir> ; bash -c "key1=val1 key2=val2 command"
            remote_command_list = [f"cd {os.getcwd()};"]
            remote_command_list.append(
                "bash -c "
                + quote(f"{' '.join([f'{key}={quote(val)}' for key, val in environment.items()])} " + command)
            )

            remote_command_str = " ".join(remote_command_list)
            logger.debug("run remote_command_str=%s", remote_command_str)

            self.stdin, self.stdout, self.stderr = self.ssh_client.exec_command(remote_command_str)
        except Exception as ex:
            logger.error("run Unhandled exception: %s", ex)
            raise ex
        finally:
            logger.debug("--run")
        return True

    def _remote_failure_details(self):
        """Collect the remote process' exit status and everything it wrote to stderr.

        Only call this once the channel has closed; both reads block otherwise.
        """
        err_info = ""
        try:
            if self.stderr is not None:
                # Drain all of it: a traceback's first line is just boilerplate,
                # the useful part is at the end.
                err = self.stderr.read()  # type: ignore
                if isinstance(err, bytes):
                    err = err.decode(errors="replace")
                err_info = err.strip()
        except Exception as ex:
            logger.debug("ssh_host=%s could not read remote stderr: %s", self.hostname, ex)

        exit_status = None
        try:
            channel = self.stdin.channel  # type: ignore
            if channel.exit_status_ready():
                exit_status = channel.recv_exit_status()
        except Exception as ex:
            logger.debug("ssh_host=%s could not read remote exit status: %s", self.hostname, ex)

        return exit_status, err_info

    def send_message(self, msg):
        try:
            logger.debug("ssh_host=%s send_message=%s", self.hostname, msg)
            self.stdin.write(msg.uncompressed_serialize())  # type: ignore
            self.stdin.write(b"\n")  # type: ignore
            self.stdin.flush()  # type: ignore
        except Exception as ex:
            # A send fails with a bare "Socket is closed" once the remote process has
            # died. Report why it died rather than this downstream symptom.
            if self.stdin is not None and self.stdin.channel.closed:  # type: ignore
                exit_status, err_info = self._remote_failure_details()
                logger.error(
                    'ssh_host=%s send_message failed - remote process gone, exit_status=%s stderr="%s"',
                    self.hostname,
                    exit_status,
                    err_info,
                )
                raise RemoteProcessError(
                    exit_status if exit_status is not None else -1,
                    err_info
                    or f"remote process on {self.hostname} exited before {type(msg).__name__} could be sent",
                ) from ex
            logger.debug("Unhandled exception in send_message: %s", ex)
            raise
        # finally:
        #     logger.debug("--send_message ssh_host=%s", self.hostname)
        return True

    def recv_message(self, restrict=None):
        # logger.debug('ssh_host=%s ++recv_message(restrict=%s)', self.hostname, restrict)

        msg = None
        line = ""

        try:
            line = self.stdout.readline().strip()  # type: ignore
            if line:
                logger.debug('ssh_host=%s recv_message line="%s"', self.hostname, line)
                msg = messages.parse(line, restrict=restrict)
                # logger.debug("ssh_host=%s recv_message parsed=%s", self.hostname, msg)

            if self.stdout.channel.closed:  # type: ignore
                exit_status, err_info = self._remote_failure_details()
                logger.debug(
                    "ssh_host=%s recv_message - channel closed - exit_status=%s", self.hostname, exit_status
                )
                if exit_status not in (0, None):
                    # Raise even with empty stderr, otherwise the failure is lost here and
                    # only resurfaces later as "Socket is closed" on the next send.
                    if err_info:
                        logger.error('ssh_host=%s remote stderr:\n%s', self.hostname, err_info)
                    raise RemoteProcessError(
                        exit_status,
                        err_info or f"remote process on {self.hostname} exited with status {exit_status}",
                    )
        except RemoteProcessError:
            raise
        except Exception as ex:
            logger.debug("ssh_host=%s recv_message - unhandled exception: %s", self.hostname, ex)
            raise

        return msg

    def disconnect(self):
        logger.debug("++ssh_host=%s disconnect - closing ssh_client", self.hostname)
        self.ssh_client.close()
        logger.debug("--ssh_host=%s disconnect", self.hostname)
        return True
