import os
import logging
import paramiko
import socket
import time

from abc import ABC, abstractmethod
from shlex import quote

from . import messages
from .exceptions import RemoteProcessError
from typing import Union, Optional


logger = logging.getLogger(__name__)


class RemoteHost(ABC):
    def __init__(self, hostname, rank):
        self.hostname : str = hostname
        self.rank : int = rank

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

    SSH_ENV_VARNAMES = ("PYTHONPATH", "PATH",)

    def __init__(self, hostname, rank):
        super().__init__(hostname, rank)
        self.stdin : Optional[paramiko.ChannelFile] = None
        self.stdout : Optional[paramiko.ChannelFile] = None
        self.stderr : Optional[paramiko.ChannelFile] = None

    def connect(self):
        # TODO: Allow user to specify private keyfile
        self.pkey = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))
        self.ssh_client = paramiko.SSHClient()

        # TODO Move loading of configfile to a common location
        config_path = os.path.expanduser("~/.ssh/config")
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config = paramiko.SSHConfig()
                config.parse(f)
                host_config = config.lookup(self.hostname)
        else:
            host_config = {}

        connect_params = {
            "hostname": host_config.get("hostname", self.hostname),
            "port": int(host_config.get("port", 22)),
            "username": host_config.get("user"),
        }

        if "identityfile" in host_config:
            key_filename = os.path.expanduser(host_config["identityfile"][0])
            connect_params["key_filename"] = key_filename

        self.policy = paramiko.AutoAddPolicy()  # Security risk
        self.ssh_client.set_missing_host_key_policy(self.policy)
        self.ssh_client.connect(**connect_params)
        return True

    def execute_command(self, command):
        logger.debug("++run ssh_host=%s rank=%d cmd=%s", self.hostname, self.rank, command)
        try:
            environment = {key: os.environ.get(key, "") for key in self.SSH_ENV_VARNAMES}
            environment["DRAGON_RUN_RANK"] = str(self.rank)

            # cd <dir> ; key1=val1 key2=val2 command
            remote_command_list = [f"cd {os.getcwd()};"]
            remote_command_list.extend([f"{key}={quote(val)}" for key, val in environment.items()])
            remote_command_list.append(command)

            remote_command_str = " ".join(remote_command_list)
            logger.debug("run remote_command_str=%s", remote_command_str)

            self.stdin, self.stdout, self.stderr = self.ssh_client.exec_command(
                remote_command_str
            )
        except Exception as ex:
            logger.error("run Unhandled exception: %s", ex)
            raise ex
        finally:
            logger.debug("--run")
        return True

    def send_message(self, msg):
        try:
            logger.debug("ssh_host=%s send_message=%s", self.hostname, msg)
            self.stdin.write(msg.uncompressed_serialize()) # type: ignore
            self.stdin.write(b"\n") # type: ignore
            self.stdin.flush() # type: ignore
        except Exception as ex:
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
            line = self.stdout.readline().strip() # type: ignore
            if line:
                logger.debug('ssh_host=%s recv_message line="%s"', self.hostname, line)
                msg = messages.parse(line, restrict=restrict)
                # logger.debug("ssh_host=%s recv_message parsed=%s", self.hostname, msg)

            if self.stdout.channel.closed: # type: ignore
                err_info = self.stderr.readline().strip() # type: ignore
                exit_status = self.stdin.channel.recv_exit_status() # type: ignore
                logger.debug('ssh_host=%s recv_message - channel closed - exit_status=%d', self.hostname, exit_status)
                if exit_status != 0:
                    if err_info:
                        logger.debug('ssh_host=%s recv_message err_info="%s"', self.hostname, err_info)
                        raise RemoteProcessError(
                            self.stdin.channel.recv_exit_status(), # type: ignore
                            err_info,
                        )
        except RemoteProcessError:
            raise
        except Exception as ex:
            logger.debug("ssh_host=%s recv_message - unhandled exception: %s", self.hostname, ex)
            raise

        return msg

    def disconnect(self):
        self.ssh_client.close()
        return True
