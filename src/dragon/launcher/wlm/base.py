import os
from abc import ABC, abstractmethod
import json
import logging
import subprocess
import signal
from enum import Enum
from shlex import quote
from typing import Optional

from ...infrastructure.facts import PROCNAME_LA_BE
from ...infrastructure.parameters import this_process
from ...infrastructure.node_desc import NodeDescriptor
from ...infrastructure.util import NewlineStreamWrapper


class NetworkConfigState(Enum):
    """Enumerated states of Dragon FrontEnd"""

    NONE = 0
    IN_PROGRESS = 1
    CONFIG_DONE = 2


class BaseWLM(ABC):

    def __init__(self, wlm, network_prefix, port, nnodes):
        self.wlm = wlm
        self.NNODES = nnodes
        self.NETWORK_CFG_HELPER_LAUNCH_CMD = [
            "dragon-network-config-launch-helper",
            "--network-prefix",
            f"{network_prefix}",
            "--port",
            f"{port}",
        ]
        self.NETWORK_CFG_HELPER_LAUNCH_SHELL_CMD = [
            "dragon-network-config-launch-helper",
            "--network-prefix",
            f"{quote(network_prefix)}",
            "--port",
            f"{port}",
        ]
        self.NET_CONF_CACHE = this_process.net_conf_cache  # type: ignore

        self._BE_LAUNCHARGS = (
            f"{PROCNAME_LA_BE}"
            " --ip-addr {ip_addr}"
            " --host-id {host_id}"
            " --frontend-sdesc {frontend_sdesc}"
            " --network-prefix {network_prefix}"
            " --overlay-port {overlay_port}"
        )

        self.LOGGER = logging.getLogger("WorkloadManager")

        self.node_descriptors = {}

        self._sigint_trigger = None
        self._sigint_triggered = False
        self.config_helper = None
        self._hosts = None

        self._state = NetworkConfigState.NONE

    @classmethod
    def check_for_wlm_support(cls) -> bool:
        raise NotImplementedError

    @classmethod
    def check_for_allocation(cls) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _get_wlm_job_id(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _launch_network_config_helper(self) -> subprocess.Popen:
        raise NotImplementedError

    def _parse_network_configuration(self) -> None:
        last_node_descriptor_count = 0
        assert self.config_helper is not None
        stdout_stream = NewlineStreamWrapper(self.config_helper.stdout)
        stderr_stream = NewlineStreamWrapper(self.config_helper.stderr)
        node_returns = 0
        temp_uniqueness_guarantee = {}
        while node_returns != self.NNODES:

            lines = []
            node_descriptor_count = len(self.node_descriptors.keys())
            if last_node_descriptor_count != node_descriptor_count:
                self.LOGGER.debug(f"received {node_descriptor_count} of {self.NNODES} expected NodeDescriptors")
                last_node_descriptor_count = node_descriptor_count

            if self.config_helper.poll():  # Is the helper process still running?
                if self.config_helper.returncode != 0:  # Did the helper process exit with non-zero error code?
                    out, err = self.config_helper.communicate()
                    raise RuntimeError(str(err))

            while stdout_stream.poll():
                line = stdout_stream.recv()

                # sattach returns an empty string if nothing to report. ignore
                if line.strip() == "":
                    break
                else:
                    lines.append(line)

                # Only check stderr while we don't yet have the output on stdout. Once
                # we have the output, ignore stderr because in some circumstances we keep
                # reading empty strings anyway.
                if stderr_stream.poll():
                    err_line = stderr_stream.recv()
                    if len(err_line.strip()) == 0:
                        break

                    try:
                        _, msg = err_line.split(": ", maxsplit=1)
                    except ValueError:
                        print(f"Network Config Error Dettected: {err_line}", flush=True)
                        break

                    if len(msg.strip()) > 0:
                        print(f"Network Config Error Detected: {msg.strip()}", flush=True)
                        break

            for line in lines:
                self.LOGGER.debug(f"{line=}")
                node_index, node_desc = line.split(": ", maxsplit=1)
                if " " in node_index:
                    node_index = node_index.split(" ")[-1]
                if str(node_index) not in temp_uniqueness_guarantee.keys():
                    self.LOGGER.debug(json.loads(node_desc))
                    node = NodeDescriptor.from_sdict(json.loads(node_desc))
                    assert node is not None
                    if len(node.ip_addrs) > 0:  # type: ignore
                        self.node_descriptors[str(node_returns)] = node
                        temp_uniqueness_guarantee[str(node_index)] = node_returns
                    node_returns += 1

        self.LOGGER.debug(f"received {self.NNODES} of {self.NNODES} expected NodeDescriptors")

    def _sigint_teardown(self):
        """Safely teardown network config infrastructure"""

        try:
            assert self.config_helper is not None
            if self.config_helper.poll() is None:
                self.LOGGER.info("Transmitting SIGINT to config helper")
                self.config_helper.send_signal(signal.SIGINT)
                self.config_helper.wait()
        except NotImplementedError:  # AICI-1917 drun does not support send_signal or terminate
            pass
        except AttributeError:
            pass

        raise KeyboardInterrupt

    def _sigint_handler(self, *args):
        """Handler for SIGINT signals for graceful teardown"""
        self._sigint_triggered = True
        if self._state == NetworkConfigState.NONE or self._state == NetworkConfigState.CONFIG_DONE:
            self._sigint_teardown()

    @abstractmethod
    def _supports_net_conf_cache(self) -> bool:
        raise NotImplementedError

    def load_net_conf_cache(self):
        """ """
        if os.path.isfile(self.NET_CONF_CACHE):
            try:
                with open(self.NET_CONF_CACHE, "r") as inf:
                    data = json.load(inf)

                    if data["wlm"] == self.wlm and data["job_id"] == self._get_wlm_job_id():
                        self.LOGGER.debug("Loading cached network data")
                        for node_index, node_desc in data["nodes"].items():
                            self.node_descriptors[str(node_index)] = NodeDescriptor.from_sdict(node_desc)
                        return

            except (ValueError, json.JSONDecodeError):
                pass

            # Remove old cached data that doesn't match our current job
            os.remove(self.NET_CONF_CACHE)

    def save_net_conf_cache(self) -> None:
        with open(self.NET_CONF_CACHE, "w", encoding="utf-8") as outf:
            data = {
                "wlm": self.wlm,
                "job_id": self._get_wlm_job_id(),
                "nodes": {node_id: node_desc.get_sdict() for node_id, node_desc in self.node_descriptors.items()},
            }
            json.dump(data, outf, ensure_ascii=False, indent=4)

    def get_allocation_node_count(self) -> int:
        return self.NNODES

    def get_network_config(self, sigint_trigger=None) -> dict[str, NodeDescriptor]:

        try:
            self.orig_handler = signal.signal(signal.SIGINT, self._sigint_handler)

            self._state = NetworkConfigState.IN_PROGRESS

            if self._supports_net_conf_cache():
                self.load_net_conf_cache()

            # if we weren't able to load a cached network conf,
            # launch the config helper
            if not self.node_descriptors:
                self.LOGGER.debug("Launching config helper.")
                self.config_helper = self._launch_network_config_helper()

                if sigint_trigger == -2:
                    signal.raise_signal(signal.SIGINT)

                self.LOGGER.debug("Parsing configuration data.")
                self._parse_network_configuration()

                self.LOGGER.debug("Waiting for config helper to exit.")
                self.config_helper.wait()

                if self.config_helper.returncode != 0 and self.config_helper.returncode != -9:
                    out, err = self.config_helper.communicate()
                    raise RuntimeError(str(err))

                self.LOGGER.debug("Closing config helper's stdout handle.")
                assert self.config_helper.stdout is not None
                assert self.config_helper.stderr is not None
                self.config_helper.stdout.close()
                self.config_helper.stderr.close()

                if self._supports_net_conf_cache():
                    self.save_net_conf_cache()

            if sigint_trigger == -1:
                signal.raise_signal(signal.SIGINT)

            self._state = NetworkConfigState.CONFIG_DONE
            self.LOGGER.debug("Completed gathering Network Configuration")

            if self._sigint_triggered:
                self._sigint_teardown()

            # Set the handler back before leaving
            signal.signal(signal.SIGINT, self.orig_handler)

            # return
            return self.node_descriptors

        except Exception as e:
            raise RuntimeError(e)

    def _get_dragon_launch_be_args(
        self,
        fe_ip_addr: str,
        fe_host_id: str,
        frontend_sdesc: str,
        network_prefix: str,
        overlay_port: int,
        transport_test_env: bool,
    ) -> list[str]:
        """Return the launch arguments for the backend process."""
        try:
            assert self._BE_LAUNCHARGS is not None
            be_args = self._BE_LAUNCHARGS.format(
                ip_addr=fe_ip_addr,
                host_id=fe_host_id,
                frontend_sdesc=frontend_sdesc,
                network_prefix=network_prefix,
                overlay_port=overlay_port,
            ).split()
            if transport_test_env:
                be_args.append("--transport-test")
            return be_args
        except Exception:
            raise RuntimeError("Unable to construct backend launcher arg list")

    @abstractmethod
    def _get_wlm_launch_be_args(self, args_map: dict, launch_args: list):
        """
        Abstract method to return WLM specific command line arguments
        to use to launch the backend process.
        """
        raise NotImplementedError

    def _get_launch_be_args(
        self,
        args_map: dict,
        launch_args: list,
        nnodes: Optional[int] = None,
        nodelist: Optional[list[str]] = None,
        hostname: Optional[str] = None,
    ):
        """Get arguments for WLM to launch the backend"""

        try:
            if hostname is not None:
                args_map["hostname"] = hostname

            if nnodes is not None and nodelist is not None:
                args_map["nnodes"] = nnodes
                args_map["nodelist"] = ",".join(nodelist)

            wlm_args = self._get_wlm_launch_be_args(args_map=args_map, launch_args=launch_args)

        except Exception:
            raise RuntimeError("Unable to generate WLM backend launch args")

        return wlm_args

    def launch_backend(
        self,
        nnodes: int,
        node_ip_addrs: Optional[list[str]],
        nodelist: list[str],
        args_map: dict,
        fe_ip_addr: str,
        fe_host_id: str,
        frontend_sdesc: str,
        network_prefix: str,
        overlay_port: int,
        transport_test_env: bool,
    ) -> subprocess.Popen:
        try:
            dragon_be_args = self._get_dragon_launch_be_args(
                fe_ip_addr=fe_ip_addr,
                fe_host_id=fe_host_id,
                frontend_sdesc=frontend_sdesc,
                network_prefix=network_prefix,
                overlay_port=overlay_port,
                transport_test_env=transport_test_env,
            )

            wlm_launch_args = self._get_launch_be_args(
                args_map=args_map,
                launch_args=dragon_be_args,
                nnodes=nnodes,
                nodelist=nodelist,
            )
            self.LOGGER.info(f"launch be with {wlm_launch_args}")

            wlm_proc = subprocess.Popen(
                wlm_launch_args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, start_new_session=True
            )
        except Exception:
            raise RuntimeError("Unable to launch backend using workload manager launch")

        return wlm_proc
