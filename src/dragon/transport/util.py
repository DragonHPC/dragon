"""Utilities for transport agents."""

import json
import logging
import subprocess
import os
from pathlib import Path

from ..infrastructure import facts as dfacts
from ..infrastructure.util import get_host_info, port_check


hsta_config_dict = {"fabric_ep_addrs_available": False}
first_time_using_hsta_for_config = True


def get_fabric_backend():
    config_file_path = dfacts.CONFIG_FILE_PATH

    if config_file_path.exists():
        with open(config_file_path) as config_file:
            config_dict = json.load(config_file)

        # Get all the runtimes
        backends = [(key.split("_")[0], config_dict[key]) for key in config_dict.keys() if "runtime" in key]

        # If there's more than 1 runtime, do some selection work
        if len(backends) > 1:
            backend_names, _ = zip(*backends)

            # if tcp is included, always use it
            if "tcp" in backend_names:
                return "tcp", None

            # otherwise return the first that's not TCP
            else:
                for backend_name, backend_lib in backends:
                    # Handle list of paths
                    if isinstance(backend_lib, list):
                        backend_lib = ":".join(backend_lib)
                    if "tcp" not in backend_name:
                        return backend_name, backend_lib

        # If there's only one, return it
        else:
            name, lib = backends[0]
            # Handle list of paths
            if isinstance(lib, list):
                lib = ":".join(lib)
            return name, lib

    return None, None


def create_hsta_env(nic_idx):
    from ..dlogging.util import DragonLoggingServices as dls

    log = logging.getLogger(dls.TA).getChild("create_hsta_env")

    env = dict(os.environ)

    # set the debugging flag
    logging_enabled = (log.getEffectiveLevel() == logging.DEBUG)
    if logging_enabled:
        env["DRAGON_HSTA_DEBUG"] = "1"
    else:
        env["DRAGON_HSTA_DEBUG"] = "0"

    # set the nic idx for this HSTA agent
    env["DRAGON_HSTA_NIC_IDX"] = str(nic_idx)

    HSTA_GW_ENV_PREFIX = "DRAGON_HSTA_GW"

    # Make sure the HSTA binary is in the path
    bin_path = os.path.join(dfacts.DRAGON_BASE_DIR, "bin")
    env["PATH"] = bin_path + ":" + env["PATH"]

    for i in range(dfacts.NUM_GW_TYPES):
        hsta_key = f"{HSTA_GW_ENV_PREFIX}{i + 1}_TIDX{nic_idx}"
        j = (dfacts.NUM_GW_TYPES * nic_idx) + i + 1
        dragon_key = f"{dfacts.GW_ENV_PREFIX}{j}"
        if dragon_key in os.environ:
            env[hsta_key] = os.environ[dragon_key]
            log.info(f"setting env[{hsta_key}] = {env[dragon_key]} for agent with thread_idx = {nic_idx}")

    # updating LD_LIBRARY_PATH below, so make sure the key exists
    if "LD_LIBRARY_PATH" not in env:
        env["LD_LIBRARY_PATH"] = ""

    # Configure LD_LIBRARY_PATH so HSTA can load shared libraries
    if "CRAY_LD_LIBRARY_PATH" in env:
        # Make sure LD_LIBRARY_PATH is first. For ssh launch, CRAY_LD_LIBRARY_PATH can
        # get swapped to PrgEnv-cray on the backend, and then everything gets messed up.
        ld_library_path = str(env["LD_LIBRARY_PATH"]).split(os.pathsep)
        ld_library_path.extend(env.get("CRAY_LD_LIBRARY_PATH", "").split(os.pathsep))
        env["LD_LIBRARY_PATH"] = os.pathsep.join(filter(None, ld_library_path))

    # set envars for fabric backend
    backend_name, backend_lib_path = get_fabric_backend()
    env["_DRAGON_HSTA_BACKEND_NAME"] = backend_name
    if backend_lib_path is not None:
        env["_DRAGON_HSTA_BACKEND_LIB"] = backend_lib_path

    log.info(f"config found in dragon-config.json: {backend_name=}, {backend_lib_path=}")

    # add dfabric and fabric backend paths to LD_LIBRARY_PATH
    dfabric_lib_path = Path(dfacts.DRAGON_BASE_DIR) / "lib"
    env["LD_LIBRARY_PATH"] = f"{dfabric_lib_path}:" + str(env.get("LD_LIBRARY_PATH", ""))
    if backend_lib_path is not None:
        env["LD_LIBRARY_PATH"] = f"{backend_lib_path}:" + str(env.get("LD_LIBRARY_PATH", ""))

    log.info("LD_LIBRARY_PATH=%s", env["LD_LIBRARY_PATH"])

    if "PMI_CONTROL_FD" in env:
        # this forces PMI to avoid using the WLM supplied file descriptor
        # for the listening socket
        del env["PMI_CONTROL_FD"]

        # choosing a new port for PMI to avoid "already in use" error
        found_port = False
        min_port = 1025
        max_port = 65536
        _, ip_addrs = get_host_info(dfacts.DEFAULT_TRANSPORT_NETIF)
        ip_addr = ip_addrs[0]

        for port in range(min_port, max_port):
            if port_check((ip_addr, port)):
                env["PMI_CONTROL_PORT"] = str(port)
                found_port = True
                break

        assert found_port, "unable to find free port for PMI"

    # set this for SS10 to avoid hitting resource issues
    env["FI_OFI_RXM_RX_SIZE"] = "8192"

    # use software tag matching on cassini networks for now
    # TODO: improve this by limiting the number of posted receives
    env["FI_CXI_RX_MATCH_MODE"] = "hybrid"

    # disable Cray MPI GPU support
    env["MPICH_GPU_SUPPORT_ENABLED"] = "0"

    # tame ucx worker/endpoint adddress size
    # TODO: disabling this for now, since it seems buggy
    # env['UCX_UNIFIED_MODE'] = 'y'

    # TODO: maybe set UCX_NET_DEVICES as well

    is_k8s = (os.getenv("KUBERNETES_SERVICE_HOST") and os.getenv("KUBERNETES_SERVICE_PORT")) != None
    if is_k8s:
        env["DRAGON_HSTA_UCX_NO_MEM_REGISTER"] = "1"

    return env


def get_fabric_ep_addrs(num_nics, use_hsta):
    """Return list of libfabric endpoint names (via fi_getname) for each nic on this node"""
    global hsta_config_dict
    global first_time_using_hsta_for_config

    if first_time_using_hsta_for_config:
        if use_hsta and "DRAGON_BASE_DIR" in os.environ:
            from ..dlogging.util import DragonLoggingServices as dls

            log = logging.getLogger(dls.TA).getChild("get_fabric_ep_addrs")

            first_time_using_hsta_for_config = False

            hsta_binary = dfacts.HSTA_BINARY
            if not hsta_binary.is_file():
                return False, None, None

            try:
                fabric_ep_addrs = []
                fabric_ep_addr_lens = []
                fabric_ep_addrs_available = True

                for nic_idx in range(num_nics):
                    hsta_config_output = subprocess.check_output(
                        [hsta_binary, "--dump-network-config"],
                        env=create_hsta_env(nic_idx),
                        encoding="utf-8",
                        stderr=subprocess.STDOUT,
                    )

                    hsta_config_str = hsta_config_output.strip().split("network config=")[1]
                    tmp_dict = dict(json.loads(hsta_config_str))

                    fabric_ep_addrs_available = fabric_ep_addrs_available and tmp_dict["fabric_ep_addrs_available"]

                    ep_addr = tmp_dict["fabric_ep_addrs"][0]
                    ep_addr_len = tmp_dict["fabric_ep_addr_lens"][0]

                    fabric_ep_addrs.append(ep_addr)
                    fabric_ep_addr_lens.append(ep_addr_len)

                hsta_config_dict["fabric_ep_addrs_available"] = fabric_ep_addrs_available
                hsta_config_dict["fabric_ep_addrs"] = fabric_ep_addrs
                hsta_config_dict["fabric_ep_addr_lens"] = fabric_ep_addr_lens
            except subprocess.CalledProcessError:
                log.debug("HSTA failed to run in dump-network-config mode, setting fabric_ep_addrs_available to false")
                return False, None, None

            log.debug(f"HSTA config: {hsta_config_dict}")
        else:
            return False, None, None

    ep_addrs_available = hsta_config_dict["fabric_ep_addrs_available"]
    if ep_addrs_available:
        encoded_ep_addr_list = hsta_config_dict["fabric_ep_addrs"]
        ep_addr_len_list = hsta_config_dict["fabric_ep_addr_lens"]
    else:
        encoded_ep_addr_list = None
        ep_addr_len_list = None

    return ep_addrs_available, encoded_ep_addr_list, ep_addr_len_list
