from collections import ChainMap
from collections.abc import Iterable
import logging
import os
import shutil
import subprocess
from typing import Union

from ..channels import Channel
from ..infrastructure import facts
from ..utils import B64


def resolve_args(args=None):
    # If no transport agent settings are set in launch parameters
    if not args:
        args = facts.DEFAULT_TRANSPORT_AGENT
    alias = facts.TRANSPORT_AGENT_ALIASES.get(args[0].lower())
    if alias:
        args = alias + args[1:]
    # Resolve executable
    cmd = shutil.which(args[0])
    if cmd:  # Only replace if resolved
        args[0] = cmd
    return args


def start_transport_agent(
    node_index: Union[int, str],
    in_ch_sdesc: B64,
    log_ch_sdesc: B64 = None,
    args: list[str] = None,
    env: dict[str, str] = None,
    gateway_channels: Iterable[Channel] = None,
    infrastructure: bool = False,
) -> subprocess.Popen:
    """Start the backend transport agent

    :param node_index: node index for this agent. should be in 0, N-1,
                       where N is number of backend compute nodes
    :type node_index: Union[int, str]
    :param in_ch_sdesc: Channel to be used for incoming infrastructure messages
    :type in_ch_sdesc: B64
    :param log_ch_sdesc: Channel to be used for logging to the Frontend, defaults to None
    :type log_ch_sdesc: B64, optional
    :param args: Command args for starting Transport Agents, defaults to None in which
                 case the TCP transport agent is initiated as default
    :type args: list[str], optional
    :param env: Environment variables, defaults to None
    :type env: dict[str, str], optional
    :param gateway_channels: gateway channels to be set in transport agent's environment.
                             If not set, it is assumed they are already set in the parent
                             environment or the one input, defaults to None
    :type gateway_channels: Iterable[Channel], optional
    :param infrastructure: whether this agent will be used for infrastructure messaging, defaults to False
    :type infrastructure: bool, optional
    :return: Transport agent child process
    :rtype: subprocess.Popen
    """

    from ..dlogging.util import setup_BE_logging, DragonLoggingServices as dls

    if infrastructure:
        service = dls.LA_BE
    else:
        service = dls.LS

    LOGGER = logging.getLogger(str(service)).getChild("start_transport_agent")

    # Put together and envioronment variables dict
    _env = ChainMap()
    if env is not None:
        _env.maps.append(env)
    _env.maps.append(os.environ)

    # Modify the LD_LIBRARY_PATH for launch of HSTA so it can find dragon and
    # fabric libraries
    lib_dir = os.path.join(os.path.dirname(__file__), "../lib")
    _env["LD_LIBRARY_PATH"] = lib_dir + ":" + str(_env.get("LD_LIBRARY_PATH", ""))

    if gateway_channels is not None:
        for i, ch in enumerate(gateway_channels):
            _env[f"{facts.GW_ENV_PREFIX}{i+1}"] = str(B64(ch.serialize()))

    for key, val in _env.items():
        LOGGER.debug(f"{key}: {val}")

    # TODO: This needs to be tightened up
    _args = resolve_args(args) + [str(node_index), "--ch-in-sdesc", str(in_ch_sdesc), "--log-sdesc", str(log_ch_sdesc)]

    LOGGER.debug(f"Starting transport agent: {_args}")
    return subprocess.Popen(_args, bufsize=0, stdin=subprocess.DEVNULL, env=_env)
