import subprocess
import logging
import shutil
import shlex

from ...infrastructure.facts import TransportAgentOptions, DEFAULT_OVERLAY_NETWORK_PORT, TRANSPORT_AGENT_ALIASES
from ...utils import B64
from ...dlogging.util import DragonLoggingServices as dls


def start_overlay_network(
    ch_in_sdesc: B64,
    ch_out_sdesc: B64,
    log_sdesc: B64,
    host_ids: list[str],
    ip_addrs: list[str],
    frontend: bool = False,
    env: dict = None,
):
    """Entry point to start the launcher overlay network

    :param ch_in_sdesc: Channel to be used for infrastructure messages incoming
                        from launcher frontend/backend. One component of "CTRL ch"
    :type ch_in_sdesc: B64
    :param ch_out_sdesc: Channel to be used for infrastructure messages outgoing
                         to launcher frontend/backend. One component of "CTRL ch"
    :type ch_out_sdesc: B64
    :param log_sdesc: Channel to be used for sending logging messages to the Frontend
    :type log_sdesc: B64
    :param host_ids: List of host IDs for all nodes that will be communicated with
    :type host_ids: list[str]
    :param ip_addrs: List of IP address for all nodes that will be communicated with
    :type ip_addrs: list[str]
    :param frontend: Is parent process the launcher frontned defaults to False
    :type frontend: bool, optional
    :param env: environment variables. Should container gateway channels, defaults to None
    :type env: dict, optional
    :return: Popen object for the overlay network process
    :rtype: subprocess.Popen
    """

    log = logging.getLogger(dls.ON).getChild("start_overlay_network")

    # If no transport agent settings are set in launch parameters
    args = shlex.split(str(TransportAgentOptions.TCP))
    alias = TRANSPORT_AGENT_ALIASES.get(args[0].lower())
    if alias:
        args = alias + args[1:]
    # Resolve executable
    cmd = shutil.which(args[0])
    if cmd:  # Only replace if resolved
        args[0] = cmd

    args = (
        args
        + ["0", "--infrastructure", "--ip-addrs"]
        + ip_addrs
        + ["--host-ids"]
        + host_ids
        + [
            "--ch-in-sdesc",
            str(ch_in_sdesc),
            "--ch-out-sdesc",
            str(ch_out_sdesc),
            "--port",
            str(DEFAULT_OVERLAY_NETWORK_PORT),
            "--log-sdesc",
            str(log_sdesc),
        ]
    )

    if frontend:
        args.append("--frontend")

    log.info(f"overlay args: {args}")
    # start_new_session to detach this process and avoid SIGINT going to it
    return subprocess.Popen(args, bufsize=0, env=env, start_new_session=True)
