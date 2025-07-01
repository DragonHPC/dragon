import os
import sys
import shutil

from .wlm import WLM, wlm_cls_dict


def determine_environment(args=None):
    """
    Determine if dragon should be started in multi-node or single-node mode.

    Returns:
        False if dragon should be started in single-node mode
        True if dragon should be started in multi-node mode

    Raises:
        ValueError, RuntimeError
    """

    from dragon.launcher import launchargs

    # Check if the user requested a specific launcher mode
    arg_map = launchargs.get_args(args)

    wlm = str(arg_map.get("wlm", ""))
    single_arg = arg_map["single_node_override"]
    multi_arg = arg_map["multi_node_override"]

    if single_arg and multi_arg:
        msg = """Cannot request both single node and multi-node launcher simultaneously.
Please specify only '--single-node-override' or '--multi-node-override'"""
        raise ValueError(msg)

    if single_arg and wlm != "":
        msg = "Cannot request single node deployment of Dragon and specify a workload manager."
        raise ValueError(msg)

    if single_arg:
        return False

    if multi_arg:
        return True

    # Try to determine if we're on a supported multinode system
    if wlm != "":
        # only one of these will be true
        is_pbs = wlm == str(WLM.PBS_PALS)
        is_slurm = wlm == str(WLM.SLURM)
        is_ssh = wlm == str(WLM.SSH)
        is_k8s = wlm == str(WLM.K8S)
    else:
        # Likewise, only one of these will be true (unless somehow they have both PBS and slurm installed)
        is_pbs = wlm_cls_dict[WLM.PBS_PALS].check_for_wlm_support()
        is_slurm = wlm_cls_dict[WLM.SLURM].check_for_wlm_support()
        is_ssh = False
        is_k8s = (os.getenv("KUBERNETES_SERVICE_HOST") and os.getenv("KUBERNETES_SERVICE_PORT")) != None

    if is_ssh + is_pbs + is_slurm + is_k8s >= 2:
        # adding the booleans here is a quick check that two or more are not True.
        raise RuntimeError(
            "Dragon cannot determine the correct multi-node launch mode. Please specify the workload manager with --wlm"
        )

    if is_ssh:
        return True

    if is_k8s:
        return True

    if is_pbs:
        if not wlm_cls_dict[WLM.PBS_PALS].check_for_allocation():
            msg = """Using a supported PALS with PBS config. However, no active jobs allocation
has been detected. Resubmit as part of a 'qsub' execution."""
            raise RuntimeError(msg)

        return True

    if is_slurm:
        # Check if we have nodes allocated. If not, raise an
        # exception
        if not wlm_cls_dict[WLM.SLURM].check_for_allocation():
            msg = """Executing in a Slurm environment, but with no job allocation.
    Resubmit as part of an 'salloc' or 'sbatch' execution"""
            raise RuntimeError(msg)

        return True

    return False


def get_launcher():
    multi_mode = determine_environment()

    if multi_mode:
        from dragon.launcher import dragon_multi_fe as dl
    else:
        from dragon.launcher import dragon_single as dl
    return dl


def main():
    """
    .. code-block::

        usage: dragon [-h] [-N NODE_COUNT] [--hostlist HOSTLIST | --hostfile HOSTFILE] [--network-prefix NETWORK_PREFIX] [--network-config NETWORK_CONFIG]
                      [--wlm WORKLOAD_MANAGER] [-p PORT] [--transport TRANSPORT_AGENT] [-s | -m] [-l LOG_LEVEL] [--no-label] [--basic-label] [--verbose-label]
                      [--version] [PROG] ...

        Dragon Launcher Arguments and Options

        positional arguments:
          PROG                  PROG specifies an executable program to be run on the primary compute node. In this case, the file may be either executable or not.
                                If PROG is not executable, then Python version 3 will be used to interpret it. All command-line arguments after PROG are passed to
                                the program as its arguments. The PROG and ARGS are optional.
          ARG                   Zero or more program arguments may be specified.

        optional arguments:
          -h, --help            show this help message and exit
          -N NODE_COUNT, --nodes NODE_COUNT
                                NODE_COUNT specifies the number of nodes to use. NODE_COUNT must be less or equal to the number of available nodes within the WLM
                                allocation. A value of zero (0) indicates that all available nodes should be used (the default).
          --hostlist HOSTLIST   Specify backend hostnames as a comma-separated list, eg: `--hostlist host_1,host_2,host_3`. `--hostfile` or `--hostlist` is a
                                required argument for WLM SSH and is only used for SSH
          --hostfile HOSTFILE   Specify a list of hostnames to connect to via SSH launch. The file should be a newline character separated list of hostnames.
                                `--hostfile` or `--hostlist` is a required argument for WLM SSH and is only used for SSH
          --network-prefix NETWORK_PREFIX
                                NETWORK_PREFIX specifies the network prefix the dragon runtime will use to determine which IP addresses it should use to build
                                multinode connections from. By default the regular expression r'^(hsn|ipogif|ib|eth)\\d+$' is used -- the prefix for known
                                HPE-Cray XC and EX high speed networks as well as the common eth interface. If uncertain which networks are available, the
                                following will return them in pretty formatting: `dragon-network-ifaddrs --ip --no-loopback --up --running | jq`. Prepending
                                with `srun` may be necessary to get networks available on backend compute nodes. The eth prefix is only used as a last resort
                                if no high-speed networks are found.
          --network-config NETWORK_CONFIG
                                NETWORK_CONFIG specifies a YAML or JSON file generated via a call to the launcher's network config tool that successfully generated
                                a corresponding YAML or JSON file (eg: `dragon-network-config --output-to-yaml`) describing the available backend compute nodes
                                specified either by a workload manager (this is what the tool provides). Alternatively, one can be generated manually as is needed
                                in the case of ssh-only launch. An example with keywords and formatting can be found in the documentation
          --wlm WORKLOAD_MANAGER, -w WORKLOAD_MANAGER
                                Specify what workload manager is used. Currently supported WLMs are: slurm, pbs+pals, ssh
          -p PORT, --port PORT  PORT specifies the port to be used for multinode communication. By default, 7575 is used.
          --transport TRANSPORT_AGENT, -t TRANSPORT_AGENT
                                TRANSPORT_AGENT selects which transport agent will be used for backend node-to-node communication. By default, the high speed
                                transport agent (hsta) is selected. Currently supported agents are: hsta, tcp
          -s, --single-node-override
                                Override automatic launcher selection to force use of the single node launcher
          -m, --multi-node-override
                                Override automatic launcher selection to force use of the multi-node launcher
          -l LOG_LEVEL, --log-level LOG_LEVEL
                                The Dragon runtime enables the output of diagnostic log messages to multiple different output devices. Diagnotic log messages can be
                                seen on the Dragon stderr console, via a combined 'dragon_*.log' file, or via individual log files created by each of the Dragon
                                'actors' (Global Services, Local Services, etc). By default, the Dragon runtime disables all diagnostic log messaging. Passing one
                                of NONE, DEBUG, INFO, WARNING, ERROR, or CRITICAL to this option, the Dragon runtime will enable the specified log verbosity. When
                                enabling DEBUG level logging, the Dragon runtime will limit the stderr and combined dragon log file to INFO level messages. Each
                                actor's log file will contain the complete log history, including DEBUG messages. This is done to help limit the number of messages
                                sent between the Dragon frontend and the Dragon backend at scale. To override the default logging behavior and enable specific
                                logging to one or more Dragon output devices, the LOG_LEVEL option can be formatted as a keyword=value pair, where the KEYWORD is
                                one of the Dragon log output devices (stderr, dragon_file or actor_file), and the VALUE is one of NONE, DEBUG, INFO, WARNING, ERROR
                                or CRITICAL (eg `-l dragon_file=INFO -l actor_file=DEBUG`). Multiple -l|--log-level options may be passed to enable the logging
                                desired.
          --no-label
          --basic-label
          --verbose-label
          --version             show program's version number and exit
    """
    from dragon import _patch_multiprocessing

    _patch_multiprocessing()
    dl = get_launcher()
    sys.exit(dl.main())


if __name__ == "__main__":
    sys.exit(main())
