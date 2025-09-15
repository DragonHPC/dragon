import sys
from typing import Any

try:
    # Python 3.10+ provides EntryPoints._from_text()
    from importlib.metadata import EntryPoints
except ImportError:
    # Python 3.9 provides EntryPoint._from_text()
    from importlib.metadata import EntryPoint

    # Emulate Python 3.10 interface
    class EntryPoints:
        @staticmethod
        def _from_text(text):
            return EntryPoint._from_text(text)


PROCNAME_GS = "dragon-globalservices"
PROCNAME_LA_FE = "dragon"
PROCNAME_LA_BE = "dragon-backend"
PROCNAME_LS = "dragon-localservices"
PROCNAME_TCP_TA = "dragon-tcp"
PROCNAME_OVERLAY_TA = "dragon-overlay-tcp"
PROCNAME_OOB_TA = "dragon-oob-tcp"
PROCNAME_RDMA_TA = "dragon-hsta"
PROCNAME_NETWORK_CONFIG = "dragon-network-config"
PROCNAME_NETWORK_CONFIG_LAUNCH_HELPER = "dragon-network-config-launch-helper"
PROCNAME_NETWORK_CONFIG_SHUTDOWN_HELPER = "dragon-network-config-shutdown-helper"
PROCNAME_NETWORK_IFADDRS = "dragon-network-ifaddrs"
PROCNAME_HSTA_CONFIG = "dragon-config"
PROCNAME_GLOBAL_CLEANUP_DEPRECATED = "dragon-cleanup-deprecated"
PROCNAME_GLOBAL_CLEANUP = "dragon-cleanup"
PROCNAME_LOCAL_CLEANUP = "dragon-node-cleanup"
PROCNAME_HPAGES_CLEANUP = "dragon-hugepages-cleanup"
PROCNAME_JUPYTER = "dragon-jupyter"
PROCNAME_DRUN = "drun"
PROCNAME_DRBE = "drbe"
PROCNAME_DHOSTS = "dhosts"

# TODO Refactor frontend entry point. See ../__main__.py and
# TODO ../launcher/launch_selector.py.

# TODO Refactor local services entry point. Need CLI option to select single
# TODO vs multi mode.

# TODO Refactor global services entry point. Need to replace facts
# TODO GS_SINGLE_LAUNCH_CMD and GS_MULTI_LAUNCH_CMD, which are used in
# TODO ../launcher/dragon_single.py and ../launcher/launch_multi_ls.py,
# TODO respectively. Combine into single CLI entry point with option to
# TODO configure mode: single vs multi.

entry_points = {
    "console_scripts": [
        f"{PROCNAME_LA_FE} = dragon.launcher.launch_selector:main",
        f"{PROCNAME_LA_BE} = dragon.launcher.dragon_multi_be:main",
        f"{PROCNAME_LS} = dragon.launcher.launch_multi_ls:main",
        f"{PROCNAME_GS} = dragon.globalservices.server:multi",
        f"{PROCNAME_TCP_TA} = dragon.transport.tcp.__main__:main",
        f"{PROCNAME_RDMA_TA}-if = dragon.transport.hsta.__main__:main",
        f"{PROCNAME_NETWORK_CONFIG} = dragon.launcher.network_config:main",
        f"{PROCNAME_NETWORK_CONFIG_LAUNCH_HELPER} = dragon.launcher.network_config:deliver_backend_node_descriptor",
        f"{PROCNAME_NETWORK_CONFIG_SHUTDOWN_HELPER} = dragon.launcher.network_config:shutdown_backend_node_deliverer",
        f"{PROCNAME_NETWORK_IFADDRS} = dragon.transport.ifaddrs:main",
        f"{PROCNAME_HSTA_CONFIG} = dragon.infrastructure.config:hsta_config",
        f"{PROCNAME_HPAGES_CLEANUP} = dragon.infrastructure.config:hugepages_cleanup",
        f"{PROCNAME_GLOBAL_CLEANUP_DEPRECATED} = dragon.launcher.util:exec_dragon_cleanup",
        f"{PROCNAME_GLOBAL_CLEANUP} = dragon.tools.dragon_cleanup:drun",
        f"{PROCNAME_JUPYTER} = dragon.jupyter.server:start_server",
        f"{PROCNAME_DRUN} = dragon.tools.dragon_run.drun:main",
        f"{PROCNAME_DRBE} = dragon.tools.dragon_run.drbe:main",
        f"{PROCNAME_DHOSTS} = dragon.tools.dragon_run.dhosts:main",
    ]
}


def _get_console_scripts(eps):
    for k, v in entry_points.items():
        if k == "console_scripts":
            str_ep = "[console_scripts]\n" + "\n".join(v)
            eps = EntryPoints._from_text(str_ep)
            _cscripts = {ep.name: ep for ep in eps if ep.group == "console_scripts"}
    return _cscripts


console_scripts = _get_console_scripts(entry_points)

# Sanity check
if not console_scripts:
    raise RuntimeError("No console scripts found")


def console_script_args(name: str, *args: Any) -> list:
    """Returns list of command line arguments for the specified console script."""
    if name not in console_scripts:
        raise ValueError(f"Unknown console script: {name}")
    _args = [sys.executable, "-m", "dragon.cli", name]
    _args.extend(args)
    return _args
