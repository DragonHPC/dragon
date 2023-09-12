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


PROCNAME_GS = 'dragon-globalservices'
PROCNAME_LA_FE = 'dragon'
PROCNAME_LA_BE = 'dragon-backend'
PROCNAME_LS = 'dragon-localservices'
PROCNAME_TCP_TA = 'dragon-tcp'
PROCNAME_OVERLAY_TA = 'dragon-overlay-tcp'
PROCNAME_NETWORK_CONFIG = 'dragon-network-config'
PROCNAME_NETWORK_CONFIG_LAUNCH_HELPER = 'dragon-network-config-launch-helper'
PROCNAME_NETWORK_CONFIG_SHUTDOWN_HELPER = 'dragon-network-config-shutdown-helper'
PROCNAME_NETWORK_IFADDRS='dragon-network-ifaddrs'

# TODO Refactor frontend entry point. See ../__main__.py and
# TODO ../launcher/launch_selector.py.

# TODO Refactor local services entry point. Need CLI option to select single
# TODO vs multi mode.

# TODO Refactor global services entry point. Need to replace facts
# TODO GS_SINGLE_LAUNCH_CMD and GS_MULTI_LAUNCH_CMD, which are used in
# TODO ../launcher/dragon_single.py and ../launcher/launch_multi_ls.py,
# TODO respectively. Combine into single CLI entry point with option to
# TODO configure mode: single vs multi.

entry_points = f"""
[console_scripts]
{PROCNAME_LA_FE} = dragon.launcher.launch_selector:main
{PROCNAME_LA_BE} = dragon.launcher.dragon_multi_be:main
{PROCNAME_LS} = dragon.launcher.launch_multi_ls:main
{PROCNAME_GS} = dragon.globalservices.server:multi
{PROCNAME_TCP_TA} = dragon.transport.tcp.__main__:main
{PROCNAME_NETWORK_CONFIG} = dragon.launcher.network_config:main
{PROCNAME_NETWORK_CONFIG_LAUNCH_HELPER} = dragon.launcher.network_config:deliver_backend_node_descriptor
{PROCNAME_NETWORK_CONFIG_SHUTDOWN_HELPER} = dragon.launcher.network_config:shutdown_backend_node_deliverer
{PROCNAME_NETWORK_IFADDRS} = dragon.transport.ifaddrs:main
"""


# Mapping of script name to EntryPoint object
console_scripts = {
    ep.name: ep
    for ep in EntryPoints._from_text(entry_points)
    if ep.group == 'console_scripts'
}

# Sanity check
if not console_scripts:
    raise RuntimeError(f'No console scripts found')


def console_script_args(name: str, *args: Any) -> list:
    """Returns list of command line arguments for the specified console script."""
    if name not in console_scripts:
        raise ValueError(f'Unknown console script: {name}')
    _args = [sys.executable, '-m', 'dragon.cli', name]
    _args.extend(args)
    return _args
