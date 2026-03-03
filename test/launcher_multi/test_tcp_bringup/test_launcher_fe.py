import time
import logging
import os
import subprocess

from dragon.managed_memory import MemoryPool
from dragon.dlogging.util import setup_logging
from dragon.channels import Channel, register_gateways_from_env
from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import messages as dmsg
from dragon.infrastructure.connection import Connection, ConnectionOptions
from dragon.infrastructure.node_desc import NodeDescriptor

from dragon.launcher.channel_tree import start_channel_tree
from dragon.launcher.network_config import NetworkConfig, WLM

from dragon.utils import B64

setup_logging(basename="frontend", level=logging.DEBUG)
LOGGER = logging.getLogger("la_fe")
NUM_NODES = 1
ENV = None


if __name__ == "__main__":

    # Get the node config for the backend
    LOGGER.debug("Setting up")
    net = NetworkConfig.from_wlm(workload_manager=WLM.SLURM, port=7779, network_prefix=dfacts.DEFAULT_TRANSPORT_NETIF)
    net_config = net.get_network_config()

    # Add the frontend config
    net_config["f"] = NodeDescriptor.get_local_node_network_conf(port_range=7779)

    LOGGER.debug(f"network config: {net_config}")

    # Create my memory pool
    fe_mpool = MemoryPool(
        int(dfacts.DEFAULT_SINGLE_DEF_SEG_SZ),
        f"{os.getuid()}_{os.getpid()}_0" + dfacts.DEFAULT_POOL_SUFFIX,
        dfacts.default_pool_muid_from_index(1),
    )

    # Create my receiving channel:
    fe_cuid = 1544
    local_cuid = 1654
    gw_cuid = 6000

    # Channel for backend to come to
    fe_inbound = Channel(fe_mpool, fe_cuid)
    encoded_inbound = B64(fe_inbound.serialize())
    encoded_inbound_str = str(encoded_inbound)
    conn_in = Connection(inbound_initializer=fe_inbound)

    # Channel for tcp to tell me it's up
    local_ch = Channel(fe_mpool, local_cuid)
    local_in = Connection(inbound_initializer=local_ch)

    # Create a gateway channel for my tcp agent
    gw_ch = Channel(fe_mpool, gw_cuid)
    LOGGER.info("Memory pools and channels created")

    # Set gateway in my environment and register them
    encoded_ser_gw = B64(gw_ch.serialize())
    encoded_ser_gw_str = str(encoded_ser_gw)
    os.environ[dfacts.GW_ENV_PREFIX + "1"] = encoded_ser_gw_str
    register_gateways_from_env()

    # start my transport agent
    nnodes = len(net_config) - 1  # Exclude the frontend node from this
    host_ids = [str(node_desc.host_id) for node_desc in net_config.values()]
    ip_addrs = [node_desc.ip_addrs[0] for node_desc in net_config.values()]
    LOGGER.debug(f"standing up tcp agent with gw: {encoded_ser_gw_str}")
    tree_proc = start_channel_tree(None, str(B64(local_ch.serialize())), encoded_ser_gw_str, host_ids, ip_addrs)

    # Wait on a started message
    LOGGER.info("Channel tree initializing...")
    ping_back = dmsg.parse(local_in.recv())
    LOGGER.debug(f"recvd msg type {type(ping_back)}")  # Start my backend

    LOGGER.debug("standing up backend")
    backend_args = ["srun", f"--nodes={nnodes}", "python3", "./test_launcher_be.py", "--ip-addrs"]
    backend_args = (
        backend_args
        + ip_addrs
        + ["--host-ids"]
        + host_ids
        + ["--frontend-sdesc", encoded_inbound_str, "--node-index", "0"]
    )
    srun_procs = subprocess.Popen(args=backend_args)
    LOGGER.debug("backend started")

    # Try getting a backend channel descriptor
    be_ch_desc = conn_in.recv()
    LOGGER.debug(f"received descriptor: {be_ch_desc}")

    be_sdesc = B64.from_str(be_ch_desc)
    be_ch = Channel.attach(be_sdesc.decode(), mem_pool=fe_mpool)
    conn_options = ConnectionOptions(default_pool=fe_mpool)
    conn_out = Connection(outbound_initializer=be_ch, options=conn_options)
    msg_out = "hi from the frontend"
    LOGGER.debug(f"sending to be: {msg_out}")
    conn_out.send("hi from the frontend")
    time.sleep(5)
    ############### DO MY TESTING

    # TEARDOWN
    LOGGER.info("Tearing down")

    conn_in.close()
    conn_out.close()

    tree_proc.terminate()
    tree_proc.wait(timeout=3)
    tree_proc.kill()

    gw_ch.destroy()
    local_in.close()
    local_ch.destroy()
    fe_inbound.destroy()
    fe_mpool.destroy()

    LOGGER.debug("Teardown completed")
