import logging
import os
import argparse

from dragon.managed_memory import MemoryPool
from dragon.dlogging.util import setup_logging
from dragon.channels import Channel, register_gateways_from_env
from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import messages as dmsg

from dragon.infrastructure.connection import Connection, ConnectionOptions
from dragon.launcher.channel_tree import start_channel_tree
from dragon.utils import B64

setup_logging(basename="backend", level=logging.DEBUG)
LOGGER = logging.getLogger("la_be")


def get_args():

    parser = argparse.ArgumentParser(
        prog="launcher-be", description="Arguments for standing up backend infrastructure tree for Dragon"
    )
    parser.add_argument("--ip-addrs", metavar="FRONTEND_IP", dest="ip_addrs", nargs="+", type=str)
    parser.add_argument("--local-sdesc", dest="local_sdesc", type=str)
    parser.add_argument("--host-ids", dest="host_ids", type=str, nargs="+")

    parser.add_argument("-n", "--node-index", dest="node_index", type=int)
    parser.add_argument("--frontend-sdesc", dest="frontend_sdesc")

    args = parser.parse_args()
    return args


if __name__ == "__main__":

    nnodes = 1

    args = get_args()
    LOGGER.info(f"args: {args}")
    # Create my memory pool
    be_mpool = MemoryPool(
        int(dfacts.DEFAULT_SINGLE_DEF_SEG_SZ),
        f"{os.getuid()}_{os.getpid()}_{args.node_index}" + dfacts.DEFAULT_POOL_SUFFIX,
        dfacts.default_pool_muid_from_index(args.node_index),
    )

    # Create my receiving channel:
    be_cuid = 1545
    local_cuid = 1600
    gw_cuid = 6001

    fe_sdesc = B64.from_str(args.frontend_sdesc)
    be_inbound = Channel(be_mpool, be_cuid)
    local_ch = Channel(be_mpool, local_cuid)
    local_in = Connection(inbound_initializer=local_ch)
    overlay_in = Connection(inbound_initializer=be_inbound)

    # Create a gateway channel for myself
    gw_ch = Channel(be_mpool, gw_cuid)

    # Set gateway in my environment and register them
    encoded_ser_gw = B64(gw_ch.serialize())
    encoded_ser_gw_str = str(encoded_ser_gw)
    os.environ[dfacts.GW_ENV_PREFIX + "1"] = encoded_ser_gw_str
    register_gateways_from_env()

    LOGGER.info("Memory pools and channels created")

    # start my transport agent
    LOGGER.debug(f"standing up tcp agent with gw: {encoded_ser_gw_str}")
    tree_proc = start_channel_tree(
        args.node_index, str(B64(local_ch.serialize())), encoded_ser_gw_str, args.host_ids, args.ip_addrs
    )

    # Wait on a message telling me it's good to go.
    LOGGER.info("Channel tree initializing....")

    ping_back = dmsg.parse(local_in.recv())
    LOGGER.debug(f"recvd msg type {type(ping_back)}")

    # Connect to frontend, provide a mem pool since it will try to use
    # the default pool that doesn't exist yet since there aren't any overlay transport channels yet
    be_outbound = Channel.attach(fe_sdesc.decode(), mem_pool=be_mpool)
    conn_options = ConnectionOptions(default_pool=be_mpool)
    overlay_out = Connection(outbound_initializer=be_outbound, options=conn_options)

    # Send my serialized descriptor to the frontend
    overlay_out.send(str(B64(be_inbound.serialize())))
    LOGGER.info(f"sent my serialized descriptor to the frontend: {str(B64(be_inbound.serialize()))}")
    # Try getting a backend channel descriptor
    handshake_msg = overlay_in.recv()
    LOGGER.info(f"backend recvd: {handshake_msg}")

    ############### DO MY TESTING

    # TEARDOWN
    LOGGER.info("Tearing down")

    LOGGER.info("Received all TAHalted")

    overlay_out.close()
    overlay_in.close()

    tree_proc.terminate()
    tree_proc.wait(timeout=3)
    tree_proc.kill()

    gw_ch.destroy()
    local_in.close()
    local_ch.destroy()
    be_inbound.destroy()
    be_mpool.destroy()

    LOGGER.debug("Teardown completed")
