from collections import ChainMap
import logging
import os
import shlex
import time
import unittest

from dragon.channels import Channel
from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import messages as dmsg
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.launcher.util import next_tag
from dragon.managed_memory import MemoryPool
from dragon.native.queue import Queue as DQueue
from dragon.transport import start_transport_agent


LOGGER = logging.getLogger("test.transport")
NUM_GW_CHANNELS_PER_NODE = 1


def setUpModule():
    logging.basicConfig(
        format="%(asctime)s %(levelname)-9s %(process)d %(name)s:%(filename)s:%(lineno)d %(message)s",
        level=logging.NOTSET,
    )
    logging.disable(logging.NOTSET)


def tearDownModule():
    logging.disable()


class LocalServicesInterface:

    NUM_NODES = 1
    ENV = None

    @classmethod
    def setUpClass(cls):
        if not hasattr(cls, "ARGS"):
            raise NotImplementedError("Missing args to launch transport agent")

    def setUp(self):
        LOGGER.debug("Setting up")

        self.nodes = {
            i: {
                "host_name": f"node-{i}",
                "host_id": str(i),
                "ip_addrs": [f"127.0.0.1:{i+8000}"],
            }
            for i in range(self.NUM_NODES)
        }

        # Create channels for each node
        for i, n in self.nodes.items():
            # Create memory pool
            n["mempool"] = MemoryPool(
                int(dfacts.DEFAULT_SINGLE_DEF_SEG_SZ),
                f"{os.getuid()}_{os.getpid()}_{i}" + dfacts.DEFAULT_POOL_SUFFIX,
                dfacts.default_pool_muid_from_index(i),
            )
            # Create agent I/O channels
            n["ls_in_ch"] = Channel(n["mempool"], dfacts.localservices_cuid_from_index(i))
            n["ta_in_ch"] = Channel(n["mempool"], dfacts.transport_cuid_from_index(i))
            # Wrap channels in DQueues with MessagePickler (required by the transport agent)
            n["ls_in_q"] = DQueue(main_channel=n["ls_in_ch"], pool=n["mempool"], pickler=dmsg.MessagePickler())
            n["ta_in_q"] = DQueue(main_channel=n["ta_in_ch"], pool=n["mempool"], pickler=dmsg.MessagePickler())
            # Local services channel descriptor (DQueue descriptor for the TA to send back to LS)
            n["ls_cd"] = n["ls_in_q"].serialize()
            # Create gateway channels
            n["gw_chs"] = [
                Channel(n["mempool"], dfacts.gw_cuid_from_index(i, NUM_GW_CHANNELS_PER_NODE) + j)
                for j in range(NUM_GW_CHANNELS_PER_NODE)
            ]

        LOGGER.info("Memory pools and channels created")

        # Use the nodes dict to construct a dictionary of NodeDescriptor objects
        nodes_desc = {
            i: NodeDescriptor(
                ip_addrs=node["ip_addrs"], host_id=node["host_id"], ls_cd=node["ls_cd"], host_name=node["host_name"]
            )
            for i, node in self.nodes.items()
        }

        # Create LAChannelsInfo
        la_ch_info = dmsg.LAChannelsInfo(
            tag=next_tag(),
            nodes_desc=nodes_desc,
            gs_qd="",
            num_gw_channels=NUM_GW_CHANNELS_PER_NODE,
        )

        LOGGER.info(f"LAChannelsInfo created: {la_ch_info.get_sdict()}")

        # Start agents
        for i, n in self.nodes.items():
            n["agent"] = start_transport_agent(
                node_index=str(i),
                in_ch_sdesc=n["ta_in_q"].serialize(),
                args=self.ARGS,
                env=self.ENV,
                gateway_channels=n["gw_chs"],
            )

        LOGGER.info("Agents started")

        # Send LAChannelsInfo
        for n in self.nodes.values():
            n["ta_in_q"].put(la_ch_info)

        LOGGER.info("Sent all LAChannelsInfo")

        # Wait for each node to reply with pings
        for i, n in self.nodes.items():
            ping_msg = n["ls_in_q"].get()
            assert isinstance(ping_msg, dmsg.TAPingLS), f"Did not receive TAPingLS from node {i}"

        LOGGER.info("Received all TAPingLS")
        LOGGER.debug("Setup completed")

    def tearDown(self):
        LOGGER.info("Tearing down")

        # Halt
        for n in self.nodes.values():
            n["ta_in_q"].put(dmsg.LSHaltTA(next_tag()))

        LOGGER.info("Sent all LSHaltTA")

        # Wait for each node to reply
        for i, n in self.nodes.items():
            halted_msg = n["ls_in_q"].get()
            assert isinstance(halted_msg, dmsg.TAHalted), f"Did noot receive TAHalted from node {i}"

        LOGGER.info("Received all TAHalted")

        for n in self.nodes.values():
            # Stop agent
            n["agent"].terminate()
            n["agent"].wait(timeout=3)
            n["agent"].kill()
            # Destroy gateway channels
            for ch in n["gw_chs"]:
                ch.destroy()
            # Close DQueues (release channel references)
            n["ta_in_q"].close()
            n["ls_in_q"].close()
            # Destroy channels (externally managed, DQueue does not own them)
            n["ta_in_ch"].destroy()
            n["ls_in_ch"].destroy()
            # Destroy memory pool
            n["mempool"].destroy()

        LOGGER.debug("Teardown completed")

    def test_allow_agent_control_loops_to_poll_once(self):
        # TODO Send a GatewayMessage to each agent and verify it arrived
        # Wait for control loop to poll at least once
        time.sleep(6)


class TCPTestCase(LocalServicesInterface, unittest.TestCase):

    NUM_NODES = 3
    ARGS = shlex.split(f"tcp --log-level {logging.getLevelName(LOGGER.getEffectiveLevel())} --no-dragon-logging")


@unittest.skip
class HSTATestCase(LocalServicesInterface, unittest.TestCase):

    ARGS = shlex.split(f"hsta --log-level {logging.getLevelName(LOGGER.getEffectiveLevel())} --no-dragon-logging")


if __name__ == "__main__":
    unittest.main()
