from collections import ChainMap
import logging
import os
import shlex
import time
import unittest

from dragon.channels import Channel
from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import messages as dmsg
from dragon.infrastructure.connection import Connection
from dragon.launcher.util import next_tag
from dragon.managed_memory import MemoryPool
from dragon.transport import start_transport_agent
from dragon.utils import B64


LOGGER = logging.getLogger('test.transport')
NUM_GW_CHANNELS_PER_NODE=1


def setUpModule():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-9s %(process)d %(name)s:%(filename)s:%(lineno)d %(message)s',
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
        if not hasattr(cls, 'ARGS'):
            raise NotImplementedError('Missing args to launch transport agent')

    def setUp(self):
        LOGGER.debug('Setting up')

        self.nodes = {
            i: {
                'host_name': f'node-{i}',
                'host_id': str(i),
                'ip_addrs': [f'127.0.0.1:{i+8000}'],
            } for i in range(self.NUM_NODES)
        }

        # Create channels for each node
        for i, n in self.nodes.items():
            # Create memory pool
            n['mempool'] = MemoryPool(
                int(dfacts.DEFAULT_SINGLE_DEF_SEG_SZ),
                f'{os.getuid()}_{os.getpid()}_{i}' + dfacts.DEFAULT_POOL_SUFFIX,
                dfacts.default_pool_muid_from_index(i),
            )
            # Create agent I/O connection
            n['conn'] = Connection(
                inbound_initializer=Channel(n['mempool'], dfacts.shepherd_cuid_from_index(i)),
                outbound_initializer=Channel(n['mempool'], dfacts.transport_cuid_from_index(i)),
            )
            # Set shepherd's channel (i.e., input for local services)
            n['shep_cd'] = B64.bytes_to_str(n['conn'].inbound_chan.serialize())
            # Create gateway channels
            n['gw_chs'] = [Channel(n['mempool'], dfacts.gw_cuid_from_index(i, NUM_GW_CHANNELS_PER_NODE) + j) for j in range(NUM_GW_CHANNELS_PER_NODE)]

        LOGGER.info('Memory pools and channels created')

        # Create LAChannelsInfo
        la_ch_info = dmsg.LAChannelsInfo(
            tag=next_tag(),
            nodes_desc=self.nodes,
            gs_cd='',
            num_gw_channels=NUM_GW_CHANNELS_PER_NODE,
        )

        LOGGER.info(f'LAChannelsInfo created: {la_ch_info.get_sdict()}')

        # Start agents
        for i, n in self.nodes.items():
            n['agent'] = start_transport_agent(
                node_index=str(i),
                in_ch_sdesc=B64(n['conn'].outbound_chan.serialize()),
                args=self.ARGS,
                env=self.ENV,
                gateway_channels=n['gw_chs'],
            )

        LOGGER.info('Agents started')

        # Send LAChannelsInfo
        for n in self.nodes.values():
            n['conn'].send(la_ch_info.serialize())

        LOGGER.info('Sent all LAChannelsInfo')

        # Wait for each node to reply with pings
        for i, n in self.nodes.items():
            ping_msg = dmsg.parse(n['conn'].recv())
            assert isinstance(ping_msg, dmsg.TAPingSH), f'Did not receive TAPingSH from node {i}'

        LOGGER.info('Received all TAPingSH')
        LOGGER.debug('Setup completed')

    def tearDown(self):
        LOGGER.info('Tearing down')

        # Halt
        halt_msg = dmsg.SHHaltTA(next_tag()).serialize()
        for n in self.nodes.values():
            n['conn'].send(halt_msg)

        LOGGER.info('Sent all SHHaltTA')

        # Wait for each node to reply
        for i, n in self.nodes.items():
            halted_msg = dmsg.parse(n['conn'].recv())
            assert isinstance(halted_msg, dmsg.TAHalted), f'Did noot receive TAHalted from node {i}'

        LOGGER.info('Received all TAHalted')

        for n in self.nodes.values():
            # Stop agent
            n['agent'].terminate()
            n['agent'].wait(timeout=3)
            n['agent'].kill()
            # Destroy gateway channels
            for ch in n['gw_chs']:
                ch.destroy()
            # Close the connection
            n['conn'].close()
            # Destroy I/O channels since they're externally managed
            n['conn'].outbound_chan.destroy()
            n['conn'].inbound_chan.destroy()
            # Destroy memory pool
            n['mempool'].destroy()

        LOGGER.debug('Teardown completed')

    def test_allow_agent_control_loops_to_poll_once(self):
        # TODO Send a GatewayMessage to each agent and verify it arrived
        # Wait for control loop to poll at least once
        time.sleep(6)


class TCPTestCase(LocalServicesInterface, unittest.TestCase):

    NUM_NODES = 3
    ARGS = shlex.split(f'tcp --log-level {logging.getLevelName(LOGGER.getEffectiveLevel())} --no-dragon-logging')


@unittest.skip
class HSTATestCase(LocalServicesInterface, unittest.TestCase):

    ARGS = shlex.split(f'hsta --log-level {logging.getLevelName(LOGGER.getEffectiveLevel())} --no-dragon-logging')


if __name__ == '__main__':
    unittest.main()
