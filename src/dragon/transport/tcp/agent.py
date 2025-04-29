import asyncio
from collections import defaultdict
from collections.abc import Iterable
import logging
from typing import Optional

from .client import Client
from .server import Server
from .task import TaskMixin
from .transport import Address, LOOPBACK_ADDRESS_IPv4, Transport

from ...dtypes import DEFAULT_WAIT_MODE, WaitMode
from ...infrastructure.node_desc import NodeDescriptor


LOGGER = logging.getLogger("dragon.transport.tcp.agent")


class Agent:
    """TCP Transport Agent.

    Requires `Transport` instance and a mapping of node indexes to addresses.
    The `Server` will be automatically created unless specified. Use
    `Agent.new_client` to create a `Client`, though an initial set may also be
    given. For example::

        nodes = {
            1: Address.from_netloc('127.0.0.1:8888'),
            2: Address.from_netloc('127.0.0.1:7777'),
        }

        transport = StreamTransport(nodes[1])

        try:
            with Agent(transport, nodes) as agent:
                # Customize control loop
                while True:
                    await asyncio.sleep(5.0)
        finally:
            await cancel_all_tasks()

    """

    _running = False

    def __init__(
        self,
        transport: Transport,
        nodes: dict[int, Address],
        server: Optional[Server] = None,
        clients: Optional[Iterable[Client]] = None,
        wait_mode: WaitMode = DEFAULT_WAIT_MODE,
    ):
        if server is None:
            server = Server(transport, wait_mode=wait_mode)
        if clients is None:
            clients = set()
        else:
            clients = set(clients)
        self._transport = transport
        self._server = server
        self._clients = clients
        self._wait_mode = wait_mode
        self.nodes = nodes

    @classmethod
    def loopback(cls, addr: Address = LOOPBACK_ADDRESS_IPv4):
        """Create an `Agent` instance initialized with a loopback transport."""
        transport = Transport(addr)
        nodes = defaultdict(lambda: transport.addr)
        return cls(transport, nodes)

    async def __aenter__(self):
        if not self.is_running():
            await self.start()
        return self

    async def __aexit__(self, *exc):
        if self.is_running():
            await self.stop()
        return False

    def is_running(self):
        return getattr(self, "_running", False)

    async def start(self):
        LOGGER.debug("Agent starting")
        if isinstance(self._transport, TaskMixin):
            await self._start_transport()
        self._start_server()
        self._start_clients()
        self._running = True
        LOGGER.debug("Agent started")

    async def stop(self):
        LOGGER.debug("Agent stopping")
        del self._running
        await self._stop_clients()
        if self._server.is_running():
            await self._stop_server()
        if isinstance(self._transport, TaskMixin):
            if self._transport.is_running():
                await self._stop_transport()
        LOGGER.debug("Agent stopped")

    async def _start_transport(self):
        assert isinstance(self._transport, TaskMixin), "Transport is not a TaskMixin"
        self._transport.start(name=f"Transport-{self._transport.addr}")
        await self._transport.wait_serving()
        LOGGER.debug("Transport started")

    async def _stop_transport(self):
        assert isinstance(self._transport, TaskMixin), "Transport is not a TaskMixin"
        assert self._transport.is_running(), "Transport not running"
        await self._transport.stop()
        LOGGER.debug("Transport stopped")

    def _start_server(self):
        self._server.start(name="Server")
        LOGGER.debug("Server started")

    async def _stop_server(self):
        assert self._server.is_running(), "Server not running"
        await self._server.stop()
        LOGGER.debug("Server stopped")

    def _start_clients(self):
        for c in self._clients:
            if not c.is_running():
                self._start_client(c)
        LOGGER.debug("Clients started")

    async def _stop_clients(self):
        await asyncio.gather(*[self._stop_client(c) for c in self._clients if c.is_running()], return_exceptions=True)
        LOGGER.debug("Clients stopped")

    @staticmethod
    def _start_client(client):
        name = f"Client-{id(client)}"
        client.start(name)
        LOGGER.debug(f"{name} started")

    @staticmethod
    async def _stop_client(client):
        name = f"Client-{id(client)}"
        assert client.is_running(), f"{name} not running"
        try:
            await client.stop()
        except:
            LOGGER.exception(f"{name} errored while stopping")
            raise
        LOGGER.debug(f"{name} stopped")

    def new_client(self, channel_sdesc: bytes):
        """Create a `Client` and start it if the agent is already running.

        :param channel_sdesc: Serialized channel descriptor
        :return: Created `Client`
        """
        client = Client(channel_sdesc, self._transport, self.nodes, wait_mode=self._wait_mode)
        self.add_client(client)
        return client

    def add_client(self, client: Client):
        """Add `Client` and start it if the agent is already running.

        :param Client: Client instance to add
        """
        self._clients.add(client)
        if self.is_running():
            self._start_client(client)

    def _update_client_nodes(self, node_update_map: dict[int, Address]):
        """Update nodes dictionary for routing messages in the client

        :param nodes: Nodes to add to our internal data
        :type nodes: list[NodeDescriptor]
        """
        for client in self._clients:
            client.update_nodes(node_update_map)

    def update_nodes(self, nodes: list[NodeDescriptor]):
        """Update the nodes dictionary used to route messages

        :param nodes: Nodes to add to our internal data
        :type nodes: list[NodeDescriptor]
        """
        # Update local object
        try:
            node_update_map = {}

            for node in nodes:
                try:
                    addr = Address.from_netloc(str(node.ip_addrs[0]))
                    node_update_map[int(node.host_id)] = Address(addr.host, addr.port or node.port)
                except Exception:
                    LOGGER.critical(f"Failed to update agent node-address mapping for {node}")
                    raise

            self.nodes.update(node_update_map)
        except Exception:
            LOGGER.critical(f"Failed to update agent node-address mapping")
            raise

        # Update clients
        self._update_client_nodes(node_update_map)


if __name__ == "__main__":
    from .task import cancel_all_tasks

    async def main():
        try:
            async with Agent.loopback() as agent:
                # run control loop
                while agent.is_running():
                    await asyncio.sleep(5.0)
        finally:
            await cancel_all_tasks()

    asyncio.run(main())
