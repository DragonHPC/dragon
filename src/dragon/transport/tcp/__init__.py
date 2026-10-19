"""Dragon TCP transport agent.

.. note::

    This library uses `asyncio` to provide concurrent I/O, which means an event
    loop is required. Calls to functions which lead to blocking I/O should be
    run via a thread-pool executor (e.g., via `asyncio.to_thread`) to avoid
    blocking the event loop.  By default, this package uses the default executor
    which may be customized by `asyncio.loop.set_default_executor`.

The Dragon TCP transport agent is comprised of four primary components:

*   `StreamTransport` -- Used by `Server` and `Client` instances to reliably
    communicate with peers. Responsible for establishing and managing
    connections as well as outstanding requests.

*   `Server` -- Handles `Request` messages from clients and replies with
    `Response` messages as appropriate.

*   `Client` -- Processes `GatewayMessages` to fulfill *send*, *receive*, and
    *poll* operations on remote channels.

*   *(TODO)* `control.Manager` -- Manages the agents main thread of execution
    and handles control commands from local services.

.. rubric:: Local Transport Agent

For testing purposes it may be convenient to implement a transport agent
that only sends messages to itself. The key difference is to use `Transport`
instead of `StreamTransport`::

    import asyncio

    from dragon.transport.tcp import cancel_all_tasks, Client, Server, Transport, LOOPBACK_ADDRESS_IPv4

    async def agent(channels):
        # Create a Transport instance to handle Request/Response I/O.
        # By default, Transport._handle_send() implements a loopback
        # strategy by calling Tranport._handle_recv().
        transport = Transport(LOOPBACK_ADDRESS_IPv4)

        # Create a Server instance to process Requests (i.e., remote
        # channel operations).
        server = Server(transport)

        # Using the default transport, the node mapping should
        # ensure every address is the transport's address, for
        # consistency.
        nodes = defaultdict(lambda: transport.addr)

        # Create Client instances for each gateway channel to send
        # Requests to the Server and process Responses in order to
        # complete gateway messages. This example assumes the gateway channels
        # have been created and the corresponding serialized channel
        # descriptors are passed in via the `channels` list.
        clients = [Client(ch_sd, transport, nodes) for ch_sd in channels]

        # NOTE: Transport is not a TaskMixin as there is nothing that requires a
        # separate thread of execution, so there is no need to call
        # `transport.start()` or `await transport.wait_started()` as is the case
        # with StreamTransport.

        # Start the server
        server.start(name=f'Transport-server')

        # Start each client
        for idx, c in enumerate(clients):
            c.start(name=f'Transport-client-{idx}')

        # At this point, the server and client tasks are running and you
        # can do whatever, but if you need a control loop...
        try:
            while True:
                await asyncio.sleep(5.0)
        finally:
            # Shut down clients and server
            await asyncio.gather(*[c.stop() for c in clients], return_exceptions=True)
            try:
                await server.stop()
            except:
                pass
            # Cancel all remaining tasks for good measure
            await cancel_all_tasks()

    if __name__ == '__main__':
        SERIALIZED_CHANNEL_DESCRIPTORS = [...]
        asyncio.run(agent(SERIALIZED_CHANNEL_DESCRIPTORS))
"""

from .transport import Address, StreamTransport, Transport, LOOPBACK_ADDRESS_IPv4, LOOPBACK_ADDRESS_IPv6
from .client import Client
from .server import Server
from .task import cancel_all_tasks
