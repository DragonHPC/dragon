import asyncio
from functools import partial, singledispatchmethod
import logging
from typing import Optional

from ...channels import GatewayMessage, Channel, ChannelEmpty, ChannelRecvTimeout
from ...dtypes import WaitMode, DEFAULT_WAIT_MODE
from ...infrastructure.node_desc import NodeDescriptor

from .errno import get_errno, DRAGON_TIMEOUT
from .io import UUIDBytesIO
from .messages import (
    ErrorResponse,
    EventRequest,
    EventResponse,
    RecvRequest,
    RecvResponse,
    Request,
    Response,
    SendMemoryRequest,
    SendRequest,
    SendResponse,
    SendReturnMode,
)
from .task import TaskMixin, run_forever
from .transport import Address, Transport
from .util import create_msg


LOGGER = logging.getLogger("dragon.transport.tcp.client")


class Client(TaskMixin):
    """Attaches to channel, receives gateway messages, and processes requests
    with the corresponding transport server.
    """

    def __init__(
        self,
        channel_sdesc: bytes,
        transport: Transport,
        nodes: Optional[dict[int, Address]] = None,
        wait_mode: WaitMode = DEFAULT_WAIT_MODE,
    ):
        self.channel_sdesc = channel_sdesc
        self.transport = transport
        if nodes is None:
            nodes = {}
        self.nodes = nodes
        self._channel = None
        self._recv_handle = None
        self._wait_mode = wait_mode
        self._background_tasks = set()

    @run_forever
    async def run(self) -> None:
        await asyncio.to_thread(self.open)
        try:
            while True:
                msg = await self.recv()
                LOGGER.debug(f"Received gateway message: {msg}")
                try:
                    task = self.process(msg)
                except BaseException as e:
                    LOGGER.exception(f"Error processing gateway message: {msg}")

                    # Try to complete the gateway message with the error
                    try:
                        msg.complete_error(get_errno(e))
                    except:
                        LOGGER.exception("Failed to complete gateway message with error")

                    # Ensure gateway message is destroyed
                    try:
                        msg.destroy()
                    except:
                        LOGGER.exception("Failed to destroy gateway message")

                    # Finally, ignore Exceptions, but not BaseExceptions (e.g., KeyboardInterrupt)
                    try:
                        raise e
                    except Exception:
                        continue
                else:
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                    LOGGER.debug(f"Processed gateway message: {msg}")

        finally:
            await asyncio.to_thread(self.close)

    def open(self) -> None:
        self._channel = Channel.attach(self.channel_sdesc)
        try:
            self._recv_handle = self._channel.recvh(wait_mode=self._wait_mode)
            self._recv_handle.open()
        except:
            self._channel.detach()
            raise

    def close(self) -> None:
        self._recv_handle.close()
        self._channel.detach()

    async def recv(self, interval: float = 0.1) -> GatewayMessage:
        # TODO Consider revisiting this recv loop
        try:
            msg = self._recv_handle.recv(blocking=False)
        except (ChannelEmpty, ChannelRecvTimeout):
            while True:
                try:
                    msg = await asyncio.to_thread(self._recv_handle.recv, timeout=interval)
                    break
                except (ChannelRecvTimeout, ChannelEmpty):
                    continue
                except asyncio.CancelledError:
                    await asyncio.sleep(interval)
                    raise
        try:
            return GatewayMessage.from_message(msg)
        finally:
            msg.destroy()

    def update_nodes(self, node_update_map: dict[int, Address]):
        """Update the node dictionary for routing gateway requests

        :param nodes: Nodes to add to our internal data
        :type nodes: list[NodeDescriptor]
        """
        try:
            self.nodes.update(node_update_map)
        except Exception:
            LOGGER.critical(f"Failed to update client node-address mapping")
            raise

    def process(self, msg: GatewayMessage) -> asyncio.Task:
        # Look up destination node address
        try:
            to_addr = self.nodes[msg.target_hostid]
        except KeyError:
            raise ValueError(f"Unknown target host ID: {msg.target_hostid}")
        # Create Request from gateway message
        req = create_request(msg)
        # Send request
        fut = self.transport.write_request(req, to_addr)
        # Handle responses asynchronously. Ordering of responses is
        # intentionally not guaranteed since the remote server is expected to
        # process requests in the order they are received.
        return asyncio.create_task(self.wait_for_response(fut, msg, req))

    async def wait_for_response(self, fut: asyncio.Future, msg: GatewayMessage, req: Request) -> None:
        try:
            # SendResponses to SendRequests with return modes IMMEDIATELY or
            # WHEN_BUFFERED may be received before the request has been sent
            # (if remote) or processed (if local). Waiting for the request's
            # I/O event is required for WHEN_BUFFERED return mode to guarantee
            # local requests are properly processed.
            if isinstance(req, SendRequest) and req.return_mode == SendReturnMode.WHEN_BUFFERED:
                await req._io_event.wait()
            # Wait for response
            try:
                resp, addr = await fut
            except BaseException as e:
                LOGGER.exception(f"Error awaiting response to gateway message: {msg}")
                # Create local ErrorResponse response; seqno=None implies it
                # was never actually sent/received.
                resp = ErrorResponse(None, get_errno(e), f"Error awaiting response to gateway message: {msg}")
                # Inidicate local transport address (i.e., myself)
                addr = self.transport.addr
            # Handle response and complete the gateway message
            try:
                await self.handle_response(resp, addr, msg)
            except BaseException:
                LOGGER.exception(f"Error handling response to gateway message: {msg}")
            finally:
                # Set the I/O event on the response. Critical for properly
                # handling RecvResponse messages when the transport does NOT use
                # write_message() and read_message() to handle message I/O,
                # e.g., Transport.
                resp._io_event.set()
        finally:
            # Destroy the gateway message
            msg.destroy()

    @singledispatchmethod
    async def handle_response(self, resp: Response, addr: Address, msg: GatewayMessage) -> None:
        raise NotImplementedError(f"Unsupported response type: {type(resp)}")

    @handle_response.register
    async def _(self, resp: ErrorResponse, addr: Address, msg: GatewayMessage) -> None:
        """Handle ErrorResponse messages."""
        # Log errors except time outs on events
        if not (msg.is_event_kind and resp.errno == DRAGON_TIMEOUT):
            LOGGER.error(f"Request {resp.seqno} errored: {resp.errno}: {resp.text}")
        # Need to complete the message in order to pass back the response's
        # error code.
        msg.complete_error(resp.errno)

    @handle_response.register
    async def _(self, resp: SendResponse, addr: Address, msg: GatewayMessage) -> None:
        """Handle SendResponse messages."""
        assert msg.is_send_kind
        msg.send_complete()

    @handle_response.register
    async def _(self, resp: RecvResponse, addr: Address, msg: GatewayMessage) -> None:
        """Handle RecvResponse messages."""
        assert msg.is_get_kind
        try:
            msg_recv = await asyncio.to_thread(
                create_msg,
                resp.payload,
                resp.clientid,
                resp.hints,
                self._channel,
                msg.deadline,
                msg.get_dest_mem_descr_ser,
            )
        except BaseException as e:
            try:
                msg.complete_error(get_errno(e))
            except:
                LOGGER.exception("Failed to complete gateway message with error")
            raise e
        msg.get_complete(msg_recv)

    @handle_response.register
    async def _(self, resp: EventResponse, addr: Address, msg: GatewayMessage) -> None:
        """Handle EventResponse messages."""
        assert msg.is_event_kind
        msg.event_complete(resp.result, resp.errno)


def create_request(msg: GatewayMessage) -> Request:
    if msg.is_send_kind:
        if msg.is_send_return_immediately:
            send_return_mode = SendReturnMode.IMMEDIATELY
        elif msg.is_send_return_when_buffered:
            send_return_mode = SendReturnMode.WHEN_BUFFERED
        elif msg.is_send_return_when_deposited:
            send_return_mode = SendReturnMode.WHEN_DEPOSITED
        elif msg.is_send_return_when_received:
            send_return_mode = SendReturnMode.WHEN_RECEIVED
        else:
            raise ValueError("Unsupported send return mode")
        sendhid = UUIDBytesIO.decode(msg.send_payload_message_attr_sendhid)
        clientid = msg.send_payload_message_attr_clientid
        hints = msg.send_payload_message_attr_hints
        payload = msg.send_payload_message
        mem_sd = msg.send_dest_mem_descr_ser
        if mem_sd is None:
            cls = partial(
                SendRequest,
                return_mode=send_return_mode,
                sendhid=sendhid,
                clientid=clientid,
                hints=hints,
                payload=payload,
            )
        else:
            cls = partial(
                SendMemoryRequest,
                return_mode=send_return_mode,
                sendhid=sendhid,
                clientid=clientid,
                hints=hints,
                payload=payload,
                mem_sd=mem_sd,
            )
    elif msg.is_get_kind:
        cls = RecvRequest
    elif msg.is_event_kind:
        cls = partial(EventRequest, mask=msg.event_mask)
    else:
        raise ValueError("Unknown kind of gateway message")
    req = cls(seqno=None, timeout=0.0, channel_sd=msg.target_ch_ser)
    req.deadline = msg.deadline
    return req
