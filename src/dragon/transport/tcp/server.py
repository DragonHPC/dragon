import asyncio
from collections import defaultdict
from functools import singledispatchmethod
import logging
import sys
import traceback

from .errno import get_errno, DRAGON_TIMEOUT
from .messages import (
    ErrorResponse,
    EventRequest,
    EventResponse,
    RecvRequest,
    RecvResponse,
    SendRequest,
    SendResponse,
    SendReturnMode,
)
from .task import TaskMixin, run_forever
from .transport import Address, Transport
from .util import poll_channel, recv_msg, send_msg
from ...channels import Channel, ChannelSendH
from ...dtypes import DEFAULT_WAIT_MODE, WaitMode

LOGGER = logging.getLogger("dragon.transport.tcp.server")

# Useful if running python tests on code and wanting to print to screen.
# class LOGGER:
#     info = print
#     debug = print
#     exception = print
#     error = print
#     critical = print
#     warning = print

class Server(TaskMixin):
    """Process `Request` messages from clients and reply with `Response`
    messages as appropriate.
    """

    IDLE_PROCESS_TIMEOUT = 60.0

    def __init__(self, transport: Transport, wait_mode: WaitMode = DEFAULT_WAIT_MODE):
        self.transport = transport
        self._process_tasks = {}
        self._requests = defaultdict(asyncio.Queue)
        self._background_tasks = set()
        self._wait_mode = wait_mode
        self._send_handles = {}
        self._cuid_to_send_handles = {}

    @run_forever
    async def run(self):
        while True:
            req, addr = await self.transport.read_request()
            if isinstance(req, SendRequest):
                key = (req.channel_sd, req.sendhid)
            elif isinstance(req, RecvRequest):
                key = (req.channel_sd, RecvRequest)
            else:
                key = (req.channel_sd, EventRequest)

            self._requests[key].put_nowait((req, addr))
            self._ensure_process_task(key)

    def _ensure_process_task(self, key):
        # Check if send task is still running
        try:
            task = self._process_tasks[key]
        except KeyError:
            task = None
        else:
            if task.done():
                # Remove reference to task
                del self._process_tasks[key]
                task = None
        if task is None:
            # (Re-)Start send task
            self._process_tasks[key] = asyncio.create_task(self.process(key))

    @run_forever
    async def process(self, key):
        while True:
            try:
                req, addr = await asyncio.wait_for(self._requests[key].get(), self.IDLE_PROCESS_TIMEOUT)
            except asyncio.TimeoutError:
                if self._requests[key].empty():
                    # Idle timeout and still no requests to process, shutdown
                    del self._requests[key]
                    # Comment left here because these are helpful in debugging deadlock situations.
                    # LOGGER.debug(f'Removing queue for key={key}')
                    # LOGGER.debug(f'Exiting Task {self._process_tasks[key]}')
                    # LOGGER.debug(f'Here are the remaining Queue sizes')
                    # LOGGER.debug(f'{[self._requests[key].qsize() for key in self._requests]}')
                    # LOGGER.debug(f'Here are the remaining Tasks')
                    del self._process_tasks[key]
                    # LOGGER.debug(f'{[self._process_tasks[key] for key in self._process_tasks]}')
                    break
                continue

            # Dispatch request to corresponding handler
            try:
                await self.handle_request(req, addr)
            except BaseException as e:
                # Send ErrorResponse
                ex_type, ex_value, ex_tb = sys.exc_info()
                tb_str = "".join(traceback.format_exception(ex_type, ex_value, ex_tb))
                resp = ErrorResponse(req.seqno, get_errno(e), f"Error while handling request: {tb_str}")
                self.transport.write_response(resp, addr)
                LOGGER.error(f"Error while handling request: {tb_str}")

                # Ignore Exceptions but not BaseExceptions
                try:
                    raise
                except Exception:
                    pass
            finally:
                # Set the I/O event on the request. Critical for properly
                # handling SendRequest messages when the transport does NOT use
                # write_message() and read_message() to handle message I/O,
                # e.g., Transport. See Client.wait_for_response().
                req._io_event.set()

    @singledispatchmethod
    async def handle_request(self, req, addr: Address) -> None:
        raise NotImplementedError(f"Unsupported message type: {type(req)}")

    @handle_request.register
    async def _(self, req: SendRequest, addr: Address) -> None:

        def get_send_handle(ch: Channel, sendhid: int, wait_mode: WaitMode) -> ChannelSendH:
            # This internal function looks to see if a send
            # handle for the channel, sendhid pair already exists.
            # If so, it returns it. If not, it creates one and
            # remembers it for cleanup later.
            key = (ch.cuid, sendhid)
            if key in self._send_handles:
                h = self._send_handles[key]
            else:
                h = ch.sendh(wait_mode=wait_mode)
                self._send_handles[key] = h
                if ch.cuid in self._cuid_to_send_handles:
                    self._cuid_to_send_handles[ch.cuid].add((h, key))
                else:
                    self._cuid_to_send_handles[ch.cuid] = set([(h, key)])
                    ch.notify_on_destroy()
                h.open()
            return h

        try:
            # The following code is needed only for unit test cases,
            # but does not impact performance. The unit tests fake
            # sends so we get a Payload object for payload while
            # in real life we get a bytes object as payload.
            # The difference is how it gets written across the wire.
            if isinstance(req.payload, bytes):
                payload = req.payload
            else:
                payload = req.payload.data

            await asyncio.to_thread(
                send_msg,
                req.channel_sd,
                req.clientid,
                req.hints,
                payload,
                req.deadline,
                req.sendhid,
                get_send_handle,
                getattr(req, "mem_sd", None),
                copy_on_send=not req._io_event.is_set(),
                wait_mode=self._wait_mode,
            )
        except Exception as e:
            LOGGER.error(f"Exception during send_msg: {e}")

        if req.return_mode in (SendReturnMode.WHEN_DEPOSITED, SendReturnMode.WHEN_RECEIVED):
            resp = SendResponse(req.seqno)
            self.transport.write_response(resp, addr)

    @handle_request.register
    async def _(self, req: RecvRequest, addr: Address) -> None:
        clientid, hints, msg_bytes = await asyncio.to_thread(
            recv_msg, req.channel_sd, req.deadline, wait_mode=self._wait_mode
        )
        task = None  # Ensure task is defined for use in exception handler
        try:
            # Create the response. Note the use of a bytearray for the
            # response payload.
            resp = RecvResponse(req.seqno, clientid, hints, msg_bytes)
            # Write the response
            self.transport.write_response(resp, addr)
        except:
            raise

    @handle_request.register
    async def _(self, req: EventRequest, addr: Address) -> None:
        def close_send_handles(cuid: int) -> None:
            # This function cleans up open send handles when a
            # cleanup event is forwarded to the server. The channels
            # library notifies the transport via a gateway message
            # when requested to do so by the notify_on_destroy method
            # (see above). This code makes the assumption that the
            # transport can handle an event sent to it (via a gateway
            # message) that is directed to a channel on its own node.
            # In other words, the map from host ids to nodes is expected
            # to include its own node.
            if cuid in self._cuid_to_send_handles:
                for h, key in self._cuid_to_send_handles[cuid]:
                    try:
                        h.close()
                    except:
                        pass

                    del self._send_handles[key]

                del self._cuid_to_send_handles[cuid]

        send_resp, ok, result = await asyncio.to_thread(
            poll_channel, req.channel_sd, req.mask, req.deadline, close_send_handles
        )
        if send_resp:
            if ok:
                # Although the result should always be an int when ok is True,
                # explicitly return 0 when result is None just to be safe.
                resp = EventResponse(req.seqno, result.rc, result.value)
            else:
                resp = ErrorResponse(req.seqno, DRAGON_TIMEOUT, "Poll timed out")
            self.transport.write_response(resp, addr)
