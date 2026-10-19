"""Dragon Queue + Distributed Dictionary implementation of CommunicationProtocol."""

from __future__ import annotations

import asyncio
import queue as _queue
from typing import Optional

from ....native.queue import Queue

from .protocol import CommunicationProtocol
from .message import Message


class DragonQueueProtocol(CommunicationProtocol):
    """Communication protocol backed by Dragon Queue and Dragon Distributed Dictionary.

    Each agent gets one instance of this class wrapping its own input queue.
    The agent calls ``receive()`` to pull messages from its queue and
    ``publish`` / ``read`` / ``delete`` to interact with a shared DDict that
    is passed in per-call.

    Parameters
    ----------
    serialized_input_queue:
        A serialized Dragon queue handle representing this agent's input
        queue.  Obtained by serializing a ``dragon.native.queue.Queue`` instance.
    """

    def __init__(self, queue: Queue=None) -> None:
        if queue is not None:
            self.queue = queue
        else:
            self.queue = Queue()
        self.serialized_queue = self.queue.serialize()
    # -- queue operations ----------------------------------------------------

    def send(self, message: Message) -> None:
        """Send *message* to the recipient's queue.

        Deserializes the target queue from
        ``message.recipient_serialized_queue`` and puts the serialized message
        onto it.

        .. note::
            Not called in the current dispatcher-based flow.  Kept for future
            direct agent-to-agent or A2A communication scenarios.
        """
        self.queue.put(message)

    async def receive(self, timeout: Optional[float] = None) -> Optional[Message]:
        """Await a message on this agent's input queue without blocking the event loop.

        ``dragon.native.queue.Queue.get()`` is a synchronous blocking call.
        Calling it directly on the event loop thread would freeze all other
        concurrent asyncio Tasks for the full duration of the timeout (up to
        1 second per poll cycle when the queue is idle).

        ``asyncio.to_thread`` submits the blocking call to Python's default
        ``ThreadPoolExecutor``.  The pool reuses a persistent idle thread for
        repeated calls — no thread is created or destroyed per invocation, so
        overhead is negligible even when the queue is empty for extended periods
        (e.g., 30+ seconds between messages).  The event loop remains free to
        run other Tasks (LLM calls, tool calls, etc.) while the pool thread
        waits on the Dragon queue.

        Returns
        -------
        Message or None
            Deserialized :class:`Message`, or ``None`` on timeout.
        """
        try:
            data = await asyncio.to_thread(self.queue.get, timeout=timeout)
            if isinstance(data, Message):
                return data
            return Message(**data)
        except _queue.Empty:
            return None

    def destroy(self):
        self.queue.destroy()
