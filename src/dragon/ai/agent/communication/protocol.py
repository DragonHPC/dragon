"""Abstract base for communication protocols."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from .message import Message



class CommunicationProtocol(ABC):
    """Abstract communication protocol used by Dragon agents.

        * ``receive`` reads from the agent's own input queue.
        * ``send`` sends a message to another agent's queue. It is reserved for
            future use and is not called by dispatchers in the current implementation.
    """

    # -- queue operations ----------------------------------------------------

    @abstractmethod
    def send(self, message: Message) -> None:
        """Send *message* to the recipient's queue.

        The concrete implementation should deserialize the target queue from
        ``message.recipient_serialized_queue`` and put the serialized message
        onto it.

        .. note::
            Not used by the current dispatcher-based flow but kept for future
            development (e.g. direct agent-to-agent or A2A communication).
        """

    @abstractmethod
    async def receive(self, timeout: Optional[float] = None) -> Optional[Message]:
        """Await a message on the agent's own input queue.

        Parameters
        ----------
        timeout:
            Maximum seconds to wait.  ``None`` means block indefinitely.

        Returns
        -------
        Message or None
            The deserialized message, or ``None`` if *timeout* was reached.
        """
