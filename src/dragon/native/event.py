"""The Dragon native event is an object that is used for signaling between threads or processes.

One thread or process signals an event and the other waits. When the waiting thread is signalled,
the waiting thread is activated.
"""

import logging

from ..channels import Channel, ChannelEmpty, ChannelFull, ChannelRecvH, ChannelSendH
from ..dtypes import WHEN_DEPOSITED
from ..globalservices.channel import release_refcnt, create, get_refcnt

from ..infrastructure.channel_desc import ChannelOptions
from ..infrastructure.parameters import this_process
from ..infrastructure.facts import default_pool_muid_from_index

LOGGER = logging.getLogger(__name__)

_DEF_MUID = default_pool_muid_from_index(this_process.index)


class Event:
    """This class implements Event objects with a Dragon channel.
    The event sends and receives bytes of the message with the respective functions set and clear.
    Wait is the function that waits for bytes to be written into the channel
    """

    _BYTEORDER = "big"

    def _cleanup(self) -> None:
        try:
            if not self._closed:
                self._reader.close()
                self._writer.close()
                self._channel.detach()
                release_refcnt(self._cuid)
        except Exception:
            pass  # all of those may fail if the channel has disappeared during shutdown
        self._closed = True

    def __del__(self):
        try:
            self._cleanup()
            del self._reader
            del self._writer
            del self._channel
        except:
            pass

    def __init__(self, m_uid: int = _DEF_MUID):
        """Initialize an event object
        :param m_uid: memory pool to create the channel in, defaults to _DEF_MUID
        :type m_uid: int, optional
        """

        LOGGER.debug(f"Init Event with  m_uid={m_uid}")

        self._closed = True

        the_options = ChannelOptions(ref_count=True)
        the_options.local_opts.capacity = 1

        descr = create(m_uid=m_uid, options=the_options)
        self._channel = Channel.attach(descr.sdesc)
        get_refcnt(descr.c_uid)

        self._cuid = descr.c_uid
        self._reset()
        self._closed = False

    def _reset(self):

        self._lmsg = this_process.my_puid.to_bytes(8, byteorder=self._BYTEORDER)
        self._reader = ChannelRecvH(self._channel)
        self._writer = ChannelSendH(self._channel, return_mode=WHEN_DEPOSITED)
        self._reader.open()
        self._writer.open()

    def __getstate__(self) -> tuple:

        if self._closed:
            raise ValueError(f"Event {self!r} is closed")

        return self._channel.serialize(), self._cuid

    def __setstate__(self, state) -> None:

        self._closed = True

        sdesc, self._cuid = state

        get_refcnt(self._cuid)
        self._channel = Channel.attach(sdesc)

        self._reset()

        self._closed = False

    def __enter__(self):
        self.set()

    def __exit__(self, *args):
        self.clear()

    def set(self) -> None:
        """Set the event.

        Set writes a messsage into the channel.

        :return: None
        :rtype: NoneType
        """

        LOGGER.debug(f"Set Event {self!r}")
        try:
            self._writer.send_bytes(self._lmsg, timeout=0)

        except ChannelFull:
            pass

    def is_set(self) -> bool:
        """Returns if the event has been set. Polls the channel for the message.

        :return: Results from a channel poll with a timeout set to 0.
        :rtype: bool
        """

        LOGGER.debug(f"Is Set")
        return self._channel.poll(timeout=0)

    def wait(self, timeout: float = None) -> bool:
        """Wait for the timeout to elapse and poll for the message in the channel.

        :param timeout: how long to block in seconds, defaults to None
        :type timeout: float, optional
        :return: if the event has been set
        :rtype: bool
        """

        LOGGER.debug(f"Clear Event{self!r} and timeout{timeout!r}")
        if timeout is not None and timeout < 0:
            timeout = 0
        return self._channel.poll(timeout=timeout)

    def clear(self) -> None:
        """Clears the event once the message from the channel has been received.

        Clear receives a messsage from the channel

        :return: None
        :rtype: NoneType
        """
        LOGGER.debug(f"Clear Event{self!r}")

        try:
            _ = self._reader.recv_bytes(timeout=0)

        except ChannelEmpty:
            pass

    def destroy(self):
        """Destroys the underlying Dragon resources so the space can be re-used.

        Destroys underlying Dragon resource. Only do this when you know all
        references to the object have been deleted.

        :return: None
        :rtype: NoneType
        """
        try:
            self._channel.destroy()
        except:
            pass
