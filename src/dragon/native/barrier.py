"""The Dragon native barrier is an object for parties number of threads. The
class can be initialized with a number of threads, an action, and timeout. The
functions are wait, reset, and abort.
"""

import sys
import logging
import os
import time

import dragon
from ..channels import Channel, ChannelBarrierReady, ChannelBarrierBroken, EventType
from ..infrastructure.facts import default_pool_muid_from_index
from ..infrastructure.parameters import this_process
from ..infrastructure.channel_desc import ChannelOptions
from ..localservices.options import ChannelOptions as ShepherdChannelOptions
from ..globalservices.channel import create, get_refcnt, release_refcnt

LOGGER = logging.getLogger(__name__)
_DEF_MUID = default_pool_muid_from_index(this_process.index)


class BrokenBarrierError(ChannelBarrierBroken):
    pass


class Barrier:
    """This class implements Barrier objects with a Dragon channel.
    The Barrier handles parties of threads that are 2 and more, number of
    waiting threads, and whether or not the barrier is broken.
    """

    def __init__(self, parties: int = 2, action: callable = None, timeout: float = None, m_uid: int = _DEF_MUID):
        """Initialize a barrier object
        :param parties: number of parties for the barrier to act upon
        :type parties: int, optional, > 1
        :param action: class that the barrier calls upon
        :type callable: optional
        :param timeout: timeout for barrier
        :type timeout: float, optional
        :param m_uid: memory pool to create the channel in, defaults to _DEF_MUID
        :type m_uid: int, optional
        """

        LOGGER.debug(f"Init Dragon Native Barrier with m_uid={m_uid}")

        if timeout is not None and timeout < 0:
            raise ValueError("The timeout in init is less than 0 for the Dragon Native Barrier implementation.")

        if timeout is None:
            timeout = 1000000000  # so we can do math with it.

        self._timeout = timeout
        self._action = action
        self._channel = None

        if not isinstance(parties, int):
            raise ValueError("The parties value must be an integer.")

        if parties < 1:
            raise ValueError("The number of parties must be at least two.")

        if callable(action) is False and action is not None:
            raise ValueError("The action argument must be callable or None.")

        sh_channel_options = ShepherdChannelOptions(capacity=parties)
        gs_channel_options = ChannelOptions(ref_count=True, local_opts=sh_channel_options)
        descriptor = create(m_uid, options=gs_channel_options)
        self._channel = Channel.attach(descriptor.sdesc)
        self._parties = parties

    def __del__(self):
        try:
            cuid = self._channel.cuid
            try:
                self._channel.detach()
            except:
                pass
            try:
                release_refcnt(cuid)
            except:
                pass
        except AttributeError:
            pass

    def __getstate__(self):
        return (self._channel.serialize(), self._action, self._timeout, self._parties)

    def __setstate__(self, state):
        (serialized_bytes, self._action, self._timeout, self._parties) = state
        self._channel = Channel.attach(serialized_bytes)
        get_refcnt(self._channel.cuid)

    def _wait(self, timeout: float = None) -> int:

        # Use the barrier default timeout if None is provided.
        if timeout is None:
            timeout = self._timeout
        elif timeout < 0:
            raise ValueError("The timeout in wait is less than 0 for the Dragon Native Barrier implementation.")

        try:
            recvh = self._channel.recvh()
            recvh.open()

            # What follows is our barrier implementation over channel.poll(). Every
            # waiter registers itself by calling poll with POLLBARRIER on the
            # channel.  The last waiter will raise a ChannelBarrierReady
            # exception to execute the action callable. The other waiters will
            # return and wait in `recvh.recv(timeout=rest_timeout)`.

            try:
                # Register the waiter with the timeout.
                # This poll may not return immediately. That is, if wait() is called twice,
                # we are in the second wait() already and other processes are still picking
                # up their messages from the first wait() (WithProcessesTestBarrier.test_thousand)
                beg = time.monotonic()
                if not self._channel.poll(event_mask=EventType.POLLBARRIER, timeout=timeout):
                    raise ChannelBarrierBroken(f"Channel poll on POLLBARRIER_WAIT return -1.")

            # When the last party polls, the channel barrier ready exception is generated.
            except ChannelBarrierReady:
                try:
                    # The action is called once by the last process to poll. The
                    # action is called before any of them released.
                    if self._action is not None:
                        self._action()
                except Exception as ex:
                    raise RuntimeError(f"There was an error calling the action: {ex}") from ex

                try:  # release the waiters by writing the messages
                    self._channel.poll(event_mask=EventType.POLLBARRIER_RELEASE, timeout=0)
                except ChannelBarrierBroken as ex:
                    raise BrokenBarrierError(f"Couldn't release barrier, it is broken.")

            # the timeout needs to be adjust by how long we waited registering in POLLBARRIER further up
            end = time.monotonic()
            rest_timeout = max(0, timeout - (end - beg))

            # all but the last wait to pickup a message from receive handle
            msg = recvh.recv(timeout=rest_timeout)
            msg_val = int.from_bytes(msg.bytes_memview(), byteorder=sys.byteorder, signed=True)
            msg.destroy()

            # -1 can come back if the Barrier has been broken. If this happens,
            # another process raised the BrokenBarrierException and we do not need to call abort().
            if msg_val < 0 or msg_val >= self.parties:
                raise BrokenBarrierError(f"Got a value of {msg_val} from waiting on the Barrier.")

            return msg_val

        # We have gotten a timeout either during POLLBARRIER or during the recv. This breaks the barrier
        # and raises a BrokenBarrierError,
        except TimeoutError as ex:
            try:
                self.abort()
            except:
                pass
            raise BrokenBarrierError(f"The Dragon Barrier timed out with {ex}")

        # we might attempt to wait on a broken barrier. No need to call abort(),
        # Barrier is already broken.
        except (BrokenBarrierError, ChannelBarrierBroken) as ex:
            raise BrokenBarrierError(f"This Dragon Barrier is broken.")

        # Here we have to catch any exception, because for instance, once the
        # Barrier times out, it might get destroyed which would destroy
        # the underlying managed memory for the channel and then the channel
        # attach is going to fail. So catch any error here because severe
        # errors indicate the Barrier is not functioning. Try to abort, but
        # ignore that if it fails. Provide the exception back to user so they
        # see the text anyway.
        except Exception as ex:
            try:
                self.abort()
            except:
                pass
            raise BrokenBarrierError(f"There was an unexpected error of {repr(ex)} which broke the Barrier.")

        finally:
            recvh.close()

    def wait(self, timeout: float = None) -> int:
        """When all the processes party to the barrier have called wait, they are
        all released simultaneously.  The timeout for wait takes precedence over
        constructor.
        :param timeout: timeout for barrier
        :type timeout: float, optional
        :rtype: None
        """
        LOGGER.debug("Barrier wait with timeout %s", timeout)
        return self._wait(timeout=timeout)

    def reset(self) -> None:
        """Barrier is set to empty state. Any threads waiting receive
        BrokenBarrierException.
        :rtype: None
        """
        LOGGER.debug(f"Barrier {self!r} Reset")

        try:
            self._channel.poll(event_mask=EventType.POLLRESET)
        except Exception as ex:
            raise BrokenBarrierError(f"The reset failed with the error of {repr(ex)}.")

    def abort(self) -> None:
        """The barrier is broken. Any threads that are in wait state will
        receive BrokenBarrierError.
        :rtype: None
        """
        LOGGER.debug(f"Barrier {self!r} abort")

        try:
            self._channel.poll(event_mask=EventType.POLLBARRIER_ABORT, timeout=0)
        except Exception as ex:
            raise BrokenBarrierError(f"The abort failed with the error of {repr(ex)}.")

    def destroy(self) -> None:
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

    @property
    def parties(self):
        return self._parties

    @property
    def n_waiting(self):
        return self._channel.barrier_count

    @property
    def broken(self):
        return self._channel.broken_barrier
