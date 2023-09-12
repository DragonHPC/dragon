"""Internal objects required for infrastructure process communication.
"""


import multiprocessing  # for BufferTooShort
import enum
import numbers
import pickle
import threading
import logging
import time
from typing import ClassVar

import dragon.dtypes as dtypes
import dragon.channels as dch
from .channel_desc import ChannelOptions
import dragon.globalservices.channel as dgchan
import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.facts as dfacts
from dragon.managed_memory import MemoryPool
from dragon.utils import B64

LOG = logging.getLogger('infrastructure.connection')

class ConnectionOptions:
    """Options for dragon.Connections

        Separate object because this will expand over time.

        There are a number of different facts about how one might
        want a Connection object to behave vs. its underlying
        Channels and its interaction with Global Services, together
        with anticipated need to let someone customize how the object
        behaves for performance.  This object is meant to organize
        all these into one object.
    """

    class CreationPolicy(enum.Enum):
        EXTERNALLY_MANAGED = enum.auto()
        PRE_CREATED = enum.auto()
        RECEIVER_CREATES = enum.auto()
        SENDER_CREATES = enum.auto()

    def __init__(self, *, creation_policy=CreationPolicy.EXTERNALLY_MANAGED,
                 min_block_size=None, large_block_size=None, huge_block_size=None, default_pool=None):
        self.min_block_size = min_block_size
        self.large_block_size = large_block_size
        self.huge_block_size = huge_block_size
        self.creation_policy = creation_policy
        self.default_pool = default_pool

class PipeOptions:
    """Options for the Pipe factory.

        Separate object because this, too, will expand over time.

        Same rationale as for ConnectionOptions.
    """

    # TODO: PE-38342, CreationPolicy.FIRST for first user to make channel
    # where-ever they happen to be, joining with a soft create
    class CreationPolicy(enum.Enum):
        EXTERNAL = enum.auto()
        EARLY = enum.auto()
        RECEIVER_CREATES = enum.auto()
        SENDER_CREATES = enum.auto()

    def __init__(self, *,
                 creation_policy=CreationPolicy.EARLY,
                 conn_options=None):

        self.creation_policy = creation_policy

        if conn_options is None:
            self.conn_options = ConnectionOptions()
        else:
            self.conn_options = conn_options

        if self.creation_policy == self.CreationPolicy.EARLY:
            self.conn_options.creation_policy = self.conn_options.CreationPolicy.PRE_CREATED
        elif self.creation_policy == self.CreationPolicy.SENDER_CREATES:
            self.conn_options.creation_policy = self.conn_options.CreationPolicy.SENDER_CREATES
        elif self.creation_policy == self.CreationPolicy.RECEIVER_CREATES:
            self.conn_options.creation_policy = self.conn_options.CreationPolicy.RECEIVER_CREATES
        elif self.creation_policy == self.CreationPolicy.EXTERNAL:
            self.creation_policy = self.conn_options.CreationPolicy.EXTERNALLY_MANAGED
        else:
            raise NotImplementedError('close case')


class CUID(int):
    """A wrapper around int to allow waiting on a cuid.
    """

    def thread_wait(self, timeout, done_ev, ready):
        if timeout is None:
            timeout = 1000000
        start = time.monotonic()
        delta = min(0.01, timeout) # TODO: figure out the value for delta
        timed_out = False

        if not done_ev.is_set():
            while not timed_out and not done_ev.is_set():
                done_res = self.conn.poll(delta)
                if not done_ev.is_set():
                    if done_res:
                        ready.append(self)
                        done_ev.set()
                timed_out = (time.monotonic() - start) > timeout
            done_ev.set()  # if not set here, timed out.


class Connection:
    """Dragon infrastructure's internal connection class. """
    class State(enum.Enum):
        CLOSED = enum.auto()  # we have closed and may have deleted backing channels we think we own
        ATTACHED = enum.auto()  # we have channels and we're attached, but haven't sent anything
        READY = enum.auto()  # we have got channel objects and a send was called
        UNBACKED = enum.auto()  # initialized with names but we have not created yet
        UNATTACHED = enum.auto()  # initialized with descriptors but we have not attached yet

    def __init__(self, *, inbound_initializer=None, outbound_initializer=None,
                 options=None, policy=dparm.POLICY_USER):
        """Initializes the Connection object.

            The object is initialized in an inbound direction, an outbound
            direction, or both.  These initializers can be either
            - a Channel object, whose lifecycle is assumed externally managed
            - a bytes-like descriptor, attached to lazily, destroyed automatically
            - a string name, for a rendezvous in Global services.

            TODO: document current behavior and design intent MUCH more thoroughly

        :param inbound_initializer: an initializer for the inbound channel
        :param outbound_initializer: an initializer for the outbound channel
        :param options: ConnectionOptions object
        :param policy: A Policy object.
        """

        self.at_eof = False
        self.ghost = False  # whether to omit sending EOT on close.

        if inbound_initializer is None and outbound_initializer is None:
            raise ConnectionError('at least one initializer is required')

        if options is None:
            self.options = ConnectionOptions()
        else:
            self.options = options

        self.write_adapter_options = {}
        if self.options.min_block_size is not None:
            self.write_adapter_options["small_blk_size"] = self.options.min_block_size

        if self.options.large_block_size is not None:
            self.write_adapter_options["large_blk_size"] = self.options.large_block_size

        if self.options.huge_block_size is not None:
            self.write_adapter_options["huge_blk_size"] = self.options.huge_block_size

        if not isinstance(policy, dparm.Policy):
            raise ValueError('The class of service must be a dragon.infrastructure.parameters.Policy value.')

        self.policy = policy
        self.inbound_chan = None
        self.outbound_chan = None
        self.write_adapter = None
        self.read_adapter = None
        self._detach_pool = None
        my_cp = self.options.creation_policy

        if isinstance(inbound_initializer, dch.Channel) or isinstance(outbound_initializer, dch.Channel):
            if my_cp != my_cp.EXTERNALLY_MANAGED and my_cp != my_cp.PRE_CREATED:
                msg = f'init with Channels and {my_cp.name} lifecycle not supported'
                raise ConnectionError(msg)

            self.inbound_chan = inbound_initializer
            self.outbound_chan = outbound_initializer
            self.inbound_initializer = None
            self.outbound_initializer = None
            self.state = self.State.ATTACHED
            if my_cp == my_cp.PRE_CREATED:
                self._incref_inbound_chan()
        else:
            self.inbound_initializer = inbound_initializer
            self.outbound_initializer = outbound_initializer
            if isinstance(inbound_initializer, str) or isinstance(outbound_initializer, str):
                self.state = self.State.UNBACKED
            else:  # assume channel descriptor
                if my_cp != my_cp.PRE_CREATED:
                    msg = f'init with Channels and {my_cp.name} lifecycle not supported'
                    raise ConnectionError(msg)
                self.state = self.State.UNATTACHED

    def _incref_inbound_chan(self):
        if isinstance(self.inbound_chan, dch.Channel):
            dgchan.get_refcnt(self.inbound_chan.cuid)

    def _decref_inbound_chan(self):
        if isinstance(self.inbound_chan, dch.Channel):
            dgchan.release_refcnt(self.inbound_chan.cuid)

    def __getstate__(self):
        state = dict(self.__dict__)
        state["write_adapter"] = None
        state["read_adapter"] = None
        state["_detach_pool"] = None
        # assume we're passing this to another process. Not a good final solution.
        if self.options.creation_policy == ConnectionOptions.CreationPolicy.PRE_CREATED:
            self._incref_inbound_chan()
        return state

    def open(self):
        # should construct or attach to the actual channels if not ready.
        if self.state == self.State.CLOSED:
            raise ConnectionError('reopening closed Connection not allowed')
        elif self.state == self.State.ATTACHED:
            self.state = self.State.READY
            return
        elif self.state == self.State.READY:
            return
        elif self.state == self.State.UNBACKED:
            if self.options.creation_policy == self.options.CreationPolicy.SENDER_CREATES:
                if self.outbound_initializer is not None:
                    target_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)
                    descriptor = dgchan.create(target_muid, self.outbound_initializer)
                    self.outbound_chan = dch.Channel.attach(descriptor.sdesc)
                    self.outbound_initializer = None

                if self.inbound_initializer is not None:
                    chan_desc = dgchan.join(self.inbound_initializer)
                    self.inbound_chan = dch.Channel.attach(chan_desc.sdesc)
                    self.inbound_initializer = None
            elif self.options.creation_policy == self.options.CreationPolicy.RECEIVER_CREATES:
                if self.inbound_initializer is not None:
                    target_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)
                    descriptor = dgchan.create(target_muid, self.inbound_initializer)
                    self.inbound_chan = dch.Channel.attach(descriptor.sdesc)
                    self.inbound_initializer = None

                if self.outbound_initializer is not None:
                    chan_desc = dgchan.join(self.outbound_initializer)
                    self.outbound_chan = dch.Channel.attach(chan_desc.sdesc)
                    self.outbound_initializer = None
            else:
                raise NotImplementedError('close case')

            self.state = self.State.READY

        elif self.state == self.State.UNATTACHED:
            if self.outbound_initializer is not None:
                self.outbound_chan = dch.Channel.attach(self.outbound_initializer)
                self.outbound_initializer = None

            if self.inbound_initializer is not None:
                self.inbound_chan = dch.Channel.attach(self.inbound_initializer)
                self.inbound_initializer = None

            self.state = self.State.READY

        else:
            raise NotImplementedError('open case')

    def _setup_write_adapter(self):
        if not self.outbound_chan.is_local:
            write_options = self.write_adapter_options
            if self.options.default_pool is None:
                try:
                    self._detach_pool = MemoryPool.attach(B64.str_to_bytes(dparm.this_process.default_pd))
                    write_options["buffer_pool"] = self._detach_pool
                except:
                    raise OSError("unable to attach to default Managed Memory pool")
            else:
                write_options["buffer_pool"] = self.options.default_pool
            self.write_adapter = dch.Peer2PeerWritingChannelFile(self.outbound_chan, options=write_options)
        else:
            self.write_adapter = dch.Peer2PeerWritingChannelFile(self.outbound_chan,
                                                                 options=self.write_adapter_options, wait_mode=self.policy.wait_mode)

    def _setup_read_adapter(self):
        self.read_adapter = dch.Peer2PeerReadingChannelFile(self.inbound_chan, wait_mode=self.policy.wait_mode)

    def _check_inbound(self):
        if self.inbound_chan is None and self.inbound_initializer is None:
            raise OSError(f'No receiving on a {self.__class__.__name__} that is not enabled for reading')

        if self.at_eof:
            raise EOFError()

        if self.state != self.State.READY:
            self.open()

    def _check_outbound(self):
        if self.outbound_chan is None and self.outbound_initializer is None:
            raise OSError(f'No sending on a {self.__class__.__name__} that is not enabled for writing')

        if self.state != self.State.READY:
            self.open()

    def send(self, obj):
        self._check_outbound()
        if self.write_adapter is None:
            self._setup_write_adapter()

        try:
            self.write_adapter.open()
            pickle.dump(obj, file=self.write_adapter, protocol=5)

        except Exception as e:
            raise ConnectionError(f"Could not complete send operation: {e}")
        finally:
            self.write_adapter.close()

    def recv(self):
        self._check_inbound()
        if self.read_adapter is None:
            self._setup_read_adapter()

        try:
            self.read_adapter.open()
            send_type, msg_len = self.read_adapter.check_header()

            if send_type == dch.ChannelAdapterMsgTypes.EOT:
                self.at_eof = True
                raise EOFError()
            elif send_type == dch.ChannelAdapterMsgTypes.RAW_BYTES:
                # multiprocessing.Connection unit tests assume you can go both ways.
                self.read_adapter.advance_raw_header(msg_len)
                buf = bytearray(msg_len)
                self.read_adapter.readinto(memoryview(buf))
                obj = pickle.loads(buf)
            else:
                obj = pickle.load(self.read_adapter)
        except EOFError:
            raise EOFError
        except Exception as e:
            raise ConnectionError(f"Could not complete receive operation: {e}")
        finally:
            self.read_adapter.close()

        return obj

    def ghost_close(self):
        """Force the connection into a closed state without sending EOT

        This 'ghosts' the receiver on the other end, which won't
        be getting an EOT message from this object, but when using
        externally managed Channels on this object that is what one
        might want to do, in order to transfer use of that Channel.
        """
        assert self.options.creation_policy == self.options.CreationPolicy.EXTERNALLY_MANAGED
        self.state = self.State.CLOSED
        if self.options.creation_policy == ConnectionOptions.CreationPolicy.PRE_CREATED:
            self._decref_inbound_chan()

    def thread_wait(self, timeout, done_ev, ready):
        """Thread waiter signaling with an ev.
        """
        if timeout is None:
            timeout = 1000000
        start = time.monotonic()
        delta = min(0.01, timeout) # TODO: figure out the value for delta
        timed_out = False

        if not done_ev.is_set():
            while not timed_out and not done_ev.is_set():
                done_res = self.poll(delta)
                if not done_ev.is_set():
                    if done_res:
                        ready.append(self)
                        done_ev.set()
                timed_out = (time.monotonic() - start) > timeout
            done_ev.set()  # if not set here, timed out.

    def close(self):
        if self.state == self.State.CLOSED:
            pass
        elif self.state == self.State.UNBACKED:
            pass  # nothing got created, so nothing to clean up.
        elif self.state == self.State.UNATTACHED:
            # todo: Should get the c_uid from the inbound_initializer (here a serialized descriptor) and destroy it.
            # todo: ask for a way to get this from the descriptor directly without attaching to anything
            pass
        elif self.state == self.State.READY or self.state == self.State.ATTACHED:
            # todo: look at cleanup policy here.  If we are holding around adapters
            # we may need to flush them.
            if self.state == self.State.READY and self.outbound_chan is not None and not self.ghost:
                try:
                    if self.write_adapter is None:
                        self._setup_write_adapter()
                    self.write_adapter.open()
                    self.write_adapter.write_eot()
                except dch.ChannelError:  # receiver may have destroyed channel
                    pass
                finally:
                    self.write_adapter.close()

                if self._detach_pool is not None:
                    self._detach_pool.detach()

            if self.options.creation_policy == ConnectionOptions.CreationPolicy.EXTERNALLY_MANAGED:
                # don't detach from or destroy anything
                pass
            elif self.options.creation_policy == ConnectionOptions.CreationPolicy.PRE_CREATED:
                # TODO: technically we should detach here, but we need refcounting in Channels to be merged first
                self._decref_inbound_chan()
            else:
                if self.outbound_chan is not None:
                    try:
                        self.outbound_chan.detach()
                    except dch.ChannelError:
                        self.outbound_chan = None

                if self.inbound_chan is not None:
                    inbound_cuid = self.inbound_chan.cuid

                    try:
                        self.inbound_chan.detach()
                    except dch.ChannelError:
                        pass

                    dgchan.destroy(inbound_cuid)
        else:
            raise NotImplementedError('close case')

        self.state = self.State.CLOSED
        return

    @property
    def inbound_channel(self):
        return self.inbound_chan

    @property
    def outbound_channel(self):
        return self.outbound_chan

    @property
    def closed(self):
        """True if the connection is closed"""
        return self.state == self.State.CLOSED

    @property
    def readable(self):
        """True if the connection is readable"""
        return self.inbound_initializer is not None or self.inbound_chan is not None

    @property
    def writable(self):
        """True if the connection is writable"""
        return self.outbound_initializer is not None or self.outbound_chan is not None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_inst, exc_tb):
        self.close()

    # Just a way to hang an attribute on an integer,
    # TODO: should be the real c_uid.
    def fileno(self):
        if self.state == self.State.READY:
            the_cuid = CUID(-2)
        else:
            the_cuid = CUID(-1)

        the_cuid.conn = self
        return the_cuid

    def poll(self, timeout=0.0, event_mask=dch.POLLIN):
        if self.at_eof:
            return True

        self._check_inbound()

        # multiprocessing tests interpret negative timeouts as 0.
        if isinstance(timeout, numbers.Number):
            if timeout < 0:
                timeout = 0

        return self.inbound_chan.poll(wait_mode=self.policy.wait_mode, event_mask=event_mask, timeout=timeout)

    def send_bytes(self, buffer, offset=0, size=None):
        self._check_outbound()

        # lifted from multiprocessing.connection.Connection
        m = memoryview(buffer)
        if m.itemsize > 1:
            m = memoryview(bytes(m))
        n = len(m)
        if offset < 0:
            raise ValueError("offset is negative")
        if n < offset:
            raise ValueError("buffer length < offset")
        if size is None:
            size = n - offset
        elif size < 0:
            raise ValueError("size is negative")
        elif offset + size > n:
            raise ValueError("buffer length < offset + size")
        # end lift

        try:
            if self.write_adapter is None:
                self._setup_write_adapter()
            self.write_adapter.open()
            self.write_adapter.write_raw_header(size)
            self.write_adapter.write(m[offset:offset + size])
        except Exception as e:
            raise ConnectionError(f"Could not complete send bytes operation: {e}")
        finally:
            self.write_adapter.close()

    # Todo: refactor recv_bytes and recv_bytes_into? duplicative
    def recv_bytes(self, maxlength=None):
        self._check_inbound()

        try:
            if self.read_adapter is None:
                self._setup_read_adapter()

            self.read_adapter.open()
            send_type, msg_len = self.read_adapter.check_header()

            if send_type == dch.ChannelAdapterMsgTypes.EOT:
                self.at_eof = True
                raise EOFError()
            elif send_type == dch.ChannelAdapterMsgTypes.PICKLE_PROT_5:
                # Would like to raise, but base case multiprocessing unit tests
                # abuse the interface, so we need to implement a way to get
                # the pickled rep out.  This is slow but people shouldn't
                # be doing it.
                # raise RuntimeError('recv_bytes called on a Connection whose sender called send')
                buf = bytearray()
                self.read_adapter.set_side_buf(buf)
                _ = pickle.load(self.read_adapter)
                if maxlength is not None and len(buf) > maxlength:
                    self.close()
                    raise OSError(f'recv_bytes (from send) maxlength={maxlength} but msg_len={msg_len}')

            else:
                if maxlength is not None and msg_len > maxlength:
                    self.close()  # base multiprocessing says connection is unusable after maxlength error; we could
                    # potentially stay around
                    raise OSError(f'recv_bytes maxlength={maxlength} but msg_len={msg_len}')

                self.read_adapter.advance_raw_header(msg_len)
                buf = bytearray(msg_len)
                self.read_adapter.readinto(memoryview(buf))
        except Exception as e:
            raise ConnectionError(f"Could not complete receive bytes operation: {e}")
        finally:
            self.read_adapter.close()

        return buf

    def recv_bytes_into(self, buffer, offset=0):
        self._check_inbound()

        with memoryview(buffer) as m:
            # Get byte size of arbitrary buffer
            byte_size = m.itemsize * len(m)
            if offset < 0:
                raise ValueError("negative offset")
            elif offset > byte_size:
                raise ValueError("offset too large")

            try:
                if self.read_adapter is None:
                    self._setup_read_adapter()
                self.read_adapter.open()
                send_type, msg_len = self.read_adapter.check_header()

                if send_type == dch.ChannelAdapterMsgTypes.EOT:
                    self.at_eof = True
                    raise EOFError()
                elif send_type == dch.ChannelAdapterMsgTypes.PICKLE_PROT_5:
                    raise RuntimeError('recv_bytes_into called on a Connection whose sender called send')

                if msg_len + offset > byte_size:
                    self.read_adapter.advance_raw_header(msg_len)
                    msg = self.read_adapter.read(msg_len)
                    raise multiprocessing.BufferTooShort(msg)

                self.read_adapter.advance_raw_header(msg_len)
                target = m.cast('B')
                self.read_adapter.readinto(target[offset:offset + msg_len])
            except Exception as e:
                raise ConnectionError(f"Could not complete recv_bytes_into operation: {e}")
            finally:
                self.read_adapter.close()

        return msg_len


_UNIQ_CHAN_LOCK = threading.Lock()
_CHAN_CTR = 0


def get_next_ctr():
    global _CHAN_CTR
    with _UNIQ_CHAN_LOCK:
        tmp = _CHAN_CTR
        _CHAN_CTR += 1

    return tmp


def make_uniq_chan_names():
    base_name = f'anon_{dparm.this_process.my_puid}_{get_next_ctr()}'
    return base_name, base_name + '_rev'


def Pipe(duplex=True, *, channels=None, options=None):
    """Pipe constructor, producing two Connections.

    The channels parameter can be None, indicating that
    this is an anonymous Pipe requiring a new channel to be constructed
    and eventually destroyed by the receiver.

    It can also be a Channel object or descriptor in which case
    the two Connections returned will communicate through the channel, with
    one being in read mode and the other in write mode.

    Finally, if it is a tuple, it is expected to have two elements which are
    both channels or channel descriptors and each of which can send and receive.

    If the channels parameter is None, then the duplex parameter governs
    whether the connections are full duplex and whether two new channels
    get created or only one.

    If not duplex, the first one is the reader and the second the writer

    The options object carries lifecycle and creation options for the Pipe call
    and also policy settings for the Connection objects that result, as well as
    policy on how the Connections objects should interact with the pool when
    sending data.

    :param channels: None or a Channel object or a 2-element tuple of distinct Channel objects
    :param duplex: default True, whether to produce
    :param options: a PipeOptions object
    :return: a pair of Connection objects.
    """

    if options is None:
        options = PipeOptions()

    if channels is not None:
        options.creation_policy = options.CreationPolicy.EXTERNAL
        options.conn_options.creation_policy = options.conn_options.CreationPolicy.EXTERNALLY_MANAGED

        if isinstance(channels, dch.Channel):
            first = Connection(inbound_initializer=channels, options=options.conn_options)
            second = Connection(outbound_initializer=channels, options=options.conn_options)
        elif 2 == len(channels):
            assert channels[0] is not channels[1]
            first = Connection(inbound_initializer=channels[0], outbound_initializer=channels[1])
            second = Connection(inbound_initializer=channels[1], outbound_initializer=channels[0])
        else:
            raise ValueError('unexpected channels/initializer: {channels}')
    else:
        if options.creation_policy == options.CreationPolicy.EARLY:
            options.conn_options.creation_policy = options.conn_options.CreationPolicy.PRE_CREATED
            target_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)
            chan_options = ChannelOptions(ref_count=True)
            first_desc = dgchan.create(target_muid, options=chan_options).sdesc
            first_channel = dch.Channel.attach(first_desc)

            if duplex:
                second_desc = dgchan.create(target_muid, options=chan_options).sdesc
                second_channel = dch.Channel.attach(second_desc)
                first = Connection(inbound_initializer=first_channel,
                                   outbound_initializer=second_channel,
                                   options=options.conn_options)
                second = Connection(inbound_initializer=second_channel,
                                    outbound_initializer=first_channel,
                                    options=options.conn_options)
                # decref because all counting of the Channel happens in Connection now. Our create did one inc.
                dgchan.release_refcnt(first_channel.cuid)
                dgchan.release_refcnt(second_channel.cuid)
            else:
                first = Connection(inbound_initializer=first_channel, options=options.conn_options)
                second = Connection(outbound_initializer=first_channel, options=options.conn_options)
                # decref because all counting of the Channel happens in Connection now. Our create did one inc.
                dgchan.release_refcnt(first_channel.cuid)
        else:
            assert options.creation_policy != options.CreationPolicy.EXTERNAL

            firstname, secondname = make_uniq_chan_names()

            if duplex:
                first = Connection(inbound_initializer=firstname, outbound_initializer=secondname,
                                   options=options.conn_options)
                second = Connection(inbound_initializer=secondname, outbound_initializer=firstname,
                                    options=options.conn_options)
            else:
                first = Connection(inbound_initializer=firstname, options=options.conn_options)
                second = Connection(outbound_initializer=firstname, options=options.conn_options)

    return first, second


class Address:
    """Address class for dragon servers and listeners and all that
    """

    def __init__(self, user_name=''):
        self.user_name = user_name
        self.salt = dparm.this_process.my_puid

    def __str__(self):
        return self.user_name

    @property
    def cname(self):
        return f'{self.user_name!s}.{self.salt!s}'


class Listener:
    def __init__(self, address='', family='', backlog=1, authkey=None):
        pass


def Client(address='', family='', authkey=None):
    pass
