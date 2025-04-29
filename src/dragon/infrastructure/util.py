"""Python infrastructure utilities.

This file is where to put useful internal utilities and wrappers and stuff.

It's also to solve the problem of 'I don't know what the right long term way is
to do something, but I need a version of it now to make progress'.  The
initial solution here makes all the uses consistent and we can refactor more
easily if it needs to change.

Rules of engagement:

Anyone can put anything into this file.

Nothing in here is user facing.

Once there are a few things doing similar kinds of things in this file,
they need to be broken out into another file.
"""

from collections.abc import Iterable
import fcntl
import heapq
import io
from itertools import chain
import logging
import os
import re
import selectors
import sys
import time
from warnings import warn
from .parameters import this_process
from socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM, gethostname, inet_aton
import struct
from .facts import FIRST_PUID


def _get_access_modes(f):
    """Returns (readable, writeable) pair of bools corresponding to the access
    mode of the specified file-like object (or file descriptor).
    """
    mode = fcntl.fcntl(f, fcntl.F_GETFL) & os.O_ACCMODE
    readable = writeable = mode == os.O_RDWR
    return (readable or mode == os.O_RDONLY, writeable or mode == os.O_WRONLY)


class NewlineStreamWrapper:
    """Helper class for sending json objects through streams.

    This class sends newline-delimited newline-free strings
    through streams with an interface similar to multiprocessing.Connection.

    It is used for sending around JSON-encoded infrastructure
    messages through streams and in testing.

    TODO: consider whether this belongs in base code.  It feels more
    like something that should only be in a test bench.
    """

    def __init__(self, stream, read_intent=None, write_intent=None):
        """
        read_intent and write_intent are used to ensure
        clients are using the right operations on the stream.
        """
        try:
            readable, writeable = _get_access_modes(stream)
        except (TypeError, io.UnsupportedOperation):
            # Fallback to original defaults instead of raising the ValueError
            # most likely to be raised by sel.register() below.
            readable, writeable = True, True
        self.read_intent = bool(readable if read_intent is None else read_intent)
        self.write_intent = bool(writeable if write_intent is None else write_intent)
        if self.read_intent and not readable:
            warn(f"Intend to read a stream not open for reading: {stream}")
        if self.write_intent and not writeable:
            warn(f"Intend to write a stream not open for writing: {stream}")
        self.stream = stream

    def send(self, data):
        """Perform the write operation of the given data into the stream.
        Assert that the write_intent is set, ensuring the client is using the
        right operation on the stream.

        :param data: data to be written into the stream
        :type data: string
        """
        try:
            assert self.write_intent, "sending to a read wrap"
            assert isinstance(data, str)
            msg_str = data.strip().replace("\n", " ") + "\n"

            # try to tolerate if the stream is a text mode stream
            if isinstance(self.stream, io.TextIOBase):
                self.stream.write(msg_str)
            else:
                self.stream.write(msg_str.encode())

            self.stream.flush()
        except ValueError:
            # If we get a ValueError exception the underlying
            # stream is closed. This means things are going away
            # anyway. So nothing to do in this case.
            pass

    def recv(self):
        """Perform the read operation on the stream. Assert that the read_intent
        is set, ensuring the client is using the right operation on the stream.
        """
        assert self.read_intent, "receiving from a write wrap"
        line = self.stream.readline()

        # try to tolerate if the stream is a text stream
        if isinstance(line, str):
            stuff = line
        else:
            stuff = line.decode()

        return stuff

    def poll(self, timeout=0):
        """Poll for the read I/O events on the registered file objects of type stream.
        Collect the selector if already set and its associated events, else the default
        selector and register with the read event, to wait upon. Also, assert that the
        ride_intent is set, ensuring the client is using the right operation on the stream.

        :param timeout: If timeout > 0, specifies the maximum wait time in seconds
        :type timeout: int, defaults to 0
        :return: True if read I/O event is ready on the selected stream
        :rtype: boolean
        """
        assert self.read_intent, "requires read intent"

        # To support SSH launch, see if there's a poll method provided by stream. The
        # ssh launch does provide one:
        try:
            return self.stream.poll(timeout=timeout)
        except AttributeError:
            pass

        try:
            sel = self.selector
        except AttributeError:
            sel = self.selector = selectors.DefaultSelector()
            sel.register(self.stream, selectors.EVENT_READ)
        events = sel.select(timeout)
        for key, mask in events:
            if key.fileobj is not self.stream:
                continue
            if mask & selectors.EVENT_READ:
                return True
        return False

    def close(self):
        """Close thes selector and the associated stream"""
        try:
            sel = self.selector
        except AttributeError:
            pass
        else:
            sel.close()
        self.stream.close()


class PriorityMultiMap:
    """Class combining a multimap with a priority queue to allow timeouts.

    Correct behavior relies on keys never being reused.

    This is used in Global Services to manage requests for timeout
    notification from multiple sources.

    TODO: this needs some unit tests
    TODO: consider reimplementing in terms of a collections.Counter object!
    """

    def __init__(self):
        self.internal_items = dict()
        self.dead_keys = set()
        self.timeout_keys = set()
        self.timeout_pq = list()
        self.deadlines = dict()  # key: (key,value), value: deadline

    def put(self, key, value, timeout=None):
        """Store (key, value) pair to the map (internal items), used by the global requests
        to manage timeout requests with the associated deadlines of multiple sources.
        Can be used for all channels, pools, and processes.

        :param key: identifier of the requested source(typically name)
        :type key: string
        :param value: value of the identifier(typically id)
        :type value: int
        :param timeout: used to evaluate the deadline of the (key, value) pair, if provided
        :type timeout: int, defaults to Nonde
        """
        if key in self.internal_items:
            self.internal_items[key].add(value)
        else:
            self.internal_items[key] = {value}

        if timeout is not None:
            deadline = time.time() + timeout
            self.deadlines[(key, value)] = deadline
            heapq.heappush(self.timeout_pq, (deadline, key, value))
            self.timeout_keys.add(key)

            if key in self.dead_keys:
                self.dead_keys.remove(key)

    def time_left(self, key, value):
        """Provides the deadline (if stored) for the given (key, value) pair.

        :param key: identifier of the source
        :type key: string
        :param value: value of the identifier
        :type value: int
        :return: timeout/deadline of the given (key, value) pair
        :rtype: int
        """
        if (key, value) in self.deadlines:
            return self.deadlines[(key, value)]

    def get(self, key):
        """Provides the value for the stored key in the map

        :param key: identifier of the source
        :type key: string
        :return: value of the identifier
        :rtype: int
        """
        return self.internal_items[key]

    def next_deadline(self):
        """Returns the timeout/deadline of the front item in the timeout priority queue, if present

        :return: value of the deadline
        :rtype: int
        """
        if self.timeout_pq:
            return self.timeout_pq[0][0]
        else:
            return None

    def get_timed_out(self):
        """Remove all the requests in the timeout priority queue, taking the opportunity to
        clear the associated deadlines and the dead keys

        :return: List of all the (key, value) tuples of the timed out requests
        :rtype: List
        """
        rv = []
        now = time.time()
        while self.timeout_pq and self.timeout_pq[0][0] <= now:
            key = self.timeout_pq[0][1]
            if key not in self.dead_keys:
                rv.append(self.timeout_pq[0][1:])

            if self.deadlines and (self.timeout_pq[0][1], self.timeout_pq[0][2]) in self.deadlines:
                del self.deadlines[(self.timeout_pq[0][1], self.timeout_pq[0][2])]

            heapq.heappop(self.timeout_pq)

        # take opportunity to flush dead keys
        if not self.timeout_pq:
            self.dead_keys.clear()

        return rv

    def remove(self, k):
        """Removes the source from the map, with all the values of it placed in the queue,
        along with the timeout/deadline management of the source

        :param k: identifier of the object/source(typically name)
        :type k: string
        """
        if self.deadlines:
            if k in self.internal_items and self.internal_items[k]:
                for v in self.internal_items[k]:
                    if (k, v) in self.deadlines:
                        del self.deadlines[(k, v)]

        if k in self.internal_items:
            del self.internal_items[k]
        if k in self.timeout_keys:
            self.dead_keys.add(k)
            self.timeout_keys.remove(k)

    def remove_one(self, k, v):
        """Removes the request of a particular source placed in the queue,
        along with removing the timeout/deadline management of the associated source, if set.

        :param k: identifier of the source(typically name)
        :type k: string
        :param v: value of the identifier(typically id)
        :type v: int
        """
        if v in self.internal_items[k]:
            self.internal_items[k].remove(v)

            if not self.internal_items[k]:
                self.remove(k)

        if self.deadlines:
            if (k, v) in self.deadlines:
                del self.deadlines[(k, v)]

    def keys(self):
        """Helps with all the keys of the global requests placed in the priority map

        :return: List of all the keys of the queue items
        :rtype: List
        """
        return self.internal_items.keys()

    def values(self):
        """Helps with all the values of the global requests placed in the priority map

        :return: List of all the values of the queue items
        :rtype: List
        """
        return self.internal_items.values()

    def items(self):
        """Helps with all the items(key, value) pairs of the
            global requests placed in the priority map

        :return: List of all the tuples(key, value) of the queue items
        :rtype: List
        """
        return self.internal_items.items()

    def __contains__(self, key):
        return key in self.internal_items


# TODO: this needs a better name
class AbsorbingChannel:
    """Object that absorbs all sends, remembering the last one; useful in GS"""

    def __init__(self):
        self.msg = None

    def send(self, msg):
        self.msg = msg


def survey_dev_shm():
    """Looks at what is in /dev/shm owned by current user

    :return: set of filenames owned by current user in /dev/shm
    """

    my_id = os.getuid()
    with os.scandir("/dev/shm") as it:
        # during a certain shutdown race condition, FileNotFoundError may be raised.
        try:
            rv = {entry.name for entry in it if entry.stat().st_uid == my_id}
            return rv
        except FileNotFoundError:
            pass


def compare_dev_shm(previous):
    """Warns if there are files owned by current user in /dev/shm not previously seen

    :param previous: set of previously seen filenames
    :return: None
    """

    now = survey_dev_shm()

    # Due to a race condition, it is possible that None is returned.
    if now is None:
        return

    remaining = now - previous

    if remaining:
        print(f"warning: {len(remaining)} leftover files in /dev/shm: ")

    for fn in remaining:
        print(fn)


def route(msg_type, routing_table, metadata=None):
    """Decorator routing adapter.

    This is a function decorator used to accumulate handlers for a particular
    kind of message into a routing table indexed by type, used
    by server classes processing infrastructure messages.

    The routing table is usually a class attribute and the message type
    is typically an infrastructure message.  The metadata, if used,
    is typically information used to check that the state the server object
    is in, is appropriate for that type of message.

    :param msg_type: the type of an infrastructure message class
    :param routing_table: dict, indexed by msg_type with values a tuple (function, metadata)
    :param metadata: metadata to check before function is called, usage dependent
    :return: the function.
    """

    def decorator_route(f):
        assert msg_type not in routing_table
        routing_table[msg_type] = (f, metadata)
        return f

    return decorator_route


# TODO: PE-37739, the full on client debug hookup story.
def mk_fifo_debugger(basename, *, override_bph=False, quiet=False):
    dbg_in_fn = basename + "_dbg_in"
    dbg_out_fn = basename + "_dbg_out"
    os.mkfifo(dbg_in_fn)
    os.mkfifo(dbg_out_fn)
    if not quiet:
        print(f"fifos made: in: {dbg_in_fn} out: {dbg_out_fn}; waiting on open")
    sys.stdout.flush()
    in_fifo = open(dbg_in_fn, "r")
    out_fifo = open(dbg_out_fn, "w")
    import pdb

    debugger = pdb.Pdb(stdin=in_fifo, stdout=out_fifo)

    if override_bph:
        sys.breakpointhook = debugger.set_trace

    return pdb.Pdb(stdin=in_fifo, stdout=out_fifo)


def to_str(x):
    """Convert anything to a string"""
    if x is None:
        return ""
    if hasattr(x, "decode"):
        return x.decode()
    return str(x)


def to_str_iter(seq):
    """Iterate over sequence yielding string conversion of each item.

    >>> list(_to_str_iter(['hello', 'hello'.encode(), Path('/hello'), None, 0, False]))
    ['hello', 'hello', '/hello', '', '0', 'False']
    >>> list(_to_str_iter('hello'))
    ['hello']
    >>> list(_to_str_iter('hello'.encode()))
    ['hello']
    >>> list(_to_str_iter(pathlib.Path('/hello')))
    ['/hello']
    >>> list(_to_str_iter(None))
    Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
    File "<stdin>", line 30, in _to_str_iter
    TypeError: 'NoneType' object is not iterable
    >>> list(_to_str_iter(0))
    Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
    File "<stdin>", line 7, in _to_str_iter
    TypeError: 'int' object is not iterable
    >>> list(_to_str_iter(False))
    Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
    File "<stdin>", line 7, in _to_str_iter
    TypeError: 'bool' object is not iterable
    """
    if isinstance(seq, (str, bytes, os.PathLike)):
        yield to_str(seq)
    else:
        yield from map(to_str, seq)


def range_expr(
    s: str, prog: re.Pattern = re.compile(r"(?P<start>\d+)(?:-(?P<stop>\d+)(?::(?P<step>\d+))?)?$")
) -> Iterable[int]:
    """Return iterable corresponding to the given range expression of
    the form START[-STOP[:STEP]][,...].

    NOTE: Range expressions are intended to be compatible with the --cpu-list
    expressions from taskset(1).

    >>> tuple(range_expr('0-10:2'))
    (0, 2, 4, 6, 8, 10)
    >>> tuple(range_expr('0-10:2,0-10:3'))
    (0, 2, 4, 6, 8, 10, 0, 3, 6, 9)
    >>> set(range_expr('0-10:2,0-10:3'))
    {0, 2, 3, 4, 6, 8, 9, 10}
    """
    iters = []
    for expr in s.split(","):
        m = prog.match(expr)
        if m is None:
            raise ValueError(f"Invalid range: {expr}")
        args = list(map(int, filter(None, m.groups())))
        if len(args) == 1:
            # Singular value specified
            args.append(
                args[0] + 1,
            )
        else:
            # Add one to stop value, to make the range inclusive
            args[1] += 1
        iters.append(range(*args))
    return chain.from_iterable(iters)


def enable_logging(level=logging.DEBUG):
    logging.basicConfig(stream=sys.stdout, level=level)


def user_print(*args, **kwargs):
    if this_process.my_puid >= FIRST_PUID:
        kwargs["flush"] = True
        print(*args, **kwargs)


def port_check(ip_port):
    s = socket(AF_INET, SOCK_STREAM)
    try:
        s.bind(ip_port)
    except Exception:
        if s is not None:
            s.close()
        return False
    else:
        s.close()
        return True


def get_port():
    host = gethostname()
    min_port = 1025
    max_port = 65536

    for port in range(min_port, max_port + 1):
        if port_check((host, port)):
            return port

    raise RuntimeError("No available ports")


def get_host_info(network_prefix) -> tuple[str, str, list[str]]:
    """Return username, hostname, and list of IP addresses."""
    from dragon.transport.ifaddrs import getifaddrs, InterfaceAddressFilter
    from ..dlogging.util import DragonLoggingServices as dls

    log = logging.getLogger(dls.LA_BE).getChild("get_host_info")
    _user = os.environ.get("USER", str(os.getuid()))

    ifaddr_filter = InterfaceAddressFilter()
    ifaddr_filter.af_inet(inet6=False)  # Disable IPv6 for now
    ifaddr_filter.up_and_running()
    try:
        ifaddrs = list(filter(ifaddr_filter, getifaddrs()))
    except OSError:
        log.exception("Failed to get network interface addresses")
        raise
    if not ifaddrs:
        _msg = "No network interface with an AF_INET address was found"
        log.error(_msg)
        raise RuntimeError(_msg)

    try:
        re_prefix = re.compile(network_prefix)
    except (TypeError, NameError):
        _msg = "expected a string regular expression for network interface network_prefix"
        log.error(_msg)
        raise RuntimeError(_msg)

    ifaddr_filter.clear()
    ifaddr_filter.name_re(re.compile(re_prefix))
    ip_addrs = [ifa["addr"]["addr"] for ifa in filter(ifaddr_filter, ifaddrs)]
    if not ip_addrs:
        _msg = f"No IP addresses found matching regex pattern: {network_prefix}"
        log.error(_msg)
        raise RuntimeError(_msg)
    log.info(f"Found IP addresses: {','.join(ip_addrs)}")

    return _user, ip_addrs


# get external IP address
def get_external_ip_addr():
    s = socket(AF_INET, SOCK_DGRAM)
    s.settimeout(0)
    connected = False
    try:
        # doesn't even have to be reachable
        s.connect(("10.254.254.254", 1))
        connected = True
        ip_addr = s.getsockname()[0]
        s.close()
        return ip_addr
    except Exception:
        if connected:
            s.close
        raise


def rt_uid_from_ip_addrs(fe_ext_ip_addr, head_node_ip_addr):
    fe_ext_packed_ip = inet_aton(fe_ext_ip_addr)
    head_node_packed_ip = inet_aton(head_node_ip_addr)

    fe_ext_int = struct.unpack("!L", fe_ext_packed_ip)[0]
    head_node_int = struct.unpack("!L", head_node_packed_ip)[0]

    return (fe_ext_int << 32) | head_node_int
