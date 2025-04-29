"""Dragon's replacement for the Multiprocessing Connection and Pipe objects"""

import os
import pickle

import multiprocessing.connection
import multiprocessing.context
from multiprocessing.util import debug, sub_debug, info, sub_warning

import inspect  # Only needed for demo, not critical functionality
from pprint import pprint  # Only needed for demo, not critical functionality

_original_os_write = os.write
_original_os_read = os.read
_original_os_close = os.close
_original_os_pipe = os.pipe

# This file mostly contains dead code. The real implementation is in infrastructure.connection


class ConnGlueBase:
    def __init__(self, ucid, note, *args, **kwargs):
        self.ucid = ucid
        self.note = note

    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())})'

    def get_fd(self):
        # TODO: Eliminate once not using file descriptors anymore and modify
        #       connection.wait() accordingly.
        return int(self.ucid)

    @staticmethod
    def extract_fd(conn_fileno):
        # TODO: Eliminate once not using file descriptors anymore.
        # Nice thing is, to track down all the places where a file descriptor
        # is still really being used, just search for this func's invocation.
        return conn_fileno.get_fd() if hasattr(conn_fileno, "get_fd") else conn_fileno

    def write(self, *args, **kwargs):
        sub_debug(f"{self.__class__.__name__}.write called len(args[0])={len(args[0])} args={args}")
        fd = self.__class__.extract_fd(self)
        return _original_os_write(fd, *args, **kwargs)

    def read(self, *args, **kwargs):
        sub_debug(f"{self.__class__.__name__}.read called args={args}")
        try:
            fd = self.__class__.extract_fd(self)
            retval = _original_os_read(fd, *args, **kwargs)
        except Exception as e:
            sub_warning(f"{self.__class__.__name__}.read exception: {e}")
            raise e
        return retval

    def close(self, *args, **kwargs):
        sub_debug(f"{self.__class__.__name__}.close called with ({args}, {kwargs}) by pid={os.getpid()}")
        # if inspections:
        #    frame = inspect.currentframe()
        #    pprint(f"{inspect.getouterframes(frame)}")
        fd = self.__class__.extract_fd(self)
        return _original_os_close(fd, *args, **kwargs)

    @classmethod
    def pipe(cls, _log_note=""):
        a, b = _original_os_pipe()
        retval = (
            cls(str(a), f"r from {cls.__name__}.pipe {_log_note}"),
            cls(str(b), f"w from {cls.__name__}.pipe {_log_note}"),
        )
        debug(f"{cls.__name__}.pipe called, returning: {retval}")
        return retval

    def fileno(self):
        # TODO: Fix the dependency on this in pool._wait_for_updates() which
        # depends upon connection.wait() (very similar to need for fix to
        # DragonConnection.fileno()) by either monkeypatching connection.wait()
        # (and making sure pool picks it up) or by providing a viable object
        # for the selectors module to use to monitor and wait on.
        info(f"{self.__class__.__name__}.fileno() still being called; mind the TODO here")
        return self.get_fd()


try:
    from dragon.infrastructure.connection import CUID, Connection, Pipe as DragonPipe
    from .. import managed_memory
    from .. import channels
    from .process import PUID
except ImportError as e:
    raise ImportError(f"import dragon failed: {e}")
else:
    ##################################################
    # Use dragon.infrastructure.connection.Connection
    ##################################################

    class ConnGlue(ConnGlueBase):
        "Glue around dragon.infrastructure.connection.Connection."

        _mem_pool = None
        _last_channel_uid = None

        def __init__(self, ucid, note, lowerlevel_conn, *args, **kwargs):
            # TODO: Obtain ucid from lowerlevel_conn instead of requiring separate input?
            # TODO: Enforce uniqueness or trust this in lowerlevel_conn objects and leave well enough alone?
            self.ucid = ucid
            self.note = note
            if isinstance(lowerlevel_conn, bytes):
                self.lowerlevel_conn = pickle.loads(lowerlevel_conn)
            else:
                self.lowerlevel_conn = lowerlevel_conn  # MinConnection
            self.read_buf = b""  # TODO: Verify either this is needed or eliminate it

        def __repr__(self):
            try:
                pickled_lowerlevel_conn = pickle.dumps(self.lowerlevel_conn)
            except (ImportError, AttributeError):
                pickled_lowerlevel_conn = "unavailable during shutdown"
            return (
                f"{self.__class__.__name__}(ucid={self.ucid!r}, note={self.note!r}, lowerlevel_c"
                f"onn={pickled_lowerlevel_conn!r})"
            )

        def write(self, data, *args, **kwargs):
            sub_debug(f"{self.__class__.__name__}.write called len(data)={len(data)} args={args} kwargs={kwargs}")
            try:
                self.lowerlevel_conn.send_bytes(data)
            except Exception as e:
                sub_warning(f"{self.__class__.__name__}.write exception: {e}")
                raise
            return len(data)

        def read(self, n_bytes=None, *args, **kwargs):
            # TODO: Should this be "read up to n_bytes" instead of "read until n_bytes avail"?
            # Buffer receiving from the Connection object.
            sub_debug(f"{self.__class__.__name__}.read called n_bytes={n_bytes} args={args} kwargs={kwargs}")
            _read_buf = self.read_buf

            limit = n_bytes if (n_bytes is not None) else (len(_read_buf) + 1)
            while len(_read_buf) < limit:
                try:
                    temp = self.lowerlevel_conn.recv_bytes()
                    _read_buf += temp
                except EOFError:
                    return b""  # Indicate EOF with an empty.
                except Exception as e:
                    sub_warning(f"{self.__class__.__name__}.read exception: e={e}")
                    raise

            cutpoint = n_bytes if (n_bytes is not None) else len(_read_buf)
            retval, self.read_buf = _read_buf[:cutpoint], _read_buf[cutpoint:]

            return retval

        def close(self, *args, **kwargs):
            sub_debug(f"{self.__class__.__name__}.close called with ({args}, {kwargs}) by os.getpid()={os.getpid()}")
            self.lowerlevel_conn.close()
            super().close(*args, **kwargs)  # TODO: Remove once no longer employing file descriptors at all.


inspections = False


class DragonConnection(multiprocessing.connection.Connection):
    _write = ConnGlue.write
    _read = ConnGlue.read

    def __init__(self, handle: ConnGlue, readable=True, writable=True):
        debug(
            f"{self.__class__.__name__}.__init__(handle={handle}, readable={readable}, writable={writable}) was "
            f"called!"
        )
        if not readable and not writable:
            raise ValueError("at least one of `readable` and `writable` must be True")
        # TODO: Extra processing of handle as appropriate for dragon.
        # self._handle = handle.__index__()
        # assert self._handle >= 0
        self._handle = handle
        self._readable = readable
        self._writable = writable

    def _close(self, _close=ConnGlue.close):
        debug(f"{self.__class__.__name__}.close() was called on {self._handle}!")
        try:
            _close(self._handle)
        except OSError:
            # TEMPORARY to see if we can make things work for now with Popen.DupFd
            pass

    def _send(self, buf, write=_write):
        "TODO: Wrapper to pick up dragon-specific _write."
        return super()._send(buf, write)

    def _recv(self, size, read=_read):
        "TODO: Wrapper to pick up dragon-specific _read."
        return super()._recv(size, read)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._handle}, readable={self._readable}, writable={self._writable})"

    def fileno(self):
        # TODO: Eliminate once file descriptors not used and fix connection.wait()
        # Leveraged by multiprocessing.connection.wait() which in turn
        # indirectly calls selectors._fileobj_to_fd().
        info("DragonConnection.fileno() still being called; mind the TODO here")
        retval = super().fileno()
        fd = ConnGlue.extract_fd(retval)
        return fd


def Pipe(duplex=True, *, original=None, use_base_impl=True):
    """Generate two Connection objects to be used by only one process each.

    :param duplex: If True, both ends of the Pipe may send and receive. Optional, defaults to True.
    :type duplex: bool
    :return: tuple of connection objects
    """
    if use_base_impl:
        if original == None:
            raise NameError(f"Dragon patch of Multiprocessing not correct.")
        else:
            return original(duplex)
    else:
        return DragonPipe(duplex)


class DragonListener(multiprocessing.connection.Listener):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"DragonListener is not implemented, yet")
        super().__init__(*args, **kwargs)


class BaseImplListener(multiprocessing.connection.Listener):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


def DragonClient(address, family=None, authkey=None):
    raise NotImplementedError(f"DragonClient is not implemented, yet")


def Listener(address=None, family=None, backlog=1, authkey=None, *, ctx=None, use_base_impl=True):
    if use_base_impl:
        return BaseImplListener(address=address, family=family, backlog=backlog, authkey=authkey)
    else:
        return DragonListener(address=address, family=family, backlog=backlog, authkey=authkey)


def Client(address, family=None, authkey=None, *, original=None, use_base_impl=True):
    if use_base_impl:
        if original == None:
            raise NameError(f"Dragon patch of Multiprocessing not correct.")
        else:
            return original(address, family=family, authkey=authkey)
    else:
        return DragonClient(address, family=family, authkey=authkey)


def reduce_connection(conn):
    debug(f"reduce_connection(conn={conn}) invoked by os.getpid()={os.getpid()}")
    # Switching from reduction.DupFd to get_spawning_popen().DupFd has the
    # consequence that the file descriptor will not be appended to the list in
    # the Popen instance's _fds attribute.  Only file descriptors in that _fds
    # list will be passed to util.spawnv_passfds() for marking as inheritable
    # and closing prior to vfork (spawning the child process).
    df = multiprocessing.context.reduction.DupFd(conn.fileno())
    # TODO: Replace the above line with the following one when
    #       DragonConnection objects no longer employ true file descriptors.
    # df = multiprocessing.context.get_spawning_popen().DupFd(conn.fileno())
    return rebuild_connection, (df, conn.readable, conn.writable, conn._handle)


def rebuild_connection(df, readable, writable, handle):
    fd = df.detach()  # TODO: adjust for DragonConnection having no file descriptor
    debug(
        f'rebuild_connection({df}->{getattr(fd, "__index__", int)()}, readable={readable}, writable={writable}) invo'
        f"ked by os.getpid()={os.getpid()}"
    )
    if not isinstance(multiprocessing.connection.os, PseudoOS):
        # Ensure child processes have this activated to aid tracking/debugging.
        activate_monkeypatch_connection_os()
    # Original returned Connection(), necessitating monkeypatching of
    # Connection until this function was reimplemented for DragonConnection.
    return DragonConnection(handle, readable, writable)


multiprocessing.connection.reduction.register(DragonConnection, reduce_connection)


class PseudoOS:
    __os = os

    def __getattr__(self, name):
        info(f"{self.__class__.__name__}.{name} requested")
        return getattr(self.__os, name)

    def write(self, *args, **kwargs):
        sub_warning(f"{self.__class__.__name__}.write called")
        return self.__os.write(*args, **kwargs)

    def read(self, *args, **kwargs):
        sub_warning(f"{self.__class__.__name__}.read called")
        return self.__os.read(*args, **kwargs)

    def close(self, *args, **kwargs):
        sub_warning(f"{self.__class__.__name__}.close called with ({args}, {kwargs})")
        return self.__os.close(*args, **kwargs)

    def pipe(self):
        retval = self.__os.pipe()
        sub_warning(f"{self.__class__.__name__}.pipe called, returning: {retval}")
        if inspections:
            frame = inspect.currentframe()
            pprint(f"{inspect.getouterframes(frame)}")
        return retval


def activate_monkeypatch_connection_os():
    multiprocessing.connection.os = PseudoOS()
