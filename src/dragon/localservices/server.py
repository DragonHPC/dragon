import os
import sys
import errno
import traceback
import time
import logging
import queue
import threading
import io
import subprocess
import json
import collections
import selectors
import psutil
from dataclasses import dataclass
import tempfile
import shutil

from .. import channels as dch
from .. import managed_memory as dmm
from .. import dtypes as dt
from .. import fli
from ..rc import DragonError

from .. import pmod
from .. import utils as dutils
from ..infrastructure import messages as dmsg
from ..infrastructure import util as dutil
from ..infrastructure import facts as dfacts
from ..infrastructure import parameters as parms
from ..infrastructure import connection as dconn
from ..infrastructure import parameters as dp
from ..infrastructure.queue import InfraQueue
from ..infrastructure.gpu_desc import apply_gpu_affinity

## This is defined only if the build used PMIx.
## If it didn't passing shouldn't cause issue
try:
    from ..dpmix import DragonPMIxJob
except ImportError:
    DragonPMIxJob = None

from ..dlogging import util as dlog
from ..dlogging.util import DragonLoggingServices as dls
from ..utils import B64, b64encode, b64decode
from typing import Optional

_TAG = 0
_TAG_LOCK = threading.Lock()


def get_new_tag():
    global _TAG
    with _TAG_LOCK:
        tmp = _TAG
        _TAG += 1
    return tmp


ProcessProps = collections.namedtuple(
    "ProcessProps",
    [
        "p_uid",
        "critical",
        "r_c_uid",
        "stdin_req",
        "stdout_req",
        "stderr_req",
        "stdin_connector",
        "stdout_connector",
        "stderr_connector",
        "layout",
        "local_cuids",
        "local_muids",
        "creation_msg_tag",
    ],
)


@dataclass
class ExternalProcessProps:
    """Tracks resources for a co-located external process provisioned by LS."""

    p_uid: int
    os_pid: int
    local_cuids: set
    local_muids: set
    is_external: bool = True


@dataclass
class ExternalRegistration:
    attempt_id: str
    p_uid: int
    os_pid: int
    process_start_time: str
    ls_return_qd: str
    gs_return_cuid: int
    gs_return_muid: int
    gs_return_cd: str
    gs_return_qd: str
    state: str = "LOCAL_PREPARED"


def _with_dragon_dyld_path(env):
    """Return a copy of `env` with the Dragon lib dir prepended to DYLD_LIBRARY_PATH.

    macOS dyld does not expand the $ORIGIN token used by the rpaths built into the
    Dragon binaries, so @rpath/libdragon.so only resolves via DYLD_LIBRARY_PATH.
    """
    the_env = dict(os.environ if env is None else env)
    lib_dir = str(dfacts.DRAGON_LIB_DIR)
    dyld_path = the_env.get("DYLD_LIBRARY_PATH", "")
    dyldfallback_path = the_env.get("DYLD_FALLBACK_LIBRARY_PATH", "")

    if lib_dir not in dyld_path.split(os.pathsep):
        the_env["DYLD_LIBRARY_PATH"] = os.pathsep.join(filter(None, [lib_dir, dyld_path]))

    if lib_dir not in dyldfallback_path.split(os.pathsep):
        the_env["DYLD_FALLBACK_LIBRARY_PATH"] = os.pathsep.join(filter(None, [lib_dir, dyldfallback_path]))

    return the_env


class PopenProps(subprocess.Popen):
    def __init__(self, props: ProcessProps, *args, **kwds):
        assert isinstance(props, ProcessProps)
        self.props = props
        # if your kwds are going to have a non-None env then it needs to be edited before the process is initialized
        if props.layout is not None:
            if props.layout.gpu_core and props.layout.accelerator_env:
                # gpu_core list must be turned into a string in the form "0,1,2" etc
                if isinstance(kwds["env"], dict):
                    apply_gpu_affinity(kwds["env"], props.layout.gpu_core, props.layout.accelerator_env)
                else:
                    # this might be unnecessary because the environment might always be a dict.
                    apply_gpu_affinity(os.environ, props.layout.gpu_core, props.layout.accelerator_env)

        try:
            cores = ""
            # This set the core string to the proper value for the dragon-popen which sets the affinity
            # on itself (once started) and then execvp's to start the actual code. In this way a process sets
            # its own affinity according to the policy before executing any user code.
            if props.layout is not None and props.layout.cpu_core:
                cores = " ".join([str(core) for core in props.layout.cpu_core])
            else:
                cores = " ".join([str(core) for core in range(dutils.get_cpu_count())])

        except Exception as ex:
            # TODO: this should be possibly be logged, but it can fail here if the process is short lived enough
            pass

        # args is a tuple where args[0] is really the list of args. Hence the odd looking wrapper_args.
        wrapper_args = (([dfacts.POPEN_WRAPPER, cores] + args[0]),)

        # dragon-popen resolves libdragon.so through @rpath, but the embedded rpaths use the
        # Linux $ORIGIN token which dyld does not expand. Every process LS starts goes through
        # this wrapper, so point dyld at the Dragon lib dir here rather than at each call site.
        if sys.platform == "darwin":
            kwds["env"] = _with_dragon_dyld_path(kwds.get("env"))

        super().__init__(*wrapper_args, **kwds)
        # Assuming this is basically a free call, default the afinity to "everything" just in case

        # XXX Affinity settings are only inherited by grandchild processes
        # XXX created after this point in time. Any grandchild processes
        # XXX started when the child process starts up most certainly will
        # XXX not have the desired affinity. To guarantee CPU affinity settings
        # XXX processes should be launched with e.g. taskset(1) or otherwise
        # XXX be configured to set their own affinity as appropriate.


class PMIxGroupResources:
    def __init__(self, ls_ch=None, ret_ch=None, buffered_resp_ch=None):
        self._pmix_server_up = False

        self.ls_ch = ls_ch
        self.ret_ch = ret_ch
        self.buffered_resp_ch = buffered_resp_ch
        self._job = None
        self._job_destroyed = False

    @property
    def job(self):
        return self._job

    @job.setter
    def job(self, job: DragonPMIxJob):
        self._job = job

    def destroy_job(self):
        try:
            self._job.finalize_job()
        except Exception as e:
            raise RuntimeError("Unable to cleanup PMIx jobs: %s", e)

        try:
            self.ls_ch.destroy()
        except Exception:
            pass

        try:
            self.ret_ch.destroy()
        except Exception:
            pass

        try:
            self.buffered_resp_ch.destroy()
        except Exception:
            pass

        self._job_destroyed = True

    def destroy_server(self):
        self._job.finalize_server()


class TerminationException(Exception):
    pass


class InputConnector:
    def __init__(self, conn: dconn.Connection):
        self._proc = None
        self._conn = conn
        self._log = logging.getLogger("LS.input connector")
        self._closed = False

    def add_proc_info(self, proc: PopenProps):
        self._proc = proc

    def poll(self):
        return self._conn.poll()

    def forward(self):
        try:
            if self._closed:
                return False

            data = None
            while self._conn.poll(timeout=0):
                data = self._conn.recv()
                self._proc.stdin.write(data.encode("utf-8"))
                self._proc.stdin.flush()
                self._log.info("Stdin data that was written:%s" % data)
        except EOFError as ex:
            return True
        except TimeoutError:
            return False
        except:
            self._log.info(
                "The input could not be forwarded from cuid=%s to process pid=%s"
                % (self._conn.inbound_chan.cuid, self._proc.pid)
            )

        return False

    def __hash__(self):
        return hash(self._conn.inbound_channel.cuid)

    def __eq__(self, other):
        # since all cuid's are unique we can compare cuids. In addition,
        # if we want we can ask if a cuid is in a set of connectors in O(1) time.
        return hash(self) == hash(other)

    def close(self):
        if self._closed:
            return

        try:
            if self._conn is not None:
                if self.poll():
                    self.forward()
                self._conn.close()
        except:
            pass

        try:
            self._proc.stdin.flush()
            self._proc.stdin.close()
        except:
            pass

        self._closed = True

    @property
    def proc_is_alive(self):
        return self._proc.returncode is None

    @property
    def conn(self):
        return self._conn

    @property
    def cuid(self):
        if self._conn is None:
            return None

        return self._conn.inbound_channel.cuid


class OutputConnector:
    def __init__(
        self, be_in, puid, hostname, out_err, conn: dconn.Connection = None, root_proc=None, critical_proc=False
    ):
        self._be_in = be_in
        self._puid = puid
        self._hostname = hostname
        self._out_err = out_err
        self._conn = conn
        self._root_proc = root_proc
        self._log = logging.getLogger("LS.output connector")
        self._writtenTo = False
        self._critical_proc = critical_proc
        self._closed = False
        self._proc = None
        self._fwd_stdout = True
        self._fwd_stderr = True

    def __hash__(self):
        return hash(self._conn.outbound_channel.cuid)

    def __eq__(self, other):
        # since all cuid's are unique we can compare cuids. In addition,
        # if we want we can ask if a cuid is in a set of connectors in O(1) time.
        return hash(self) == hash(other)

    def end_stderr_forwarding(self):
        """Tells the connector to stop forwarding stderr to the frontend"""
        self._fwd_stderr = False

    def end_stdout_forwarding(self):
        """Tells the connector to stop forwarding stdout to the frotnend"""
        self._fwd_stdout = False

    def _sendit(self, block):
        if self._closed:
            return

        if len(block) > 0:
            self._writtenTo = True

        forward_msg = True
        if self._fwd_stderr is False and self._out_err == dmsg.LSFwdOutput.FDNum.STDERR.value:
            forward_msg = False
        elif self._fwd_stdout is False and self._out_err == dmsg.LSFwdOutput.FDNum.STDOUT.value:
            forward_msg = False

        if forward_msg:
            if self._conn is None:
                try:
                    self._be_in.put(
                        dmsg.LSFwdOutput(
                            tag=get_new_tag(),
                            idx=parms.this_process.index,
                            p_uid=self._puid,
                            data=block,
                            fd_num=self._out_err,
                            pid=self._proc.pid,
                            hostname=self._hostname,
                        )
                    )
                except Exception:
                    pass

                return

            try:
                # A process has requested that output be forwarded from this process to it.
                # Because a connection could be owned by a parent and if the parent
                # exits the connection could be destroyed, we'll use exception handling
                # here and as a backup, we'll forward lost output to the launcher.
                self._conn.send(block)
            except Exception:
                try:
                    self._be_in.put(
                        dmsg.LSFwdOutput(
                            tag=get_new_tag(),
                            idx=parms.this_process.index,
                            p_uid=self._puid,
                            data="[orphaned output]: " + block,
                            fd_num=self._out_err,
                            pid=self._proc.pid,
                            hostname=self._hostname,
                        )
                    )
                except Exception:
                    pass

    def add_proc_info(self, proc):
        self._proc = proc

    def forward(self, data):
        if "kj/filesystem" in data or ": warning: PWD environment" in data:
            return

        if self._conn is not None:
            while len(data) > 300:
                chunk = data[:300]
                self._sendit(chunk)
                data = data[300:]

            if len(data) > 0:
                self._sendit(data)
        else:
            self._sendit(data)

    def flush(self):
        is_stderr = self._out_err == dmsg.LSFwdOutput.FDNum.STDERR.value

        try:
            file_obj = self.file_obj

            if file_obj is not None:
                io_data = file_obj.read(dmsg.LSFwdOutput.MAX)
            else:
                io_data = None

        except EOFError:
            io_data = None
        except ValueError:  # race, file could be closed
            io_data = None
        except OSError:  # race, file is closed
            io_data = None

        if not io_data:  # at EOF because we just selected
            return True  # To indicate EOF

        try:
            str_data = io_data.decode()
        except:
            str_data = str(io_data)

        # This is temporary and code to ignore warnings coming from capnproto. The
        # capnproto library has been modified to not send the following warning.
        # kj/filesystem-disk-unix.c++:1734: warning: PWD environment variable ...
        # The warning does not show up normally but does in our build pipeline for
        # now until the pycapnp project is updated. So we eliminate it here for now.
        if "kj/" in str_data and ": warning:" in str_data:
            return False

        if not is_stderr and self._puid == dfacts.GS_PUID:
            raise TerminationException(str_data)
        else:
            self.forward(str_data)

        if self._critical_proc and is_stderr:
            raise TerminationException(str_data)

        return False

    def close(self):
        if self._closed:
            return

        self.flush()

        try:
            self.file_obj.close()
        except:
            pass

        if not self._root_proc:
            # Don't call close on the connection unless a root proc.
            # Children share the same connection so we don't want to
            # close it from a child.
            self._closed = True
            return

        if self._conn is not None and not self._writtenTo:
            # If it is not written to yet, the connection must be
            # written to before it is closed so that EOF gets signaled
            # for the receiving process.
            try:
                self._conn.send("")
            except:
                pass
            self._writtenTo = True

        if self._conn is not None:
            try:
                self._conn.close()
            except:
                pass

        self._closed = True

    @property
    def file_obj(self):
        is_stderr = self._out_err == dmsg.LSFwdOutput.FDNum.STDERR.value

        if is_stderr:
            return self._proc.stderr

        return self._proc.stdout

    @property
    def proc_is_alive(self):
        return self._proc.returncode is None

    @property
    def puid(self):
        return self._puid

    @property
    def conn(self):
        return self._conn

    @property
    def cuid(self):
        if self._conn is None:
            return None
        return self._conn.outbound_channel.cuid


class FileOutputConnector:
    """Writes subprocess pipe output directly to a file on disk.

    Two-hop architecture: subprocess -> PIPE -> FileOutputConnector -> file.
    The subprocess always uses subprocess.PIPE so the selector in watch_output
    can detect available data. This connector reads from the pipe and writes
    to the specified file.

    Thread-safe via internal lock (watch_output and watch_death both call flush).
    """

    def __init__(self, file_handle, puid, out_err):
        """
        :param file_handle: Already-opened file in "a" mode.
        :param puid: Process UID for identification.
        :param out_err: dmsg.LSFwdOutput.FDNum.STDOUT.value or STDERR.value
        """
        self._file_handle = file_handle
        self._puid = puid
        self._out_err = out_err
        self._proc = None
        self._closed = False
        self._lock = threading.Lock()
        self._log = logging.getLogger("LS.file output connector")

    def add_proc_info(self, proc):
        self._proc = proc

    def end_stderr_forwarding(self):
        """No-op for file mode. File captures everything intentionally."""
        pass

    def end_stdout_forwarding(self):
        """No-op for file mode. File captures everything intentionally."""
        pass

    def flush(self):
        """Read available data from subprocess pipe, write to file.

        Returns True on EOF, False otherwise (same contract as OutputConnector.flush).
        """
        with self._lock:
            if self._closed:
                return True

            try:
                file_obj = self.file_obj
                # not limiting read size here since the file is on disk and we want to flush out as much as possible each time to avoid keeping too much in memory. If this becomes an issue we can always add a max read size later.
                io_data = file_obj.read(dfacts.DEFAULT_FILEOUTPUTCONNECTOR_MAX_READ) if file_obj else None
            except (EOFError, ValueError, OSError):
                io_data = None

            if not io_data:
                return True  # EOF

            try:
                str_data = io_data.decode()
            except:
                str_data = str(io_data)

            # This is temporary and code to ignore warnings coming from capnproto. The
            # capnproto library has been modified to not send the following warning.
            # kj/filesystem-disk-unix.c++:1734: warning: PWD environment variable ...
            # The warning does not show up normally but does in our build pipeline for
            # now until the pycapnp project is updated. So we eliminate it here for now.
            if "kj/" in str_data and ": warning:" in str_data:
                return False

            try:
                self._file_handle.write(str_data)
                self._file_handle.flush()
            except OSError as e:
                self._log.warning("Failed to write to output file: %s", e)

            return False

    def close(self):
        """Close the output file handle. Idempotent (handles double-close from shared connector)."""
        with self._lock:
            if self._closed:
                return

            # Drain remaining data from subprocess pipe
            try:
                file_obj = self.file_obj
                if file_obj:
                    remaining = file_obj.read(dfacts.DEFAULT_FILEOUTPUTCONNECTOR_MAX_READ)
                    try:
                        str_data = remaining.decode()
                    except:
                        str_data = str(remaining)

                    # This is temporary and code to ignore warnings coming from capnproto. The
                    # capnproto library has been modified to not send the following warning.
                    # kj/filesystem-disk-unix.c++:1734: warning: PWD environment variable ...
                    # The warning does not show up normally but does in our build pipeline for
                    # now until the pycapnp project is updated. So we eliminate it here for now.
                    if "kj/" in str_data and ": warning:" in str_data:
                        return False

                    if remaining:
                        self._file_handle.write(str_data)
            except (EOFError, ValueError, OSError):
                pass

            try:
                self._file_handle.flush()
                self._file_handle.close()
            except OSError:
                pass

            self._closed = True

    @property
    def file_obj(self):
        """Returns the subprocess pipe file object (for selector registration).

        This is the subprocess pipe fd, NOT the output file handle.
        """
        is_stderr = self._out_err == dmsg.LSFwdOutput.FDNum.STDERR.value
        return self._proc.stderr if is_stderr else self._proc.stdout

    @property
    def proc_is_alive(self):
        return self._proc.returncode is None

    @property
    def puid(self):
        return self._puid


def mk_response_pairs(resp_cl, ref):
    err_cl = resp_cl.Errors

    def success_resp(**kwargs):
        return resp_cl(tag=get_new_tag(), ref=ref, err=err_cl.SUCCESS, **kwargs)

    def fail_resp(msg, **kwargs):
        return resp_cl(tag=get_new_tag(), ref=ref, err=err_cl.FAIL, err_info=msg, **kwargs)

    return success_resp, fail_resp


def mk_output_connection_over_channel(ch_desc):
    channel_descriptor = B64.str_to_bytes(ch_desc)
    the_channel = dch.Channel.attach(channel_descriptor)
    return dconn.Connection(
        outbound_initializer=the_channel,
        options=dconn.ConnectionOptions(min_block_size=512),
        policy=dp.POLICY_INFRASTRUCTURE,
    )


def mk_input_connection_over_channel(ch_desc):
    channel_descriptor = B64.str_to_bytes(ch_desc)
    the_channel = dch.Channel.attach(channel_descriptor)
    return dconn.Connection(
        inbound_initializer=the_channel,
        options=dconn.ConnectionOptions(min_block_size=512),
        policy=dp.POLICY_INFRASTRUCTURE,
    )


class AvailableLocalCUIDS:
    """Internal only class that manages Process Local CUIDs"""

    # We reserve dfacts.MAX_NODES cuids for the main channel for each local services.
    LOCAL_CUID_RANGE = (dfacts.BASE_LS_CUID + dfacts.RANGE_LS_CUID) - dfacts.MAX_NODES
    AVAILABLE_PER_NODE = LOCAL_CUID_RANGE // dfacts.MAX_NODES

    def __init__(self, node_index):
        if node_index >= dfacts.MAX_NODES:  # 0 <= node_index < MAX_NODES
            raise RuntimeError(
                "A Local Services nodes has node_index=%s which is greater than max allowed." % node_index
            )

        self._node_index = node_index
        self._active = set()
        self._initial_cuid = (
            dfacts.BASE_LS_CUID + dfacts.MAX_NODES + node_index * AvailableLocalCUIDS.AVAILABLE_PER_NODE
        )
        self._next_available_cuid = self._initial_cuid
        self._last_available = self._initial_cuid + AvailableLocalCUIDS.AVAILABLE_PER_NODE - 1

    @property
    def next(self):
        # Re-using cuids is dangerous. If a process is still using a channel that has been destroyed,
        # it may end up writing into a newer channel with the same cuid. We will insure this likely
        # never happens by not re-using cuids until we run out. We should never run out. There are
        # a few less than 2**39 cuids available for each node.
        if len(self._active) == AvailableLocalCUIDS.AVAILABLE_PER_NODE:
            raise RuntimeError("Ran out of Process Local CUIDs")

        searching = True
        while searching:
            cuid = self._next_available_cuid
            if not cuid in self._active:
                searching = False

            self._next_available_cuid += 1

            if self._next_available_cuid > self._last_available:
                # We can pretty safely wrap around when we reach 2**35
                self._next_available_cuid = self._initial_cuid

        self._active.add(cuid)
        return cuid

    def reclaim(self, cuids):
        self._active.difference_update(cuids)

    def is_available(self, cuid):
        return not cuid in self._active

    def reserve(self, cuid):
        if not self.is_available(cuid):
            raise RuntimeError("Attempted reservation for local cuid %s which is already in use." % cuid)
        self._active.add(cuid)


class ExternalPUIDExhausted(RuntimeError):
    pass


class AvailableExternalPUIDS:
    """Monotonic, non-recycling allocator for this node's external PUID block."""

    def __init__(self, node_index):
        self._first, self._limit = dfacts.external_puid_bounds(node_index)
        self._next = self._first
        self._lock = threading.Lock()

    @property
    def next(self):
        with self._lock:
            if self._next == self._limit:
                raise ExternalPUIDExhausted("external PUID block is exhausted")
            puid = self._next
            self._next += 1
            return puid

    @property
    def issued(self):
        with self._lock:
            return self._next - self._first

    @property
    def remaining(self):
        with self._lock:
            return self._limit - self._next


class AvailableLocalMUIDS:
    """Internal only class that manages Process Local MUIDs"""

    def __init__(self, node_index):
        if node_index >= dfacts.MAX_NODES:  # 0 <= node_index < MAX_NODES
            raise RuntimeError(
                "A Local Services nodes has node_index=%s which is greater than max allowed." % node_index
            )

        self._node_index = node_index
        self._initial_muid = dfacts.local_pool_first_muid(node_index)
        self._next_available_muid = self._initial_muid
        self._last_available = self._initial_muid + dfacts.RANGE_LS_MUIDS_PER_NODE - 1
        self._active = set()

    @property
    def next(self):
        # Re-using muids is dangerous. If a process is still using a pool that has been destroyed,
        # it may end up writing into a newer pool with the same muid. We will insure this likely
        # never happens by not re-using muids until we run out. We should never run out. There are
        # 2**35 pool muids available for each node.
        if len(self._active) == dfacts.RANGE_LS_MUIDS_PER_NODE:
            raise RuntimeError("Ran out of Process Local MUIDs")

        searching = True
        while searching:
            muid = self._next_available_muid
            if not muid in self._active:
                searching = False

            self._next_available_muid += 1

            if self._next_available_muid > self._last_available:
                # We can pretty safely wrap around when we reach 2**35
                self._next_available_muid = self._initial_muid

        self._active.add(muid)
        return muid

    def reclaim(self, muids):
        self._active.difference_update(muids)

    def is_available(self, muid):
        return not muid in self._active

    def reserve(self, muid):
        if not self.is_available(muid):
            raise RuntimeError("Attempted reservation for local pool %s which is already in use." % muid)
        self._active.add(muid)


def send_fli_response(resp_msg, ser_resp_fli):
    resp_fli = fli.FLInterface.attach(b64decode(ser_resp_fli))
    sendh = resp_fli.sendh()
    sendh.send_bytes(resp_msg.serialize())
    sendh.close()
    resp_fli.detach()


class LocalServer:
    """Handles local services messages in normal processing.

    This object does not handle startup/teardown - instead
     it expects to be given whatever channels/pools have been made for it
     by startup, handles to what it needs to talk to in normal processing,
     and offers a 'run' and 'cleanup' method.
    """

    _DTBL = {}  # dispatch router, keyed by type of local services message

    SHUTDOWN_RESP_TIMEOUT = 0.010  # seconds, 10 ms
    QUIESCE_TIME = 1  # seconds, 1 second, join timeout for thread shutdown.
    OOM_SLOW_SLEEP_TIME = 5  # seconds, the oom_monitor sleeps that long in normal conditions
    OOM_RAPID_SLEEP_TIME = 1  # seconds, when nearing OOM

    def __init__(self, channels=None, pools=None, hostname="NONE", attach_auth_token=None):
        self.new_procs = queue.SimpleQueue()  # outbound PopenProps of newly started processes
        self.new_channel_input_monitors = queue.SimpleQueue()
        self.exited_channel_output_monitors = queue.SimpleQueue()
        self.exited_channel_input_monitors = queue.SimpleQueue()
        self.hostname = hostname
        self.attach_auth_token = attach_auth_token
        self.cuid_to_input_connector = {}
        self.node_index = parms.this_process.index
        self.local_cuids = AvailableLocalCUIDS(self.node_index)
        self.local_muids = AvailableLocalMUIDS(self.node_index)
        self.external_puids = AvailableExternalPUIDS(self.node_index)
        self.local_channels = dict()
        self.pmi_group_resources = dict()
        self.def_muid = dfacts.default_pool_muid_from_index(self.node_index)

        if channels is None:
            self.channels = {}  # key c_uid value channel
        else:
            self.channels = channels
        if pools is None:
            self.pools = {}  # key m_uid value memory pool
        else:
            self.pools = pools

        # This is the local services key/value store used for
        # bootstrapping on-node code.
        self.kvs = {}

        self.apt = {}  # active process table. key: pid, value PopenProps obj
        self.puid2pid = {}  # key: p_uid, value pid
        self.apt_lock = threading.Lock()

        # Tracks co-located external processes registered via LSExternalRegister.
        # key: os pid (int), value: ExternalProcessProps; protected by apt_lock.
        self.external_procs = {}
        self.external_registrations = {}
        self.adopted_external_cuids = set()

        self.shutdown_sig = threading.Event()
        self.gs_shutdown_sig = threading.Event()
        self.ta_shutdown_sig = threading.Event()
        self.stashed_threading_excepthook = None
        self.exit_reason = None

    def _logging_ex_handler(self, args):
        log = logging.getLogger("LS.fatal exception")
        ex_type, ex_value, ex_tb, thread = args
        if ex_type is SystemExit:
            return

        buf = io.BytesIO()
        wrap = io.TextIOWrapper(buf, write_through=True)
        traceback.print_exception(ex_type, ex_value, ex_tb, file=wrap)
        log.error("from %s:\n%s" % (thread.name, buf.getvalue().decode()))
        self._abnormal_termination("from %s:\n%s" % (thread.name, buf.getvalue().decode()))

    def make_local_channel(self):
        cuid = self.local_cuids.next
        def_pool = self.pools[self.def_muid]
        ch = dch.Channel(mem_pool=def_pool, c_uid=cuid)
        self.channels[cuid] = ch
        return ch

    def make_local_pool(self, msg):
        log = logging.getLogger("LS.local_pool")
        muid = self.local_muids.next
        name = "%s%s" % (msg.name, os.getpid())
        log.debug(
            "Creating local memory pool with muid=%s for process with msg.puid=%s and name=%s" % (muid, msg.puid, name)
        )
        pool = dmm.MemoryPool(msg.size, name, muid, pre_alloc_blocks=msg.preAllocs, min_block_size=msg.minBlockSize)
        self.pools[muid] = pool
        return pool

    def register_local_pool(self, msg):
        ser_bytes = b64decode(msg.serPool)
        muid, _ = dmm.MemoryPool.serialized_uid_fname(ser_bytes)
        self.local_muids.reserve(muid)
        pool = dmm.MemoryPool.attach(ser_bytes)
        assert muid == pool.muid
        self.pools[pool.muid] = pool
        return pool

    def deregister_local_pool(self, msg):
        ser_bytes = b64decode(msg.serPool)
        pool = dmm.MemoryPool.attach(ser_bytes)
        self.local_muids.reclaim(set([pool.muid]))
        del self.pools[pool.muid]
        return pool

    def set_shutdown(self, msg):
        log = logging.getLogger("LS.shutdown event")
        self.shutdown_sig.set()
        log.info("shutdown called after receiving %s" % repr(msg))

    def check_shutdown(self):
        return self.shutdown_sig.is_set()

    def set_gs_shutdown(self):
        log = logging.getLogger("LS.gs shutdown event")
        self.gs_shutdown_sig.set()
        log.info("set GS shutdown")

    def check_gs_shutdown(self):
        return self.gs_shutdown_sig.is_set()

    def set_ta_shutdown(self):
        log = logging.getLogger("LS.ta shutdown event")
        self.ta_shutdown_sig.set()
        log.info("set TA shutdown")

    def check_ta_shutdown(self):
        return self.ta_shutdown_sig.is_set()

    def __str__(self):
        with self.apt_lock:
            # External (attached) processes are in puid2pid but not apt.
            plist = ["\t%s:%s %s" % (p_uid, pid, self.apt.get(pid)) for p_uid, pid in self.puid2pid.items()]

        procs = "\n".join(plist)
        chans = " ".join(["%s" % k for k in self.channels.keys()])
        pools = " ".join(["%s" % k for k in self.pools.keys()])

        return "Procs:\n%s\nChans:\n%s\nPools:\n%s" % (procs, chans, pools)

    def add_proc(self, proc):
        with self.apt_lock:
            self.apt[proc.pid] = proc
            self.puid2pid[proc.props.p_uid] = proc.pid

        self.new_procs.put(proc)

    @staticmethod
    def clean_pools(pools, log):
        log.info("%s pools outstanding" % len(pools))
        for m_uid, pool in pools.items():
            try:
                pool.destroy()
            except (dmm.DragonMemoryError, dmm.DragonPoolError) as dpe:
                if dpe.lib_err != DragonError.MAP_KEY_NOT_FOUND:
                    log.warning("m_uid=%s failed: %s" % (m_uid, dpe))

    def _clean_procs(self):
        log = logging.getLogger("LS.kill procs")

        with self.apt_lock:
            log.info("%s processes outstanding" % len(self.apt))
            for p_uid, pid in self.puid2pid.items():
                # External (co-located) processes are tracked in external_procs,
                # not apt; LS did not spawn them so there is no PopenProps to
                # kill/wait on here.
                proc = self.apt.get(pid)
                if proc is None:
                    continue
                try:
                    proc.kill()

                    log.info("kill sent to p_uid=%s:proc.pid=%s" % (p_uid, proc.pid))
                except (subprocess.SubprocessError, OSError) as ose:
                    log.warning("kill on p_uid=%s: prod.pid=%s failed: %s" % (p_uid, proc.pid, ose))

            for proc in self.apt.values():
                try:
                    proc.wait(10)
                    log.info("Proc p_uid=%s:proc.pid=%s wait has completed." % (p_uid, proc.pid))

                except subprocess.SubprocessError as spe:
                    log.warning("wait on puid=%s failed: %s" % (proc.props.p_uid, spe))

                except Exception as ex:
                    log.warning("Got exception %s while waiting for proc to exit." % ex)

            self.puid2pid = {}
            self.apt = {}

    def cleanup(self):
        """Tries to destroy channels and pools and kill outstanding processes.

        None of the other threads should be running at this point.
        """
        log = logging.getLogger("LS.cleanup")

        log.info("start")

        # clean outstanding connectors
        self._output_connector_cleanup()
        self._input_connector_cleanup()

        # clean outstanding processes
        self._clean_procs()

        # We do not destroy channels because we are shutting down
        # and channels are in pools and are implicitly destroyed
        # right below. Destroying channels this late in shutdown
        # entails getting all the messages off each channel that
        # remain in the channel, which is necessary during normal
        # operations (to keep from having memory leaks on pools),
        # but is not necessary when shutting down. So we do not
        # destroy channels here. We just reset the channels dict.
        # HOWEVER, in tests we want to detach from channels so the
        # next run of local services is not confused by process-local
        # stale channel handles. So we detach here to prevent that.
        for cuid in self.channels:
            try:
                self.channels[cuid].detach()
            except:
                pass

        self.channels = {}

        self.clean_pools(self.pools, log)
        self.pools = {}

        log.info("end")

    def _abnormal_termination(self, error_str):
        """Triggers LS abnormal termination.
        Sends AbnormalTermination message to Launcher BE and logs.

        :param error_str: error message with the cause of abnormal termination
        :type error_str: string
        """
        log = logging.getLogger("LS.Abnormal termination")
        try:
            self.be_in.put(dmsg.AbnormalTermination(tag=get_new_tag(), err_info=error_str))
            log.critical("Abnormal termination sent to launcher be: %s" % error_str)
        except Exception as ex:
            log.exception("Abnormal termination exception: %s" % ex)

    def run(self, ls_in, gs_in, be_in, is_primary, ta_in=None, gw_channels=None):
        """Local services main function.

        :param ls_in: ls channel
        :type ls_in: Connection object
        :param gs_in: global services channel
        :type gs_in: Connection object
        :param be_in: launcher be channel
        :type be_in: Connection object
        :param is_primary: indicates if this is the primary LS or not
        :type is_primary: bool
        :param ta_in: transport agent channel, defaults to None
        :type ta_in: Connection object, optional
        :param gw_channels: list of gateway channels for multinode only, defaults to None
        :type gw_channels: list, optional
        """
        log = logging.getLogger("LS.ls run")
        log.info("start")

        if gw_channels is None:
            gw_channels = []

        self.ta_in = ta_in
        self.be_in = be_in
        self.gs_in = gs_in
        self.ls_in = ls_in
        self.is_primary = is_primary

        th = threading.Thread

        threads = [
            th(name="output mon", target=self.watch_output, daemon=True),
            th(name="watch death", target=self.watch_death, daemon=True),
            th(name="input mon", target=self.watch_input, daemon=True),
            th(name="OOM mon", target=self.watch_oom, daemon=True),
            th(name="watch external death", target=self.watch_external_death, daemon=True),
        ]

        self.stashed_threading_excepthook = threading.excepthook
        threading.excepthook = self._logging_ex_handler

        try:
            log.info("starting runtime service threads")
            for th in threads:
                th.start()
            log.info("runtime service threads started")
        except Exception as ex:
            self._abnormal_termination("ls run starting threads exception: %s" % ex)

        try:
            self.main_loop(ls_in)
        except Exception as ex:
            tb = traceback.format_exc()
            self._abnormal_termination("ls main loop exception: %s\n%s" % (ex, tb))

        threading.excepthook = self.stashed_threading_excepthook

        try:
            for th in threads:
                th.join(self.QUIESCE_TIME)
            for th in threads:
                if th.is_alive():
                    log.error("thread %s seems to have hung!" % th.name)
        except Exception as ex:
            self._abnormal_termination("ls run joining threads exception: %s" % ex)

        try:
            # Destroy gateway channels
            gw_count = 0
            for id, gw_ch in enumerate(gw_channels):
                gw_ch.destroy()
                try:
                    del os.environ[dfacts.GW_ENV_PREFIX + str(id + 1)]
                except KeyError:
                    pass
                gw_count += 1
            log.info("ls isPrimary=%s destroyed %s gateway channels" % (self.is_primary, gw_count))
        except Exception as ex:
            self._abnormal_termination("ls run destroying gateway channels exception: %s" % ex)

        try:
            # m12 Send LSHaltBE to BE
            # tell launcher be to shut mrnet down and detach from logging
            log.info("m12 transmitting final messsage from ls LSHaltBE")
            dlog.detach_from_dragon_handler(dls.LS)
            self.be_in.put(dmsg.LSHaltBE(tag=get_new_tag()))
        except Exception as ex:
            self._abnormal_termination("ls run sending LSHaltBE exception: %s" % ex)

        log.info("exit")

    def main_loop(self, ls_rh):
        """Monitors the main LS input channel and receives messages.
        If the received message is not one of the expected that are
        handled by corresponding route decorators, then it signals
        abnormal termination of LS.

        :param ls_rh: ls input channel
        :type ls_rh: Connection object
        """
        log = logging.getLogger("LS.main loop")
        log.info("start")

        while not self.check_shutdown():
            try:
                msg = ls_rh.get()
            except Exception as ex:
                log.info(f"LS Main Loop Got Exception: {ex=}")
                self._abnormal_termination("ls main loop get exception: %s" % ex)
                continue

            if msg is None:
                continue

            if type(msg) in LocalServer._DTBL:
                resp_msg = self._DTBL[type(msg)][0](self, msg=msg)
                if resp_msg is not None:
                    self._send_response(target_uid=msg.r_c_uid, msg=resp_msg)
            else:
                self._abnormal_termination("unexpected msg type in LocalServices: %s" % repr(msg))

        log.info("Main loop has exited. Now cleaning up local channels, pools, and other resources")

        # If we created a pmix server, clean it up now
        if bool(self.pmi_group_resources):
            for k, pmi_group_resources in self.pmi_group_resources.items():
                if isinstance(pmi_group_resources, PMIxGroupResources):
                    log.info("Destroying PMIx server associated with guid %s", k)
                    pmi_group_resources.destroy_server()
                    break
                if k == "PMOD_PATH":
                    log.info("Destroying PMOD directory")
                    shutil.rmtree(pmi_group_resources, ignore_errors=True)

        for proc in self.apt.values():
            self.cleanup_local_channels_pools(proc)
            if not proc.props.critical:
                proc.kill()

        log.info("Local channel and pool cleanup complete.")
        log.info("Exiting main_loop")

    def _send_response(self, target_uid, msg):
        """Sends response to either Global Services or Launcher BE
        depending on the target/return channel uid. Signals
        abnormal termination in the case of an unknown r_c_uid.

        :param target_uid: return channel uid, r_c_uid
        :type target_uid: int
        :param msg: message for the response
        :type msg: string
        """
        if target_uid == dfacts.GS_INPUT_CUID:
            self.gs_in.put(msg)
        elif target_uid == dfacts.launcher_cuid_from_index(parms.this_process.index):
            self.be_in.put(msg)
        else:
            self._abnormal_termination("unknown r_c_uid: %s" % repr(msg))

    @dutil.route(dmsg.LSPoolCreate, _DTBL)
    def create_pool(self, msg: dmsg.LSPoolCreate) -> None:
        log = logging.getLogger("LS.create pool")
        success, fail = mk_response_pairs(dmsg.LSPoolCreateResponse, msg.tag)

        error = ""
        if msg.m_uid in self.pools:
            error = "msg.m_uid=%s already in use" % msg.m_uid

        if not error:
            try:
                mpool = dmm.MemoryPool(msg.size, msg.name, msg.m_uid)
            except (dmm.DragonPoolError, dmm.DragonMemoryError) as dme:
                error = "%r failed: %s" % (msg, dme)

        if error:
            log.warning(error)
            resp_msg = fail(error)
        else:
            self.pools[msg.m_uid] = mpool
            encoded_desc = B64.bytes_to_str(mpool.serialize())
            resp_msg = success(desc=encoded_desc)

        return resp_msg

    @dutil.route(dmsg.LSPoolDestroy, _DTBL)
    def destroy_pool(self, msg: dmsg.LSPoolDestroy) -> None:
        log = logging.getLogger("LS.destroy pool")
        success, fail = mk_response_pairs(dmsg.LSPoolDestroyResponse, msg.tag)

        error = ""
        if msg.m_uid not in self.pools:
            error = "msg.m_uid=%s does not exist" % msg.m_uid

        if not error:
            mpool = self.pools.pop(msg.m_uid)
            try:
                mpool.destroy()
            except (dmm.DragonPoolError, dmm.DragonMemoryError) as dme:
                error = "%r failed: %s" % (msg, dme)

        if error:
            log.warning(error)
            resp_msg = fail(error)
        else:
            resp_msg = success()

        return resp_msg

    @dutil.route(dmsg.LSChannelCreate, _DTBL)
    def create_channel(self, msg: dmsg.LSChannelCreate) -> None:
        log = logging.getLogger("LS.create channel")
        log.info("Received an LSChannelCreate")
        success, fail = mk_response_pairs(dmsg.LSChannelCreateResponse, msg.tag)

        error = ""
        if msg.c_uid in self.channels:
            error = "msg.c_uid=%s already in use" % msg.c_uid

        if msg.m_uid not in self.pools:
            error = "msg.m_uid=%s does not exist" % msg.m_uid

        if not error:
            try:
                ch = dch.Channel(
                    mem_pool=self.pools[msg.m_uid],
                    c_uid=msg.c_uid,
                    block_size=msg.options.block_size,
                    capacity=msg.options.capacity,
                    semaphore=msg.options.semaphore,
                    bounded_semaphore=msg.options.bounded_semaphore,
                    initial_sem_value=msg.options.initial_sem_value,
                )
            except dch.ChannelError as cex:
                error = "%r failed: %s" % (msg, cex)

        if error:
            log.warning(error)
            resp_msg = fail(error)
        else:
            self.channels[msg.c_uid] = ch
            encoded_desc = B64.bytes_to_str(ch.serialize())
            resp_msg = success(desc=encoded_desc)
            log.info("Received and Created a channel via LSChannelCreate")

        return resp_msg

    def _local_resource_holder(self, puid):
        """Return the record tracking process-local cuids/muids for *puid*.

        For LS-spawned processes this is the PopenProps.props namedtuple; for
        attached external processes it is the ExternalProcessProps record. Both
        expose mutable ``local_cuids`` and ``local_muids`` sets. Returns None if
        *puid* has no live pid mapping.
        """
        pid = self.puid2pid.get(puid)
        if pid is None:
            return None
        proc = self.apt.get(pid)
        if proc is not None:
            return proc.props
        return self.external_procs.get(pid)

    @dutil.route(dmsg.LSCreateProcessLocalChannel, _DTBL)
    def create_process_local_channel(self, msg: dmsg.LSCreateProcessLocalChannel) -> None:
        log = logging.getLogger("LS.create local channel")
        log.info("Received an LSCreateProcessLocalChannel")

        # TODO: Add tracking of LS created local channels so we can destroy them as they become free
        if not msg.puid in self.puid2pid and msg.puid != 1:
            resp_msg = dmsg.LSCreateProcessLocalChannelResponse(
                tag=get_new_tag(),
                ref=msg.tag,
                err=DragonError.INVALID_ARGUMENT,
                errInfo=f"Cannot create channel for non-existent local process on node. PUID {msg.puid}",
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        try:
            ch = self.make_local_channel()
            holder = self._local_resource_holder(msg.puid)
            if holder is not None:
                holder.local_cuids.add(ch.cuid)
            self.local_channels[ch.cuid] = ch
        except dt.DragonException as dex:
            error = "%r failed: %s" % (msg, dex)
            resp_msg = dmsg.LSCreateProcessLocalChannelResponse(
                tag=get_new_tag(), ref=msg.tag, err=dex.lib_err, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return
        except Exception as ex:
            error = "%r failed: %s" % (msg, ex)
            resp_msg = dmsg.LSCreateProcessLocalChannelResponse(
                tag=get_new_tag(), ref=msg.tag, err=DragonError.INVALID_OPERATION, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        encoded_desc = b64encode(ch.serialize())
        resp_msg = dmsg.LSCreateProcessLocalChannelResponse(
            tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS, serChannel=encoded_desc
        )
        log.info("Received and Created a channel via LSCreateProcessLocalChannel with cuid=%s" % ch.cuid)
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.LSDestroyProcessLocalChannel, _DTBL)
    def destroy_process_local_channel(self, msg: dmsg.LSDestroyProcessLocalChannel) -> None:
        log = logging.getLogger("LS.destroy local channel")
        log.info("Received an LSDestroyProcessLocalChannel for cuid=%s" % msg.cuid)

        if not msg.puid in self.puid2pid:
            resp_msg = dmsg.LSDestroyProcessLocalChannelResponse(
                tag=get_new_tag(),
                ref=msg.tag,
                err=DragonError.INVALID_ARGUMENT,
                errInfo="Cannot destroy local channel for non-existent local process on node.",
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        try:
            if msg.cuid in self.local_channels:
                self.local_channels[msg.cuid].destroy()
                del self.local_channels[msg.cuid]
                holder = self._local_resource_holder(msg.puid)
                if holder is not None:
                    holder.local_cuids.discard(msg.cuid)
                self.local_cuids.reclaim([msg.cuid])

        except dt.DragonException as dex:
            error = "%r failed: %s" % (msg, dex)
            resp_msg = dmsg.LSDestroyProcessLocalChannelResponse(
                tag=get_new_tag(), ref=msg.tag, err=dex.lib_err, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return
        except Exception as ex:
            error = "%r failed: %s" % (msg, ex)
            resp_msg = dmsg.LSDestroyProcessLocalChannelResponse(
                tag=get_new_tag(), ref=msg.tag, err=DragonError.INVALID_OPERATION, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        resp_msg = dmsg.LSDestroyProcessLocalChannelResponse(tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS)
        log.info("Received and Destroyed a channel via LSDestroyProcessLocalChannel")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.LSCreateProcessLocalPool, _DTBL)
    def create_process_local_pool(self, msg: dmsg.LSCreateProcessLocalPool) -> None:
        log = logging.getLogger("LS.create local pool")
        log.info("Received an LSCreateProcessLocalPool")

        if not msg.puid in self.puid2pid:
            resp_msg = dmsg.LSCreateProcessLocalPoolResponse(
                tag=get_new_tag(),
                ref=msg.tag,
                err=DragonError.INVALID_ARGUMENT,
                errInfo="Cannot create pool for non-existent local process on node.",
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        try:
            pool = self.make_local_pool(msg)
            holder = self._local_resource_holder(msg.puid)
            if holder is not None:
                holder.local_muids.add(pool.muid)
        except dt.DragonException as dex:
            error = "%r failed: %s" % (msg, dex)
            resp_msg = dmsg.LSCreateProcessLocalPoolResponse(
                tag=get_new_tag(), ref=msg.tag, err=dex.lib_err, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return
        except Exception as ex:
            error = "%r failed: %s" % (msg, ex)
            resp_msg = dmsg.LSCreateProcessLocalPoolResponse(
                tag=get_new_tag(), ref=msg.tag, err=DragonError.INVALID_OPERATION, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        encoded_desc = b64encode(pool.serialize())
        resp_msg = dmsg.LSCreateProcessLocalPoolResponse(
            tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS, serPool=encoded_desc
        )
        log.info("Received and Created a pool via LSCreateProcessLocalPool")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.LSRegisterProcessLocalPool, _DTBL)
    def register_process_local_pool(self, msg: dmsg.LSRegisterProcessLocalPool) -> None:
        log = logging.getLogger("LS.register local pool")
        log.info("Received an LSRegisterProcessLocalPool")

        if not msg.puid in self.puid2pid:
            resp_msg = dmsg.LSRegisterProcessLocalPoolResponse(
                tag=get_new_tag(),
                ref=msg.tag,
                err=DragonError.INVALID_ARGUMENT,
                errInfo="Cannot register pool for non-existent local process on node.",
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        try:
            pool = self.register_local_pool(msg)
            holder = self._local_resource_holder(msg.puid)
            if holder is not None:
                holder.local_muids.add(pool.muid)
        except dt.DragonException as dex:
            error = "%r failed: %s" % (msg, dex)
            resp_msg = dmsg.LSRegisterProcessLocalPoolResponse(
                tag=get_new_tag(), ref=msg.tag, err=dex.lib_err, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return
        except Exception as cex:
            error = "%r failed: %s" % (msg, cex)
            resp_msg = dmsg.LSRegisterProcessLocalPoolResponse(
                tag=get_new_tag(), ref=msg.tag, err=DragonError.INVALID_OPERATION, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        resp_msg = dmsg.LSRegisterProcessLocalPoolResponse(tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS)
        log.info("Received and Registered a pool via LSRegisterProcessLocalPool")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.LSDeregisterProcessLocalPool, _DTBL)
    def deregister_process_local_pool(self, msg: dmsg.LSDeregisterProcessLocalPool) -> None:
        log = logging.getLogger("LS.deregister local pool")
        log.info("Received an LSDeregisterProcessLocalPool")

        if not msg.puid in self.puid2pid:
            resp_msg = dmsg.LSDeregisterProcessLocalPoolResponse(
                tag=get_new_tag(),
                ref=msg.tag,
                err=DragonError.INVALID_ARGUMENT,
                errInfo="Cannot deregister pool for non-existent local process on node.",
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        try:
            pool = self.deregister_local_pool(msg)
            holder = self._local_resource_holder(msg.puid)
            if holder is not None:
                holder.local_muids.discard(pool.muid)
            pool.detach()
        except dt.DragonException as dex:
            error = "%r failed: %s" % (msg, dex)
            resp_msg = dmsg.LSDeregisterProcessLocalPoolResponse(
                tag=get_new_tag(), ref=msg.tag, err=dex.lib_err, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return
        except Exception as ex:
            error = "%r failed: %s" % (msg, ex)
            resp_msg = dmsg.LSDeregisterProcessLocalPoolResponse(
                tag=get_new_tag(), ref=msg.tag, err=DragonError.INVALID_OPERATION, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        resp_msg = dmsg.LSDeregisterProcessLocalPoolResponse(tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS)
        log.info("Received and Deregistered a pool via LSDeregisterProcessLocalPool")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.LSChannelDestroy, _DTBL)
    def destroy_channel(self, msg: dmsg.LSChannelDestroy) -> None:
        log = logging.getLogger("LS.destroy channel")
        success, fail = mk_response_pairs(dmsg.LSChannelDestroyResponse, msg.tag)

        error = ""
        if msg.c_uid not in self.channels:
            error = "%s does not exist" % msg.c_uid

        if not error:
            ch = self.channels[msg.c_uid]
            try:
                ch.destroy()
            except dch.ChannelError as cex:
                error = "%r failed: %s" % (msg, cex)
            else:
                del self.channels[msg.c_uid]
                if msg.c_uid in self.adopted_external_cuids:
                    self.adopted_external_cuids.remove(msg.c_uid)
                    self.local_cuids.reclaim([msg.c_uid])

        if error:
            log.warning(error)
            resp_msg = fail(error)
        else:
            resp_msg = success()

        return resp_msg

    @dutil.route(dmsg.LSMultiProcessCreate, _DTBL)
    def create_group(self, msg: dmsg.LSMultiProcessCreate) -> None:
        log = logging.getLogger("LS.create_group")
        success, _ = mk_response_pairs(dmsg.LSMultiProcessCreateResponse, msg.tag)
        failed = False
        err_info = ""
        # Stand up the PMIx Server if PMIx backend has been requested
        try:
            base_rank = None
            log.debug("PMI group info multi proc create: %s", msg.pmi_group_info)
            if msg.pmi_group_info.backend == dfacts.PMIBackend.PMIX:
                my_node_id = msg.procs[0].pmi_info.pmix_nid  # This node's unique value

                # Parse the ranks->nidlist dict and get all the unique node ids out of it:
                pmix_nidlist = list(set(msg.pmi_group_info.nid_map.values()))

                # Create hostlist ordered by node id
                pmix_hostlist = [msg.pmi_group_info.host_map[str(nid)] for nid in pmix_nidlist]

                # Create a list of ranks where index is rank and value is node id that rank is on
                pmix_ranklist = [msg.pmi_group_info.nid_map[str(rank)] for rank in range(msg.pmi_group_info.nranks)]

                # Create a ppn for each node ordered by node id
                ppns = [len([nid for nid in pmix_ranklist if nid == current_id]) for current_id in pmix_nidlist]

                log.debug(
                    "Create PMIx resources for guid %s on node id %s | ppns %s | node_ids %s | hosts %s | rank-to-nid %s",
                    msg.guid,
                    my_node_id,
                    ppns,
                    pmix_nidlist,
                    pmix_hostlist,
                    pmix_ranklist,
                )

                # If we haven't created a PMIx server, we'll need to stand up a dictionary
                if not bool(self.pmi_group_resources):
                    self.pmi_group_resources["server_created"] = False
                self.pmi_group_resources[msg.guid] = PMIxGroupResources(
                    self.make_local_channel(), self.make_local_channel(), self.make_local_channel()
                )

                # Figure out if there's a local manager for the server:
                local_mgr_desc = None
                try:
                    log.debug("looking for PMIx ddict serialized descriptor %s", msg.pmi_group_info.pmix_desc)
                    local_mgr_desc = self.kvs[msg.pmi_group_info.pmix_desc]
                except KeyError:
                    log.debug("PMIx DDict: No local manager found for this server")

                log.debug("Starting PMIx server")
                if DragonPMIxJob is None:
                    failed = True
                    err_info = "A PMIx backend has been requested for an MPI process group, but this Dragon library was not built with PMIx support"
                    raise AttributeError(err_info)
                else:
                    log.debug("Creating PMIx job for guid %s", msg.guid)
                    self.pmi_group_resources[msg.guid].job = DragonPMIxJob(
                        msg.guid,
                        self.pmi_group_resources["server_created"],
                        msg.pmi_group_info.pmix_desc,
                        local_mgr_desc,
                        self.pmi_group_resources[msg.guid].ls_ch.serialize(),
                        self.pmi_group_resources[msg.guid].ret_ch.serialize(),
                        self.pmi_group_resources[msg.guid].buffered_resp_ch.serialize(),
                        my_node_id,
                        pmix_ranklist,
                        ppns,
                        pmix_nidlist,
                        pmix_hostlist,
                    )
                    log.debug("PMIx job created for guid %s", msg.guid)
                    if not self.pmi_group_resources["server_created"]:
                        self.pmi_group_resources["server_created"] = True
        # In case PMI stuff isn't happening
        except AttributeError:
            pass

        responses = []
        for process_create_msg in msg.procs:
            response = self.create_process(
                msg=process_create_msg, pmi_group_info=msg.pmi_group_info, base_rank=base_rank, guid=msg.guid
            )
            responses.append(response)
            if response.err == dmsg.LSProcessCreateResponse.Errors.FAIL:
                failed = True

        # always return success
        resp_msg = success(responses=responses, failed=failed, err_info=err_info)
        return resp_msg

    @dutil.route(dmsg.LSProcessCreate, _DTBL)
    def create_process(
        self,
        msg: dmsg.LSProcessCreate,
        pmi_group_info: Optional[dmsg.PMIGroupInfo] = None,
        base_rank: int = None,
        guid: int = None,
    ) -> None:
        log = logging.getLogger("LS.create process")
        success, fail = mk_response_pairs(dmsg.LSProcessCreateResponse, msg.tag)

        if msg.t_p_uid in self.puid2pid:
            error = "msg.t_p_uid=%s already exists" % msg.t_p_uid
            log.warning(error)
            self._send_response(target_uid=msg.r_c_uid, msg=fail(error))
            return

        if not msg.rundir:
            working_dir = None
        else:
            working_dir = msg.rundir

        # TODO for multinode: whose job is it to update which parameters?
        log.debug("The number of gateways per node is configured to %s" % parms.this_process.num_gw_channels_per_node)
        log.debug("Removing these from environment: %s" % parms.LaunchParameters.NODE_LOCAL_PARAMS)
        req_env = dict(msg.env)
        parms.LaunchParameters.remove_node_local_evars(req_env)

        # this is for kubernetes
        if "POD_UID" in req_env:
            del req_env["POD_UID"]
        if "POD_NAME" in req_env:
            del req_env["POD_NAME"]
        if "POD_IP" in req_env:
            del req_env["POD_IP"]
        if dfacts.DRAGON_LOGGER_SDESC in req_env:
            del req_env[dfacts.DRAGON_LOGGER_SDESC]

        the_env = dict(os.environ)
        the_env.update(req_env)

        nenv = len(the_env)
        for idx, (k, v) in enumerate(the_env.items()):
            log.debug("Env %s/%s -> %s: %s", idx, nenv, k, v)

        # Add in the local services return serialized channel descriptor.
        ls_return_ch = self.make_local_channel()
        ls_return_queue = InfraQueue(main_channel=ls_return_ch)
        the_env[dfacts.env_name(dfacts.LS_RET_QD)] = ls_return_queue.serialize()

        gs_ret_chan_resp = None
        stdin_conn = None
        stdin_resp = None
        stdout_conn = None
        stdout_resp = None
        stderr_conn = None
        stderr_resp = None
        stdout_root = False
        stderr_root = False
        stdout_file_handle = None
        stderr_file_handle = None

        if msg.gs_ret_chan_msg is not None:
            gs_ret_chan_resp = self.create_channel(msg.gs_ret_chan_msg)
            if gs_ret_chan_resp.err != dmsg.LSChannelCreateResponse.Errors.SUCCESS:
                resp_msg = fail("Failed creating the GS ret channel for new process: %s" % stdin_resp.err_info)
                return resp_msg
            gs_ret_chan = dch.Channel.attach(B64.str_to_bytes(gs_ret_chan_resp.desc))
            gs_ret_queue = InfraQueue(main_channel=gs_ret_chan)
            the_env[dfacts.ENV_GS_RET_QD] = gs_ret_queue.serialize()

        if msg.stdin_msg is not None:
            stdin_resp = self.create_channel(msg.stdin_msg)
            if stdin_resp.err != dmsg.LSChannelCreateResponse.Errors.SUCCESS:
                resp_msg = fail("Failed creating the stdin channel for new process: %s" % stdin_resp.err_info)
                return resp_msg
            stdin_conn = mk_input_connection_over_channel(stdin_resp.desc)

        if msg.stdout_msg is not None:
            stdout_resp = self.create_channel(msg.stdout_msg)
            if stdout_resp.err != dmsg.LSChannelCreateResponse.Errors.SUCCESS:
                # TBD: We need to destroy the stdin channel if it exists
                resp_msg = fail("Failed creating the stdout channel for new process: %s" % stdout_resp.err_info)
                return resp_msg
            stdout_conn = mk_output_connection_over_channel(stdout_resp.desc)
            stdout_root = True
            the_env[dfacts.STDOUT_DESC] = stdout_resp.desc
        elif dfacts.STDOUT_DESC in the_env:
            # Finding the STDOUT descriptor in the environment
            # means a parent requested PIPE and so all children
            # inherit this as well. If a child of the parent requested
            # PIPE itself, then it would have been caught above.
            stdout_conn = mk_output_connection_over_channel(the_env[dfacts.STDOUT_DESC])

        if msg.stderr_msg is not None:
            stderr_resp = self.create_channel(msg.stderr_msg)
            if stderr_resp.err != dmsg.LSChannelCreateResponse.Errors.SUCCESS:
                # TBD: We need to destroy the stdin and stdout channels if they exist
                resp_msg = fail("Failed creating the stderr channel for new process: %s" % stderr_resp.err_info)
                return resp_msg
            stderr_conn = mk_output_connection_over_channel(stderr_resp.desc)
            stderr_root = True
            the_env[dfacts.STDERR_DESC] = stderr_resp.desc
        elif msg.stderr == dmsg.STDOUT:
            if not isinstance(msg.stdout, str):
                # Existing behavior: stderr shares stdout channel
                stderr_conn = stdout_conn
                the_env[dfacts.STDERR_DESC] = stdout_resp.desc
        elif dfacts.STDERR_DESC in the_env:
            # Finding the STDERR descriptor in the environment
            # means inherit it. See above for STDOUT explanation.
            stderr_conn = mk_output_connection_over_channel(the_env[dfacts.STDERR_DESC])

        real_args = [msg.exe] + msg.args

        try:
            stdout = subprocess.PIPE
            if msg.stdout == subprocess.DEVNULL:
                # if user is requesting devnull, then we'll start
                # process that way. Otherwise, LS gets output
                # via PIPE forwards it where requested.
                stdout = subprocess.DEVNULL
            elif isinstance(msg.stdout, str):
                stdout_file_handle = open(msg.stdout, "a")

            stderr = subprocess.PIPE
            if msg.stderr == subprocess.DEVNULL:
                # Same handling as stdout explanation above.
                stderr = subprocess.DEVNULL
            elif isinstance(msg.stderr, str):
                stderr_file_handle = open(msg.stderr, "a")

            if msg.stderr == subprocess.STDOUT:
                stderr = subprocess.STDOUT

            if pmi_group_info and msg.pmi_info:
                log.debug("pmi_group_info=%s", str(pmi_group_info))
                log.debug("pmi_process_info=%s", str(msg.pmi_info))
                log.info("p_uid %s looking up pmod launch cuid" % msg.t_p_uid)

                node_index = parms.this_process.index
                inf_muid = dfacts.infrastructure_pool_muid_from_index(node_index)
                log.info("p_uid %s Setting required PMI environment variables", msg.t_p_uid)
                # For PBS, we need to tell PMI to not use a FD to get PALS info:

                the_env["MPICH_OFI_CXI_PID_BASE"] = str(msg.pmi_info.pid_base)
                the_env["DYLD_LIBRARY_PATH"] = (
                    str(dfacts.DRAGON_LIB_DIR) + ":" + str(the_env.get("DYLD_LIBRARY_PATH", ""))
                )
                the_env["FI_CXI_RX_MATCH_MODE"] = "hybrid"

                if pmi_group_info.backend == dfacts.PMIBackend.CRAY:
                    log.info("Establishing parameters for cray-pmi launch of MPI app")
                    pmod_launch_cuid = dfacts.pmod_launch_cuid_from_jobinfo(
                        dutils.host_id(), pmi_group_info.job_id, msg.pmi_info.lrank
                    )
                    log.info(
                        "p_uid %s Creating pmod launch channel using pmod_launch_cuid=%s", msg.t_p_uid, pmod_launch_cuid
                    )
                    pmod_launch_ch = dch.Channel(self.pools[inf_muid], pmod_launch_cuid)

                    # Create a PMOD sym link in a tmp directory if we don't already have one:
                    if "PMOD_PATH" not in self.pmi_group_resources:
                        pmod_path = tempfile.mkdtemp(prefix="dragon_pmod_")
                        self.pmi_group_resources["PMOD_PATH"] = str(pmod_path)
                        log.info("p_uid %s Creating PMOD directory at %s", msg.t_p_uid, pmod_path)
                        os.symlink(
                            os.path.join(dfacts.DRAGON_LIB_DIR, "libdragon.so"), os.path.join(pmod_path, "libpmod.so")
                        )
                        log.info(
                            "p_uid %s Creating libpmod.so symlink to libdragon.so in directory %s",
                            msg.t_p_uid,
                            pmod_path,
                        )

                    the_env["DRAGON_PMOD_CHILD_CHANNEL"] = str(dutils.B64(pmod_launch_ch.serialize()))
                    the_env["_DRAGON_PALS_ENABLED"] = "1"
                    the_env["DL_PLUGIN_RESILIENCY"] = "1"
                    the_env["LD_PRELOAD"] = str(dfacts.DRAGON_LIB_SO)

                    ld_paths = str(dfacts.DRAGON_LIB_DIR) + ":" + str(self.pmi_group_resources["PMOD_PATH"])

                    the_env["LD_LIBRARY_PATH"] = ld_paths + ":" + str(the_env.get("LD_LIBRARY_PATH", ""))
                    the_env["DYLD_LIBRARY_PATH"] = ld_paths + ":" + str(the_env.get("DYLD_LIBRARY_PATH", ""))
                    the_env["DYLD_FALLBACK_LIBRARY_PATH"] = the_env["DYLD_LIBRARY_PATH"]
                    the_env["PMI_CONTROL_PORT"] = str(pmi_group_info.control_port)
                    try:
                        del the_env["PMI_CONTROL_FD"]
                    except KeyError:
                        pass

                elif pmi_group_info.backend == dfacts.PMIBackend.PMIX:
                    log.info("Establishing parameters for PMIx launch of MPI app")
                    try:
                        the_env = self.pmi_group_resources[guid].job.get_client_env(the_env, msg.pmi_info.lrank)
                    except AttributeError:
                        resp_msg = fail("A PMIx backend was requested, but no PMIx server was found.")
                        return resp_msg

                    the_env["LD_LIBRARY_PATH"] = (
                        str(dfacts.DRAGON_LIB_DIR) + ":" + str(the_env.get("LD_LIBRARY_PATH", ""))
                    )
                    the_env["DYLD_LIBRARY_PATH"] = (
                        str(dfacts.DRAGON_LIB_DIR) + ":" + str(the_env.get("DYLD_LIBRARY_PATH", ""))
                    )
                    the_env["DYLD_FALLBACK_LIBRARY_PATH"] = (
                        str(dfacts.DRAGON_LIB_DIR) + ":" + str(the_env.get("DYLD_FALLBACK_LIBRARY_PATH", ""))
                    )

                    # We don't have support for GPUs in our PMIx server yet. For ANL MPICH, we need to manually
                    # disable GPU support during MPI_init:
                    the_env["MPIR_CVAR_ENABLE_GPU"] = "0"

                    nenv = len(the_env)
                    log.debug("PMIx backend nenv: %s", nenv)
                    for idx, (k, v) in enumerate(the_env.items()):
                        log.debug("Env %s/%s -> %s: %s", idx, nenv, k, v)

            if not dfacts.DRAGON_LIB_DIR in the_env.get("DYLD_LIBRARY_PATH", ""):
                the_env["DYLD_LIBRARY_PATH"] = (
                    str(dfacts.DRAGON_LIB_DIR) + ":" + str(the_env.get("DYLD_LIBRARY_PATH", ""))
                )

            stdin_connector = InputConnector(stdin_conn)

            if stdout_file_handle is not None:
                stdout_connector = FileOutputConnector(
                    file_handle=stdout_file_handle,
                    puid=msg.t_p_uid,
                    out_err=dmsg.LSFwdOutput.FDNum.STDOUT.value,
                )
            else:
                stdout_connector = OutputConnector(
                    be_in=self.be_in,
                    puid=msg.t_p_uid,
                    hostname=self.hostname,
                    out_err=dmsg.LSFwdOutput.FDNum.STDOUT.value,
                    conn=stdout_conn,
                    root_proc=stdout_root,
                    critical_proc=False,
                )

            if stderr_file_handle is not None:
                stderr_connector = FileOutputConnector(
                    file_handle=stderr_file_handle,
                    puid=msg.t_p_uid,
                    out_err=dmsg.LSFwdOutput.FDNum.STDERR.value,
                )
            elif msg.stderr == dmsg.STDOUT and stdout_file_handle is not None:
                # Shared connector - streams merged at OS level via subprocess.STDOUT
                stderr_connector = stdout_connector
            else:
                stderr_connector = OutputConnector(
                    be_in=self.be_in,
                    puid=msg.t_p_uid,
                    hostname=self.hostname,
                    out_err=dmsg.LSFwdOutput.FDNum.STDERR.value,
                    conn=stderr_conn,
                    root_proc=stderr_root,
                    critical_proc=False,
                )

            with self.apt_lock:  # race with death watcher; hold lock to get process in table.
                # The stdout_conn and stderr_conn will be filled in just below.
                the_proc = PopenProps(
                    ProcessProps(
                        p_uid=msg.t_p_uid,
                        critical=False,
                        r_c_uid=msg.r_c_uid,
                        stdin_req=msg.stdin,
                        stdout_req=msg.stdout,
                        stderr_req=msg.stderr,
                        stdin_connector=stdin_connector,
                        stdout_connector=stdout_connector,
                        stderr_connector=stderr_connector,
                        layout=msg.layout,
                        local_cuids={ls_return_ch.cuid},
                        local_muids=set(),
                        creation_msg_tag=msg.tag,
                    ),
                    real_args,
                    bufsize=0,
                    stdin=subprocess.PIPE,
                    stdout=stdout,
                    stderr=stderr,
                    cwd=working_dir,
                    env=the_env,
                )

                stdout_connector.add_proc_info(the_proc)
                stderr_connector.add_proc_info(the_proc)
                stdin_connector.add_proc_info(the_proc)

                if msg.stdin == dmsg.PIPE:
                    self.cuid_to_input_connector[msg.stdin_msg.c_uid] = stdin_connector
                    self.new_channel_input_monitors.put(stdin_connector)

                self.puid2pid[msg.t_p_uid] = the_proc.pid
                self.apt[the_proc.pid] = the_proc
                log.info("Now created process with args %s and pid=%s" % (real_args, the_proc.pid))

            if msg.pmi_info:
                if pmi_group_info.backend == dfacts.PMIBackend.CRAY:
                    pmod_nidlist = [pmi_group_info.nid_map[str(idx)] for idx in range(pmi_group_info.nranks)]
                    pmod_hostlist = [pmi_group_info.host_map[str(idx)] for idx in range(pmi_group_info.nnodes)]
                    log.debug(f"pmod_nidlist = {pmod_nidlist} | pmod_hostlist = {pmod_hostlist}")
                    log.info("p_uid %s sending cray-pmi mpi data for %s" % (msg.t_p_uid, msg.pmi_info.lrank))
                    pmod.PMOD(
                        msg.pmi_info.ppn,
                        msg.pmi_info.nid,
                        pmi_group_info.nnodes,
                        pmi_group_info.nranks,
                        pmod_nidlist,
                        pmod_hostlist,
                        pmi_group_info.job_id,
                    ).send_mpi_data(msg.pmi_info.lrank, pmod_launch_ch)
                    log.info("p_uid %s DONE: sending mpi data for %s" % (msg.t_p_uid, msg.pmi_info.lrank))

            log.info("p_uid %s created as %s" % (msg.t_p_uid, the_proc.pid))
            self.new_procs.put(the_proc)
            if msg.initial_stdin is not None and msg.initial_stdin != "":
                # we are asked to provide a string to the started process.
                log.info("Writing %s to newly created process" % msg.initial_stdin)
                proc_stdin = the_proc.stdin
                the_proc.stdin = None  # transfer ownership; avoids double-close of same fd (Python 3.13)
                proc_stdin_send = dutil.NewlineStreamWrapper(proc_stdin, read_intent=False)
                proc_stdin_send.send(msg.initial_stdin)
                log.info("The provided string was written to stdin of the process by local services.")

            resp_msg = success(
                stdin_resp=stdin_resp,
                stdout_resp=stdout_resp,
                stderr_resp=stderr_resp,
                gs_ret_chan_resp=gs_ret_chan_resp,
            )
        except (OSError, ValueError) as popen_err:
            tb = traceback.format_exc()
            error = "%r encountered %s\n%s" % (msg, popen_err, tb)
            log.warning(error)
            if stdout_file_handle is not None:
                stdout_file_handle.close()
            if stderr_file_handle is not None:
                stderr_file_handle.close()
            resp_msg = fail(error)

        return resp_msg

    @dutil.route(dmsg.LSMultiProcessKill, _DTBL)
    def kill_group(self, msg: dmsg.LSMultiProcessKill) -> None:
        log = logging.getLogger("LS.kill_group")
        log.debug("handling %s", msg)
        success, _ = mk_response_pairs(dmsg.LSMultiProcessKillResponse, msg.tag)

        responses = []
        failed = False

        # Issue the kill for each process now
        for process_kill_msg in msg.procs:
            response = self.kill_process(process_kill_msg)
            if response:
                responses.append(response)
                if response.err == dmsg.LSProcessKillResponse.Errors.FAIL:
                    failed = True

        # always return success
        resp_msg = success(responses=responses, failed=failed)
        log.debug("done handling %s", msg)
        return resp_msg

    @dutil.route(dmsg.LSProcessKill, _DTBL)
    def kill_process(self, msg: dmsg.LSProcessKill) -> None:
        log = logging.getLogger("LS.kill process")
        log.debug("handling %s", msg)
        success, fail = mk_response_pairs(dmsg.LSProcessKillResponse, msg.tag)

        if msg.hide_stderr:
            with self.apt_lock:
                try:
                    proc = self.apt[self.puid2pid[msg.t_p_uid]]
                    if proc.props.stderr_connector is not None:
                        proc.props.stderr_connector.end_stderr_forwarding()
                except Exception:
                    pass

        try:
            target = self.puid2pid[msg.t_p_uid]
        except KeyError:
            error = "%s not present" % msg.t_p_uid
            log.warning(error)
            self._send_response(target_uid=msg.r_c_uid, msg=fail(error))
            return

        try:
            os.kill(target, msg.sig)
            log.info("%r delivered to pid %s" % (msg, target))
            resp_msg = success()
        except OSError as ose:
            error = "delivering %r to pid %s encountered %s" % (msg, target, ose)
            log.warning(error)
            resp_msg = fail(error)

        return resp_msg

    @dutil.route(dmsg.LSFwdInput, _DTBL)
    def fwd_input(self, msg: dmsg.LSFwdInput) -> None:
        log = logging.getLogger("LS.fwd input handler")

        target = msg.t_p_uid
        error = ""
        with self.apt_lock:
            if target in self.puid2pid:
                targ_proc = self.apt[self.puid2pid[target]]
                if targ_proc.stdin is None:
                    error = "p_uid %s has no stdin" % target
            else:
                targ_proc = None
                error = "p_uid %s does not exist here and now" % target

        if not error:
            input_sel = selectors.DefaultSelector()
            input_sel.register(targ_proc.stdin, selectors.EVENT_WRITE)
            sel = input_sel.select(timeout=self.SHUTDOWN_RESP_TIMEOUT)
            if sel:
                try:
                    output_data = msg.input.encode()
                    if len(output_data) > dmsg.LSFwdInput.MAX:
                        log.warning("truncating request of %s to %s" % (len(output_data), dmsg.LSFwdInput.MAX))

                    fh = sel[0][0].fileobj
                    fh.write(output_data[: dmsg.LSFwdInput.MAX])
                except (OSError, BlockingIOError) as err:
                    error = "%s" % err
            else:
                error = "input of target=%s not ready for writing" % target
            input_sel.close()

        if error:
            log.warning("error=%s from %s" % (error, msg))
            if targ_proc is not None and targ_proc.stdin is not None:
                targ_proc.stdin.close()
                targ_proc.stdin = None

        if msg.confirm:
            success, fail = mk_response_pairs(dmsg.LSFwdInputErr, msg.tag)
            if error:
                resp_msg = fail(error)
            else:
                resp_msg = success()

            return resp_msg

    @dutil.route(dmsg.LSSetKV, _DTBL)
    def handle_set_kv(self, msg: dmsg.LSSetKV) -> None:
        log = logging.getLogger("LS.set key-value")
        if msg.value == "":
            if msg.key in self.kvs:
                del self.kvs[msg.key]
        else:
            self.kvs[msg.key] = msg.value
        resp_msg = dmsg.LSSetKVResponse(tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS)
        log.info("Received LSSetKV message and processed it.")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.LSGetKV, _DTBL)
    def handle_get_kv(self, msg: dmsg.LSGetKV) -> None:
        log = logging.getLogger("LS.get key-value")
        if msg.key not in self.kvs:
            resp_msg = dmsg.LSGetKVResponse(tag=get_new_tag(), ref=msg.tag, value="", err=DragonError.NOT_FOUND)
        else:
            val = self.kvs[msg.key]
            resp_msg = dmsg.LSGetKVResponse(tag=get_new_tag(), ref=msg.tag, value=val, err=DragonError.SUCCESS)
        log.info("Received LSSetKV message and processed it.")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.LSStoreCtrlEvent, _DTBL)
    def handle_store_ctrl_event(self, msg: dmsg.LSStoreCtrlEvent) -> None:
        log = logging.getLogger("LS.store_ctrl_event")
        self.kvs[dfacts.CTRL_EVENT_KVS_KEY] = json.dumps({"sdesc": msg.ctrl_event_sdesc, "cuid": msg.ctrl_event_cuid})
        log.info("Stored ctrl_event in KVS under CTRL_EVENT_KVS_KEY.")

    @dutil.route(dmsg.AbnormalTermination, _DTBL)
    def handle_abnormal_term(self, msg: dmsg.AbnormalTermination) -> None:
        log = logging.getLogger("LS.abnormal termination")
        log.info("received abnormal termination signal. starting shutdown process.")
        self._abnormal_termination(msg.err_info)

    @dutil.route(dmsg.GSHalted, _DTBL)
    def handle_gs_halted(self, msg: dmsg.GSHalted) -> None:
        self.set_gs_shutdown()
        log = logging.getLogger("LS.forward GSHalted msg")
        log.info("is_primary=True and GSHalted recvd")
        self.be_in.put(msg)

    @dutil.route(dmsg.LSTeardown, _DTBL)
    def teardown_ls(self, msg: dmsg.LSTeardown) -> None:
        log = logging.getLogger("LS.teardown LS")
        log.info("isPrimary=%s handling LSTeardown" % self.is_primary)
        self.set_shutdown(msg)

    @dutil.route(dmsg.LSHaltTA, _DTBL)
    def handle_halting_ta(self, msg):
        log = logging.getLogger("LS.forward LSHaltTA msg")
        log.info("handling %s" % msg)
        # m8 Forward LSHaltTA to TA
        self.ta_in.put(msg)

    @dutil.route(dmsg.TAHalted, _DTBL)
    def handle_ta_halted(self, msg):
        self.set_ta_shutdown()
        log = logging.getLogger("LS.forward TAHalted msg")
        log.info("handling %s" % msg)
        self.be_in.put(msg)

    @dutil.route(dmsg.LSDumpState, _DTBL)
    def dump_state(self, msg: dmsg.LSDumpState) -> None:
        log = logging.getLogger("LS.dump state")
        the_dump = "%s" % self
        if msg.filename is None:
            log.info("\n" + the_dump)
        else:
            try:
                with open(msg.filename, "w") as dump_fh:
                    dump_fh.write(the_dump)

                log.info("to %s" % msg.filename)
            except (IOError, OSError) as e:
                log.warning("failed: %s" % e)

    @dutil.route(dmsg.LSDestroyPMIx, _DTBL)
    def cleanup_pmix_channels(self, msg):
        log = logging.getLogger("LS.cleanup_pmix_channels")

        success, fail = mk_response_pairs(dmsg.LSDestroyPMIxResponse, msg.tag)
        log.debug("Cleaning up PMIx resources for guid %s", msg.guid)
        try:
            self.pmi_group_resources[msg.guid].destroy_job()
            resp_msg = success(guid=msg.guid)
            log.debug("Successfully destroyed PMIx resources for guid %s", msg.guid)
        except KeyError:
            err = f"Found no resources allocated to guid {msg.guid}"
            log.debug(err)
            resp_msg = fail(err)
        except Exception as e:
            log.debug("Encountered exception while cleaning up PMIx resources for guid %d: %s", msg.guid, e)
            resp_msg = fail(f"{e}")

        return resp_msg

    def cleanup_local_channels_pools(self, proc):
        log = logging.getLogger("LS.local cleanup")

        # Delete process local channels and reclaim their cuids.
        for cuid in proc.props.local_cuids:
            try:
                self.channels[cuid].destroy()
            except Exception as ex:
                log.info("Could not destroy process local channel on process exit. Error:%s" % repr(ex))

            try:
                del self.channels[cuid]
            except Exception as ex:
                log.info("Could not remove process local channel on process exit. Error:%s" % repr(ex))

        self.local_cuids.reclaim(proc.props.local_cuids)
        proc.props.local_cuids.clear()

        # Delete process local pools and reclaim their muids.
        for muid in proc.props.local_muids:
            try:
                self.pools[muid].destroy()
                log.info("Destroyed process local pool with muid=%s upon process exit." % muid)
            except Exception as ex:
                log.info("Could not destroy process local pool on process exit. Error:%s" % repr(ex))

            try:
                del self.pools[muid]
            except Exception as ex:
                log.info("Could not remove process local pool on process exit. Error:%s" % repr(ex))

        self.local_muids.reclaim(proc.props.local_muids)
        proc.props.local_muids.clear()

    def _cleanup_external_resources(self, props: ExternalProcessProps):
        """Destroy and reclaim the process-local channels and pools owned by an
        external process.

        Mirrors cleanup_local_channels_pools for ExternalProcessProps.
        Caller must NOT hold apt_lock (channel/pool destroy may block briefly).
        """
        log = logging.getLogger("LS.external cleanup")
        for cuid in props.local_cuids:
            try:
                self.channels[cuid].destroy()
            except Exception as ex:
                log.info("Could not destroy external process local channel. Error:%s" % repr(ex))
            try:
                del self.channels[cuid]
            except Exception as ex:
                log.info("Could not remove external process local channel. Error:%s" % repr(ex))
        self.local_cuids.reclaim(props.local_cuids)
        props.local_cuids.clear()

        for muid in props.local_muids:
            try:
                self.pools[muid].destroy()
                log.info("Destroyed external process local pool with muid=%s." % muid)
            except Exception as ex:
                log.info("Could not destroy external process local pool. Error:%s" % repr(ex))
            try:
                del self.pools[muid]
            except Exception as ex:
                log.info("Could not remove external process local pool. Error:%s" % repr(ex))
        self.local_muids.reclaim(props.local_muids)
        props.local_muids.clear()

    def _retire_external_registration(self, p_uid):
        for attempt_id, registration in tuple(self.external_registrations.items()):
            if registration.p_uid == p_uid:
                del self.external_registrations[attempt_id]

    @dutil.route(dmsg.LSProcessDetach, _DTBL)
    def detach_external_process(self, msg: dmsg.LSProcessDetach):
        """Clean up resources for a co-located external process that is detaching.

        Destroys the LS-ret channel(s) in the external process's local_cuids,
        removes it from external_procs and puid2pid, and replies
        LSProcessDetachResponse to GS.
        """
        log = logging.getLogger("LS.detach external process")
        log.info("Received LSProcessDetach for t_p_uid=%s" % msg.t_p_uid)

        success, fail = mk_response_pairs(dmsg.LSProcessDetachResponse, msg.tag)

        try:
            # Locate the external entry by p_uid
            props = None
            with self.apt_lock:
                for pid, ep in list(self.external_procs.items()):
                    if ep.p_uid == msg.t_p_uid:
                        props = ep
                        del self.external_procs[pid]
                        self.puid2pid.pop(msg.t_p_uid, None)
                        self._retire_external_registration(msg.t_p_uid)
                        break

            if props is None:
                raise KeyError("No external process found with t_p_uid=%s" % msg.t_p_uid)

            # Destroy the LS-ret channel/pool resources and reclaim their uids
            self._cleanup_external_resources(props)

            log.info("Detached external p_uid=%s" % msg.t_p_uid)
            resp_msg = success()

        except Exception as ex:
            err_info = "detach_external_process failed: %s\n%s" % (ex, traceback.format_exc())
            log.error(err_info)
            resp_msg = fail(err_info)

        return resp_msg

    @dutil.route(dmsg.LSExternalRegister, _DTBL)
    def handle_external_register(self, msg: dmsg.LSExternalRegister):
        """Provision an external process locally and publish its resources to GS."""
        log = logging.getLogger("LS.handle external register")
        log.info("Received LSExternalRegister from pid=%s" % msg.pid)
        response_queue = None
        registration = None
        provisional_ls_return_ch = None
        provisional_gs_return_cuid = None
        try:
            response_queue = InfraQueue.attach(msg.proc_resp_sdesc)
            runtime_incarnation = os.environ.get("DRAGON_RT_UID", "")
            if msg.auth_token != self.attach_auth_token:
                response_queue.put(
                    dmsg.LSExternalResourcesReady(
                        tag=get_new_tag(),
                        ref=msg.tag,
                        err=dmsg.LSExternalResourcesReady.Errors.AUTH_FAILURE,
                        runtime_incarnation=runtime_incarnation,
                        attempt_id=msg.attempt_id,
                        node_index=self.node_index,
                    )
                )
                return None
            if not msg.attempt_id:
                raise ValueError("attempt_id is required")
            if msg.runtime_incarnation != runtime_incarnation:
                raise ValueError("runtime incarnation mismatch")

            process_start_time = str(psutil.Process(msg.pid).create_time())
            registration = self.external_registrations.get(msg.attempt_id)
            if registration is not None:
                if registration.os_pid != msg.pid or registration.process_start_time != process_start_time:
                    raise ValueError("attempt_id conflicts with a different process incarnation")
            else:
                p_uid = self.external_puids.next
                ls_return_ch = self.make_local_channel()
                provisional_ls_return_ch = ls_return_ch
                ls_return_qd = InfraQueue(main_channel=ls_return_ch).serialize()

                gs_return_cuid = self.local_cuids.next
                provisional_gs_return_cuid = gs_return_cuid
                gs_return_muid = dfacts.infrastructure_pool_muid_from_index(self.node_index)
                gs_return_ch = dch.Channel(mem_pool=self.pools[gs_return_muid], c_uid=gs_return_cuid)
                self.channels[gs_return_cuid] = gs_return_ch
                self.adopted_external_cuids.add(gs_return_cuid)
                gs_return_cd = B64.bytes_to_str(gs_return_ch.serialize())
                gs_return_qd = InfraQueue(main_channel=gs_return_ch).serialize()

                registration = ExternalRegistration(
                    attempt_id=msg.attempt_id,
                    p_uid=p_uid,
                    os_pid=msg.pid,
                    process_start_time=process_start_time,
                    ls_return_qd=ls_return_qd,
                    gs_return_cuid=gs_return_cuid,
                    gs_return_muid=gs_return_muid,
                    gs_return_cd=gs_return_cd,
                    gs_return_qd=gs_return_qd,
                )
                with self.apt_lock:
                    self.external_registrations[msg.attempt_id] = registration
                    self.puid2pid[p_uid] = msg.pid
                    self.external_procs[msg.pid] = ExternalProcessProps(
                        p_uid=p_uid,
                        os_pid=msg.pid,
                        local_cuids={ls_return_ch.cuid},
                        local_muids=set(),
                    )

            num_gw = int(os.environ.get(dfacts.PREFIX + dfacts.NUM_GW_CHANNELS_PER_NODE_VAR, 0))
            gw_env = {
                key: os.environ[key]
                for key in (f"{dfacts.GW_ENV_PREFIX}{index + 1}" for index in range(num_gw))
                if key in os.environ
            }
            if len(gw_env) != num_gw:
                raise RuntimeError("local gateway environment is incomplete")

            response_queue.put(
                dmsg.LSExternalResourcesReady(
                    tag=get_new_tag(),
                    ref=msg.tag,
                    err=dmsg.LSExternalResourcesReady.Errors.SUCCESS,
                    runtime_incarnation=runtime_incarnation,
                    attempt_id=msg.attempt_id,
                    node_index=self.node_index,
                    p_uid=registration.p_uid,
                    ls_return_qd=registration.ls_return_qd,
                    gs_return_qd=registration.gs_return_qd,
                    num_gw_channels_per_node=num_gw,
                    gw_env=gw_env,
                    default_pd=parms.this_process.default_pd,
                    inf_pd=parms.this_process.inf_pd,
                )
            )

            if registration.state == "LOCAL_PREPARED":
                registration.state = "SUBMISSION_POSSIBLE"
                prepared = dmsg.GSExternalRegisterPrepared(
                    tag=get_new_tag(),
                    node_index=self.node_index,
                    p_uid=registration.p_uid,
                    pid=registration.os_pid,
                    process_start_time=registration.process_start_time,
                    user_name=None,
                    gs_return_cuid=registration.gs_return_cuid,
                    gs_return_muid=registration.gs_return_muid,
                    gs_return_cd=registration.gs_return_cd,
                    gs_return_qd=registration.gs_return_qd,
                )
                self.gs_in.put(prepared)
            return None
        except ExternalPUIDExhausted:
            if response_queue is not None:
                response_queue.put(
                    dmsg.LSExternalResourcesReady(
                        tag=get_new_tag(),
                        ref=msg.tag,
                        err=dmsg.LSExternalResourcesReady.Errors.PUID_EXHAUSTED,
                        runtime_incarnation=os.environ.get("DRAGON_RT_UID", ""),
                        attempt_id=msg.attempt_id,
                        node_index=self.node_index,
                    )
                )
            return None
        except Exception as ex:
            log.exception("external registration provisioning failed")
            if registration is None or registration.state == "LOCAL_PREPARED":
                with self.apt_lock:
                    self.external_registrations.pop(msg.attempt_id, None)
                    props = self.external_procs.pop(msg.pid, None)
                    if props is not None:
                        self.puid2pid.pop(props.p_uid, None)
                if props is not None:
                    self._cleanup_external_resources(props)
                elif provisional_ls_return_ch is not None:
                    self.channels.pop(provisional_ls_return_ch.cuid, None)
                    try:
                        provisional_ls_return_ch.destroy()
                    except Exception:
                        pass
                    self.local_cuids.reclaim([provisional_ls_return_ch.cuid])
                if provisional_gs_return_cuid is not None:
                    channel = self.channels.pop(provisional_gs_return_cuid, None)
                    if channel is not None:
                        try:
                            channel.destroy()
                        except Exception:
                            pass
                    self.adopted_external_cuids.discard(provisional_gs_return_cuid)
                    self.local_cuids.reclaim([provisional_gs_return_cuid])

            if response_queue is not None and (registration is None or registration.state == "LOCAL_PREPARED"):
                response_queue.put(
                    dmsg.LSExternalResourcesReady(
                        tag=get_new_tag(),
                        ref=msg.tag,
                        err=dmsg.LSExternalResourcesReady.Errors.FAIL,
                        errInfo=str(ex),
                        runtime_incarnation=os.environ.get("DRAGON_RT_UID", ""),
                        attempt_id=msg.attempt_id,
                        node_index=self.node_index,
                    )
                )
            return None
        finally:
            del response_queue

    @dutil.route(dmsg.LSExternalRegisterCancel, _DTBL)
    def handle_external_register_cancel(self, msg: dmsg.LSExternalRegisterCancel):
        """Forward a validated client release request to GS."""
        log = logging.getLogger("LS.cancel external register")
        runtime_incarnation = os.environ.get("DRAGON_RT_UID", "")
        if msg.runtime_incarnation != runtime_incarnation or not msg.attempt_id:
            log.warning("ignoring invalid external registration cancellation")
            return None

        with self.apt_lock:
            registration = self.external_registrations.get(msg.attempt_id)
            if registration is None:
                log.info("external registration attempt %s is no longer active", msg.attempt_id)
                return None
            if msg.p_uid not in (0, registration.p_uid):
                log.warning("external cancellation PUID does not match attempt %s", msg.attempt_id)
                return None
            if registration.state == "CANCELLING":
                return None
            registration.state = "CANCELLING"

        self.gs_in.put(
            dmsg.GSExternalRegisterCancel(
                tag=get_new_tag(),
                node_index=self.node_index,
                p_uid=registration.p_uid,
                attempt_id=registration.attempt_id,
                pid=registration.os_pid,
                process_start_time=registration.process_start_time,
                gs_return_cuid=registration.gs_return_cuid,
                gs_return_muid=registration.gs_return_muid,
                gs_return_cd=registration.gs_return_cd,
                gs_return_qd=registration.gs_return_qd,
            )
        )
        return None

    def watch_external_death(self):
        """Daemon thread that polls for death of registered external (non-child) processes.

        External processes are not children of LS so os.waitpid cannot be used.
        Every ~2 s we probe each registered pid with os.kill(pid, 0); an OSError
        with errno ESRCH means the process is gone.  On detection we:
          1. Send LSProcessExit to GS (creation_msg_tag=None is safe; GS marks DEAD).
          2. Destroy and reclaim the process's LS-ret channel(s).
          3. Remove the entry from external_procs / puid2pid.

        The thread exits when check_shutdown() is True.
        """
        log = logging.getLogger("LS.watch external death")
        log.info("starting")

        POLL_INTERVAL = 2.0  # seconds between liveness sweeps
        SLICE = 0.25

        while not self.check_shutdown():
            slept = 0.0
            while slept < POLL_INTERVAL and not self.check_shutdown():
                time.sleep(SLICE)
                slept += SLICE
            if self.check_shutdown():
                break

            # Snapshot under lock so we don't hold it during blocking channel ops
            with self.apt_lock:
                snapshot = list(self.external_procs.items())

            for pid, props in snapshot:
                try:
                    os.kill(pid, 0)  # signal 0: existence check only, no signal sent
                except OSError as ose:
                    if ose.errno != errno.ESRCH:
                        # EPERM etc. — process exists but we can't signal it; skip
                        continue
                    # ESRCH: no such process — it has exited
                    log.info("external process pid=%s p_uid=%s has exited; cleaning up" % (pid, props.p_uid))

                    # Remove from tracking
                    with self.apt_lock:
                        self.external_procs.pop(pid, None)
                        self.puid2pid.pop(props.p_uid, None)
                        self._retire_external_registration(props.p_uid)

                    # Destroy the LS-ret channel(s)/pool(s) owned by this external proc
                    self._cleanup_external_resources(props)

                    # Notify GS of the death (matches watch_death construction exactly)
                    resp = dmsg.LSProcessExit(
                        tag=get_new_tag(),
                        exit_code=-1,
                        p_uid=props.p_uid,
                        creation_msg_tag=None,
                    )
                    self.gs_in.put(resp)
                    log.info("sent LSProcessExit for external p_uid=%s" % props.p_uid)

        log.info("exit")

    def watch_death(self):
        """Thread monitors the demise of child processes of this process.

        Not all children do we care about; only the ones in our process group.

        :return: None, but exits on self.check_shutdown()
        """

        log = logging.getLogger("LS.watch death")
        log.info("starting")

        while not self.check_shutdown():
            try:
                died_pid, exit_status = os.waitpid(0, os.WNOHANG)
            except ChildProcessError:  # no child processes at the moment
                # There is no error here. There just isn't a child process.
                died_pid, exit_status = (0, 0)

            if died_pid:
                with self.apt_lock:
                    try:
                        proc = self.apt[died_pid]

                        try:
                            proc.props.stderr_connector.flush()
                        except Exception as ex:
                            log.debug("Got exception while flushing stderr: %s" % ex)

                        try:
                            proc.props.stdout_connector.flush()
                        except Exception as ex:
                            log.debug("Got exception while flushing stdout: %s" % ex)

                        proc = self.apt.pop(died_pid)
                        self.puid2pid.pop(proc.props.p_uid)
                    except KeyError:
                        log.warning("unknown child pid %s exited!" % died_pid)
                        proc = None

                if proc is None:
                    continue

                ecode = os.waitstatus_to_exitcode(exit_status)
                log.info("p_uid: %s pid: %s ecode=%s" % (proc.props.p_uid, died_pid, ecode))
                resp = dmsg.LSProcessExit(
                    tag=get_new_tag(),
                    exit_code=ecode,
                    p_uid=proc.props.p_uid,
                    creation_msg_tag=proc.props.creation_msg_tag,
                )

                if proc.props.p_uid != dfacts.GS_PUID:
                    if proc.props.r_c_uid is None:
                        self.gs_in.put(resp)
                        log.info("transmit %s via gs_in" % repr(resp))
                    else:
                        r_c_uid = proc.props.r_c_uid
                        self._send_response(target_uid=r_c_uid, msg=resp)
                        log.info("transmit %s via _send_response" % repr(resp))

                # Delete process local channels and pools and reclaim their uids.
                self.cleanup_local_channels_pools(proc)

                # If we haven't received LSTeardown yet
                if proc.props.critical and not self.check_shutdown():
                    if proc.props.p_uid == dfacts.GS_PUID:
                        # if this is GS and we haven't received GSHalted yet and LSTeardown
                        # has not been received then this is an abnormal termination condition.
                        if self.is_primary and (not self.check_gs_shutdown()) and not self.check_shutdown():
                            # Signal abnormal termination and notify Launcher BE
                            err_msg = "LS watch death - GS exited - puid %s" % proc.props.p_uid
                            self._abnormal_termination(err_msg)
                    elif dfacts.is_transport_puid(proc.props.p_uid):
                        if (not self.check_ta_shutdown()) and (not self.check_shutdown()):
                            err_msg = "LS watch death - TA exited - puid %s" % proc.props.p_uid
                            self._abnormal_termination(err_msg)
                    else:
                        # Signal abnormal termination and notify Launcher BE
                        err_msg = "LS watch death - critical process exited - puid %s" % proc.props.p_uid
                        self._abnormal_termination(err_msg)

                try:  # keep subprocess from giving spurious ResourceWarning
                    proc.wait(0)
                    # Remember to close any open connections for stdout and stderr.
                    # If they weren't opened, the close methods will handle that. The
                    # underlying channel will be decref'ed when the LSProcessExit is
                    # received by global services (see server.py in GS).
                    if proc.props.stdout_connector is not None:
                        self.exited_channel_output_monitors.put(proc.props.stdout_connector)
                    if proc.props.stderr_connector is not None:
                        self.exited_channel_output_monitors.put(proc.props.stderr_connector)
                    if proc.props.stdin_connector is not None:
                        self.exited_channel_input_monitors.put(proc.props.stdin_connector)
                except OSError:
                    pass
            else:
                time.sleep(self.SHUTDOWN_RESP_TIMEOUT)

        log.info("exit")

    def watch_oom(self):
        log = logging.getLogger("LS.watch oom")
        log.info("starting")
        pid = os.getpid()
        lparms = dp.this_process.from_env()
        interval = self.OOM_SLOW_SLEEP_TIME
        total_bytes = psutil.virtual_memory().total
        # Keyed by muid so each pool Local Services knows about is tracked independently.
        pool_events = {}
        pool_consecutive_events = {}
        pool_event_min_pct = {}

        try:
            bytes_val = lparms.memory_warning_bytes
            if bytes_val == 0:
                bytes_val = total_bytes

            warn_bytes_env = 100 - 100 * bytes_val / total_bytes
            warn_pct_env = 100 - lparms.memory_warning_pct
            warn_pct = max(warn_bytes_env, warn_pct_env)
        except:
            warn_pct = 90

        log.info("warn_bytes_env=%s warn_pct_env=%s warn_pct=%s" % (warn_bytes_env, warn_pct_env, warn_pct))

        try:
            bytes_val = lparms.memory_critical_bytes
            if bytes_val == 0:
                bytes_val = total_bytes

            critical_bytes_env = 100 - 100 * bytes_val / total_bytes
            critical_pct_env = 100 - lparms.memory_critical_pct
            critical_pct = max(critical_bytes_env, critical_pct_env)
        except:
            critical_pct = 98

        try:
            warn_pool_pct = 100 - lparms.def_pool_warn_pct
        except:
            warn_pool_pct = 90

        log.info(
            "critical_bytes_env=%s critical_pct_env=%s critical_pct=%s"
            % (critical_bytes_env, critical_pct_env, critical_pct)
        )

        try:
            suppress_warnings = lparms.warnings_off != "False"
        except:
            suppress_warnings = False

        try:
            while not self.check_shutdown():
                # Check Memory Utilization First
                mem_utilization = psutil.virtual_memory().percent
                mem_bytes = psutil.virtual_memory().available
                if mem_utilization >= critical_pct:
                    critical_msg = (
                        "OOM CRITICAL: Free memory on node %s with hostname %s is less than %s%% with %s bytes available. Runtime is coming down.\n"
                        % (parms.this_process.index, self.hostname, int(100 - mem_utilization) + 1, mem_bytes)
                    )
                    self.be_in.put(
                        dmsg.LSFwdOutput(
                            tag=get_new_tag(),
                            idx=parms.this_process.index,
                            p_uid=parms.this_process.my_puid,
                            data=critical_msg,
                            fd_num=dmsg.LSFwdOutput.FDNum.STDERR.value,
                            pid=pid,
                            hostname=self.hostname,
                        )
                    )
                    self._abnormal_termination(critical_msg)
                elif mem_utilization >= warn_pct:
                    warning_msg = (
                        "\nOOM WARNING: Free memory on node %s with hostname %s is less than %s%% with %s bytes still available.\n"
                        % (parms.this_process.index, self.hostname, int(100 - mem_utilization) + 1, mem_bytes)
                    )
                    log.info(warning_msg)
                    if not suppress_warnings:
                        self.be_in.put(
                            dmsg.LSFwdOutput(
                                tag=get_new_tag(),
                                idx=parms.this_process.index,
                                p_uid=parms.this_process.my_puid,
                                data=warning_msg,
                                fd_num=dmsg.LSFwdOutput.FDNum.STDERR.value,
                                pid=pid,
                                hostname=self.hostname,
                            )
                        )

                # Now Check Utilization of every pool Local Services knows about. This
                # includes the default and infrastructure pools plus any pool created on
                # behalf of a process, such as the DDict manager pools.
                for muid, pool in list(self.pools.items()):
                    try:
                        pool_pct = pool.utilization
                    except Exception as ex:
                        # Another thread can destroy a pool while we are sampling it.
                        log.debug("Could not read utilization of pool muid=%s: %s" % (muid, ex))
                        pool_consecutive_events.pop(muid, None)
                        continue

                    if pool_pct >= warn_pool_pct:
                        pool_event_min_pct[muid] = min(pool_event_min_pct.get(muid, 100), pool_pct)
                        pool_consecutive_events[muid] = pool_consecutive_events.get(muid, 0) + 1

                        if (pool_consecutive_events[muid] % 100) == 5:
                            pool_events[muid] = pool_events.get(muid, 0) + 1
                            # if this is the 5th consecutive event mod 100, then warn the user,
                            # but don't keep warning if it doesn't change. Every 100 iterations
                            # means several minutes between notifications.
                            warning_msg = (
                                "\nPOOL FREE SPACE WARNING: Utilization of pool muid=%s on node %s with hostname %s is at %s%%.\n"
                                % (muid, parms.this_process.index, self.hostname, pool_pct)
                            )
                            log.info(warning_msg)
                            if not suppress_warnings:
                                self.be_in.put(
                                    dmsg.LSFwdOutput(
                                        tag=get_new_tag(),
                                        idx=parms.this_process.index,
                                        p_uid=parms.this_process.my_puid,
                                        data=warning_msg,
                                        fd_num=dmsg.LSFwdOutput.FDNum.STDERR.value,
                                        pid=pid,
                                        hostname=self.hostname,
                                    )
                                )
                    else:
                        pool_consecutive_events[muid] = 0

                if mem_utilization > 95:
                    interval = self.OOM_RAPID_SLEEP_TIME
                else:
                    interval = self.OOM_SLOW_SLEEP_TIME

                # Finally check the Shared Memory Utilization which may be different than general memory utilization.
                free_shm_space, total_shm_space = dutils.shm_space()
                used_shm_pct = 100 - ((free_shm_space / total_shm_space) * 100)

                if used_shm_pct >= critical_pct:
                    critical_msg = (
                        "OOM CRITICAL: SHARED memory on node %s with hostname %s is less than %s%% with %s bytes available. Runtime is coming down.\n"
                        % (parms.this_process.index, self.hostname, int(100 - used_shm_pct) + 1, free_shm_space)
                    )
                    self.be_in.put(
                        dmsg.LSFwdOutput(
                            tag=get_new_tag(),
                            idx=parms.this_process.index,
                            p_uid=parms.this_process.my_puid,
                            data=critical_msg,
                            fd_num=dmsg.LSFwdOutput.FDNum.STDERR.value,
                            pid=pid,
                            hostname=self.hostname,
                        )
                    )
                    self._abnormal_termination(critical_msg)
                elif used_shm_pct >= warn_pct:
                    warning_msg = (
                        "\nOOM WARNING: SHARED memory on node %s with hostname %s is less than %s%% with %s bytes still available.\n"
                        % (parms.this_process.index, self.hostname, int(100 - used_shm_pct) + 1, free_shm_space)
                    )
                    log.info(warning_msg)
                    if not suppress_warnings:
                        self.be_in.put(
                            dmsg.LSFwdOutput(
                                tag=get_new_tag(),
                                idx=parms.this_process.index,
                                p_uid=parms.this_process.my_puid,
                                data=warning_msg,
                                fd_num=dmsg.LSFwdOutput.FDNum.STDERR.value,
                                pid=pid,
                                hostname=self.hostname,
                            )
                        )

                # Only poll every 5 seconds normally and every 1 second when running low on mem.
                # By waiting on the shutdown event with a timeout of interval, we effectively
                # sleep for the interval during normal processing and wake up immediately when shutting down.
                self.shutdown_sig.wait(interval)
        except Exception as ex:
            tb = traceback.format_exc()
            self._abnormal_termination("LS OOM daemon exception: %s\n%s" % (ex, tb))

        try:
            for muid in sorted(pool_events):
                warning_msg = (
                    "\nPOOL FREE SPACE WARNING: There were %s events recorded where the\n"
                    + "utilization of pool muid=%s on node %s with hostname %s exceeded %s%%.\n"
                    + "You may want to increase the pool size.\n"
                ) % (pool_events[muid], muid, parms.this_process.index, self.hostname, pool_event_min_pct[muid])
                log.info(warning_msg)
                if not suppress_warnings:
                    self.be_in.put(
                        dmsg.LSFwdOutput(
                            tag=get_new_tag(),
                            idx=parms.this_process.index,
                            p_uid=parms.this_process.my_puid,
                            data=warning_msg,
                            fd_num=dmsg.LSFwdOutput.FDNum.STDERR.value,
                            pid=pid,
                            hostname=self.hostname,
                        )
                    )

            log.info("Exiting")
        except:
            pass

    def update_watch_set(self, connectors, dead_connector):
        changed = False
        while not self.new_channel_input_monitors.empty():
            changed = True
            connector = self.new_channel_input_monitors.get()
            connectors.add(connector)

        if dead_connector is not None:
            changed = True
            connectors.discard(dead_connector)

        return changed

    def watch_input(self):
        """Thread monitors inbound traffic directed to stdin of a process."""

        log = logging.getLogger("LS.watch input")

        log.info("starting")

        connectors = set()
        channel_set = None
        dead_connector = None

        # Gets us the default pool.
        node_index = parms.this_process.index
        def_muid = dfacts.default_pool_muid_from_index(node_index)
        def_pool = self.pools[def_muid]

        while not self.check_shutdown():
            EOF = False

            new_channel_set_needed = self.update_watch_set(connectors, dead_connector)

            if len(connectors) == 0:
                time.sleep(self.SHUTDOWN_RESP_TIMEOUT)
                if channel_set is not None:
                    del channel_set
                    channel_set = None
            else:
                try:
                    if new_channel_set_needed:
                        del channel_set
                        channel_list = [connector.conn.inbound_channel for connector in connectors]
                        channel_set = dch.ChannelSet(def_pool, channel_list)

                    dead_connector = None

                    connector = None
                    channel, event = channel_set.poll(self.SHUTDOWN_RESP_TIMEOUT)
                    connector = self.cuid_to_input_connector[channel.cuid]
                    if event == dch.EventType.POLLIN:
                        EOF = connector.forward()

                    if EOF or event == dch.EventType.POLLNOTHING or not connector.proc_is_alive:
                        dead_connector = connector
                        connector.close()

                except dch.ChannelSetTimeout:
                    pass
                except Exception as ex:
                    # Any error is likely due to the child exiting
                    log.info("InputConnector Error:%s" % repr(ex))
                    try:
                        if connector is not None:
                            dead_connector = connector
                            connector.close()
                    except:
                        pass

        log.info("exiting")

    class WatchingSelector(selectors.DefaultSelector):
        """Enhanced DefaultSelector to register stdout/stderr of PopenProps

        Automates registering the file handles with the selector base class
        and maps it to an OutputConnector object to handle the logic for
        forwarding data where it needs to go.
        """

        def add_proc_streams(self, server, proc: PopenProps):
            # carried data is (ProcessProps, closure to make LSFwdOutput, whether stderr or not)
            try:
                self.register(proc.stdout, selectors.EVENT_READ, data=proc.props.stdout_connector)
            except ValueError:  # file handle could be closed or None: a race, so must catch
                pass
            except KeyError as ke:  # already registered
                self.unregister(proc.stdout)
                # log.warning("ke=%s, proc.stdout=%s, new props=%s" % (ke, proc.stdout, proc.props))
                self.register(proc.stdout, selectors.EVENT_READ, data=proc.props.stdout_connector)

            try:
                self.register(proc.stderr, selectors.EVENT_READ, data=proc.props.stderr_connector)
            except ValueError:
                pass
            except KeyError as ke:
                self.unregister(proc.stderr)
                # log.warning("ke=%s, proc.stderr=%s, new props=%s" % (ke, proc.stderr, proc.props))
                self.register(proc.stderr, selectors.EVENT_READ, data=proc.props.stderr_connector)

    def watch_output(self):
        """Thread monitors outbound std* activity from processes we started.

            Any stderr activity on a 'critical' (e.g. infrastructure) process
            running under this local services will cause an error shutdown.

        :return: None, exits on self.check_shutdown()
        """
        log = logging.getLogger("LS.watch output")

        log.info("starting")

        stream_sel = LocalServer.WatchingSelector()

        while not self.check_shutdown():
            work = stream_sel.select(timeout=self.SHUTDOWN_RESP_TIMEOUT)

            for sel_k, _ in work:
                output_connector = sel_k.data

                try:
                    EOF = output_connector.flush()
                except TerminationException as ex:
                    EOF = False
                    handled = False
                    str_data = str(ex)

                    try:  # did we get a GSHalted?
                        msg = dmsg.parse(b64decode(str_data))
                        if isinstance(msg, dmsg.GSHalted):
                            handled = True
                            self._DTBL[dmsg.GSHalted][0](self, msg=msg)
                            EOF = True
                    except (ValueError, json.JSONDecodeError):
                        pass

                    if not handled:
                        err_msg = "output from critical puid %s" % output_connector.puid
                        log.error(err_msg)
                        log.error("output is:\n%s\n" % str_data)
                        # Signal abnormal termination and notify Launcher BE
                        self._abnormal_termination(err_msg)

                if EOF:
                    try:
                        stream_sel.unregister(output_connector.file_obj)
                    except:
                        pass
                    try:
                        output_connector.close()
                    except:
                        pass

            if self.check_shutdown():
                break

            try:
                while True:
                    new_proc = self.new_procs.get_nowait()
                    try:
                        stream_sel.add_proc_streams(self, new_proc)
                    except:
                        pass
            except queue.Empty:
                pass

            self._output_connector_cleanup(stream_sel)
            self._input_connector_cleanup()

        stream_sel.close()
        log.info("exit")

    def _input_connector_cleanup(self):
        try:
            while True:
                exited_proc_connector = self.exited_channel_input_monitors.get_nowait()
                try:
                    exited_proc_connector.close()
                except:
                    pass
        except queue.Empty:
            pass

    def _output_connector_cleanup(self, stream_sel=None):
        try:
            while True:
                exited_proc_connector = self.exited_channel_output_monitors.get_nowait()
                try:
                    stream_sel.unregister(exited_proc_connector.file_obj)
                except:
                    pass
                try:
                    exited_proc_connector.close()
                except:
                    pass
        except queue.Empty:
            pass
