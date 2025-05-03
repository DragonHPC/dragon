import os
import sys
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

from .. import channels as dch
from .. import managed_memory as dmm
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


class PopenProps(subprocess.Popen):
    def __init__(self, props: ProcessProps, *args, **kwds):
        assert isinstance(props, ProcessProps)
        self.props = props
        # if your kwds are going to have a non-None env then it needs to be edited before the process is initialized
        if props.layout is not None:
            if props.layout.gpu_core and props.layout.accelerator_env:
                # gpu_core list must be turned into a string in the form "0,1,2" etc
                if isinstance(kwds["env"], dict):
                    kwds["env"][props.layout.accelerator_env] = ",".join(str(core) for core in props.layout.gpu_core)
                else:
                    # this might be unnecessary because the environment might always be a dict.
                    os.environ[props.layout.accelevator_env] = ",".join(str(core) for core in props.layout.gpu_core)
        super().__init__(*args, **kwds)
        # Assuming this is basically a free call, default the afinity to "everything" just in case
        try:
            os.sched_setaffinity(self.pid, range(os.cpu_count()))
            if props.layout is not None:
                if props.layout.cpu_core:
                    os.sched_setaffinity(self.pid, props.layout.cpu_core)
        except:
            # TODO: this should be possibly be logged, but it can fail here if the process is short lived enough
            pass

        # XXX Affinity settings are only inherited by grandchild processes
        # XXX created after this point in time. Any grandchild processes
        # XXX started when the child process starts up most certainly will
        # XXX not have the desired affinity. To guarantee CPU affinity settings
        # XXX processes should be launched with e.g. taskset(1) or otherwise
        # XXX be configured to set their own affinity as appropriate.


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
                "The input could not be forwarded from cuid=%s to process pid=%s" % (self._conn.inbound_chan.cuid, self._proc.pid)
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
        if len(block) > 0:
            self._writtenTo = True

        forward_msg = True
        if self._fwd_stderr is False and self._out_err == dmsg.SHFwdOutput.FDNum.STDERR.value:
            forward_msg = False
        elif self._fwd_stdout is False and self._out_err == dmsg.SHFwdOutput.FDNum.STDOUT.value:
            forward_msg = False

        if forward_msg:
            if self._conn is None:
                try:
                    self._be_in.send(
                        dmsg.SHFwdOutput(
                            tag=get_new_tag(),
                            idx=parms.this_process.index,
                            p_uid=self._puid,
                            data=block,
                            fd_num=self._out_err,
                            pid=self._proc.pid,
                            hostname=self._hostname,
                        ).serialize()
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
                    self._be_in.send(
                        dmsg.SHFwdOutput(
                            tag=get_new_tag(),
                            idx=parms.this_process.index,
                            p_uid=self._puid,
                            data="[orphaned output]: " + block,
                            fd_num=self._out_err,
                            pid=self._proc.pid,
                            hostname=self._hostname,
                        ).serialize()
                    )
                except Exception:
                    pass

    def add_proc_info(self, proc):
        self._proc = proc

    def forward(self, data):
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
        is_stderr = self._out_err == dmsg.SHFwdOutput.FDNum.STDERR.value

        try:
            file_obj = self.file_obj

            if file_obj is not None:
                io_data = file_obj.read(dmsg.SHFwdOutput.MAX)
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

        str_data = io_data.decode()

        # This is temporary and code to ignore warnings coming from capnproto. The
        # capnproto library has been modified to not send the following warning.
        # kj/filesystem-disk-unix.c++:1703: warning: PWD environment variable ...
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
        is_stderr = self._out_err == dmsg.SHFwdOutput.FDNum.STDERR.value

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


def mk_response_pairs(resp_cl, ref):
    err_cl = resp_cl.Errors

    def success_resp(desc=None, **kwargs):
        if desc is not None:
            return resp_cl(tag=get_new_tag(), ref=ref, err=err_cl.SUCCESS, desc=desc, **kwargs)
        else:
            return resp_cl(tag=get_new_tag(), ref=ref, err=err_cl.SUCCESS, **kwargs)

    def fail_resp(msg):
        return resp_cl(tag=get_new_tag(), ref=ref, err=err_cl.FAIL, err_info=msg)

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
    LOCAL_CUID_RANGE = (dfacts.BASE_SHEP_CUID + dfacts.RANGE_SHEP_CUID) - dfacts.MAX_NODES
    AVAILABLE_PER_NODE = LOCAL_CUID_RANGE // dfacts.MAX_NODES

    def __init__(self, node_index):
        if node_index >= dfacts.MAX_NODES:  # 0 <= node_index < MAX_NODES
            raise RuntimeError("A Local Services nodes has node_index=%s which is greater than max allowed." % node_index)

        self._node_index = node_index
        self._active = set()
        self._initial_cuid = (
            dfacts.BASE_SHEP_CUID + dfacts.MAX_NODES + node_index * AvailableLocalCUIDS.AVAILABLE_PER_NODE
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


class AvailableLocalMUIDS:
    """Internal only class that manages Process Local MUIDs"""

    def __init__(self, node_index):
        if node_index >= dfacts.MAX_NODES:  # 0 <= node_index < MAX_NODES
            raise RuntimeError("A Local Services nodes has node_index=%s which is greater than max allowed." % node_index)

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
    """Handles shepherd messages in normal processing.

    This object does not handle startup/teardown - instead
     it expects to be given whatever channels/pools have been made for it
     by startup, handles to what it needs to talk to in normal processing,
     and offers a 'run' and 'cleanup' method.
    """

    _DTBL = {}  # dispatch router, keyed by type of shepherd message

    SHUTDOWN_RESP_TIMEOUT = 0.010  # seconds, 10 ms
    QUIESCE_TIME = 1  # seconds, 1 second, join timeout for thread shutdown.
    OOM_SLOW_SLEEP_TIME = 5 # seconds, the oom_monitor sleeps that long in normal conditions
    OOM_RAPID_SLEEP_TIME = 1 # seconds, when nearing OOM

    def __init__(self, channels=None, pools=None, transport_test_mode=False, hostname="NONE"):
        self.transport_test_mode = transport_test_mode
        self.new_procs = queue.SimpleQueue()  # outbound PopenProps of newly started processes
        self.new_channel_input_monitors = queue.SimpleQueue()
        self.exited_channel_output_monitors = queue.SimpleQueue()
        self.hostname = hostname
        self.cuid_to_input_connector = {}
        self.node_index = parms.this_process.index
        self.local_cuids = AvailableLocalCUIDS(self.node_index)
        self.local_muids = AvailableLocalMUIDS(self.node_index)
        self.local_channels = dict()
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
        log.debug("Creating local memory pool with muid=%s for process with msg.puid=%s and name=%s" % (muid, msg.puid, name))
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
            plist = ["\t%s:%s %s" % (p_uid, pid, self.apt[pid]) for p_uid, pid in self.puid2pid.items()]

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
                log.warning("m_uid=%s failed: %s" % (m_uid, dpe))

    def _clean_procs(self):
        log = logging.getLogger("LS.kill procs")

        with self.apt_lock:
            log.info("%s processes outstanding" % len(self.apt))
            for p_uid, pid in self.puid2pid.items():
                proc = self.apt[pid]
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
            self.be_in.send(dmsg.AbnormalTermination(tag=get_new_tag(), err_info=error_str).serialize())
            log.critical("Abnormal termination sent to launcher be: %s" % error_str)
        except Exception as ex:
            log.exception("Abnormal termination exception: %s" % ex)

    def run(self, shep_in, gs_in, be_in, is_primary, ta_in=None, gw_channels=None):
        """Local services main function.

        :param shep_in: ls channel
        :type shep_in: Connection object
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
        self.is_primary = is_primary

        th = threading.Thread

        threads = [
            th(name="output mon", target=self.watch_output, daemon=True),
            th(name="watch death", target=self.watch_death, daemon=True),
            th(name="input mon", target=self.watch_input, daemon=True),
            th(name="OOM mon", target=self.watch_oom, daemon=True),
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
            self.main_loop(shep_in)
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

        if self.transport_test_mode and self.exit_reason is None:
            # In the transport test mode, normal set_shutdown was called.
            log.info("beginning normal shutdown during transport test mode run")

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
            # m12 Send SHHaltBE to BE
            # tell launcher be to shut mrnet down and detach from logging
            log.info("m12 transmitting final messsage from ls SHHaltBE")
            dlog.detach_from_dragon_handler(dls.LS)
            self.be_in.send(dmsg.SHHaltBE(tag=get_new_tag()).serialize())
        except Exception as ex:
            self._abnormal_termination("ls run sending SHHaltBE exception: %s" % ex)

        log.info("exit")

    def main_loop(self, shep_rh):
        """Monitors the main LS input channel and receives messages.
        If the received message is not one of the expected that are
        handled by corresponding route decorators, then it signals
        abnormal termination of LS.

        :param shep_rh: ls input channel
        :type shep_rh: Connection object
        """
        log = logging.getLogger("LS.main loop")
        log.info("start")

        while not self.check_shutdown():
            msg_pre = shep_rh.recv()

            if msg_pre is None:
                continue

            if isinstance(msg_pre, str) or isinstance(msg_pre, bytearray):
                try:
                    msg = dmsg.parse(msg_pre)
                except (json.JSONDecodeError, KeyError, NotImplementedError, ValueError, TypeError) as err:
                    self._abnormal_termination("msg\n%s\nfailed parse!\n%s" % (msg_pre, err))
                    continue
            else:
                msg = msg_pre

            if type(msg) in LocalServer._DTBL:
                resp_msg = self._DTBL[type(msg)][0](self, msg=msg)
                if resp_msg is not None:
                    self._send_response(target_uid=msg.r_c_uid, msg=resp_msg)
            else:
                self._abnormal_termination("unexpected msg type: %s" % repr(msg))

        log.info("Main loop has exited. Now cleaning up local channels and pools")

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
            self.gs_in.send(msg.serialize())
        elif target_uid == dfacts.launcher_cuid_from_index(parms.this_process.index):
            self.be_in.send(msg.serialize())
        else:
            self._abnormal_termination("unknown r_c_uid: %s" % repr(msg))

    @dutil.route(dmsg.SHPoolCreate, _DTBL)
    def create_pool(self, msg: dmsg.SHPoolCreate) -> None:
        log = logging.getLogger("LS.create pool")
        success, fail = mk_response_pairs(dmsg.SHPoolCreateResponse, msg.tag)

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
            resp_msg = success(encoded_desc)

        return resp_msg

    @dutil.route(dmsg.SHPoolDestroy, _DTBL)
    def destroy_pool(self, msg: dmsg.SHPoolDestroy) -> None:
        log = logging.getLogger("LS.destroy pool")
        success, fail = mk_response_pairs(dmsg.SHPoolDestroyResponse, msg.tag)

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

    @dutil.route(dmsg.SHChannelCreate, _DTBL)
    def create_channel(self, msg: dmsg.SHChannelCreate) -> None:
        log = logging.getLogger("LS.create channel")
        log.info("Received an SHChannelCreate")
        success, fail = mk_response_pairs(dmsg.SHChannelCreateResponse, msg.tag)

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
                    initial_sem_value=msg.options.initial_sem_value
                )
            except dch.ChannelError as cex:
                error = "%r failed: %s" % (msg, cex)

        if error:
            log.warning(error)
            resp_msg = fail(error)
        else:
            self.channels[msg.c_uid] = ch
            encoded_desc = B64.bytes_to_str(ch.serialize())
            resp_msg = success(encoded_desc)
            log.info("Received and Created a channel via SHChannelCreate")

        return resp_msg

    @dutil.route(dmsg.SHCreateProcessLocalChannel, _DTBL)
    def create_process_local_channel(self, msg: dmsg.SHCreateProcessLocalChannel) -> None:
        log = logging.getLogger("LS.create local channel")
        log.info("Received an SHCreateProcessLocalChannel")

        if not msg.puid in self.puid2pid:
            resp_msg = dmsg.SHCreateProcessLocalChannelResponse(
                tag=get_new_tag(),
                ref=msg.tag,
                err=DragonError.INVALID_ARGUMENT,
                errInfo="Cannot create channel for non-existent local process on node.",
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        try:
            ch = self.make_local_channel()
            self.apt[self.puid2pid[msg.puid]].props.local_cuids.add(ch.cuid)
            self.local_channels[ch.cuid] = ch
        except dch.ChannelError as cex:
            error = "%r failed: %s" % (msg, cex)
            resp_msg = dmsg.SHCreateProcessLocalChannelResponse(
                tag=get_new_tag(), ref=msg.tag, err=DragonError.INVALID_OPERATION, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        encoded_desc = b64encode(ch.serialize())
        resp_msg = dmsg.SHCreateProcessLocalChannelResponse(
            tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS, serChannel=encoded_desc
        )
        log.info("Received and Created a channel via SHCreateProcessLocalChannel with cuid=%s" % ch.cuid)
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.SHDestroyProcessLocalChannel, _DTBL)
    def destroy_process_local_channel(self, msg: dmsg.SHDestroyProcessLocalChannel) -> None:
        log = logging.getLogger("LS.destroy local channel")
        log.info("Received an SHDestroyProcessLocalChannel for cuid=%s" % msg.cuid)

        if not msg.puid in self.puid2pid:
            resp_msg = dmsg.SHDestroyProcessLocalChannelResponse(
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
                self.apt[self.puid2pid[msg.puid]].props.local_cuids.remove(msg.cuid)
                self.local_cuids.reclaim([msg.cuid])

        except Exception as ex:
            error = "%r failed: %s" % (msg, ex)
            resp_msg = dmsg.SHDestroyProcessLocalChannelResponse(
                tag=get_new_tag(), ref=msg.tag, err=DragonError.INVALID_OPERATION, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        resp_msg = dmsg.SHDestroyProcessLocalChannelResponse(tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS)
        log.info("Received and Destroyed a channel via SHDestroyProcessLocalChannel")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.SHCreateProcessLocalPool, _DTBL)
    def create_process_local_pool(self, msg: dmsg.SHCreateProcessLocalPool) -> None:
        log = logging.getLogger("LS.create local pool")
        log.info("Received an SHCreateProcessLocalPool")

        if not msg.puid in self.puid2pid:
            resp_msg = dmsg.SHCreateProcessLocalPoolResponse(
                tag=get_new_tag(),
                ref=msg.tag,
                err=DragonError.INVALID_ARGUMENT,
                errInfo="Cannot create pool for non-existent local process on node.",
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        try:
            pool = self.make_local_pool(msg)
            self.apt[self.puid2pid[msg.puid]].props.local_muids.add(pool.muid)
        except Exception as cex:
            error = "%r failed: %s" % (msg, cex)
            resp_msg = dmsg.SHCreateProcessLocalPoolResponse(
                tag=get_new_tag(), ref=msg.tag, err=DragonError.INVALID_OPERATION, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        encoded_desc = b64encode(pool.serialize())
        resp_msg = dmsg.SHCreateProcessLocalPoolResponse(
            tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS, serPool=encoded_desc
        )
        log.info("Received and Created a pool via SHCreateProcessLocalPool")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.SHRegisterProcessLocalPool, _DTBL)
    def register_process_local_pool(self, msg: dmsg.SHRegisterProcessLocalPool) -> None:
        log = logging.getLogger("LS.register local pool")
        log.info("Received an SHRegisterProcessLocalPool")

        if not msg.puid in self.puid2pid:
            resp_msg = dmsg.SHRegisterProcessLocalPoolResponse(
                tag=get_new_tag(),
                ref=msg.tag,
                err=DragonError.INVALID_ARGUMENT,
                errInfo="Cannot register pool for non-existent local process on node.",
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        try:
            pool = self.register_local_pool(msg)
            self.apt[self.puid2pid[msg.puid]].props.local_muids.add(pool.muid)
        except Exception as cex:
            error = "%r failed: %s" % (msg, cex)
            resp_msg = dmsg.SHRegisterProcessLocalPoolResponse(
                tag=get_new_tag(), ref=msg.tag, err=DragonError.INVALID_OPERATION, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        resp_msg = dmsg.SHRegisterProcessLocalPoolResponse(tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS)
        log.info("Received and Registered a pool via SHRegisterProcessLocalPool")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.SHDeregisterProcessLocalPool, _DTBL)
    def deregister_process_local_pool(self, msg: dmsg.SHDeregisterProcessLocalPool) -> None:
        log = logging.getLogger("LS.deregister local pool")
        log.info("Received an SHDeregisterProcessLocalPool")

        if not msg.puid in self.puid2pid:
            resp_msg = dmsg.SHDeregisterProcessLocalPoolResponse(
                tag=get_new_tag(),
                ref=msg.tag,
                err=DragonError.INVALID_ARGUMENT,
                errInfo="Cannot deregister pool for non-existent local process on node.",
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        try:
            pool = self.deregister_local_pool(msg)
            self.apt[self.puid2pid[msg.puid]].props.local_muids.remove(pool.muid)
            pool.detach()
        except Exception as cex:
            error = "%r failed: %s" % (msg, cex)
            resp_msg = dmsg.SHDeregisterProcessLocalPoolResponse(
                tag=get_new_tag(), ref=msg.tag, err=DragonError.INVALID_OPERATION, errInfo=error
            )
            send_fli_response(resp_msg, msg.respFLI)
            return

        resp_msg = dmsg.SHDeregisterProcessLocalPoolResponse(tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS)
        log.info("Received and Deregistered a pool via SHDeregisterProcessLocalPool")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.SHChannelDestroy, _DTBL)
    def destroy_channel(self, msg: dmsg.SHChannelDestroy) -> None:
        log = logging.getLogger("LS.destroy channel")
        success, fail = mk_response_pairs(dmsg.SHChannelDestroyResponse, msg.tag)

        error = ""
        if msg.c_uid not in self.channels:
            error = "%s does not exist" % msg.c_uid

        if not error:
            ch = self.channels.pop(msg.c_uid)
            try:
                ch.destroy()
            except dch.ChannelError as cex:
                error = "%r failed: %s" % (msg, cex)

        if error:
            log.warning(error)
            resp_msg = fail(error)
        else:
            resp_msg = success()

        return resp_msg

    @dutil.route(dmsg.SHMultiProcessCreate, _DTBL)
    def create_group(self, msg: dmsg.SHMultiProcessCreate) -> None:
        log = logging.getLogger("LS.create_group")
        success, fail = mk_response_pairs(dmsg.SHMultiProcessCreateResponse, msg.tag)

        responses = []
        failed = False
        for process_create_msg in msg.procs:
            response = self.create_process(process_create_msg, msg.pmi_group_info)
            responses.append(response)
            if response.err == dmsg.SHProcessCreateResponse.Errors.FAIL:
                failed = True

        # always return success
        resp_msg = success(responses=responses, failed=failed)
        return resp_msg

    @dutil.route(dmsg.SHProcessCreate, _DTBL)
    def create_process(self, msg: dmsg.SHProcessCreate, pmi_group_info: Optional[dmsg.PMIGroupInfo] = None) -> None:
        log = logging.getLogger("LS.create process")
        success, fail = mk_response_pairs(dmsg.SHProcessCreateResponse, msg.tag)

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

        the_env = dict(os.environ)
        the_env.update(req_env)

        # Add in the local services return serialized channel descriptor.
        shep_return_ch = self.make_local_channel()
        shep_return_cd = b64encode(shep_return_ch.serialize())
        the_env[dfacts.env_name(dfacts.SHEP_RET_CD)] = shep_return_cd

        gs_ret_chan_resp = None
        stdin_conn = None
        stdin_resp = None
        stdout_conn = None
        stdout_resp = None
        stderr_conn = None
        stderr_resp = None
        stdout_root = False
        stderr_root = False

        if msg.gs_ret_chan_msg is not None:
            gs_ret_chan_resp = self.create_channel(msg.gs_ret_chan_msg)
            if gs_ret_chan_resp.err != dmsg.SHChannelCreateResponse.Errors.SUCCESS:
                resp_msg = fail("Failed creating the GS ret channel for new process: %s" % stdin_resp.err_info)
                return resp_msg
            desc = gs_ret_chan_resp.desc
            the_env[dfacts.ENV_GS_RET_CD] = desc

        if msg.stdin_msg is not None:
            stdin_resp = self.create_channel(msg.stdin_msg)
            if stdin_resp.err != dmsg.SHChannelCreateResponse.Errors.SUCCESS:
                resp_msg = fail("Failed creating the stdin channel for new process: %s" % stdin_resp.err_info)
                return resp_msg
            stdin_conn = mk_input_connection_over_channel(stdin_resp.desc)

        if msg.stdout_msg is not None:
            stdout_resp = self.create_channel(msg.stdout_msg)
            if stdout_resp.err != dmsg.SHChannelCreateResponse.Errors.SUCCESS:
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
            if stderr_resp.err != dmsg.SHChannelCreateResponse.Errors.SUCCESS:
                # TBD: We need to destroy the stdin and stdout channels if they exist
                resp_msg = fail("Failed creating the stderr channel for new process: %s" % stderr_resp.err_info)
                return resp_msg
            stderr_conn = mk_output_connection_over_channel(stderr_resp.desc)
            stderr_root = True
            the_env[dfacts.STDERR_DESC] = stderr_resp.desc
        elif msg.stderr == dmsg.STDOUT:
            # We put stderr connection in environment so any subprocesses
            # will also write to the stdout connection.
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

            stderr = subprocess.PIPE
            if msg.stderr == subprocess.DEVNULL:
                # Same handling as stdout explanation above.
                stderr = subprocess.DEVNULL

            if msg.stderr == subprocess.STDOUT:
                stderr = subprocess.STDOUT

            if pmi_group_info and msg.pmi_info:
                log.debug("pmi_group_info=%s", str(pmi_group_info))
                log.debug("pmi_process_info=%s", str(msg.pmi_info))
                log.info("p_uid %s looking up pmod launch cuid" % msg.t_p_uid)
                pmod_launch_cuid = dfacts.pmod_launch_cuid_from_jobinfo(
                    dutils.host_id(), pmi_group_info.job_id, msg.pmi_info.lrank
                )

                log.info("p_uid %s Creating pmod launch channel using pmod_launch_cuid=%s" % (msg.t_p_uid, pmod_launch_cuid))
                node_index = parms.this_process.index
                inf_muid = dfacts.infrastructure_pool_muid_from_index(node_index)
                pmod_launch_ch = dch.Channel(self.pools[inf_muid], pmod_launch_cuid)
                the_env["DRAGON_PMOD_CHILD_CHANNEL"] = str(dutils.B64(pmod_launch_ch.serialize()))

                log.info("p_uid %s Setting required PMI environment variables" % msg.t_p_uid)
                # For PBS, we need to tell PMI to not use a FD to get PALS info:
                try:
                    del the_env["PMI_CONTROL_FD"]
                except KeyError:
                    pass

                the_env["PMI_CONTROL_PORT"] = str(pmi_group_info.control_port)
                the_env["MPICH_OFI_CXI_PID_BASE"] = str(msg.pmi_info.pid_base)
                the_env["DL_PLUGIN_RESILIENCY"] = "1"
                the_env["LD_PRELOAD"] = str(dfacts.DRAGON_LIB_SO)
                the_env["LD_LIBRARY_PATH"] = str(dfacts.DRAGON_LIB_DIR) + ":" + str(the_env.get("LD_LIBRARY_PATH", ""))
                the_env["_DRAGON_PALS_ENABLED"] = "1"
                the_env["FI_CXI_RX_MATCH_MODE"] = "hybrid"

            stdin_connector = InputConnector(stdin_conn)

            stdout_connector = OutputConnector(
                be_in=self.be_in,
                puid=msg.t_p_uid,
                hostname=self.hostname,
                out_err=dmsg.SHFwdOutput.FDNum.STDOUT.value,
                conn=stdout_conn,
                root_proc=stdout_root,
                critical_proc=False,
            )

            stderr_connector = OutputConnector(
                be_in=self.be_in,
                puid=msg.t_p_uid,
                hostname=self.hostname,
                out_err=dmsg.SHFwdOutput.FDNum.STDERR.value,
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
                        local_cuids=set([shep_return_ch.cuid]),
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
                log.info("p_uid %s sending mpi data for %s" % (msg.t_p_uid, msg.pmi_info.lrank))
                pmod.PMOD(
                    msg.pmi_info.ppn,
                    msg.pmi_info.nid,
                    pmi_group_info.nnodes,
                    pmi_group_info.nranks,
                    pmi_group_info.nidlist,
                    pmi_group_info.hostlist,
                    pmi_group_info.job_id,
                ).send_mpi_data(msg.pmi_info.lrank, pmod_launch_ch)
                log.info("p_uid %s DONE: sending mpi data for %s" % (msg.t_p_uid, msg.pmi_info.lrank))

            log.info("p_uid %s created as %s" % (msg.t_p_uid, the_proc.pid))
            self.new_procs.put(the_proc)
            if msg.initial_stdin is not None and msg.initial_stdin != "":
                # we are asked to provide a string to the started process.
                log.info("Writing %s to newly created process" % msg.initial_stdin)
                proc_stdin = os.fdopen(the_proc.stdin.fileno(), "wb")
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
            resp_msg = fail(error)

        return resp_msg

    @dutil.route(dmsg.SHMultiProcessKill, _DTBL)
    def kill_group(self, msg: dmsg.SHMultiProcessKill) -> None:
        log = logging.getLogger("LS.kill_group")
        log.debug("handling %s", msg)
        success, fail = mk_response_pairs(dmsg.SHMultiProcessKillResponse, msg.tag)

        responses = []
        failed = False

        # Issue the kill for each process now
        for process_kill_msg in msg.procs:
            response = self.kill_process(process_kill_msg)
            if response:
                responses.append(response)
                if response.err == dmsg.SHProcessKillResponse.Errors.FAIL:
                    failed = True

        # always return success
        resp_msg = success(responses=responses, failed=failed)
        log.debug("done handling %s", msg)
        return resp_msg

    @dutil.route(dmsg.SHProcessKill, _DTBL)
    def kill_process(self, msg: dmsg.SHProcessKill) -> None:
        log = logging.getLogger("LS.kill process")
        log.debug("handling %s", msg)
        success, fail = mk_response_pairs(dmsg.SHProcessKillResponse, msg.tag)

        if msg.hide_stderr:
            with self.apt_lock:
                try:
                    proc = self.apt[self.puid2pid[msg.t_p_uid]]
                    if proc.props.stderr_connector is not None:
                        proc.props.stderr_connector.end_stderr_forwarding()
                except:
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

    @dutil.route(dmsg.SHFwdInput, _DTBL)
    def fwd_input(self, msg: dmsg.SHFwdInput) -> None:
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
                    if len(output_data) > dmsg.SHFwdInput.MAX:
                        log.warning("truncating request of %s to %s" % (len(output_data), dmsg.SHFwdInput.MAX))

                    fh = sel[0][0].fileobj
                    fh.write(output_data[: dmsg.SHFwdInput.MAX])
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
            success, fail = mk_response_pairs(dmsg.SHFwdInputErr, msg.tag)
            if error:
                resp_msg = fail(error)
            else:
                resp_msg = success()

            return resp_msg

    @dutil.route(dmsg.SHSetKV, _DTBL)
    def handle_set_kv(self, msg: dmsg.SHSetKV) -> None:
        log = logging.getLogger("LS.set key-value")
        if msg.value == "":
            if msg.key in self.kvs:
                del self.kvs[msg.key]
        else:
            self.kvs[msg.key] = msg.value
        resp_msg = dmsg.SHSetKVResponse(tag=get_new_tag(), ref=msg.tag, err=DragonError.SUCCESS)
        log.info("Received SHSetKV message and processed it.")
        send_fli_response(resp_msg, msg.respFLI)

    @dutil.route(dmsg.SHGetKV, _DTBL)
    def handle_get_kv(self, msg: dmsg.SHSetKV) -> None:
        log = logging.getLogger("LS.get key-value")
        if not msg.key in self.kvs:
            resp_msg = dmsg.SHGetKVResponse(tag=get_new_tag(), ref=msg.tag, value="", err=DragonError.NOT_FOUND)
        else:
            val = self.kvs[msg.key]
            resp_msg = dmsg.SHGetKVResponse(tag=get_new_tag(), ref=msg.tag, value=val, err=DragonError.SUCCESS)
        log.info("Received SHSetKV message and processed it.")
        send_fli_response(resp_msg, msg.respFLI)

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
        self.be_in.send(msg.serialize())

    @dutil.route(dmsg.SHTeardown, _DTBL)
    def teardown_ls(self, msg: dmsg.SHTeardown) -> None:
        log = logging.getLogger("LS.teardown LS")
        log.info("isPrimary=%s handling SHTeardown" % self.is_primary)
        self.set_shutdown(msg)

    @dutil.route(dmsg.SHHaltTA, _DTBL)
    def handle_halting_ta(self, msg):
        log = logging.getLogger("LS.forward SHHaltTA msg")
        log.info("handling %s" % msg)
        # m8 Forward SHHaltTA to TA
        self.ta_in.send(msg.serialize())

    @dutil.route(dmsg.TAHalted, _DTBL)
    def handle_ta_halted(self, msg):
        self.set_ta_shutdown()
        log = logging.getLogger("LS.forward TAHalted msg")
        log.info("handling %s" % msg)
        self.be_in.send(msg.serialize())

    @dutil.route(dmsg.SHDumpState, _DTBL)
    def dump_state(self, msg: dmsg.SHDumpState) -> None:
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
                resp = dmsg.SHProcessExit(
                    tag=get_new_tag(),
                    exit_code=ecode,
                    p_uid=proc.props.p_uid,
                    creation_msg_tag=proc.props.creation_msg_tag,
                )

                if proc.props.p_uid != dfacts.GS_PUID:
                    if proc.props.r_c_uid is None:
                        self.gs_in.send(resp.serialize())
                        log.info("transmit %s via gs_in" % repr(resp))
                    else:
                        r_c_uid = proc.props.r_c_uid
                        self._send_response(target_uid=r_c_uid, msg=resp)
                        log.info("transmit %s via _send_response" % repr(resp))

                # Delete process local channels and pools and reclaim their uids.
                self.cleanup_local_channels_pools(proc)

                # If we haven't received SHTeardown yet
                if proc.props.critical and not self.check_shutdown():
                    if proc.props.p_uid == dfacts.GS_PUID:
                        # if this is GS and we haven't received GSHalted yet and SHTeardown
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
                    # underlying channel will be decref'ed when the SHProcessExit is
                    # received by global services (see server.py in GS).
                    if proc.props.stdout_connector is not None:
                        self.exited_channel_output_monitors.put(proc.props.stdout_connector)
                    if proc.props.stderr_connector is not None:
                        self.exited_channel_output_monitors.put(proc.props.stderr_connector)
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

        log.info("critical_bytes_env=%s critical_pct_env=%s critical_pct=%s" % (critical_bytes_env, critical_pct_env, critical_pct))

        try:
            suppress_warnings = lparms.warnings_off != "False"
        except:
            suppress_warnings = False

        try:
            while not self.check_shutdown():
                mem_utilization = psutil.virtual_memory().percent
                mem_bytes = psutil.virtual_memory().available
                if mem_utilization >= critical_pct:
                    critical_msg = "OOM CRITICAL: Free memory on node %s with hostname %s is less than %s% with %s bytes available. Runtime is coming down.\n" % (parms.this_process.index, self.hostname, int(100-mem_utilization)+1, mem_bytes)
                    self.be_in.send(
                        dmsg.SHFwdOutput(
                            tag=get_new_tag(),
                            idx=parms.this_process.index,
                            p_uid=parms.this_process.my_puid,
                            data=critical_msg,
                            fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value,
                            pid=pid,
                            hostname=self.hostname,
                        ).serialize()
                    )
                    self._abnormal_termination(critical_msg)
                elif mem_utilization >= warn_pct:
                    warning_msg = "\nOOM WARNING: Free memory on node %s with hostname %s is less than %s% with %s bytes still available.\n" % (parms.this_process.index, self.hostname, int(100-mem_utilization)+1, mem_bytes)
                    log.info(warning_msg)
                    if not suppress_warnings:
                        self.be_in.send(
                            dmsg.SHFwdOutput(
                                tag=get_new_tag(),
                                idx=parms.this_process.index,
                                p_uid=parms.this_process.my_puid,
                                data=warning_msg,
                                fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value,
                                pid=pid,
                                hostname=self.hostname,
                            ).serialize()
                        )

                if mem_utilization > 95:
                    interval = self.OOM_RAPID_SLEEP_TIME
                else:
                    interval = self.OOM_SLOW_SLEEP_TIME

                # Only poll every 5 seconds normally and every 1 second when running low on mem.
                # By waiting on the shutdown event with a timeout of interval, we effectively
                # sleep for the interval during normal processing and wake up immediately when shutting down.
                self.shutdown_sig.wait(interval)
        except Exception as ex:
            tb = traceback.format_exc()
            self._abnormal_termination("LS OOM daemon exception: %s\n%s" % (ex, tb))

        log.info("Exiting")

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

    def watch_output(self):
        """Thread monitors outbound std* activity from processes we started.

            Any stderr activity on a 'critical' (e.g. infrastructure) process
            running under this shepherd will cause an error shutdown.

        :return: None, exits on self.check_shutdown()
        """
        log = logging.getLogger("LS.watch output")

        log.info("starting")

        class WatchingSelector(selectors.DefaultSelector):
            """Enhanced DefaultSelector to register stdout/stderr of PopenProps

            Automates registering the file handles with the selector base class
            and maps it to an OutputConnector object to handle the logic for
            forwarding data where it needs to go.
            """

            def add_proc_streams(self, server, proc: PopenProps):
                # carried data is (ProcessProps, closure to make SHFwdOutput, whether stderr or not)
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

        stream_sel = WatchingSelector()

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
                        msg = dmsg.parse(str_data)
                        if isinstance(msg, dmsg.GSHalted):
                            handled = True
                            self._DTBL[dmsg.GSHalted][0](self, msg=msg)
                            EOF = True
                    except json.JSONDecodeError:
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

        stream_sel.close()
        log.info("exit")
