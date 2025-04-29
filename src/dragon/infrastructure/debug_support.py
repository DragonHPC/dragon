import atexit
import os
import pdb
import sys
import traceback

import dragon.channels as dch
import dragon.managed_memory as dmm

import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.parameters as dp
import dragon.utils as du
import dragon.infrastructure.connection as dconn

_DEBUG_OUT = None  # once initialized, attached dch.Channel
_DEBUG_IN = None
_LOCAL_BE = None
_LOCAL_BE_CONN = None

_BREAKPOINT_FH = None


def handle_breakpoint(msg):
    # print some stuff out and update the breakpoint file
    global _BREAKPOINT_FH
    if _BREAKPOINT_FH is None:
        _BREAKPOINT_FH = open(dfacts.BREAKPOINT_FILENAME, "w")

    _BREAKPOINT_FH.write(msg.serialize() + "\n")
    _BREAKPOINT_FH.flush()
    sys.stdout.write(f"### breakpoint:  p_uid {msg.p_uid} on node {msg.index}\n")
    sys.stdout.flush()


def _cleanup_debug_channels():
    # TODO: figure out other cleanup to do here.  Should there be a switch to
    # try to destroy all the debug channels?  Some of this might better be under
    # the control of the reader, but the input channel can probably go
    out_adapter = DbgAdapter(_DEBUG_OUT)
    out_adapter.write(f"### debug exit for: {dp.this_process.my_puid} on {dp.this_process.index}\n\n")
    _DEBUG_OUT.detach()
    _LOCAL_BE.detach()
    _DEBUG_IN.destroy()


def _connect_debug_channels():
    global _DEBUG_OUT
    global _DEBUG_IN
    global _LOCAL_BE
    global _LOCAL_BE_CONN

    inf_pool_descr = du.B64.str_to_bytes(dp.this_process.inf_pd)
    inf_pool = dmm.MemoryPool.attach(inf_pool_descr)

    my_pid = os.getpid()

    dbg_out_cuid = dfacts.BASE_DEBUG_CUID * (2 * my_pid)
    dbg_in_cuid = dfacts.BASE_DEBUG_CUID * (2 * my_pid + 1)

    _DEBUG_OUT = dch.Channel(mem_pool=inf_pool, c_uid=dbg_out_cuid)
    _DEBUG_IN = dch.Channel(mem_pool=inf_pool, c_uid=dbg_in_cuid)

    be_ch_descr = du.B64.str_to_bytes(dp.this_process.local_be_cd)
    _LOCAL_BE = dch.Channel.attach(be_ch_descr)
    _LOCAL_BE_CONN = dconn.Connection(outbound_initializer=_LOCAL_BE)

    atexit.register(_cleanup_debug_channels)


class DbgAdapter:
    def __init__(self, chan):
        self.chan = chan
        self.sendh = self.chan.sendh()
        self.sendh.open()
        self.recvh = self.chan.recvh()
        self.recvh.open()

    def read(self):
        return self.recvh.recv_bytes().decode()

    def readline(self):
        return self.read()

    def write(self, data):
        self.sendh.send_bytes(data.encode())
        return len(data)

    def close(self):
        pass

    def flush(self):
        pass


# if true, write to the breakpoints file locally.
_TESTING_DEBUG_HOOK = False


def _get_bk_msg():
    if _DEBUG_OUT is None:
        _connect_debug_channels()

    bk_msg = dmsg.Breakpoint(
        tag=0,
        p_uid=dp.this_process.my_puid,
        index=dp.this_process.index,
        out_desc=du.B64.bytes_to_str(_DEBUG_OUT.serialize()),
        in_desc=du.B64.bytes_to_str(_DEBUG_IN.serialize()),
    )

    if _TESTING_DEBUG_HOOK:
        handle_breakpoint(bk_msg)

    return bk_msg


def dragon_debug_hook():
    bk_msg = _get_bk_msg()

    _LOCAL_BE_CONN.send(bk_msg.serialize())
    out_adapter = DbgAdapter(_DEBUG_OUT)
    out_adapter.write(f"### breakpoint: p_uid {dp.this_process.my_puid} on node {dp.this_process.index}")
    pdb.Pdb(stdin=DbgAdapter(_DEBUG_IN), stdout=out_adapter, skip=[f"{__name__}*"]).set_trace()


def dragon_exception_hook(ex_type, ex_obj, ex_tb):
    bk_msg = _get_bk_msg()

    _LOCAL_BE_CONN.send(bk_msg.serialize())
    out_adapter = DbgAdapter(_DEBUG_OUT)
    out_adapter.write(
        f"### breakpoint: uncaught exception {ex_type!s} "
        f"p_uid {dp.this_process.my_puid} on node "
        f"{dp.this_process.index}"
    )

    ex_lines = traceback.format_exception(ex_type, ex_obj, ex_tb)
    for line in ex_lines:
        out_adapter.write("### " + line)

    pdb.Pdb(stdin=DbgAdapter(_DEBUG_IN), stdout=out_adapter).post_mortem(ex_tb)
