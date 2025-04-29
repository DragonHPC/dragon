"""Global server context object and entry point"""

import enum
import logging
import os
import pickle
import sys
import time
import threading

from queue import Queue
from .. import channels as dch

from ..infrastructure import channel_desc
from ..infrastructure import pool_desc
from ..infrastructure import process_desc
from ..infrastructure import group_desc

from .channel_int import ChannelContext
from .pool_int import PoolContext
from .process_int import ProcessContext
from .node_int import NodeContext
from .group_int import GroupContext

from ..dtypes import DragonError

from . import startup
from .policy_eval import ResourceLayout, PolicyEvaluator
from ..infrastructure.policy import Policy

from ..infrastructure import facts as dfacts
from ..infrastructure import messages as dmsg
from ..infrastructure import util as dutil
from ..infrastructure import connection as dconn
from ..infrastructure import parameters as dparm
from ..dlogging import util as dlog
from ..dlogging.util import DragonLoggingServices as dls
from ..utils import B64, set_procname


class ServerError(Exception):
    pass


class GlobalContext(object):
    """Global Context for the dragon runtime

    This object is meant to hold whatever info is maintained
    for all channels, pools, and processes and contain the main
    message processing and dispatch code.

    Ultimately parts of this will need to be distributed.

    Important member variables:

    process_table - key is p_uid, values are ProcessContext
    channel_table - key is c_uid, values are ChannelContext
    pool_table - key is m_uid, values are PoolContext
    node_table - key is h_uid, values are NodeContext
    group_table - key is g_uid, values are GroupContext

    process_names - key is string name of process, value is p_uid
    channel_names - key is string name of channel, value is c_uid
    pool_names - key is string name of pool, value is m_uid
    node_names - key is string name of node, value is h_uid
    group_names - key is string name of group, value is g_uid
    """

    class LaunchModes(enum.Enum):
        SINGLE = enum.auto()
        MULTI = enum.auto()
        TEST_STANDALONE_SINGLE = enum.auto()
        TEST_STANDALONE_MULTI = enum.auto()

    class RunState(enum.Enum):
        STARTING = enum.auto()
        WAITING_FOR_HEAD = enum.auto()
        HAS_HEAD = enum.auto()
        SHUTTING_DOWN = enum.auto()

    DTBL = {}  # dispatch table for handlers, no metadata

    def __init__(self, test_gs_stdout=None):
        self.process_table = dict()  # key = p_uid, value = ProcessContext
        self.channel_table = dict()  # key = c_uid, value = ChannelContext
        self.pool_table = dict()  # key = m_uid, value = PoolContext
        self.node_table = dict()  # key = h_uid, value = NodeContext
        self.group_table = dict()  # key = g_uid, value = GroupContext

        self.process_names = dict()  # key = name, value = p_uid
        self.channel_names = dict()  # key = name, value = c_uid
        self.pool_names = dict()  # key = name, value = m_uid
        self.node_names = dict()  # key = name, value = h_uid
        self.group_names = dict()  # key = name, value = g_uid

        # used by the group of resources objects, in group_int.py
        self.resource_to_group_map = (
            dict()
        )  # key = resource uid e.g. p_uid, value = tuple including g_uid, and position of the resource in the GSGroupCreate.items list
        self.group_to_pending_resource_map = (
            dict()
        )  # used for pending resources of a group, key = (msg tag, g_uid), value = resource_context
        self.group_resource_count = (
            dict()
        )  # key = g_uid, value = list of items corresponding to the multiplicity of each list item in GroupDescriptor.sets
        self.group_destroy_resource_count = (
            dict()
        )  # is applied only when destroy has been called, key = g_uid, value = list of items corresponding to the multiplicity of each list item in GroupDescriptor.sets

        self.pending = dict()  # key = tag, value = continuation on response
        self.pending_group_destroy = (
            dict()
        )  # is applied only to groups of resources when destroy is called, key = uid, value = continuation on response
        self.pending_process_exits = dict()  # key = creation msg tag, value = SHProcessExit msg

        self.pending_sends = Queue()

        self.pending_join = dutil.PriorityMultiMap()  # pending joins, key = p_uid val = set of join req for this p_uid
        self.pending_join_list = (
            dutil.PriorityMultiMap()
        )  # pending joins, key = frozenset of p_uid, val = set of join req for this set of p_uid
        self.pending_channel_joins = dutil.PriorityMultiMap()  # pending channel joins  key = channel names

        self.puid_join_list_status = (
            dict()
        )  # this is a dict of dicts that holds the p_uid status for every muti-join wait request
        # key is a tuple with of (msg.tag, msg.p_uid) and value is a dict with: key = p_uid and val = (status, info)

        self.sh_pings_recvd = 0  # added to as we receive the sh pings
        self.shep_inputs = [None]
        self.gs_input = None
        self.bela_input = None
        self.test_connections = None
        self.head_puid = set()
        self._tag = 0
        self._next_puid = dfacts.FIRST_PUID
        self._next_cuid = dfacts.FIRST_CUID
        self._next_muid = dfacts.FIRST_MUID
        self._next_guid = dfacts.FIRST_GUID

        if test_gs_stdout is None:
            self.msg_stdout = dutil.NewlineStreamWrapper(sys.stdout)
        else:
            self.msg_stdout = test_gs_stdout

        self._launch_mode = None
        self._state = GlobalContext.RunState.STARTING
        self._startup_logger = logging.getLogger(dls.GS).getChild("startup:")
        self._shutdown_logger = logging.getLogger(dls.GS).getChild("shutdown:")
        self._dispatch_logger = logging.getLogger(dls.GS).getChild("dispatch:")
        self._process_logger = logging.getLogger(dls.GS).getChild("proc:")
        self._pool_logger = logging.getLogger(dls.GS).getChild("pool:")
        self._channel_logger = logging.getLogger(dls.GS).getChild("channel:")
        self._node_logger = logging.getLogger(dls.GS).getChild("node:")
        self._group_logger = logging.getLogger(dls.GS).getChild("group:")

        self.circ_index = 0  # helper index for implementing the circular order of choosing shepherds

        self.policy_eval = None  # Policy Evaluator, init'd in `handle_sh_ping`
        self.resilient_groups = (
            False  # Whether to handle Groups as if they are a group of individually critical processes
        )
        # and restart should be attempted upon one of their failures

    ################ Support Functions for GS #######################################

    def dump_to(self, fh):
        """Dumps a human readable representation of current state to a file-like object.

        May want eventually to have a version that essentially pickles
        the server state apart from handles, but that isn't this.
        """

        fh.write(f"state:\t\t{self._state.name}\n")
        fh.write(f"mode:\t\t{self._launch_mode.name}\n")
        fh.write(f"current tag:\t{self.tag}\n")
        fh.write(f"head_puid:\t{self.head_puid}\n")

        fh.write("\nProcesses:\n")
        for ctx in self.process_table.values():
            fh.write(f"{ctx!s}\n")

        # TODO: PE-35937, dump channels and pools
        fh.write("\nPending join:\n")

        for p_uid, waiters in self.pending_join.items():
            fh.write(f"\t{p_uid}:\t")
            fh.write(", ".join([f"{wr.p_uid}:{wr.r_c_uid}" for wr in waiters]))
            fh.write("\n")

        for p_uid_list, waiters in self.pending_join_list.items():
            fh.write(f"\t{p_uid_list}:\t")
            fh.write(", ".join([f"{wr.p_uid}:{wr.r_c_uid}" for wr in waiters]))
            fh.write("\n")

        fh.write("\nChannels:\n")
        fh.write("--none for now---\n")

    @property
    def tag(self):
        return self._tag

    def tag_inc(self):
        """Returns current tag counter and increments it"""
        tmp = self._tag
        self._tag += 1
        return tmp

    def new_puid_and_default_name(self):
        tmp = self._next_puid
        self._next_puid += 1
        return tmp, dfacts.DEFAULT_PROCESS_NAME_BASE + str(tmp)

    def new_cuid_and_default_name(self):
        tmp = self._next_cuid
        self._next_cuid += 1
        return tmp, dfacts.DEFAULT_CHANNEL_NAME_BASE + str(tmp)

    def new_muid_and_default_name(self):
        tmp = self._next_muid
        self._next_muid += 1
        return tmp, dfacts.DEFAULT_POOL_NAME_BASE + str(tmp)

    def new_guid_and_default_name(self):
        tmp = self._next_guid
        self._next_guid += 1
        return tmp, dfacts.DEFAULT_GROUP_NAME_BASE + str(tmp)

    # TODO: check that the message came from somewhere that makes sense.
    # for instance clients shouldn't get to send teardown messages
    def next_msg(self, timeout=0):
        # for basic unit testing
        if self.gs_input is None:
            return None

        # TODO: consider reworking call interface considering
        # single message assumption
        if self.gs_input.poll(timeout):
            rv = dmsg.parse(self.gs_input.recv())  # blocking recv
        else:
            rv = None

        return rv

    # comes up with the handle needed to reply to this message.
    def get_reply_handle(self, msg):
        log = self._dispatch_logger

        if self._launch_mode in {
            self.LaunchModes.MULTI,
            self.LaunchModes.SINGLE,
            self.LaunchModes.TEST_STANDALONE_SINGLE,
        }:
            if 1 == msg.r_c_uid:
                log.debug("this is stdout")
                return self.msg_stdout
            elif 2 == msg.r_c_uid:
                raise ServerError("channel 2 requested as r_c_uid but this is gs input")
            elif dfacts.BASE_BE_CUID <= msg.r_c_uid < (dfacts.BASE_BE_CUID + dfacts.RANGE_BE_CUID):
                log.debug("this is bela")
                return self.bela_input
            elif dfacts.BASE_SHEP_CUID <= msg.r_c_uid < (dfacts.BASE_SHEP_CUID + dfacts.RANGE_SHEP_CUID):
                log.debug("this is shep")
                return self.shep_inputs[dfacts.index_from_shepherd_cuid(msg.r_c_uid)]
            elif self.test_connections and msg.r_c_uid in self.test_connections:
                log.debug("this is test connections")
                return self.test_connections[msg.r_c_uid]
            elif msg.r_c_uid in self.channel_table:
                # TODO PE-37246 GS Channel attachment ageing policy
                context = self.channel_table[msg.r_c_uid]
                if context.attached_connection is None:
                    underlying_channel = dch.Channel.attach(context.descriptor.sdesc)

                    if context.is_reading:
                        context.attached_connection = dconn.Connection(
                            inbound_initializer=underlying_channel, policy=dparm.POLICY_INFRASTRUCTURE
                        )
                    else:
                        context.attached_connection = dconn.Connection(
                            outbound_initializer=underlying_channel, policy=dparm.POLICY_INFRASTRUCTURE
                        )

                log.debug("this is context and channel_table")
                return context.attached_connection
            else:
                raise ServerError(f"cuid {msg.r_c_uid} requested but unknown")
        else:
            raise NotImplementedError("unknown launch mode")

    # TODO: design/collaboration approach for placement
    # interfaces.  tickets to follow...

    def choose_shepherd(self, msg: dmsg.GSProcessCreate):
        """
        Pick the shepherd to launch a process on. If the GSProcessCreate
        request includes a ResourceLayout request, use that. Otherwise,
        roundrobin the placement across the available nodes.
        """
        log = self._dispatch_logger
        assert isinstance(msg, dmsg.GSProcessCreate)

        if not msg.layout:
            index = self.circ_index
            self.circ_index = (self.circ_index + 1) % len(self.shep_inputs)
        else:
            assert isinstance(msg.layout, ResourceLayout)
            assert msg.layout.h_uid in self.node_table.keys()
            index = self.node_table[msg.layout.h_uid].ls_index

        # Get huid for ProcessDescriptor
        huid = [huid for huid, node in self.node_table.items() if node.ls_index == index][0]

        log.debug("choose shepherd %d (huid %d) for %s", index, huid, msg)
        return index, huid

    # picks the node to put a pool on
    # should raise from here if the options or state lead to nonsense
    # note this can't be a pure method on anything
    # inside the message because sometimes the choice
    # will depend on system state.
    def choose_pool_node(self, msg):
        assert isinstance(msg, dmsg.GSPoolCreate)
        if msg.p_uid in self.process_table:
            return self.process_table[msg.p_uid].descriptor.node  # the node of the requesting process
        else:
            # we have to retain this way for cases where the launcher (p_uid is 0) is the requesting process
            return 0

    # this ought to respect whether the m_uid asked for
    # is on the node that is asked for.   Might need a convention
    # for this, like 0 == u pick.
    # Might also want to take into account a logging pool m_uid. Not
    # sure since GS may never be involved, but worth looking at.
    def choose_channel_node(self, msg):
        log = self._dispatch_logger

        assert isinstance(msg, dmsg.GSChannelCreate)
        if msg.m_uid < dfacts.FIRST_LOGGING_MUID:
            # Then this is an infrastructure Pool m_uid.
            if msg.p_uid in self.process_table:
                log.debug("Choosing channel node on branch 1")
                return self.process_table[msg.p_uid].descriptor.node  # the node of the requesting process
            else:
                log.debug("Choosing channel node on branch 2")
                # we have to retain this way for cases where the launcher (p_uid is 0) is the requesting process
                return dfacts.index_from_infrastructure_pool_muid(msg.m_uid)
        else:
            if msg.m_uid in self.pool_table:
                log.debug("Choosing channel node on branch 3")
                return self.pool_table[msg.m_uid].descriptor.node
            else:
                # this is a predefined memory pool
                if dfacts.is_pre_defined_pool(msg.m_uid):
                    log.debug("Choosing channel node on branch 4")
                    return dfacts.index_from_default_pool_muid(msg.m_uid)
                else:
                    log.debug("Choosing channel node on branch 5")
                    # the requested pool to create the channel on does not exist
                    log.debug(f"The requested pool with muid {msg.m_uid} to create the channel on, does not exist.")
                    raise ValueError(f"The requested m_uid is not valid {msg.m_uid}")

    # This function uses the routing table to call the correct method for an
    # infrastructure message. See the @route decorated methods.
    def dispatch(self, msg):
        log = self._dispatch_logger
        if type(msg) in GlobalContext.DTBL:
            GlobalContext.DTBL[type(msg)][0](self, msg)
        else:
            log.warning(f"{msg} isn't implemented")
            raise NotImplementedError(f"Message type {type(msg)} is not handled, yet")

    @staticmethod
    def _resolve_uid(name, uid, names, table, what, idname):
        errmsg = ""

        if name:
            if name in names:
                uid = names[name]
                assert uid in table
                found = True
            else:
                errmsg = f"no {what} named {name}"
                found = False
        else:
            found = uid in table
            if not found:
                errmsg = f"no {idname} {uid}"

        return uid, found, errmsg

    def resolve_puid(self, name=None, p_uid=None):
        """Checks if a process exists given name and/or p_uid.

        Given name or p_uid specifying a process, resolves
        the p_uid and tells whether it is present.

        If the name is specified the p_uid parm is ignored.

        Returns: (p_uid, found, errmsg) where
            p_uid is the p_uid
            found whether that p_uid exists
            errmsg what is wrong if it does not exist.
        """
        return self._resolve_uid(name, p_uid, self.process_names, self.process_table, "process", "p_uid")

    def resolve_muid(self, name, m_uid=-1):
        """Checks if a memory pool exists given name and/or m_uid.

        see docstring on _resolve_puid for more details.
        """
        return self._resolve_uid(name, m_uid, self.pool_names, self.pool_table, "pool", "m_uid")

    def resolve_cuid(self, name, c_uid=-1):
        """Checks if a channel exists given name and/or c_uid.

        see docstring on _resolve_puid for more details.
        """
        return self._resolve_uid(name, c_uid, self.channel_names, self.channel_table, "channel", "c_uid")

    def resolve_huid(self, name, h_uid=-1):
        """Checks if a node exists given name and/or h_uid.

        see docstring on _resolve_puid for more details.
        """
        return self._resolve_uid(name, h_uid, self.node_names, self.node_table, "node", "h_uid")

    def resolve_guid(self, name, g_uid=-1):
        """Checks if a group exists given name and/or g_uid.

        see docstring on _resolve_puid for more details.
        """
        return self._resolve_uid(name, g_uid, self.group_names, self.group_table, "group", "g_uid")

    def do_timeouts(self):
        log = self._process_logger

        gspjr = dmsg.GSProcessJoinResponse
        timed_out = self.pending_join.get_timed_out()
        for join_request in timed_out:
            log.debug(f"join timed out: {join_request}")
            p_uid, req_msg = join_request
            reply_channel = self.get_reply_handle(req_msg)
            rm = gspjr(tag=self.tag_inc(), ref=req_msg.tag, err=gspjr.Errors.TIMEOUT)
            reply_channel.send(rm.serialize())
            self.pending_join.remove_one(p_uid, req_msg)
            log.debug(f"timed out join response to {req_msg!s}: {rm!s}")

        gspjlr = dmsg.GSProcessJoinListResponse
        timed_out = self.pending_join_list.get_timed_out()
        for join_request in timed_out:
            # TODO AICI-1422 Implement verbose logging options
            # log.debug('multi-join timed out: %s', join_request)
            p_uid_list, req_msg = join_request  # p_uid_list is a frozenset
            reply_channel = self.get_reply_handle(req_msg)
            for p_uid in p_uid_list:
                self.puid_join_list_status[(req_msg.tag, req_msg.p_uid)][p_uid] = (gspjlr.Errors.TIMEOUT.value, None)
            rm = gspjlr(
                tag=self.tag_inc(),
                ref=req_msg.tag,
                puid_status=self.puid_join_list_status[(req_msg.tag, req_msg.p_uid)],
            )
            reply_channel.send(rm.serialize())
            self.pending_join_list.remove_one(p_uid_list, req_msg)
            del self.puid_join_list_status[(req_msg.tag, req_msg.p_uid)]  # we are done with this msg req

        gscjr = dmsg.GSChannelJoinResponse
        timed_out = self.pending_channel_joins.get_timed_out()
        for channel_join_request in timed_out:
            log.debug(f"channel join timed out: {channel_join_request}")
            name, req_msg = channel_join_request
            reply_channel = self.get_reply_handle(req_msg)
            rm = gscjr(tag=self.tag_inc(), ref=req_msg.tag, err=gscjr.Errors.TIMEOUT)
            reply_channel.send(rm.serialize())
            self.pending_channel_joins.remove_one(name, req_msg)
            log.debug(f"timed out join response to {req_msg!s}: {rm!s}")

        join_deadline = self.pending_join.next_deadline()
        join_list_deadline = self.pending_join_list.next_deadline()
        channel_join_deadline = self.pending_channel_joins.next_deadline()

        if join_deadline is None and join_list_deadline is None and channel_join_deadline is None:
            return None
        elif join_deadline is None and join_list_deadline is None:
            return max(0, channel_join_deadline - time.time())
        elif join_deadline is None and channel_join_deadline is None:
            return max(0, join_list_deadline - time.time())
        elif join_list_deadline is None and channel_join_deadline is None:
            return max(0, join_deadline - time.time())
        elif join_deadline is None:
            nearest_deadline = min(join_list_deadline, channel_join_deadline)
            return max(0, nearest_deadline - time.time())
        elif join_list_deadline is None:
            nearest_deadline = min(join_deadline, channel_join_deadline)
            return max(0, nearest_deadline - time.time())
        elif channel_join_deadline is None:
            nearest_deadline = min(join_deadline, join_list_deadline)
            return max(0, nearest_deadline - time.time())
        else:
            nearest_deadline = min(join_deadline, join_list_deadline, channel_join_deadline)
            return max(0, nearest_deadline - time.time())

    def run_global_server(
        self,
        mode=LaunchModes.SINGLE,
        test_gs_input=None,
        test_shep_inputs=None,
        test_bela_input=None,
        test_gs_stdout=None,
        test_conns=None,
    ):
        """Main global server event loop.

        The global server loop

        :param test_conns: test connection objects - key is c_uid, value is
                           handle that you can send to or recv from.

        :param mode: How are we running this
        :param test_gs_input: Global Services input handle
        :param test_shep_inputs: List of shepherd handles for the whole allocation.
        :param test_gs_stdout: Global Services stdout message handle
        :param test_bela_input: Backend/Launcher input handle.
        :param test_conns: a c_uid keyed dictionary of test channel handles
        """
        assert mode == self._launch_mode

        self.test_connections = test_conns

        log = logging.getLogger(dls.GS).getChild("main_loop")
        log.info("entered")
        # for setting inputs in test
        if test_gs_input is not None:
            self.gs_input = test_gs_input

        if test_shep_inputs is not None:
            self.shep_inputs = test_shep_inputs

        if test_bela_input is not None:
            self.bela_input = test_bela_input

        if test_gs_stdout is not None:
            self.msg_stdout = test_gs_stdout

        while self._state != self.RunState.SHUTTING_DOWN:
            if self._state == self.RunState.HAS_HEAD:

                # In cases where we are sending a large amount of
                # messages, such as with the GSGroupCreate handler,
                # we can fill the GS Input Queue with responses and
                # basically cause the GS / TA / LS to be unable to
                # send/receive any messages. To prevent this, we'll
                # enqueue pending sends and interleave sending and
                # receiving messages to allow us to process responses
                # on the input queue.

                while self.pending_sends.qsize() > 0 and not self.gs_input.poll(timeout=0):
                    chan, msg = self.pending_sends.get()
                    chan.send(msg)

                next_timeout = self.do_timeouts()
            else:
                next_timeout = None

            the_msg = self.next_msg(next_timeout)
            if the_msg is not None:
                self.dispatch(the_msg)

        log = self._shutdown_logger
        log.info("main loop exit")

    def run_startup(
        self,
        mode=LaunchModes.SINGLE,
        test_gs_input=None,
        test_shep_inputs=None,
        test_bela_input=None,
        test_gs_stdout=None,
    ):
        """Complete startup activities with other actors.

        Normal exit indicates bringup went fine.
        Exception indicates bringup failed for some reason.

        The idea is to vary this for different scenarios,
        for instance tests.

        :param mode: How are we bringing this up
        :param test_shep_inputs: List of shepherd handles for the whole allocation.
        :param test_gs_input: Global Services input handle
        :param test_bela_input: Launcher/back end input handle
        :param test_gs_stdout: Global Services stdout message handle

        """

        self._launch_mode = mode

        if self.LaunchModes.SINGLE == self._launch_mode:
            self.shep_inputs, self.gs_input, self.bela_input, self.num_nodes = startup.startup_single(self)
        elif self.LaunchModes.MULTI == self._launch_mode:
            self.shep_inputs, self.gs_input, self.bela_input, self.num_nodes = startup.startup_multi(self)
        elif self.LaunchModes.TEST_STANDALONE_SINGLE == self._launch_mode:
            if test_shep_inputs is None:
                test_shep_inputs = [None]

            my_inputs = startup.startup_single(self, test_gs_input, test_shep_inputs[0], test_bela_input)

            self.shep_inputs, self.gs_input, self.bela_input, self.num_nodes = my_inputs

        elif self.LaunchModes.TEST_STANDALONE_MULTI == self._launch_mode:
            my_inputs = startup.startup_multi(self, test_gs_input, test_shep_inputs, test_bela_input)
            self.shep_inputs, self.gs_input, self.bela_input, self.num_nodes = my_inputs
        else:
            raise NotImplementedError("unknown mode")

    def add_node_from_SHPingGS(self, msg) -> None:
        """Create node contexts and h_uids from the node descriptors we have received
        from LS during startup via SHPingGS.

        :param messages: SHPingGS messages received during startup sequence
        """
        ctx = NodeContext.construct(msg)

        self.node_table[ctx.h_uid] = ctx
        self.node_names[ctx.name] = ctx.h_uid

        self._node_logger.info(f"Added node {ctx.name}, h_uid={ctx.h_uid}")

    def detach_from_channels(self):
        # this does the infrastructure channels.  Detaching from
        # the return channels or anything else that is open should happen in the
        # cleaning up user state part, which should have happened before this
        # gets called
        log = logging.getLogger("teardown_detach")

        real_channels = []
        if isinstance(self.gs_input, dconn.Connection):
            real_channels.append(self.gs_input)

        for shep in self.shep_inputs:
            if isinstance(shep, dconn.Connection):
                real_channels.append(shep)

        if isinstance(self.bela_input, dconn.Connection):
            real_channels.append(self.bela_input)

        if not real_channels:
            return

        log.debug("detaching from channels")

        # no need to detach from pool(s) due to refcounting
        # inside the channels library
        # also, GS does not explicitly attach to any pool
        for ch in real_channels:
            if ch.outbound_chan is not None:
                ch.outbound_chan.detach()

            if ch.inbound_chan is not None:
                ch.inbound_chan.detach()

        log.debug("channels detached")

    def run_teardown(self, test_gs_input=None, test_shep_inputs=None, test_bela_input=None, test_gs_stdout=None):
        """Complete teardown activities with other actors.

        Normal exit = happy path
        Exception indicates a failure

        :param test_gs_input: Global Services input handle
        :param test_shep_inputs: list of shepherd handles for the whole allocation.
        :param test_gs_stdout: Global Services stdout message handle
        :param test_bela_input: Back end/launcher message input handle
        """

        assert self.RunState.SHUTTING_DOWN == self._state

        if test_gs_stdout is not None:
            self.msg_stdout = test_gs_stdout

        if test_bela_input is not None:
            self.bela_input = test_bela_input

        log = self._shutdown_logger

        gait = self.tag_inc  # 'get and inc tag'

        log.info("teardown procedure entered")

        self.detach_from_channels()
        log.debug("detached from channels")

        self.msg_stdout.send(dmsg.GSHalted(tag=gait()).serialize())
        log.info(f"gs sent GSHalted to primary shep via stdout")

        log.info("teardown complete")

    ################ Message Handlers for GS #######################################
    @dutil.route(dmsg.SHPingGS, DTBL)
    def handle_sh_ping(self, msg):
        log = self._startup_logger
        gait = self.tag_inc  # 'get and inc tag'

        log.info(f"Received SHPingGS from Local Services on node {msg.idx}")

        # Add the node into Global Services
        self.add_node_from_SHPingGS(msg)

        self.sh_pings_recvd += 1
        log.info(f"GS has received {self.sh_pings_recvd} of an expected {self.num_nodes} SHPingGS messages.")

        if self.sh_pings_recvd == self.num_nodes:
            # And done receiving SHPingGS's
            log.info("received SHPingGS from all ls - m11")
            self.bela_input.send(dmsg.GSIsUp(tag=gait()).serialize())
            log.info("gs signaled to la_be it is up (GSIsUP) - m12.1")
            self._state = self.RunState.WAITING_FOR_HEAD
            # Set up global PolicyEvaluator here
            # When we get to elasticity we can do more by creating the evaluator here
            # Get a list of NodeDescriptors from NodeContext
            node_descrs = [x.descriptor for x in self.node_table.values()]
            self.policy_eval = PolicyEvaluator(node_descrs, Policy.global_policy())

    @dutil.route(dmsg.GSTeardown, DTBL)
    def handle_gs_teardown(self, msg):
        log = self._shutdown_logger
        log.info("Received the GSTeardown message. Now shutting down Global Services.")
        self._state = self.RunState.SHUTTING_DOWN

    @dutil.route(dmsg.GSProcessCreate, DTBL)
    def handle_process_create(self, msg):
        log = self._process_logger

        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)
        # here the constructor might have multiple steps so
        # it has to control its own pending completions.
        # The RunState is set to HAS_HEAD in this function as well.
        ProcessContext.construct(self, msg, reply_channel)

    @dutil.route(dmsg.SHProcessCreateResponse, DTBL)
    def handle_process_create_response(self, msg):
        log = self._process_logger
        log.debug(f"process create response tag {msg.tag} ref {msg.ref}")
        assert msg.ref in self.pending
        handler = self.pending.pop(msg.ref)
        handler(msg)
        if msg.ref in self.pending_process_exits:
            process_exit_msg = self.pending_process_exits.pop(msg.ref)
            self.handle_process_exit(process_exit_msg)

    @dutil.route(dmsg.GSProcessList, DTBL)
    def handle_process_list(self, msg):
        log = self._process_logger
        log.debug(f"handling {msg!s}")

        reply_channel = self.get_reply_handle(msg)

        rm = dmsg.GSProcessListResponse(
            tag=self.tag_inc(),
            ref=msg.tag,
            err=dmsg.GSProcessListResponse.Errors.SUCCESS,
            plist=list(self.process_table.keys()),
        )

        reply_channel.send(rm.serialize())
        log.debug(f"response to {msg!s}: {rm!s}")

    @dutil.route(dmsg.GSProcessQuery, DTBL)
    def handle_process_query(self, msg):
        log = self._process_logger
        log.debug(f"handling {msg}")

        reply_channel = self.get_reply_handle(msg)
        target_uid, found, errmsg = self.resolve_puid(msg.user_name, msg.t_p_uid)

        if not found:
            rm = dmsg.GSProcessQueryResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSProcessQueryResponse.Errors.UNKNOWN, err_info=errmsg
            )
        else:
            pdesc = self.process_table[target_uid].descriptor
            rm = dmsg.GSProcessQueryResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSProcessQueryResponse.Errors.SUCCESS, desc=pdesc
            )

        reply_channel.send(rm.serialize())
        log.debug(f"response to {msg!s}: {rm!s}")

    @dutil.route(dmsg.GSProcessKill, DTBL)
    def handle_process_kill(self, msg):
        log = self._process_logger
        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)
        ProcessContext.kill(self, msg, reply_channel)

    @dutil.route(dmsg.SHMultiProcessKillResponse, DTBL)
    def handle_sh_multi_process_kill_response(self, msg):
        log = self._node_logger
        log.debug("handling %s", msg)

        for proc_kill_response in msg.responses:
            log.info("proc_kill_response=%s", str(proc_kill_response))
            self.handle_process_kill_response(proc_kill_response)

    @dutil.route(dmsg.SHProcessKillResponse, DTBL)
    def handle_process_kill_response(self, msg):
        log = self._process_logger
        log.debug(f"handling {msg!s}")
        assert msg.ref in self.pending

        handler = self.pending.pop(msg.ref)
        handler(msg)

    @dutil.route(dmsg.GSProcessJoin, DTBL)
    def handle_process_join(self, msg):
        log = self._process_logger
        log.debug(f"handling {msg!s}")
        gspjr = dmsg.GSProcessJoinResponse
        reply_channel = self.get_reply_handle(msg)
        target_uid, found, errmsg = self.resolve_puid(msg.user_name, msg.t_p_uid)

        if not found:
            rm = gspjr(tag=self.tag_inc(), ref=msg.tag, err=gspjr.Errors.UNKNOWN, err_info=errmsg)
            reply_channel.send(rm.serialize())
            log.debug(f"unknown: response to {msg}: {rm}")
        elif target_uid == msg.p_uid:
            rm = gspjr(tag=self.tag_inc(), ref=msg.tag, err=gspjr.Errors.SELF, err_info="self-join forbidden")
            reply_channel.send(rm.serialize())
            log.debug(f"self join not allowed: {msg}")
        else:
            pctx = self.process_table[target_uid]
            pdesc = pctx.descriptor
            if process_desc.ProcessDescriptor.State.DEAD == pdesc.state:
                rm = gspjr(tag=self.tag_inc(), ref=msg.tag, err=gspjr.Errors.SUCCESS, exit_code=pdesc.ecode)
                reply_channel.send(rm.serialize())
                log.debug(f"process dead, response to {msg}: {rm}")
            else:
                if msg.timeout < 0:
                    timeout = None
                else:
                    timeout = msg.timeout / 1000000.0  # microseconds

                if timeout is not None and timeout <= 0.000100:  # 100 microseconds from now, is now.
                    rm = gspjr(tag=self.tag_inc(), ref=msg.tag, err=gspjr.Errors.TIMEOUT)
                    reply_channel.send(rm.serialize())
                else:
                    self.pending_join.put(target_uid, msg, timeout=timeout)
                    log.debug(f"join stored for {target_uid}: {msg} timeout {timeout}")

    @dutil.route(dmsg.GSProcessJoinList, DTBL)
    def handle_process_join_list(self, msg):
        log = self._process_logger
        log.debug(f"handling {msg!s}")
        gspjlr = dmsg.GSProcessJoinListResponse
        reply_channel = self.get_reply_handle(msg)
        success_num = 0
        pending_puid = []

        # holds the status of the p_uids from the join list for this particular msg
        join_key = (msg.tag, msg.p_uid)
        self.puid_join_list_status[join_key] = dict()

        if msg.timeout < 0:
            timeout = None
        else:
            timeout = msg.timeout / 1000000.0  # microseconds

        # helper function which updates the puid_join_list_status dict with the status of each puid
        def proc_join_list_helper(target_uid, found, errmsg, success_num, item):
            nonzero_exit = False
            if not found:
                self.puid_join_list_status[join_key][item] = (gspjlr.Errors.UNKNOWN.value, errmsg)
                log.debug(f"unknown target_uid: {item}")
            elif target_uid == msg.p_uid:
                self.puid_join_list_status[join_key][target_uid] = (gspjlr.Errors.SELF.value, "self-join forbidden")
                log.debug(f"self join not allowed for target_uid: {target_uid}")
            else:
                pctx = self.process_table[target_uid]
                pdesc = pctx.descriptor
                if process_desc.ProcessDescriptor.State.DEAD == pdesc.state:
                    self.puid_join_list_status[join_key][target_uid] = (gspjlr.Errors.SUCCESS.value, pdesc.ecode)
                    if pdesc.ecode != 0:
                        nonzero_exit = True
                        log.debug(f"non-zero exit {pdesc.ecode} for target_uid: {target_uid}")
                    success_num += 1
                    log.debug(f"process dead, target_uid: {target_uid}")
                else:
                    if timeout is not None and timeout <= 0.000100:  # 100 microseconds from now, is now.
                        self.puid_join_list_status[join_key][target_uid] = (gspjlr.Errors.TIMEOUT.value, None)
                    else:
                        pending_puid.append(target_uid)
                        self.puid_join_list_status[join_key][target_uid] = (gspjlr.Errors.PENDING.value, None)
            return success_num, nonzero_exit

        def continue_to_wait(join_all, success_num, pending_puid, return_on_bad_exit, nonzero_exit):

            # Always send a message if we are told to return on bad exit and have a non-zero exit code
            if return_on_bad_exit and nonzero_exit:
                return False
            # Otherwise use the original logic
            else:
                due_to_join_all = bool(join_all and pending_puid)
                due_to_partial_complete = bool(not success_num and pending_puid)
                log.debug(
                    f"due_to_join_all -> {due_to_join_all} | due_to_partial_complete -> {due_to_partial_complete}"
                )
            return due_to_partial_complete or due_to_join_all

        if msg.t_p_uid_list:
            for item in msg.t_p_uid_list:
                item = int(item)
                target_uid, found, errmsg = self.resolve_puid(None, item)
                success_num, nonzero_exit = proc_join_list_helper(target_uid, found, errmsg, success_num, item)
        if msg.user_name_list:
            for item in msg.user_name_list:
                target_uid, found, errmsg = self.resolve_puid(item, None)
                success_num, nonzero_exit = proc_join_list_helper(target_uid, found, errmsg, success_num, item)

        # Do not send a message if all the processes are still pending
        # if no exits no timeouts, or, 'all' option and some are pending (even though some may have exited)
        if continue_to_wait(msg.join_all, success_num, pending_puid, msg.return_on_bad_exit, nonzero_exit):
            self.pending_join_list.put(frozenset(pending_puid), msg, timeout=timeout)
            # TODO AICI-1422 Implement verbose logging options
            # log.debug('join stored for target_uid: %s timeout %s', pending_puid, timeout)
        else:
            rm = gspjlr(tag=self.tag_inc(), ref=msg.tag, puid_status=self.puid_join_list_status[join_key])
            reply_channel.send(rm.serialize())
            log.debug("send join list response")
            del self.puid_join_list_status[join_key]

    @dutil.route(dmsg.SHProcessExit, DTBL)
    def handle_process_exit(self, msg):
        log = self._process_logger
        log.info(f"rec {msg}")
        gspjr = dmsg.GSProcessJoinResponse
        gspjlr = dmsg.GSProcessJoinListResponse
        log.debug(f"p_uid {msg.p_uid} exited")

        if msg.creation_msg_tag in self.pending:
            log.debug(
                f"p_uid {msg.p_uid} was found in pending. delaying handling of process exit until creation is finished."
            )
            self.pending_process_exits[msg.creation_msg_tag] = msg
            return

        ctx = self.process_table[msg.p_uid]
        ctx.descriptor.state = process_desc.ProcessDescriptor.State.DEAD
        ctx.descriptor.ecode = msg.exit_code

        if ctx.request.options.make_inf_channels:
            clean_chan = dmsg.GSChannelDestroy(
                tag=self.tag_inc(), p_uid=dfacts.GS_PUID, r_c_uid=0, c_uid=ctx.descriptor.gs_ret_cuid
            )
            ChannelContext.destroy(self, clean_chan, dutil.AbsorbingChannel())

        if ctx.stdin_context is not None:
            clean_chan = dmsg.GSChannelDestroy(
                tag=self.tag_inc(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=0,
                c_uid=ctx.stdin_context.shchannelcreate_msg.c_uid,
                dec_ref=True,
            )
            ChannelContext.destroy(self, clean_chan, dutil.AbsorbingChannel())

        if ctx.stdout_context is not None:
            clean_chan = dmsg.GSChannelDestroy(
                tag=self.tag_inc(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=0,
                c_uid=ctx.stdout_context.shchannelcreate_msg.c_uid,
                dec_ref=True,
            )
            ChannelContext.destroy(self, clean_chan, dutil.AbsorbingChannel())

        if ctx.stderr_context is not None:
            clean_chan = dmsg.GSChannelDestroy(
                tag=self.tag_inc(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=0,
                c_uid=ctx.stderr_context.shchannelcreate_msg.c_uid,
                dec_ref=True,
            )
            ChannelContext.destroy(self, clean_chan, dutil.AbsorbingChannel())

        if ctx.descriptor.p_p_uid in self.process_table:
            parent_ctx = self.process_table[ctx.descriptor.p_p_uid]
            parent_ctx.descriptor.live_children.discard(ctx.descriptor.p_uid)

        if msg.p_uid in self.pending_join:
            for join_req in self.pending_join.get(msg.p_uid):
                reply_channel = self.get_reply_handle(join_req)
                rm = gspjr(
                    tag=self.tag_inc(), ref=join_req.tag, err=gspjr.Errors.SUCCESS, exit_code=ctx.descriptor.ecode
                )
                reply_channel.send(rm.serialize())
                # TODO AICI-1422 Implement verbose logging options
                # log.debug('join response to %s: %s', join_req, rm)
            self.pending_join.remove(msg.p_uid)

        to_be_erased = []
        join_reqs_to_be_erased = []
        to_be_added = []
        for puid_set in self.pending_join_list.keys():
            if msg.p_uid in puid_set:
                for join_req in self.pending_join_list.get(puid_set):
                    join_key = (join_req.tag, join_req.p_uid)
                    self.puid_join_list_status[join_key][msg.p_uid] = (
                        gspjlr.Errors.SUCCESS.value,
                        ctx.descriptor.ecode,
                    )

                    # if this is multi_join on 'any' processes request, or, set with a single member or there was a bad exit
                    # and a request to respond on it
                    if (not join_req.join_all or len(puid_set) == 1) or (
                        join_req.return_on_bad_exit and ctx.descriptor.ecode != 0
                    ):
                        reply_channel = self.get_reply_handle(join_req)
                        rm = gspjlr(
                            tag=self.tag_inc(), ref=join_req.tag, puid_status=self.puid_join_list_status[join_key]
                        )
                        reply_channel.send(rm.serialize())
                        # TODO AICI-1422 Implement verbose logging options
                        # log.debug('join response to %s: %s', join_req, rm)

                        # we are done with this join request, so let's clean things up
                        del self.puid_join_list_status[join_key]
                        join_reqs_to_be_erased.append((puid_set, join_req))

                    else:  # this is multi_join on 'all' processes request
                        # we create a new entry for waiting on the new set puid_set - msg.p_uid
                        new_puid_set = set(puid_set)  # puid_set is a frozenset
                        new_puid_set.remove(msg.p_uid)
                        to_be_added.append((puid_set, new_puid_set, join_req))
                to_be_erased.append(puid_set)

        for oldset, puidset, joinreq in to_be_added:
            if joinreq.timeout < 0:
                timeout = None
            elif joinreq.timeout == 0:
                timeout = 0
            else:
                timeout = (
                    self.pending_join_list.time_left(oldset, joinreq) - time.time()
                )  # this is the remaining time on waiting until timeout
            self.pending_join_list.put(frozenset(puidset), joinreq, timeout=timeout)

        for puid_set, join_req in join_reqs_to_be_erased:
            self.pending_join_list.remove_one(puid_set, join_req)

        for puid_set in to_be_erased:
            self.pending_join_list.remove(puid_set)

        if msg.p_uid in self.head_puid:
            log.info(f"head process id {msg.p_uid} exited")
            self.bela_input.send(dmsg.GSHeadExit(tag=self.tag_inc(), exit_code=msg.exit_code).serialize())
            # TODO: head process exit cleanup
            self.head_puid.remove(msg.p_uid)
            if not self.head_puid:
                self._state = self.RunState.WAITING_FOR_HEAD

        # GROUP related work
        # if this process is a member of a group, then update the group context
        if msg.p_uid in self.resource_to_group_map:
            guid, (lst_idx, item_idx) = self.resource_to_group_map[msg.p_uid]
            log.debug(f"process {msg.p_uid} just exited and belongs to group {guid}")
            groupctx = self.group_table[guid]
            groupctx.descriptor.sets[lst_idx][item_idx].state = process_desc.ProcessDescriptor.State.DEAD
            groupctx.descriptor.sets[lst_idx][item_idx].desc.state = process_desc.ProcessDescriptor.State.DEAD

            # if group destroy was called
            if msg.p_uid in self.pending_group_destroy:
                # this is needed to complete the pending continuation on the group_int
                self.group_to_pending_resource_map[(msg.p_uid, guid)] = self.process_table[msg.p_uid]
                handler = self.pending_group_destroy.pop(msg.p_uid)
                handler(msg)

    @dutil.route(dmsg.GSPoolCreate, DTBL)
    def handle_pool_create(self, msg):
        log = self._pool_logger
        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)
        issued, tag, context = PoolContext.construct(self, msg, reply_channel)
        if issued:
            self.pending[tag] = context.complete_construction

    @dutil.route(dmsg.SHPoolCreateResponse, DTBL)
    def handle_pool_create_response(self, msg):
        log = self._pool_logger
        log.debug(f"create response tag {msg.tag} ref {msg.ref} received")
        assert msg.ref in self.pending
        handler = self.pending.pop(msg.ref)
        handler(msg)

    @dutil.route(dmsg.GSPoolList, DTBL)
    def handle_pool_list(self, msg):
        log = self._pool_logger
        log.debug(f"handling {msg!s}")

        reply_channel = self.get_reply_handle(msg)

        rm = dmsg.GSPoolListResponse(
            tag=self.tag_inc(),
            ref=msg.tag,
            err=dmsg.GSPoolListResponse.Errors.SUCCESS,
            mlist=list(self.pool_table.keys()),
        )

        reply_channel.send(rm.serialize())
        log.debug(f"response to {msg!s}: {rm!s}")

    @dutil.route(dmsg.GSPoolQuery, DTBL)
    def handle_pool_query(self, msg):
        log = self._pool_logger
        log.debug(f"handling {msg}")

        reply_channel = self.get_reply_handle(msg)
        target_uid, found, errmsg = self.resolve_muid(msg.user_name, msg.m_uid)

        if not found:
            rm = dmsg.GSPoolQueryResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSPoolQueryResponse.Errors.UNKNOWN, err_info=errmsg
            )
        else:
            pdesc = self.pool_table[target_uid].descriptor
            rm = dmsg.GSPoolQueryResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSPoolQueryResponse.Errors.SUCCESS, desc=pdesc
            )

        reply_channel.send(rm.serialize())

        log.debug(f"response to {str(msg)}: {str(rm)}")

    @dutil.route(dmsg.GSPoolDestroy, DTBL)
    def handle_pool_destroy(self, msg):
        log = self._pool_logger
        log.debug(f"handling {str(msg)}")
        reply_channel = self.get_reply_handle(msg)
        PoolContext.destroy(self, msg, reply_channel)

    @dutil.route(dmsg.SHPoolDestroyResponse, DTBL)
    def handle_pool_destroy_response(self, msg):
        log = self._pool_logger
        log.debug(f"handling {msg}")
        assert msg.ref in self.pending
        handler = self.pending.pop(msg.ref)
        handler(msg)

    @dutil.route(dmsg.GSChannelCreate, DTBL)
    def handle_channel_create(self, msg):
        log = self._channel_logger
        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)
        issued, tag, context = ChannelContext.construct(self, msg, reply_channel)
        if issued:
            self.pending[tag] = context.complete_construction

    @dutil.route(dmsg.SHChannelCreateResponse, DTBL)
    def handle_channel_create_response(self, msg):
        log = self._channel_logger
        log.debug(f"create response tag {msg.tag} ref {msg.ref} received")

        assert msg.ref in self.pending
        handler = self.pending.pop(msg.ref)
        handler(msg)

    @dutil.route(dmsg.GSChannelDestroy, DTBL)
    def handle_channel_destroy(self, msg):
        log = self._channel_logger
        log.debug(f"handling {msg!s}")
        if msg.reply_req:
            reply_channel = self.get_reply_handle(msg)
        else:
            reply_channel = dutil.AbsorbingChannel()

        ChannelContext.destroy(self, msg, reply_channel)

    @dutil.route(dmsg.SHChannelDestroyResponse, DTBL)
    def handle_channel_destroy_response(self, msg):
        log = self._channel_logger
        log.debug(f"handling {msg}")
        assert msg.ref in self.pending
        handler = self.pending.pop(msg.ref)
        handler(msg)

    @dutil.route(dmsg.GSChannelList, DTBL)
    def handle_channel_list(self, msg):
        log = self._channel_logger
        log.debug(f"handling {msg!s}")

        reply_channel = self.get_reply_handle(msg)

        rm = dmsg.GSChannelListResponse(
            tag=self.tag_inc(),
            ref=msg.tag,
            err=dmsg.GSChannelListResponse.Errors.SUCCESS,
            clist=list(self.channel_table.keys()),
        )

        reply_channel.send(rm.serialize())
        log.debug(f"response to {str(msg)}: {str(rm)}")

    @dutil.route(dmsg.GSChannelQuery, DTBL)
    def handle_channel_query(self, msg):
        log = self._channel_logger
        # TODO AICI-1422 Implement verbose logging options
        # log.debug('handling %s', msg)

        reply_channel = self.get_reply_handle(msg)
        # TODO AICI-1422 Implement verbose logging options
        # log.debug('channel query to %s - %s', msg.user_name, msg.c_uid)
        target_uid, found, errmsg = self.resolve_cuid(msg.user_name, msg.c_uid)

        if not found:
            rm = dmsg.GSChannelQueryResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSChannelQueryResponse.Errors.UNKNOWN, err_info=errmsg
            )
        else:
            cctx = self.channel_table[target_uid]
            cdesc = cctx.descriptor

            if cctx.is_refcounted and msg.inc_refcnt and cdesc.State.ACTIVE == cdesc.state:
                cctx.refcnt += 1
                log.debug(f"channel {cctx.descriptor.c_uid}: refcnt={cctx.refcnt}")

            rm = dmsg.GSChannelQueryResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSChannelQueryResponse.Errors.SUCCESS, desc=cdesc
            )
            # TODO AICI-1422 Implement verbose logging options
            # log.debug('found descriptor: %s', cdesc)

        reply_channel.send(rm.serialize())
        # TODO AICI-1422 Implement verbose logging options
        # log.debug('response to %s: %s', msg, rm)

    @dutil.route(dmsg.GSChannelJoin, DTBL)
    def handle_channel_join(self, msg):
        log = self._channel_logger
        log.debug(f"handling {msg}")

        target_uid, found, errmsg = self.resolve_cuid(msg.name)

        handled_immediately = False
        if found:
            cdesc = self.channel_table[target_uid].descriptor
            if cdesc.State.ACTIVE == cdesc.state:
                reply_channel = self.get_reply_handle(msg)
                rm = dmsg.GSChannelJoinResponse(
                    tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSChannelJoinResponse.Errors.SUCCESS, desc=cdesc
                )
                reply_channel.send(rm.serialize())
                log.debug(f"response to {msg!s}: {rm!s}")
                handled_immediately = True
            elif cdesc.State.DEAD == cdesc.state:
                reply_channel = self.get_reply_handle(msg)
                rm = dmsg.GSChannelJoinResponse(
                    tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSChannelJoinResponse.Errors.DEAD, desc=cdesc
                )
                reply_channel.send(rm.serialize())
                log.debug(f"response to {msg!s}: {rm!s}")
                handled_immediately = True
            else:  # close case
                assert cdesc.State.PENDING == cdesc.state

        # In two cases we remember the join request:
        # - nobody has asked this channel to be created yet
        # - the channel is in the process of creation
        # In either case this gets handled when the channel create response
        # is returned or the timeout expires.
        if not handled_immediately:
            if msg.timeout < 0:
                the_timeout = None
            else:
                the_timeout = msg.timeout / 1000000.0  # microseconds

            self.pending_channel_joins.put(msg.name, msg, timeout=the_timeout)
            log.debug(f"channel join stored for {target_uid}: {msg} timeout {the_timeout}")

    @dutil.route(dmsg.GSDumpState, DTBL)
    def handle_dump(self, msg):
        log = self._process_logger
        log.debug(f"dumping to {msg.filename}")

        with open(msg.filename, "w") as fh:
            self.dump_to(fh)

        log.debug("dump complete")

    @dutil.route(dmsg.GSNodeList, DTBL)
    def handle_node_list(self, msg):
        log = self._pool_logger
        log.debug(f"handling {msg!s}")

        reply_channel = self.get_reply_handle(msg)

        rm = dmsg.GSNodeListResponse(
            tag=self.tag_inc(),
            ref=msg.tag,
            err=dmsg.GSNodeListResponse.Errors.SUCCESS,
            hlist=list(self.node_table.keys()),
        )

        reply_channel.send(rm.serialize())
        log.debug(f"response to {msg!s}: {rm!s}")

    @dutil.route(dmsg.GSNodeQueryTotalCPUCount, DTBL)
    def handle_node_query_total_cpu_count(self, msg):
        log = self._node_logger

        log.debug(f"handling {msg}")

        reply_channel = self.get_reply_handle(msg)

        total_cpus = 0
        for desc in self.node_table.values():
            total_cpus += desc.descriptor.num_cpus

        rm = dmsg.GSNodeQueryTotalCPUCountResponse(
            tag=self.tag_inc(),
            ref=msg.tag,
            err=dmsg.GSNodeQueryTotalCPUCountResponse.Errors.SUCCESS,
            total_cpus=total_cpus,
        )

        reply_channel.send(rm.serialize())
        log.debug(f"response to {msg!s}: {rm!s}")

    @dutil.route(dmsg.GSNodeQuery, DTBL)
    def handle_node_query(self, msg):
        log = self._node_logger
        log.debug(f"handling {msg}")

        reply_channel = self.get_reply_handle(msg)

        ## Get descriptor
        target_uid, found, errmsg = self.resolve_huid(msg.name, msg.h_uid)

        sdesc = self.node_table[target_uid].descriptor.sdesc

        if not found:
            log.error(errmsg)
            rm = dmsg.GSNodeQueryResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSNodeQueryResponse.Errors.UNKNOWN, err_info=errmsg
            )
        else:
            rm = dmsg.GSNodeQueryResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSNodeQueryResponse.Errors.SUCCESS, desc=sdesc
            )
            log.debug(f"found descriptor={sdesc}")

        reply_channel.send(rm.serialize())
        log.debug(f"response to {msg!s}: {rm!s}")

    @dutil.route(dmsg.SHMultiProcessCreateResponse, DTBL)
    def handle_sh_group_create_response(self, msg):
        log = self._node_logger
        log.debug("handling %s", msg)

        for proc_create_response in msg.responses:
            log.info("proc_create_response=%s", str(proc_create_response))
            self.handle_process_create_response(proc_create_response)

    @dutil.route(dmsg.GSNodeQueryAll, DTBL)
    def handle_node_query(self, msg):
        log = self._node_logger
        log.debug(f"handling {msg}")

        reply_channel = self.get_reply_handle(msg)

        descriptors = [node.descriptor for node in self.node_table.values()]

        if not descriptors:
            errmsg = "Could not retrieve list of node descriptors"
            log.error(errmsg)
            rm = dmsg.GSNodeQueryAllResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSNodeQueryAllResponse.Errors.UNKNOWN, err_info=errmsg
            )
        else:
            rm = dmsg.GSNodeQueryAllResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSNodeQueryAllResponse.Errors.SUCCESS, descriptors=descriptors
            )

        reply_channel.send(rm.serialize())
        log.debug(f"response to {msg!s}: {rm!s}")

    @dutil.route(dmsg.GSGroupCreate, DTBL)
    def handle_group_create(self, msg):
        log = self._group_logger
        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)

        # here the constructor has to control its
        # own pending completions
        GroupContext.construct(self, msg, reply_channel)

    @dutil.route(dmsg.GSGroupKill, DTBL)
    def handle_group_kill(self, msg):
        log = self._group_logger
        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)

        # here the constructor has to control its
        # own pending completions
        GroupContext.kill(self, msg, reply_channel)

    @dutil.route(dmsg.GSGroupDestroy, DTBL)
    def handle_group_destroy(self, msg):
        log = self._group_logger
        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)

        # here the constructor has to control its
        # own pending completions
        GroupContext.destroy(self, msg, reply_channel)

    @dutil.route(dmsg.GSGroupAddTo, DTBL)
    def handle_group_add_to(self, msg):
        log = self._group_logger
        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)

        GroupContext.add(self, msg, reply_channel)

    @dutil.route(dmsg.GSGroupCreateAddTo, DTBL)
    def handle_group_create_add_to(self, msg):
        log = self._group_logger
        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)

        # here the constructor has to control its
        # own pending completions
        GroupContext.create_add(self, msg, reply_channel)

    @dutil.route(dmsg.GSGroupRemoveFrom, DTBL)
    def handle_group_remove_from(self, msg):
        log = self._group_logger
        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)

        GroupContext.remove(self, msg, reply_channel)

    @dutil.route(dmsg.GSGroupDestroyRemoveFrom, DTBL)
    def handle_group_destroy_remove_from(self, msg):
        log = self._group_logger
        log.debug(f"handling {msg!s}")
        reply_channel = self.get_reply_handle(msg)

        # here the constructor has to control its
        # own pending completions
        GroupContext.destroy_remove(self, msg, reply_channel)

    @dutil.route(dmsg.GSGroupList, DTBL)
    def handle_group_list(self, msg):
        log = self._group_logger
        log.debug(f"handling {msg!s}")

        reply_channel = self.get_reply_handle(msg)

        rm = dmsg.GSGroupListResponse(
            tag=self.tag_inc(),
            ref=msg.tag,
            err=dmsg.GSGroupListResponse.Errors.SUCCESS,
            glist=list(self.group_table.keys()),
        )

        reply_channel.send(rm.serialize())
        log.debug(f"response to {msg!s}: {rm!s}")

    @dutil.route(dmsg.GSGroupQuery, DTBL)
    def handle_group_query(self, msg):
        log = self._group_logger
        log.debug(f"handling {msg}")

        reply_channel = self.get_reply_handle(msg)
        target_uid, found, errmsg = self.resolve_guid(msg.user_name, msg.g_uid)

        if not found:
            rm = dmsg.GSGroupQueryResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSGroupQueryResponse.Errors.UNKNOWN, err_info=errmsg
            )
        else:
            gdesc = self.group_table[target_uid].descriptor
            rm = dmsg.GSGroupQueryResponse(
                tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSGroupQueryResponse.Errors.SUCCESS, desc=gdesc
            )

        reply_channel.send(rm.serialize())
        log.debug(f"response to {msg!s}: {rm!s}")

    @dutil.route(dmsg.GSRebootRuntime, DTBL)
    def handle_reboot_runtime(self, msg):
        log = self._group_logger
        log.debug(f"handling {msg}")

        # Channel to tell user we got its request
        reply_channel = self.get_reply_handle(msg)

        # Send RebootRuntime to the frontend
        drr = dmsg.RebootRuntime(tag=self.tag_inc(), h_uid=msg.h_uid)
        self.bela_input.send(drr.serialize())
        log.debug(f"response to {msg!s}: {drr!s}")

        # Send response
        rm = dmsg.GSRebootRuntimeResponse(
            tag=self.tag_inc(), ref=msg.tag, err=dmsg.GSRebootRuntimeResponse.Errors.SUCCESS
        )
        reply_channel.send(rm.serialize())
        log.debug(f"response to {msg!s}: {rm!s}")


# separate process server entry:
# python3 -c "import dragon.globalservices.server as dgs; dgs.single()"
def single():
    dlog.setup_logging(basename=os.environ.get(dfacts.GS_LOG_BASE, "gs"), level=logging.DEBUG)
    log = logging.getLogger("single entry")
    log.info("Starting GS in single mode")
    log.info(f"start in pid {os.getpid()}")
    the_ctx = GlobalContext()
    log.info("GS context constructed, running startup")

    gs_stdout = dutil.NewlineStreamWrapper(sys.stdout)

    the_test_conns = {}

    if dfacts.GS_TEST_CH_EV in os.environ:
        log.debug("getting test channels")
        channel_init_bytes = B64.str_to_bytes(os.environ[dfacts.GS_TEST_CH_EV])
        channel_init = pickle.loads(channel_init_bytes)

        for initvals in channel_init:
            cuid, spoold, schand, rdng = initvals
            log.debug(f"attaching to test channel: cuid {cuid} reading {rdng}")
            chan = dch.Channel.attach(schand)
            if rdng:
                the_test_conns[cuid] = dconn.Connection(inbound_initializer=chan, policy=dparm.POLICY_INFRASTRUCTURE)
            else:
                the_test_conns[cuid] = dconn.Connection(outbound_initializer=chan, policy=dparm.POLICY_INFRASTRUCTURE)

    else:
        log.debug("no test channels here!")

    try:
        the_ctx.run_startup()
    except Exception as e:
        msg = f"fatal startup error: {type(e)}:{e}"
        log.exception(msg)
        gs_stdout.send(dmsg.AbnormalTermination(tag=0, err_info=msg).serialize())
        exit(dfacts.GS_ERROR_EXIT)

    try:
        the_ctx.run_global_server(test_conns=the_test_conns)
    except Exception as e:
        msg = f"fatal error: {type(e)}:{e}"
        log.exception(msg)
        gs_stdout.send(dmsg.AbnormalTermination(tag=0, err_info=msg).serialize())
        exit(dfacts.GS_ERROR_EXIT)

    try:
        the_ctx.run_teardown()
    except Exception as e:
        msg = f"fatal shutdown error: {type(e)}:{e}"
        log.exception(msg)
        gs_stdout.send(dmsg.AbnormalTermination(tag=0, err_info=msg).serialize())
        exit(dfacts.GS_ERROR_EXIT)


# thread internal entry
def single_thread(gs_stdout):
    log = logging.getLogger("gs")
    log.info("Starting GS in single thread mode")
    log.info(f"start in {os.getpid()}")
    the_ctx = GlobalContext(gs_stdout)
    log.info("GS context constructed, running startup")

    try:
        the_ctx.run_startup(mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE)
        the_ctx.run_global_server(mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE)
        the_ctx.run_teardown()
        log.info("normal exit")
    except Exception as e:
        log.exception("fatal error")
        gs_stdout.send(dmsg.AbnormalTermination(tag=0, err_info="fatal exception").serialize())
        time.sleep(1)
        raise e


# server entry:
# python3 -c "import dragon.globalservices.server as dgs; dgs.multi()"
def multi():

    set_procname(dfacts.PROCNAME_GS)
    dch.register_gateways_from_env()

    # Set up logging
    _, fname = dlog.setup_BE_logging(service=dls.GS)
    log = logging.getLogger(dls.GS).getChild("server.multi")
    log.info("Starting GS in multi mode")
    log.debug("getting logger sdesc")
    logger_sdesc = os.environ.get(dfacts.DRAGON_LOGGER_SDESC)
    if logger_sdesc is not None:
        logger_sdesc = B64.from_str(logger_sdesc)
    dlog.setup_BE_logging(service=dls.GS, logger_sdesc=logger_sdesc, fname=fname)
    log.info("gs logging stood up")

    the_ctx = GlobalContext()

    the_test_conns = {}
    if dfacts.GS_TEST_CH_EV in os.environ:
        log.debug("getting test channels")
        channel_init_bytes = B64.str_to_bytes(os.environ[dfacts.GS_TEST_CH_EV])
        channel_init = pickle.loads(channel_init_bytes)

        for initvals in channel_init:
            cuid, spoold, schand, rdng = initvals
            log.debug(f"attaching to test channel: cuid {cuid} reading {rdng}")
            chan = dch.Channel.attach(schand)
            if rdng:
                the_test_conns[cuid] = dconn.Connection(inbound_initializer=chan, policy=dparm.POLICY_INFRASTRUCTURE)
            else:
                the_test_conns[cuid] = dconn.Connection(outbound_initializer=chan, policy=dparm.POLICY_INFRASTRUCTURE)
    else:
        log.debug("no test channels here!")

    try:
        the_ctx.run_startup(mode=the_ctx.LaunchModes.MULTI)
    except Exception as e:
        msg = f"fatal startup error: {type(e)}:{e}"
        log.exception(msg)
        the_ctx.msg_stdout.send(dmsg.AbnormalTermination(tag=0, err_info=msg).serialize())
        exit(dfacts.GS_ERROR_EXIT)

    try:
        the_ctx.run_global_server(mode=the_ctx.LaunchModes.MULTI, test_conns=the_test_conns)
    except Exception as e:
        msg = f"fatal error: {type(e)}:{e}"
        log.exception(msg)
        the_ctx.msg_stdout.send(dmsg.AbnormalTermination(tag=0, err_info=msg).serialize())
        exit(dfacts.GS_ERROR_EXIT)

    try:
        the_ctx.run_teardown()
    except Exception as e:
        msg = f"fatal shutdown error: {type(e)}:{e}"
        log.exception(msg)
        the_ctx.msg_stdout.send(dmsg.AbnormalTermination(tag=0, err_info=msg).serialize())
