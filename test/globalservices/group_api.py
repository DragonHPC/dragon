#!/usr/bin/env python3
"""Test script for the GS group API in single node mode"""

import copy
import inspect
import logging
import multiprocessing
import os
import threading
import unittest

import dragon.channels as dch
import dragon.managed_memory as dmm

import dragon.globalservices.api_setup as dapi
import dragon.globalservices.server as dserver
import dragon.infrastructure.facts as dfacts
import dragon.dlogging.util as dlog
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.parameters as dparm
from dragon.infrastructure.node_desc import NodeDescriptor

import support.util as tsu

from dragon.globalservices.process import get_create_message, multi_join
from dragon.globalservices.process import create as process_create
from dragon.globalservices.group import create as group_create
from dragon.globalservices.group import (
    GroupError,
    add_to,
    create_add_to,
    remove_from,
    destroy_remove_from,
    kill,
    destroy,
    get_list,
    query,
)
from dragon.infrastructure.group_desc import GroupDescriptor
from dragon.infrastructure.process_desc import ProcessDescriptor
from dragon.infrastructure.policy import Policy

from dragon.utils import B64


def bringup_channels(gs_stdout, env_updates, channel_overrides, logname=""):
    dlog.setup_logging(basename="gs_" + logname, level=logging.DEBUG, force=True)
    log = logging.getLogger("gs entry")
    log.info("starting")

    dparm.this_process = dparm.LaunchParameters.from_env(env_updates)

    # reconstitute overrides here.
    test_connections = {}
    for uid, stuff in channel_overrides.items():
        _, ser_chan, reading = stuff

        chan = dch.Channel.attach(ser_chan)

        if reading:
            test_connections[uid] = dconn.Connection(inbound_initializer=chan)
        else:
            test_connections[uid] = dconn.Connection(outbound_initializer=chan)

    the_ctx = dserver.GlobalContext()

    try:
        the_ctx.run_startup(mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE, test_gs_stdout=gs_stdout)

        the_ctx.run_global_server(
            mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE,
            test_gs_stdout=gs_stdout,
            test_conns=test_connections,
        )

        the_ctx.run_teardown(test_gs_stdout=gs_stdout)
        log.info("normal exit")
    except:
        log.exception("fatal")
        gs_stdout.send(dmsg.AbnormalTermination(tag=0).serialize())


class GSGroupBaseClass(unittest.TestCase):
    def setUp(self) -> None:
        self.gs_stdout_rh, self.gs_stdout_wh = multiprocessing.Pipe(duplex=False)

        self.node_sdesc = NodeDescriptor.get_localservices_node_conf(is_primary=True).sdesc

        self.some_parms = copy.copy(dparm.this_process)

        username = os.environ.get("USER", str(os.getuid()))
        self.pool_name = "spac_" + username
        self.pool_size = 2**30
        self.pool_prealloc_blocks = None
        self.pool_uid = 17

        self.mpool = dmm.MemoryPool(self.pool_size, self.pool_name, self.pool_uid, self.pool_prealloc_blocks)
        self.some_parms.inf_pd = B64.bytes_to_str(self.mpool.serialize())

        def mk_handles(cuid):
            chan = dch.Channel(self.mpool, cuid)
            rh = dconn.Connection(inbound_initializer=chan)
            wh = dconn.Connection(outbound_initializer=chan)
            envser = B64.bytes_to_str(chan.serialize())
            return chan, rh, wh, envser

        handles = mk_handles(dfacts.GS_INPUT_CUID)
        self.gs_input_chan, self.gs_input_rh, self.gs_input_wh, self.some_parms.gs_cd = handles

        handles = mk_handles(dfacts.BASE_BE_CUID)
        self.bela_input_chan, self.bela_input_rh, self.bela_input_wh, self.some_parms.local_be_cd = handles

        handles = mk_handles(dfacts.BASE_SHEP_CUID)
        self.shep_input_chan, self.shep_input_rh, self.shep_input_wh, self.some_parms.local_shep_cd = handles

        self.gs_return_cuid = 17
        handles = mk_handles(self.gs_return_cuid)
        (
            self.proc_gs_return_chan,
            self.proc_gs_return_rh,
            self.proc_gs_return_wh,
            self.some_parms.gs_ret_cd,
        ) = handles

        dapi.test_connection_override(
            test_gs_input=self.gs_input_wh,
            test_gs_return=self.proc_gs_return_rh,
            test_gs_return_cuid=self.gs_return_cuid,
            test_shep_input=self.shep_input_wh,
        )

        self.tag = 0
        self.dut = None
        self._start_dut()
        self._start_head()

    def tearDown(self) -> None:
        self._kill_head()
        self._teardown_dut()
        self.gs_stdout_rh.close()
        self.gs_stdout_wh.close()

        # connections
        self.gs_input_rh.close()
        self.gs_input_wh.close()
        self.bela_input_rh.close()
        self.bela_input_wh.close()
        self.shep_input_rh.close()
        self.shep_input_wh.close()

        self.proc_gs_return_rh.close()
        self.proc_gs_return_wh.close()

        # underlying channels
        self.gs_input_chan.destroy()
        self.bela_input_chan.destroy()
        self.shep_input_chan.destroy()
        self.proc_gs_return_chan.destroy()
        self.mpool.destroy()

        if self.dut is not None:
            self.dut.join(0)

    def next_tag(self):
        tmp = self.tag
        self.tag += 1
        return tmp

    def _start_dut(self):
        test_name = self.__class__.__name__ + "_" + inspect.stack()[1][0].f_code.co_name

        reconst_gs_return_wh = (self.mpool.serialize(), self.proc_gs_return_chan.serialize(), False)

        test_overrides = {self.gs_return_cuid: reconst_gs_return_wh}

        the_env_wanted = self.some_parms.env()

        self.dut = multiprocessing.Process(
            target=bringup_channels,
            args=(self.gs_stdout_wh, the_env_wanted, test_overrides),
            kwargs={"logname": test_name},
            name="globalservices",
        )

        self.dut.start()

        tsu.get_and_check_type(self.shep_input_rh, dmsg.GSPingSH)

        self.gs_input_wh.send(dmsg.SHPingGS(tag=0, node_sdesc=self.node_sdesc).serialize())

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSIsUp)

    def _teardown_dut(self):
        self.gs_input_wh.send(dmsg.GSTeardown(tag=self.next_tag()).serialize())

        tsu.get_and_check_type(self.gs_stdout_rh, dmsg.GSHalted)

    def _start_head(self):
        create_msg = dmsg.GSProcessCreate(
            tag=self.next_tag(),
            p_uid=dfacts.LAUNCHER_PUID,
            r_c_uid=dfacts.BASE_BE_CUID,
            exe="dummy",
            args=[],
            env={},
            user_name="dummy",
            head_proc=True,
        )

        self.gs_input_wh.send(create_msg.serialize())

        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHProcessCreate)
        self.assertEqual(shep_msg.exe, create_msg.exe)
        self.assertEqual(shep_msg.args, create_msg.args)

        shep_reply_msg = dmsg.SHProcessCreateResponse(
            tag=0, ref=shep_msg.tag, err=dmsg.SHProcessCreateResponse.Errors.SUCCESS
        )

        self.gs_input_wh.send(shep_reply_msg.serialize())

        create_reply_msg = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSProcessCreateResponse)
        self.assertEqual(create_reply_msg.ref, create_msg.tag, "tag-ref mismatch")
        self.assertEqual(create_reply_msg.err, dmsg.GSProcessCreateResponse.Errors.SUCCESS)
        self.assertEqual(shep_msg.t_p_uid, create_reply_msg.desc.p_uid)

        self.head_puid = create_reply_msg.desc.p_uid

        # pretend the test driver is the head process.
        dparm.this_process.my_puid = self.head_puid

    def _kill_head(self):
        death_msg = dmsg.SHProcessExit(tag=self.next_tag(), p_uid=self.head_puid)

        self.gs_input_wh.send(death_msg.serialize())

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSHeadExit)

    def _create_proc(self, proc_name):
        def create_wrap(the_exe, the_run_dir, the_args, the_env, the_name, result_list):
            res = process_create(exe=the_exe, run_dir=the_run_dir, args=the_args, env=the_env, user_name=the_name)
            result_list.append(res)

        create_result = []
        create_thread = threading.Thread(
            target=create_wrap, args=("test", "/tmp", ["foo", "bar"], {}, proc_name, create_result)
        )
        create_thread.start()

        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHProcessCreate)
        shep_reply_msg = dmsg.SHProcessCreateResponse(
            tag=self.next_tag(), ref=shep_msg.tag, err=dmsg.SHProcessCreateResponse.Errors.SUCCESS
        )

        self.gs_input_wh.send(shep_reply_msg.serialize())

        create_thread.join()
        desc = create_result[0]
        return desc

    def _send_get_responses(self, nitems, result):
        if result == "fail":
            shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHMultiProcessCreate)
            responses = []
            for i in range(nitems):
                responses.append(
                    dmsg.SHProcessCreateResponse(
                        tag=self.next_tag(),
                        ref=shep_msg.procs[i].tag,
                        err=dmsg.SHProcessCreateResponse.Errors.FAIL,
                        err_info="simulated failure",
                    )
                )
            shep_reply_msg = dmsg.SHMultiProcessCreateResponse(
                tag=self.next_tag(),
                ref=shep_msg.tag,
                err=dmsg.SHMultiProcessCreateResponse.Errors.SUCCESS,
                responses=responses,
            )
            self.gs_input_wh.send(shep_reply_msg.serialize())
        elif result == "success":
            shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHMultiProcessCreate)
            responses = []
            for i in range(nitems):
                responses.append(
                    dmsg.SHProcessCreateResponse(
                        tag=self.next_tag(), ref=shep_msg.procs[i].tag, err=dmsg.SHProcessCreateResponse.Errors.SUCCESS
                    )
                )
            shep_reply_msg = dmsg.SHMultiProcessCreateResponse(
                tag=self.next_tag(),
                ref=shep_msg.tag,
                err=dmsg.SHMultiProcessCreateResponse.Errors.SUCCESS,
                responses=responses,
            )
            self.gs_input_wh.send(shep_reply_msg.serialize())
        else:
            # we create half failed processes and half successful ones
            shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHMultiProcessCreate)

            responses = []
            for i in range(0, nitems // 2):
                responses.append(
                    dmsg.SHProcessCreateResponse(
                        tag=self.next_tag(), ref=shep_msg.procs[i].tag, err=dmsg.SHProcessCreateResponse.Errors.SUCCESS
                    )
                )

            for i in range(nitems // 2, nitems):
                responses.append(
                    dmsg.SHProcessCreateResponse(
                        tag=self.next_tag(),
                        ref=shep_msg.procs[i].tag,
                        err=dmsg.SHProcessCreateResponse.Errors.FAIL,
                        err_info="simulated failure",
                    )
                )

            shep_reply_msg = dmsg.SHMultiProcessCreateResponse(
                tag=self.next_tag(),
                ref=shep_msg.tag,
                err=dmsg.SHMultiProcessCreateResponse.Errors.SUCCESS,
                responses=responses,
            )
            self.gs_input_wh.send(shep_reply_msg.serialize())

    def _create_group(self, group_items, group_policy, group_name, existing=False):
        def create_wrap(items, policy, the_name, result_list):
            res = group_create(items, policy, user_name=the_name)
            result_list.append(res)

        create_result = []
        create_thread = threading.Thread(
            target=create_wrap, args=(group_items, group_policy, group_name, create_result)
        )
        create_thread.start()

        # get the number of members inside this group in total
        nitems = 0
        for lst in group_items:
            nitems += lst[0]

        # in the case that we're trying to create a group that already exists,
        # we don't need the following code
        if not existing:  # not already existing group
            self._send_get_responses(nitems, "success")

        create_thread.join()

        desc = create_result[0]
        return desc

    def _create_group_with_failed_processes(self, group_items, group_policy, group_name, fail):
        def create_wrap(items, policy, the_name, result_list):
            res = group_create(items, policy, user_name=the_name)
            result_list.append(res)

        create_result = []
        create_thread = threading.Thread(
            target=create_wrap, args=(group_items, group_policy, group_name, create_result)
        )
        create_thread.start()

        # get the number of members inside this group in total
        nitems = 0
        for lst in group_items:
            nitems += lst[0]

        self._send_get_responses(nitems, fail)

        create_thread.join()

        desc = create_result[0]
        return desc


class GSGroupAPI(GSGroupBaseClass):
    def test_create(self):
        n = 10
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")

        self.assertEqual(descr.name, "bob")
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(type(descr.sets[0]), list)
        self.assertEqual(len(descr.sets), 1)  # 1 list in the sets
        self.assertEqual(len(descr.sets[0]), n)  # n members inside the list
        self.assertEqual(all(isinstance(item, GroupDescriptor.GroupMember) for item in descr.sets[0]), True)
        self.assertEqual(all(isinstance(item.desc, ProcessDescriptor) for item in descr.sets[0]), True)
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in descr.sets[0]), True)
        self.assertEqual(all(item.desc.p_uid == item.uid for item in descr.sets[0]), True)
        self.assertEqual(
            all(item.error_code == dmsg.GSProcessCreateResponse.Errors.SUCCESS.value for item in descr.sets[0]), True
        )

        self.assertEqual(descr.policy.placement, policy.Placement.ANYWHERE)
        self.assertEqual(descr.policy.host_name, "")
        self.assertEqual(descr.policy.host_id, -1)
        self.assertEqual(descr.policy.distribution, Policy.Distribution.ROUNDROBIN)

        # test each member's name when a user_name is provided by the user
        for lst_idx, lst in enumerate(descr.sets):
            for item_idx, item in enumerate(lst):
                self.assertEqual(item.desc.name, f"solver.{descr.g_uid}.{lst_idx}.{item_idx}")

    def test_create_multiple_tuples(self):
        n1 = 10
        n2 = 20
        process_msg1 = get_create_message(exe="test", run_dir="/tmp", args=["foo1", "bar1"], env={})
        process_msg2 = get_create_message(exe="test", run_dir="/tmp", args=["foo2", "bar2"], env={})
        items = [(n1, process_msg1.serialize()), (n2, process_msg2.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, None)

        self.assertEqual(
            descr.name, dfacts.DEFAULT_GROUP_NAME_BASE + str(dfacts.FIRST_GUID)
        )  # check that the default group name was given
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(all(isinstance(lst, list) for lst in descr.sets), True)
        self.assertEqual(len(descr.sets), 2)  # 2 lists in the sets
        self.assertEqual(len(descr.sets[0]), n1)  # n1 members inside the first list
        self.assertEqual(len(descr.sets[1]), n2)  # n2 members inside the second list
        self.assertEqual(len(descr.sets[0]) + len(descr.sets[1]), n1 + n2)  # n1+n2 members in total

        group_puids = []
        for lst in descr.sets:
            self.assertEqual(all(isinstance(item, GroupDescriptor.GroupMember) for item in lst), True)
            self.assertEqual(all(isinstance(item.desc, ProcessDescriptor) for item in lst), True)
            self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in lst), True)
            self.assertEqual(
                all(item.error_code == dmsg.GSProcessCreateResponse.Errors.SUCCESS.value for item in lst), True
            )
            self.assertEqual(all(item.desc.p_uid == item.uid for item in lst), True)

            group_puids.extend([item.uid for item in lst if item.desc.state == ProcessDescriptor.State.ACTIVE])

            # test each member's name when a user_name is NOT provided by the user and the default is used
            for item in lst:
                self.assertEqual(item.desc.name, f"dragon_process_{item.desc.p_uid}")

                # now, kill the process
                death_msg = dmsg.SHProcessExit(tag=self.next_tag(), p_uid=item.uid)
                self.gs_input_wh.send(death_msg.serialize())

        multi_join(group_puids, join_all=True)

        self.assertEqual(descr.policy.placement, policy.Placement.ANYWHERE)
        self.assertEqual(descr.policy.host_name, "")
        self.assertEqual(descr.policy.host_id, -1)
        self.assertEqual(descr.policy.distribution, Policy.Distribution.ROUNDROBIN)

    def test_create_when_group_already_exists(self):
        n = 5
        descriptors = []
        # attempt to create two identical groups
        # the first group will be created successfully
        # the second will just return the group descriptor immediately
        existing = False
        for i in range(2):
            if i == 1:
                existing = True
            process_msg = get_create_message(
                exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver"
            )
            items = [(n, process_msg.serialize())]
            policy = Policy()
            descr = self._create_group(items, policy, "bob", existing)
            descriptors.append(descr)
        self.assertEqual(descriptors[0].name, descriptors[1].name)
        self.assertEqual(descriptors[0].g_uid, descriptors[1].g_uid)

    def test_create_with_failure(self):
        """All the processes/members of the group fail to create"""

        n = 5
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group_with_failed_processes(items, policy, "bob", "fail")

        self.assertEqual(descr.name, "bob")
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(type(descr.sets[0]), list)
        self.assertEqual(len(descr.sets), 1)  # 1 list in the sets
        self.assertEqual(len(descr.sets[0]), n)  # n members inside the list
        self.assertEqual(all(isinstance(item, GroupDescriptor.GroupMember) for item in descr.sets[0]), True)
        self.assertEqual(all(isinstance(item.desc, ProcessDescriptor) for item in descr.sets[0]), True)
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.DEAD for item in descr.sets[0]), True)
        self.assertEqual(
            all(item.error_code == dmsg.GSProcessCreateResponse.Errors.FAIL.value for item in descr.sets[0]), True
        )
        self.assertEqual(all(item.desc.p_uid == item.uid for item in descr.sets[0]), True)

        self.assertEqual(descr.policy.placement, policy.Placement.ANYWHERE)
        self.assertEqual(descr.policy.host_name, "")
        self.assertEqual(descr.policy.host_id, -1)
        self.assertEqual(descr.policy.distribution, Policy.Distribution.ROUNDROBIN)

        # test each member's name when a user_name is provided by the user
        for lst_idx, lst in enumerate(descr.sets):
            for item_idx, item in enumerate(lst):
                self.assertEqual(item.desc.name, f"solver.{descr.g_uid}.{lst_idx}.{item_idx}")

    def test_create_with_half_failures(self):
        n = 10
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group_with_failed_processes(items, policy, "bob", "mix")

        self.assertEqual(descr.name, "bob")
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(type(descr.sets[0]), list)
        self.assertEqual(len(descr.sets), 1)  # 1 list in the sets
        self.assertEqual(len(descr.sets[0]), n)  # n members inside the list
        self.assertEqual(all(isinstance(item, GroupDescriptor.GroupMember) for item in descr.sets[0]), True)
        self.assertEqual(all(isinstance(item.desc, ProcessDescriptor) for item in descr.sets[0]), True)
        self.assertEqual(all(item.desc.p_uid == item.uid for item in descr.sets[0]), True)

        self.assertEqual(
            all(descr.sets[0][i].desc.state == ProcessDescriptor.State.ACTIVE for i in range(0, n // 2)), True
        )
        self.assertEqual(
            all(descr.sets[0][i].desc.state == ProcessDescriptor.State.DEAD for i in range(n // 2, n)), True
        )
        self.assertEqual(
            all(
                descr.sets[0][i].error_code == dmsg.GSProcessCreateResponse.Errors.SUCCESS.value
                for i in range(0, n // 2)
            ),
            True,
        )
        self.assertEqual(
            all(
                descr.sets[0][i].error_code == dmsg.GSProcessCreateResponse.Errors.FAIL.value for i in range(n // 2, n)
            ),
            True,
        )

        self.assertEqual(descr.policy.placement, policy.Placement.ANYWHERE)
        self.assertEqual(descr.policy.host_name, "")
        self.assertEqual(descr.policy.host_id, -1)
        self.assertEqual(descr.policy.distribution, Policy.Distribution.ROUNDROBIN)

        # test each member's name when a user_name is provided by the user
        for lst_idx, lst in enumerate(descr.sets):
            for item_idx, item in enumerate(lst):
                self.assertEqual(item.desc.name, f"solver.{descr.g_uid}.{lst_idx}.{item_idx}")

    def test_create_invalid_members(self):
        n = 10
        # send inappropriate message type
        process_msg = dmsg.SHProcessCreate(
            tag=dapi.next_tag(),
            p_uid=dfacts.GS_PUID,
            r_c_uid=dfacts.GS_INPUT_CUID,
            t_p_uid=dfacts.FIRST_PUID + 1000,
            exe="test",
            args=["foo", "bar"],
        )
        items = [(n, process_msg.serialize())]
        policy = Policy()
        with self.assertRaises(GroupError):
            descr = group_create(items, policy, "bob")

    def test_create_multiple_groups(self):
        n = 10
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        descr1 = self._create_group(items, policy, "alice")

        self.assertEqual(descr.name, "bob")
        self.assertEqual(descr1.name, "alice")

    def _kill_a_thing(self, identifier, n):
        def kill_wrap(ident, result_list):
            # send a kill request for the group
            res = kill(ident)
            result_list.append(res)

        kill_result = []
        kill_thread = threading.Thread(target=kill_wrap, args=(identifier, kill_result))
        kill_thread.start()

        sh_multi_kill_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHMultiProcessKill)

        responses = []
        for i in range(n):
            responses.append(
                dmsg.SHProcessKillResponse(
                    tag=self.next_tag(),
                    ref=sh_multi_kill_msg.procs[i].tag,
                    err=dmsg.SHProcessKillResponse.Errors.SUCCESS,
                )
            )

        sh_multi_kill_reply = dmsg.SHMultiProcessKillResponse(
            tag=self.next_tag(),
            ref=sh_multi_kill_msg.tag,
            responses=responses,
            failed=False,
            err=dmsg.SHMultiProcessKillResponse.Errors.SUCCESS,
        )
        self.gs_input_wh.send(sh_multi_kill_reply.serialize())

        kill_thread.join()
        return kill_result[0]

    def test_kill(self):
        n = 10
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in descr.sets[0]), True)
        self.assertEqual(descr.name, "bob")

        # kill an existing group of resources
        descr = self._kill_a_thing(descr.g_uid, n)
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in descr.sets[0]), True)

    def test_kill_non_existing_group(self):
        # try to destroy a non-existing group
        with self.assertRaises(GroupError):
            kill("doe")

    def test_kill_a_pending_group(self):
        # try to destroy a group that is still pending
        n = 20
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()

        def create_wrap(items, policy, the_name, result_list):
            res = group_create(items, policy, user_name=the_name)
            result_list.append(res)

        create_result = []
        create_thread = threading.Thread(target=create_wrap, args=(items, policy, "bob", create_result))
        create_thread.start()

        # immediately try to destroy the group, that is still pending
        # because the create is not complete yet; this should raise
        with self.assertRaises(GroupError):
            kill("bob")

        # complete the construction of the group
        self._send_get_responses(n, "success")
        create_thread.join()

        descr = create_result[0]
        self.assertEqual(descr.name, "bob")
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)

    def _destroy_a_group(self, identifier, n, sets):
        def destroy_wrap(ident, result_list):
            res = destroy(ident)
            result_list.append(res)

        destroy_result = []
        destroy_thread = threading.Thread(target=destroy_wrap, args=(identifier, destroy_result))
        destroy_thread.start()

        sh_multi_kill_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHMultiProcessKill)

        responses = []
        for i in range(n):
            responses.append(
                dmsg.SHProcessKillResponse(
                    tag=self.next_tag(),
                    ref=sh_multi_kill_msg.procs[i].tag,
                    err=dmsg.SHProcessKillResponse.Errors.SUCCESS,
                )
            )

        sh_multi_kill_reply = dmsg.SHMultiProcessKillResponse(
            tag=self.next_tag(),
            ref=sh_multi_kill_msg.tag,
            responses=responses,
            failed=False,
            err=dmsg.SHMultiProcessKillResponse.Errors.SUCCESS,
        )
        self.gs_input_wh.send(sh_multi_kill_reply.serialize())

        for item in sets:
            death_msg = dmsg.SHProcessExit(tag=self.next_tag(), p_uid=item.uid)
            self.gs_input_wh.send(death_msg.serialize())

        destroy_thread.join()
        return destroy_result[0]

    def test_destroy(self):
        n = 10
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in descr.sets[0]), True)
        self.assertEqual(descr.name, "bob")

        # destroy an existing group of resources
        descr = self._destroy_a_group(descr.g_uid, n, descr.sets[0])
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.DEAD for item in descr.sets[0]), True)
        self.assertEqual(int(descr.state), GroupDescriptor.State.DEAD)

    def test_destroy_alt(self):
        # first call kill on the group and then destroy
        n = 10
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in descr.sets[0]), True)
        self.assertEqual(descr.name, "bob")

        # kill an existing group of resources
        descr = self._kill_a_thing(descr.g_uid, n)
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in descr.sets[0]), True)

        # now, actually kill the processes
        for item in descr.sets[0]:
            death_msg = dmsg.SHProcessExit(tag=self.next_tag(), p_uid=item.uid)
            self.gs_input_wh.send(death_msg.serialize())

        # destroy an existing group of resources
        descr = destroy("bob")
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.DEAD for item in descr.sets[0]), True)
        self.assertEqual(int(descr.state), GroupDescriptor.State.DEAD)

    def test_destroy_non_existing_group(self):
        # try to destroy a non-existing group
        with self.assertRaises(GroupError):
            destroy("doe")

    def test_destroy_a_pending_group(self):
        # try to destroy a group that is still pending
        n = 20
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()

        def create_wrap(items, policy, the_name, result_list):
            res = group_create(items, policy, user_name=the_name)
            result_list.append(res)

        create_result = []
        create_thread = threading.Thread(target=create_wrap, args=(items, policy, "bob", create_result))
        create_thread.start()

        # immediately try to destroy the group, that is still pending
        # because the create is not complete yet; this should raise
        with self.assertRaises(GroupError):
            destroy("bob")

        # complete the construction of the group
        self._send_get_responses(n, "success")
        create_thread.join()

        descr = create_result[0]
        self.assertEqual(descr.name, "bob")
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)

    def _send_responses(self, nitems):
        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHMultiProcessCreate)
        responses = []
        for i in range(nitems):
            responses.append(
                dmsg.SHProcessCreateResponse(
                    tag=self.next_tag(), ref=shep_msg.procs[i].tag, err=dmsg.SHProcessCreateResponse.Errors.SUCCESS
                )
            )
        shep_reply_msg = dmsg.SHMultiProcessCreateResponse(
            tag=self.next_tag(),
            ref=shep_msg.tag,
            err=dmsg.SHMultiProcessCreateResponse.Errors.SUCCESS,
            responses=responses,
        )
        self.gs_input_wh.send(shep_reply_msg.serialize())

    def test_create_add_to(self):
        # first create a group
        n = 5
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")

        # now, let's add processes to the group
        n1 = 3
        add_thread = threading.Thread(target=self._send_responses, args=(n1,))
        add_thread.start()

        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="addition")
        items = [(n1, process_msg.serialize())]
        descr = create_add_to(descr.g_uid, items, policy)
        add_thread.join()

        self.assertEqual(descr.name, "bob")
        self.assertEqual(int(descr.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(type(descr.sets[0]), list)
        self.assertEqual(len(descr.sets), 2)  # 2 lists in the sets
        self.assertEqual(len(descr.sets[0]), n)  # n members inside the 1st list
        self.assertEqual(len(descr.sets[1]), n1)  # n1 members inside the 2nd list
        self.assertEqual(len(descr.sets[0]) + len(descr.sets[1]), n1 + n)  # n1+n members in total

        for lst in descr.sets:
            self.assertEqual(all(isinstance(item, GroupDescriptor.GroupMember) for item in lst), True)
            self.assertEqual(all(isinstance(item.desc, ProcessDescriptor) for item in lst), True)
            self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in lst), True)
            self.assertEqual(
                all(item.error_code == dmsg.GSProcessCreateResponse.Errors.SUCCESS.value for item in lst), True
            )
            self.assertEqual(all(item.desc.p_uid == item.uid for item in lst), True)

        # test each member's name when a user_name is provided by the user
        for lst_idx, lst in enumerate(descr.sets):
            for item_idx, item in enumerate(lst):
                if lst_idx == 0:
                    self.assertEqual(item.desc.name, f"solver.{descr.g_uid}.{lst_idx}.{item_idx}")
                elif lst_idx == 1:
                    self.assertEqual(item.desc.name, f"addition.{descr.g_uid}.{lst_idx}.{item_idx}")

    def test_add_to(self):
        # first create a group
        n = 5
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")

        # create some processes
        items = []
        n1 = 3
        for i in range(n1):
            pdescr = self._create_proc(f"alice{i}")
            self.assertEqual(pdescr.name, f"alice{i}")
            items.append(pdescr.p_uid)

        # now, let's add these processes to the group
        descr1 = add_to(descr.g_uid, items)

        self.assertEqual(descr1.name, "bob")
        self.assertEqual(int(descr1.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(len(descr1.sets), 4)  # 4 lists in the sets: 1+n1
        self.assertEqual(len(descr1.sets[0]), n)  # n members inside the 1st list

        for lst in descr1.sets:
            self.assertEqual(all(isinstance(item, GroupDescriptor.GroupMember) for item in lst), True)
            self.assertEqual(all(isinstance(item.desc, ProcessDescriptor) for item in lst), True)
            self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in lst), True)
            self.assertEqual(
                all(item.error_code == dmsg.GSProcessCreateResponse.Errors.SUCCESS.value for item in lst), True
            )
            self.assertEqual(all(item.desc.p_uid == item.uid for item in lst), True)

        # test each member's name when a user_name is provided by the user
        for lst_idx, lst in enumerate(descr1.sets):
            for item_idx, item in enumerate(lst):
                if lst_idx == 0:
                    self.assertEqual(item.desc.name, f"solver.{descr.g_uid}.{lst_idx}.{item_idx}")
                else:
                    self.assertEqual(item.desc.name, f"alice{lst_idx-1}")

    def test_remove_from(self):
        # first create a group
        n = 8
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)  # descr should have 8 items in descr.sets

        group_puids = []
        for lst in descr.sets:
            group_puids.extend([item.uid for item in lst if item.desc.state == ProcessDescriptor.State.ACTIVE])

        # now, let's remove a few processes
        # remove specific processes
        name = f"solver.{descr.g_uid}.0.0"  # this should be the name of the first created process
        descr1 = remove_from(descr.g_uid, [name, group_puids[6]])
        self.assertEqual(len(descr1.sets[0]), 6)  # descr1 should have 6 items in descr1.sets

        # update group_puids
        del group_puids[6]
        del group_puids[0]

        # remove the first n1 processes
        n1 = 3
        descr2 = remove_from(descr1.g_uid, group_puids[0:n1])
        self.assertEqual(len(descr2.sets[0]), 3)  # descr2 should have 3 items in descr2.sets

        # update group_puids
        del group_puids[0:n1]

        # compare group_puids with the final/remaining members of the group
        for i in range(len(descr2.sets[0])):
            assert group_puids[i] == descr2.sets[0][i].uid

        self.assertEqual(len(descr2.sets[0]), len(group_puids))

    def test_remove_when_nonexist(self):
        # try to remove processes that are not members of the group
        # first create a group
        n = 5
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)

        # try to remove non-existing processes
        name = f"solver1.{descr.g_uid}.0.0"
        with self.assertRaises(GroupError):
            remove_from(descr.g_uid, [name])

    def test_remove_non_existing_procs(self):
        # first create a group
        n = 3
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)

        # try to remove one valid and two non-valid procs
        with self.assertRaises(GroupError):
            remove_from(descr.g_uid, [descr.sets[0][0].uid, "rand1", "rand2"])

    def _send_kill_messages(self, n):
        sh_multi_kill_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHMultiProcessKill)

        responses = []
        for i in range(n):
            responses.append(
                dmsg.SHProcessKillResponse(
                    tag=self.next_tag(),
                    ref=sh_multi_kill_msg.procs[i].tag,
                    err=dmsg.SHProcessKillResponse.Errors.SUCCESS,
                )
            )

        sh_multi_kill_reply = dmsg.SHMultiProcessKillResponse(
            tag=self.next_tag(),
            ref=sh_multi_kill_msg.tag,
            responses=responses,
            failed=False,
            err=dmsg.SHMultiProcessKillResponse.Errors.SUCCESS,
        )
        self.gs_input_wh.send(sh_multi_kill_reply.serialize())

    def test_destroy_remove_from(self):
        # first create a group
        n = 8
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)  # descr should have 8 items in descr.sets

        group_puids = []
        for lst in descr.sets:
            group_puids.extend([item.uid for item in lst if item.desc.state == ProcessDescriptor.State.ACTIVE])

        # now, let's remove a few processes
        n1 = 2
        kill_thread = threading.Thread(target=self._send_kill_messages, args=(n1,))
        kill_thread.start()
        # remove specific processes
        name = f"solver.{descr.g_uid}.0.0"  # this should be the name of the first created process
        descr1 = destroy_remove_from(descr.g_uid, [name, group_puids[6]])
        self.assertEqual(len(descr1.sets[0]), 6)  # descr1 should have 6 items in descr1.sets
        kill_thread.join()

        # update group_puids
        del group_puids[6]
        del group_puids[0]

        # remove the first n1 processes
        n1 = 3
        kill_thread = threading.Thread(target=self._send_kill_messages, args=(n1,))
        kill_thread.start()
        descr2 = destroy_remove_from(descr1.g_uid, group_puids[0:n1])
        self.assertEqual(len(descr2.sets[0]), 3)  # descr2 should have 3 items in descr2.sets
        kill_thread.join()

        # update group_puids
        del group_puids[0:n1]

        # compare group_puids with the final/remaining members of the group
        for i in range(len(descr2.sets[0])):
            assert group_puids[i] == descr2.sets[0][i].uid

        self.assertEqual(len(descr2.sets[0]), len(group_puids))
        self.assertEqual(int(descr2.state), GroupDescriptor.State.ACTIVE)
        for lst in descr2.sets:
            group_puids.extend([item.uid for item in lst if item.desc.state == ProcessDescriptor.State.ACTIVE])

    def test_destroy_remove_from_non_existing_group(self):
        # try to destroy_remove_from a non-existing group
        with self.assertRaises(GroupError):
            destroy_remove_from("doe", ["rand"])

    def test_destroy_remove_from_already_dead_procs(self):
        # first create a group
        n = 5
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)

        puids_list = []
        for item in descr.sets[0]:
            puids_list.append(item.uid)

        # kill all the procs that belong to the group without letting the group know
        for item in puids_list:
            death_msg = dmsg.SHProcessExit(tag=self.next_tag(), p_uid=item)
            self.gs_input_wh.send(death_msg.serialize())

        # and now try to remove them from the group
        descr1 = destroy_remove_from(descr.g_uid, puids_list)
        # we end up with an active but empty group
        self.assertEqual(int(descr1.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(len(descr1.sets), 0)

    def test_destroy_remove_from_comb_already_dead_procs(self):
        # we'll ask to remove procs from which some are already dead
        # and some are normal

        # first create a group
        n = 5
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)

        puids_list = []
        for item in descr.sets[0]:
            puids_list.append(item.uid)

        # kill some of the procs that belong to the group without letting the group know
        for item in puids_list[: n - 2]:
            death_msg = dmsg.SHProcessExit(tag=self.next_tag(), p_uid=item)
            self.gs_input_wh.send(death_msg.serialize())

        kill_thread = threading.Thread(target=self._send_kill_messages, args=(1,))
        kill_thread.start()

        # remove all the members except for the last one
        descr1 = destroy_remove_from(descr.g_uid, puids_list[: n - 1])
        kill_thread.join()
        self.assertEqual(int(descr1.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(len(descr1.sets[0]), 1)

        # compare with the initial group.sets
        for i, item in enumerate(descr.sets[0]):
            if i < n - 1:
                assert item not in descr1.sets[0]
            else:
                self.assertEqual(item.uid, descr1.sets[0][0].uid)

    def test_destroy_remove_from_non_existing_comb_procs(self):
        # first create a group
        n = 3
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)

        # try to remove one valid and two non-valid procs
        with self.assertRaises(GroupError):
            destroy_remove_from(descr.g_uid, [descr.sets[0][0].uid, "rand1", "rand2"])

    def test_destroy_remove_from_non_existing_procs(self):
        # first create a group
        n = 3
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)

        # try to remove one valid and two non-valid procs
        with self.assertRaises(GroupError):
            destroy_remove_from(descr.g_uid, ["rand1", "rand2"])

    def test_remove_from_and_then_destroy_group(self):
        # first create a group
        n = 8
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)

        group_puids = []
        for lst in descr.sets:
            group_puids.extend([item.uid for item in lst if item.desc.state == ProcessDescriptor.State.ACTIVE])

        # now, let's remove a few processes
        # remove specific processes
        name = f"solver.{descr.g_uid}.0.0"  # this should be the name of the first created process
        descr1 = remove_from(descr.g_uid, [name, group_puids[6]])
        self.assertEqual(len(descr1.sets[0]), 6)  # descr1 should have 6 items in descr1.sets

        # update group_puids
        del group_puids[6]
        del group_puids[0]

        # now, let's destroy the group
        descr2 = self._destroy_a_group(descr1.g_uid, n - 2, descr1.sets[0])
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.DEAD for item in descr2.sets[0]), True)
        self.assertEqual(int(descr2.state), GroupDescriptor.State.DEAD)

    def test_kill_then_add_to(self):
        # create, kill, createAdd

        # first create a group
        n = 8
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)

        # next call kill on the group's processes
        descr1 = self._kill_a_thing(descr.g_uid, n)
        self.assertEqual(int(descr1.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in descr1.sets[0]), True)

        # last, try to add a process to the group
        # create some processes
        items = []
        n1 = 3
        for i in range(n1):
            pdescr = self._create_proc(f"alice{i}")
            self.assertEqual(pdescr.name, f"alice{i}")
            items.append(pdescr.p_uid)

        # now, let's add these processes to the group
        descr2 = add_to(descr.g_uid, items)

        self.assertEqual(descr2.name, "bob")
        self.assertEqual(int(descr2.state), GroupDescriptor.State.ACTIVE)
        self.assertEqual(len(descr2.sets), 4)  # 4 lists in the sets: 1+n1
        self.assertEqual(len(descr2.sets[0]), n)  # n members inside the 1st list

        for lst in descr2.sets:
            self.assertEqual(all(isinstance(item, GroupDescriptor.GroupMember) for item in lst), True)
            self.assertEqual(all(isinstance(item.desc, ProcessDescriptor) for item in lst), True)
            self.assertEqual(all(item.desc.state == ProcessDescriptor.State.ACTIVE for item in lst), True)
            self.assertEqual(
                all(item.error_code == dmsg.GSProcessCreateResponse.Errors.SUCCESS.value for item in lst), True
            )
            self.assertEqual(all(item.desc.p_uid == item.uid for item in lst), True)

        # test each member's name when a user_name is provided by the user
        for lst_idx, lst in enumerate(descr2.sets):
            for item_idx, item in enumerate(lst):
                if lst_idx == 0:
                    self.assertEqual(item.desc.name, f"solver.{descr.g_uid}.{lst_idx}.{item_idx}")
                else:
                    self.assertEqual(item.desc.name, f"alice{lst_idx-1}")

    def test_query(self):
        # create two groups
        n = 3
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr1 = self._create_group(items, policy, "bob")

        n = 5
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr2 = self._create_group(items, policy, "alice")

        glist = get_list()

        descr = query(glist[0])
        self.assertEqual(descr.g_uid, descr1.g_uid)
        descr = query(glist[1])
        self.assertEqual(descr.g_uid, descr2.g_uid)

    def test_query_alive(self):
        n = 3
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)

        # test query by name
        descr1 = query("bob")
        self.assertEqual(descr.get_sdict(), descr1.get_sdict())

        # test query by g_uid
        descr2 = query(descr.g_uid)
        self.assertEqual(descr.get_sdict(), descr2.get_sdict())

    def test_query_dead(self):
        n = 3
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")
        self.assertEqual(len(descr.sets[0]), n)

        descr = self._destroy_a_group(descr.g_uid, n, descr.sets[0])

        descr1 = query("bob")
        self.assertEqual(descr.get_sdict(), descr1.get_sdict())

    def test_get_list(self):
        # no groups yet
        glist = get_list()
        self.assertEqual([], glist)

        # create a couple of groups
        glist_orig = []

        n = 3
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr1 = self._create_group(items, policy, "bob")
        glist_orig.append(descr1.g_uid)

        n = 5
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr2 = self._create_group(items, policy, "alice")
        glist_orig.append(descr2.g_uid)

        glist = get_list()
        self.assertEqual(glist_orig, glist)

    def test_get_list_alive_and_dead_groups(self):
        # create a couple of groups
        glist_orig = []

        n = 3
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr1 = self._create_group(items, policy, "bob")
        glist_orig.append(descr1.g_uid)

        n = 5
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr2 = self._create_group(items, policy, "alice")
        glist_orig.append(descr2.g_uid)

        glist = get_list()
        self.assertEqual(glist_orig, glist)

        # destroy the second group
        descr = self._destroy_a_group(descr2.g_uid, n, descr2.sets[0])
        self.assertEqual(descr.g_uid, glist_orig[1])

        glist = get_list()
        # the returned list should be the same with the original one as get_list(()
        # returns both alive and dead groups
        self.assertEqual(glist_orig, glist)
