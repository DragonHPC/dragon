#!/usr/bin/env python3
"""Test script for the GS process API in single node mode"""

import time

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
import dragon.globalservices.channel as dchannel
import dragon.globalservices.pool as dpool
import dragon.globalservices.process as dproc
import dragon.globalservices.server as dserver
import dragon.infrastructure.facts as dfacts
import dragon.dlogging.util as dlog
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.parameters as dparm
from dragon.infrastructure.node_desc import NodeDescriptor
import support.util as tsu
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


class GSProcessBaseClass(unittest.TestCase):
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

    def _kill_a_thing(self, identifier):
        def kill_wrap(ident, result_list):
            res = dproc.kill(ident)
            result_list.append(res)

        kill_result = []
        kill_thread = threading.Thread(target=kill_wrap, args=(identifier, kill_result))
        kill_thread.start()

        sh_kill_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHProcessKill)
        sh_kill_reply = dmsg.SHProcessKillResponse(
            tag=self.next_tag(), ref=sh_kill_msg.tag, err=dmsg.SHProcessKillResponse.Errors.SUCCESS
        )

        self.gs_input_wh.send(sh_kill_reply.serialize())

        kill_thread.join()
        return kill_result[0]

    def _create_proc(self, proc_name, policy=None):
        def create_wrap(the_exe, the_run_dir, the_args, the_env, the_name, result_list):
            res = dproc.create(
                exe=the_exe, run_dir=the_run_dir, args=the_args, env=the_env, user_name=the_name, policy=policy
            )
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

    def _make_a_pool(self, fake_pool_name=""):
        def create_wrap(the_size, the_name, result_list):
            res = dpool.create(size=the_size, user_name=the_name)
            result_list.append(res)

        fake_pool_size = 2**30
        create_result = []
        create_thread = threading.Thread(target=create_wrap, args=(fake_pool_size, fake_pool_name, create_result))
        create_thread.start()
        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHPoolCreate)

        dummy_sdesc = B64.bytes_to_str("xxxTESTDUMMYxxx".encode())
        shep_reply_msg = dmsg.SHPoolCreateResponse(
            tag=self.next_tag(),
            ref=shep_msg.tag,
            err=dmsg.SHPoolCreateResponse.Errors.SUCCESS,
            desc=dummy_sdesc,
        )

        self.gs_input_wh.send(shep_reply_msg.serialize())

        create_thread.join()

        desc = create_result[0]

        self.pool_m_uid = desc.m_uid

        return desc

    def _make_a_channel(self, m_uid, fake_channel_name=""):
        def create_wrap(the_m_uid, the_name, result_list):
            res = dchannel.create(m_uid=the_m_uid, user_name=the_name)
            result_list.append(res)

        create_result = []
        create_thread = threading.Thread(target=create_wrap, args=(m_uid, fake_channel_name, create_result))
        create_thread.start()
        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHChannelCreate)
        dummy_sdesc = B64.bytes_to_str("xxxTESTDUMMYxxx".encode())
        shep_reply_msg = dmsg.SHChannelCreateResponse(
            tag=self.next_tag(),
            ref=shep_msg.tag,
            err=dmsg.SHChannelCreateResponse.Errors.SUCCESS,
            desc=dummy_sdesc,
        )

        self.gs_input_wh.send(shep_reply_msg.serialize())

        create_thread.join()

        desc = create_result[0]

        self.channel_c_uid = desc.c_uid

        return desc


class SingleProcAPIChannels(GSProcessBaseClass):
    def test_list(self):
        plist = dproc.get_list()
        self.assertEqual(plist, [self.head_puid])

    def test_query(self):
        plist = dproc.get_list()
        descr = dproc.query(plist[0])
        self.assertEqual(descr.p_uid, self.head_puid)

    def test_create(self):
        descr = self._create_proc("bob")

        self.assertEqual(descr.name, "bob")

    def test_dump_smoke(self):
        descr = self._create_proc("bob")

        self.assertEqual(descr.name, "bob")

        dump_msg = dmsg.GSDumpState(tag=self.next_tag(), filename="dump_file")
        self.gs_input_wh.send(dump_msg.serialize())

    def test_query_by_name(self):
        proc_name = "bob"
        desc = self._create_proc(proc_name)

        self.assertEqual(desc.name, proc_name)

        desc2 = dproc.query(proc_name)
        self.assertEqual(desc.p_uid, desc2.p_uid)

    def test_heritance(self):
        bobdesc = self._create_proc("bob")
        freddesc = self._create_proc("fred")

        the_children = {bobdesc.p_uid, freddesc.p_uid}

        mydesc = dproc.query(self.head_puid)

        first_live_children = mydesc.live_children

        bob_dies = dmsg.SHProcessExit(tag=self.next_tag(), p_uid=bobdesc.p_uid)
        self.gs_input_wh.send(bob_dies.serialize())

        mydesc_later = dproc.query(self.head_puid)

        self.assertEqual(the_children, first_live_children)
        self.assertEqual(mydesc_later.live_children, {freddesc.p_uid})
        self.assertEqual(bobdesc.p_p_uid, self.head_puid)

    def test_kill(self):
        proc_name = "bob"
        desc = self._create_proc(proc_name)

        self.assertEqual(desc.name, proc_name)

        result = self._kill_a_thing(proc_name)
        self.assertEqual(result, None)

    def test_join(self):
        proc_name = "bob"
        desc = self._create_proc(proc_name)
        self.assertEqual(desc.name, proc_name)
        test_exit_code = 17

        def join_wrap(identifier, result_list):
            res = dproc.join(identifier)
            result_list.append(res)

        join_result = []
        join_thread = threading.Thread(target=join_wrap, args=(proc_name, join_result))
        join_thread.start()

        sh_kill_reply = dmsg.SHProcessExit(tag=self.next_tag(), p_uid=desc.p_uid, exit_code=test_exit_code)

        self.gs_input_wh.send(sh_kill_reply.serialize())
        join_thread.join()
        self.assertEqual(join_result[0], test_exit_code)

    def test_join_timeout(self):
        proc_name = "bob"
        desc = self._create_proc(proc_name)

        self.assertEqual(desc.name, proc_name)

        start = time.monotonic()
        result = dproc.join(proc_name, timeout=1.0)
        self.assertEqual(result, None)
        duration = time.monotonic() - start
        # should time out in about a second
        self.assertGreater(duration, 1.0)

    def test_join_timeout_mult_req(self):
        proc_name = "bob"
        desc = self._create_proc(proc_name)

        self.assertEqual(desc.name, proc_name)

        # join on the same puid multiple times
        for _ in range(5):
            start = time.monotonic()
            res = dproc.join(proc_name, timeout=0.5)
            duration = time.monotonic() - start
            self.assertEqual(res, None)
            self.assertGreater(duration, 0.5)

    def test_pool_create(self):
        fake_pool_name = "swimming"
        desc = self._make_a_pool(fake_pool_name)
        self.assertEqual(desc.name, fake_pool_name)

    def test_pool_destroy(self):
        self._make_a_pool()

        def destroy_wrap(identifier, result_list):
            try:
                dpool.destroy(identifier)
            except dpool.PoolError as pe:
                result_list.append(pe)

        destroy_result = []
        destroy_thread = threading.Thread(target=destroy_wrap, args=(self.pool_m_uid, destroy_result))
        destroy_thread.start()
        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHPoolDestroy)
        shep_reply_msg = dmsg.SHPoolDestroyResponse(
            tag=self.next_tag(), ref=shep_msg.tag, err=dmsg.SHPoolDestroyResponse.Errors.SUCCESS
        )
        self.gs_input_wh.send(shep_reply_msg.serialize())
        destroy_thread.join()

        self.assertEqual(destroy_result, [])

    def test_pool_query(self):
        fake_pool_name = "swimming"
        desc = self._make_a_pool(fake_pool_name)

        desc2 = dpool.query(fake_pool_name)
        self.assertEqual(desc.m_uid, desc2.m_uid)

    def test_pool_list(self):
        fake_pool_name = "swimming"
        desc = self._make_a_pool(fake_pool_name)

        plist = dpool.get_list()

        self.assertEqual([desc.m_uid], plist)

    def test_channel_create(self):
        pool_desc = self._make_a_pool()

        the_channel_name = "my_channel"
        channel_desc = self._make_a_channel(pool_desc.m_uid, the_channel_name)
        self.assertEqual(channel_desc.name, the_channel_name)

    def test_channel_destroy(self):
        pool_desc = self._make_a_pool()

        the_channel_name = "my_channel"
        channel_desc = self._make_a_channel(pool_desc.m_uid, the_channel_name)
        self.assertEqual(channel_desc.name, the_channel_name)

        def destroy_wrap(identifier, result_list):
            try:
                dchannel.destroy(identifier)
            except dpool.PoolError as pe:
                result_list.append(pe)

        destroy_result = []
        destroy_thread = threading.Thread(target=destroy_wrap, args=(the_channel_name, destroy_result))
        destroy_thread.start()
        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHChannelDestroy)
        shep_reply_msg = dmsg.SHChannelDestroyResponse(
            tag=178343, ref=shep_msg.tag, err=dmsg.SHChannelDestroyResponse.Errors.SUCCESS
        )
        self.gs_input_wh.send(shep_reply_msg.serialize())
        destroy_thread.join()

        self.assertEqual(destroy_result, [])

    def test_channel_query(self):
        pool_desc = self._make_a_pool()

        the_channel_name = "my_channel"
        channel_desc = self._make_a_channel(pool_desc.m_uid, the_channel_name)
        self.assertEqual(channel_desc.name, the_channel_name)

        channel_desc2 = dchannel.query(the_channel_name)
        self.assertEqual(channel_desc.c_uid, channel_desc2.c_uid)

    def test_channel_list(self):
        pool_desc = self._make_a_pool()

        the_channel_name = "my_channel"
        channel_desc = self._make_a_channel(pool_desc.m_uid, the_channel_name)
        self.assertEqual(channel_desc.name, the_channel_name)

        clist = dchannel.get_list()
        self.assertEqual([channel_desc.c_uid], clist)

    def test_channel_join(self):
        pool_desc = self._make_a_pool()

        the_channel_name = "my_channel"

        def join_wrap(identifier, result_list):
            res = dchannel.join(identifier)
            result_list.append(res)

        join_result = []
        join_thread = threading.Thread(target=join_wrap, args=(the_channel_name, join_result))
        join_thread.start()

        channel_desc = self._make_a_channel(pool_desc.m_uid, the_channel_name)
        join_thread.join()
        self.assertEqual(join_result[0].name, channel_desc.name)

    def test_channel_join_timeout(self):
        the_channel_name = "intentionally_futile"

        def join_wrap(identifier, result_list):
            start = time.monotonic()
            try:
                _ = dchannel.join(identifier, 1.0)
            except TimeoutError:
                pass

            result_list.append(time.monotonic() - start)

        join_result = []
        join_thread = threading.Thread(target=join_wrap, args=(the_channel_name, join_result))
        join_thread.start()
        join_thread.join()
        self.assertGreater(join_result[0], 1.0)

    def test_multi_join(self):
        """Simple multi join test with infinite timeout.

        Join two processes, from which only one exits. The join returns and the
        result includes only the process exited.
        """

        proc_name = "first"
        desc_first = self._create_proc(proc_name)
        self.assertEqual(desc_first.name, proc_name)
        test_exit_code = 17
        proc_name = "second"
        desc_sec = self._create_proc(proc_name)
        self.assertEqual(desc_sec.name, proc_name)

        def join_wrap(identifiers, result_list):
            res = dproc.multi_join(identifiers)
            result_list += res[0]

        the_one_exiting = desc_first.p_uid
        join_result = []
        join_thread = threading.Thread(target=join_wrap, args=([desc_first.p_uid, desc_sec.p_uid], join_result))
        join_thread.start()

        sh_kill_reply = dmsg.SHProcessExit(tag=self.next_tag(), p_uid=the_one_exiting, exit_code=test_exit_code)
        self.gs_input_wh.send(sh_kill_reply.serialize())

        join_thread.join()

        # the join result should only include the process that exited
        self.assertEqual(join_result[0][1], test_exit_code)
        self.assertEqual(join_result[0][0], the_one_exiting)
        self.assertEqual(len(join_result), 1)

    def test_multi_join_alt(self):
        """Multi join with infinite timeout.

        Join in multiple processes. Each process in the list waits for some time and
        then exits. We join on the same list of processes repeatedly and the join result
        includes only those processes exited. The join returns as soon as there is at least
        one process in the list that has exited.
        """

        identifiers = []
        nprocs = 5
        test_exit_code = 17

        def join_wrap(identifiers, result_list):
            while True:
                res = dproc.multi_join(identifiers)
                if res[0] is not None:
                    if len(res[0]) == nprocs:
                        result_list += res[0]
                        break
                # Wait before joining again
                time.sleep(0.1)

        def exit_wrap(tag, p_uid, exit_code, handler, sleep_time):
            time.sleep(sleep_time)
            sh_kill_reply = dmsg.SHProcessExit(tag=tag, p_uid=p_uid, exit_code=exit_code)
            try:
                handler.send(sh_kill_reply.serialize())
            except ConnectionError as ex:
                # If it had this error, it is because things are shutting down. Ignore it.
                pass

        for i in range(nprocs):
            proc_name = "proc" + str(i)
            desc = self._create_proc(proc_name)
            self.assertEqual(desc.name, proc_name)
            identifiers.append(desc.p_uid)

            exit_thread = threading.Thread(
                target=exit_wrap, args=(self.next_tag(), desc.p_uid, test_exit_code, self.gs_input_wh, i + 1)
            )
            exit_thread.start()

        join_result = []
        join_thread = threading.Thread(target=join_wrap, args=(identifiers, join_result))
        join_thread.start()

        join_thread.join()
        self.assertEqual(len(join_result), nprocs)

    def test_multi_join_timeout(self):
        """Multi join with timeout.

        Join when using user_name and p_uid for the target processes.
        """

        proc_name = "first"
        desc_first = self._create_proc(proc_name)
        self.assertEqual(desc_first.name, proc_name)
        proc_name = "second"
        desc_sec = self._create_proc(proc_name)
        self.assertEqual(desc_sec.name, proc_name)

        start = time.monotonic()
        # join using both process name and p_uid
        result = dproc.multi_join(["first", desc_sec.p_uid], timeout=1.0)

        duration = time.monotonic() - start
        # should time out in about one second
        self.assertGreater(duration, 1.0)

        for entry in result[1].values():
            self.assertEqual(entry, [2, None])
        self.assertEqual(result[0], None)

    def test_multi_join_with_names_timeout(self):
        """Multi join with timeout.

        Join when using user_names for the target processes.
        """

        proc_name = "first"
        desc_first = self._create_proc(proc_name)
        self.assertEqual(desc_first.name, proc_name)
        proc_name = "second"
        desc_sec = self._create_proc(proc_name)
        self.assertEqual(desc_sec.name, proc_name)

        start = time.monotonic()
        # join using process names instead of p_uid
        result = dproc.multi_join(["first", "second"], timeout=1.0)

        duration = time.monotonic() - start
        # should time out in about one second
        self.assertGreater(duration, 1.0)

        for entry in result[1].values():
            self.assertEqual(entry, [2, None])
        self.assertEqual(result[0], None)

    def test_multi_join_alt_all(self):
        """Multi join with option 'all' active and infinite timeout.

        Join in multiple processes. Each process in the list waits for some time and
        then exits. We join on this list of processes. The join call returns as soon
        as every process in the list has exited.
        """

        identifiers = []
        nprocs = 5
        test_exit_code = 17

        def join_wrap(identifiers, result_list):
            res = dproc.multi_join(identifiers, join_all=True)
            if res[0] is not None:
                result_list += res[0]

        def exit_wrap(tag, p_uid, exit_code, handler, sleep_time):
            time.sleep(sleep_time)
            sh_kill_reply = dmsg.SHProcessExit(tag=tag, p_uid=p_uid, exit_code=exit_code)
            try:
                handler.send(sh_kill_reply.serialize())
            except ConnectionError as ex:
                # If it had this error, it is because things are shutting down. Ignore it.
                pass

        for i in range(nprocs):
            proc_name = "proc" + str(i)
            desc = self._create_proc(proc_name)
            self.assertEqual(desc.name, proc_name)
            identifiers.append(desc.p_uid)
            exit_thread = threading.Thread(
                target=exit_wrap, args=(self.next_tag(), desc.p_uid, test_exit_code, self.gs_input_wh, i + 1)
            )
            if i == 0:
                start = time.monotonic()
            exit_thread.start()

        join_result = []
        join_thread = threading.Thread(target=join_wrap, args=(identifiers, join_result))
        join_thread.start()
        join_thread.join()
        end = time.monotonic()

        self.assertEqual(len(join_result), nprocs)
        self.assertGreater(end - start, 5.0)

    def test_multi_join_timeout_all(self):
        """Multi join with 'all' and timeout.

        Join when using user_name and p_uid for the target processes.
        """

        proc_name = "first"
        desc_first = self._create_proc(proc_name)
        self.assertEqual(desc_first.name, proc_name)
        proc_name = "second"
        desc_sec = self._create_proc(proc_name)
        self.assertEqual(desc_sec.name, proc_name)

        start = time.monotonic()
        # join using both process name and p_uid
        result = dproc.multi_join(["first", desc_sec.p_uid], timeout=1.0, join_all=True)

        duration = time.monotonic() - start
        # should time out in about one second
        self.assertGreater(duration, 1.0)

        for entry in result[1].values():
            self.assertEqual(entry, [2, None])
        self.assertEqual(result[0], None)

    def test_multi_join_any_all(self):
        """Multi join without timeout.

        Create two different wait requests on the same puid set.
        One comes with 'any' option and the other with 'all' option.
        The multi_join with 'any' option returns when one process from
        the set exits. The multi_join with 'all' option returns when all
        the processes from the set exit.
        """

        identifiers = []
        nprocs = 5
        test_exit_code = 17

        def join_wrap(identifiers, result_list, join_all):
            res = dproc.multi_join(identifiers, join_all=join_all)
            if res[0] is not None:
                result_list += res[0]

        def exit_wrap(tag, p_uid, exit_code, handler, sleep_time):
            time.sleep(sleep_time)
            sh_kill_reply = dmsg.SHProcessExit(tag=tag, p_uid=p_uid, exit_code=exit_code)
            try:
                handler.send(sh_kill_reply.serialize())
            except ConnectionError as ex:
                # If it had this error, it is because things are shutting down. Ignore it.
                pass

        for i in range(nprocs):
            proc_name = "proc" + str(i)
            desc = self._create_proc(proc_name)
            self.assertEqual(desc.name, proc_name)
            identifiers.append(desc.p_uid)
            exit_thread = threading.Thread(
                target=exit_wrap, args=(self.next_tag(), desc.p_uid, test_exit_code, self.gs_input_wh, i + 1)
            )
            if i == 0:
                start_any = time.monotonic()  # this is for the 'any' case
            if i == 4:
                start_all = time.monotonic()  # this is for timing the 'all' case
            exit_thread.start()

        join_result = []
        join_result_all = []
        join_thread = threading.Thread(target=join_wrap, args=(identifiers, join_result, False))
        join_thread.start()

        join_thread_all = threading.Thread(target=join_wrap, args=(identifiers, join_result_all, True))
        join_thread_all.start()

        join_thread.join()
        end_any = time.monotonic()
        join_thread_all.join()
        end_all = time.monotonic()

        self.assertGreater(end_all - start_all, 5.0)
        self.assertGreater(end_any - start_any, 1.0)
        self.assertEqual(len(join_result), 1)
        self.assertEqual(len(join_result_all), nprocs)

    def test_multi_join_timeout_all_with_error(self):
        """Multi join with 'all' and without timeout.

        Ask to join on a set of puids and one of them does not exist.
        The rest exit normally. The result from multi_join should be None.
        """

        identifiers = []
        nprocs = 5
        test_exit_code = 17

        def join_wrap(identifiers, result_list, join_all):
            res = dproc.multi_join(identifiers, join_all=join_all)
            if res[0] is not None:
                result_list += res[0]
            else:
                result_list.append(res[0])  # for the errored case

        def exit_wrap(tag, p_uid, exit_code, handler, sleep_time):
            time.sleep(sleep_time)
            sh_kill_reply = dmsg.SHProcessExit(tag=tag, p_uid=p_uid, exit_code=exit_code)
            try:
                handler.send(sh_kill_reply.serialize())
            except ConnectionError as ex:
                # If it had this error, it is because things are shutting down. Ignore it.
                pass

        for i in range(nprocs):
            proc_name = "proc" + str(i)
            desc = self._create_proc(proc_name)
            self.assertEqual(desc.name, proc_name)
            identifiers.append(desc.p_uid)
            exit_thread = threading.Thread(
                target=exit_wrap, args=(self.next_tag(), desc.p_uid, test_exit_code, self.gs_input_wh, i + 1)
            )
            if i == 0:
                start = time.monotonic()
            exit_thread.start()

        join_result = []
        identifiers.append("ghost")
        join_thread = threading.Thread(target=join_wrap, args=(identifiers, join_result, True))
        join_thread.start()
        join_thread.join()
        end = time.monotonic()

        self.assertEqual(join_result, [None])
        self.assertGreater(end - start, 5.0)

    def test_multi_join_timeout_all_diff_sets(self):
        """Multi join with 'all' and without timeout.

        Create two different requests with 'all' option that wait
        on two sets. The one set is two items less than the
        other set.
        """

        identifiers1 = []
        identifiers2 = []
        nprocs = 6
        test_exit_code = 17

        def join_wrap(identifiers, result_list, join_all):
            res = dproc.multi_join(identifiers, join_all=join_all)
            if res[0] is not None:
                result_list += res[0]

        def exit_wrap(tag, p_uid, exit_code, handler, sleep_time):
            time.sleep(sleep_time)
            sh_kill_reply = dmsg.SHProcessExit(tag=tag, p_uid=p_uid, exit_code=exit_code)
            try:
                handler.send(sh_kill_reply.serialize())
            except ConnectionError as ex:
                # If it had this error, it is because things are shutting down. Ignore it.
                pass

        for i in range(nprocs):
            proc_name = "proc" + str(i)
            desc = self._create_proc(proc_name)
            self.assertEqual(desc.name, proc_name)
            identifiers1.append(desc.p_uid)
            identifiers2.append(desc.p_uid)
            exit_thread = threading.Thread(
                target=exit_wrap, args=(self.next_tag(), desc.p_uid, test_exit_code, self.gs_input_wh, i + 1)
            )
            if i == 0:
                start1 = time.monotonic()  # this is for the full set
            if i == 1:
                start2 = time.monotonic()  # this is for timing the set including fewer items
            exit_thread.start()

        join_result1 = []
        join_result2 = []

        join_thread1 = threading.Thread(target=join_wrap, args=(identifiers1, join_result1, True))
        join_thread1.start()

        # remove two items from the list of identifiers for the 2nd request to wait on
        identifiers2.pop(0)
        identifiers2.pop(-1)
        join_thread2 = threading.Thread(target=join_wrap, args=(identifiers2, join_result2, True))
        join_thread2.start()

        join_thread1.join()
        end1 = time.monotonic()
        join_thread2.join()
        end2 = time.monotonic()

        self.assertEqual(len(join_result1), nprocs)
        self.assertEqual(len(join_result2), nprocs - 2)
        self.assertGreater(end1 - start1, 6.0)
        self.assertGreater(end2 - start2, 5.0)

    def test_multi_join_all_test_new_timeout(self):
        """Multi join with option 'all' option and timeout.

        Join in multiple processes. Each process in the list waits for some time and
        then exits. We join on this list of processes. The join call returns as soon
        as every process in the list has exited, since the timeout used is larger
        than the sleep time of the processes. In this test we exercise the new entries
        to pending_join_list DS with the new timeout (keep shrinking the puid_set that
        we wait on, as a result to a process exiting).
        """

        identifiers = []
        nprocs = 3
        test_exit_code = 17

        def join_wrap(identifiers, result_list, timeout):
            res = dproc.multi_join(identifiers, timeout=timeout, join_all=True)
            if res[0] is not None:
                result_list += res[0]

        def exit_wrap(tag, p_uid, exit_code, handler, sleep_time):
            time.sleep(sleep_time)
            sh_kill_reply = dmsg.SHProcessExit(tag=tag, p_uid=p_uid, exit_code=exit_code)
            try:
                handler.send(sh_kill_reply.serialize())
            except ConnectionError as ex:
                # If it had this error, it is because things are shutting down. Ignore it.
                pass

        for i in range(nprocs):
            proc_name = "proc" + str(i)
            desc = self._create_proc(proc_name)
            self.assertEqual(desc.name, proc_name)
            identifiers.append(desc.p_uid)
            exit_thread = threading.Thread(
                target=exit_wrap, args=(self.next_tag(), desc.p_uid, test_exit_code, self.gs_input_wh, i + 1)
            )
            if i == 0:
                start = time.monotonic()
            exit_thread.start()

        join_result = []
        join_thread = threading.Thread(target=join_wrap, args=(identifiers, join_result, 4.0))
        join_thread.start()
        join_thread.join()
        end = time.monotonic()

        self.assertEqual(len(join_result), nprocs)
        self.assertGreater(end - start, 3.0)

    def test_multi_join_mult_req_on_same_key(self):
        """Multi join on the same puid set with timeout.

        Create two different wait requests on the same puid set with
        timeout. The first request times out and then we make the
        second one.
        """

        identifiers = []
        nprocs = 3

        for i in range(nprocs):
            proc_name = "proc" + str(i)
            desc = self._create_proc(proc_name)
            self.assertEqual(desc.name, proc_name)
            identifiers.append(desc.p_uid)

        # it doesn't really matter if it is 'any' or 'all' requests or any combination of those
        res1 = dproc.multi_join(identifiers, timeout=0.1, join_all=False)
        # want the first request to return/time out by the time we create the next request
        res2 = dproc.multi_join(identifiers, timeout=0.1, join_all=True)

        self.assertEqual(res1[0], None)
        self.assertEqual(res2[0], None)


if __name__ == "__main__":
    unittest.main()
