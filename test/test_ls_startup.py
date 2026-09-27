# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import shim_dragon_paths

from collections import deque
import os
import queue
from types import SimpleNamespace
import unittest
from unittest import mock

from dragon.infrastructure import messages as dmsg
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.localservices import local_svc


class StartupComplete(Exception):
    """Stop at the real startup function's handoff to the service loop."""


class LocalServicesStartupTest(unittest.TestCase):
    def setUp(self):
        self.ls_input = mock.Mock()
        self.la_input = mock.Mock()
        self.ta_input = mock.Mock()
        self.gs_input = mock.Mock()
        self.stdin = mock.Mock()
        self.stdout = mock.Mock()
        self.ta = mock.Mock(spec=local_svc.PopenProps)
        self.ta.communicate.return_value = (b"", b"transport unavailable")
        self.gs = mock.Mock()
        self.clock = mock.Mock()
        self.node = NodeDescriptor(host_name="worker", ip_addrs=["127.0.0.1"], fabric_ep_addrs=[])
        self.channel_info = dmsg.LAChannelsInfo(
            tag=2,
            nodes_desc={0: self.node, 1: self.node},
            gs_qd="Z3M=",
            num_gw_channels=0,
            fe_ext_ip_addr="127.0.0.1",
        )
        params = mock.Mock(local_ls_qd="bHM=", local_be_cd="YmU=", gs_qd="Z3M=", default_pd="ZHA=", inf_pd="aXA=")
        params.env.return_value = {}
        patches = [
            mock.patch.dict(os.environ),
            mock.patch.object(local_svc.os, "setpgid"),
            mock.patch.object(local_svc, "setup_BE_logging"),
            mock.patch.object(local_svc, "set_procname"),
            mock.patch.object(local_svc, "time", SimpleNamespace(monotonic=self.clock), create=True),
            mock.patch.object(local_svc.dparms, "this_process", params),
            mock.patch.object(local_svc.dch, "register_gateways_from_env"),
            mock.patch.object(local_svc.dch, "Channel"),
            mock.patch.object(local_svc.NodeDescriptor, "get_localservices_node_conf", return_value=self.node),
            mock.patch.object(local_svc.InfraQueue, "attach", return_value=self.gs_input),
            mock.patch.object(local_svc.dutil, "NewlineStreamWrapper"),
            mock.patch.object(local_svc, "start_transport_agent", return_value=self.ta),
            mock.patch.object(local_svc, "maybe_start_gs", return_value=self.gs),
        ]
        for patcher in patches:
            patched = patcher.start()
            self.addCleanup(patcher.stop)
            if getattr(patcher, "attribute", None) == "Channel":
                patched.return_value.serialize.return_value = b"channel"
        resource_patch = mock.patch.object(local_svc, "mk_inf_resources", side_effect=self._resources)
        resource_patch.start()
        self.addCleanup(resource_patch.stop)
        server_patch = mock.patch.object(local_svc, "LocalServer")
        self.server = server_patch.start()
        self.addCleanup(server_patch.stop)
        self.server.return_value.run.side_effect = StartupComplete

    def _resources(self, node_index):
        pools = {local_svc.dfacts.default_pool_muid_from_index(node_index): mock.sentinel.pool}
        return pools, {}, self.ls_input, self.la_input, "dGE=", self.ta_input, None

    def _run(self, messages, *, primary=False, transport_test=False, clock=(100.0, 103.0)):
        for mocked in (self.ls_input, self.la_input, self.ta_input, self.gs_input, self.ta, self.server):
            mocked.reset_mock()
        self.clock.side_effect = clock
        self.pending = deque([dmsg.BEPingLS(tag=1), self.channel_info, *messages])

        def receive(timeout=None):
            value = self.pending.popleft()
            if isinstance(value, Exception):
                raise value
            return value

        self.ls_input.get.side_effect = receive
        self.stdin.recv.return_value = dmsg.BENodeIdxLS(
            tag=0,
            node_idx=0 if primary else 1,
            host_name="worker",
            ip_addrs=["127.0.0.1"],
            primary=primary,
            logger_sdesc="bG9n",
            net_conf_key=0,
        ).serialize()
        local_svc.multinode(
            gs_args=["global-services"],
            gs_env={},
            ls_stdin=self.stdin,
            ls_stdout=self.stdout,
            transport_test_env=transport_test,
        )

    def _assert_started(self, messages, **options):
        later_message = dmsg.LSTeardown(tag=99)
        with self.assertRaises(StartupComplete):
            self._run([*messages, later_message], **options)
        self.server.return_value.run.assert_called_once()
        self.server.clean_pools.assert_not_called()
        self.assertEqual(list(self.pending), [later_message])
        self.ta.communicate.assert_not_called()
        self.assertEqual(sum(isinstance(c.args[0], dmsg.TAUp) for c in self.la_input.put.call_args_list), 1)
        if options.get("transport_test", False):
            self.gs_input.put.assert_not_called()
        else:
            self.gs_input.put.assert_called_once()
            reply = self.gs_input.put.call_args.args[0]
            self.assertIsInstance(reply, dmsg.LSPingGS)
            self.assertEqual(reply.idx, 0 if options.get("primary", False) else 1)
            self.assertEqual(reply.node_sdesc, self.node.sdesc)

    def _assert_rejected(self, messages, **options):
        with self.assertRaisesRegex(RuntimeError, "startup fatal error") as caught:
            self._run(messages, **options)
        self.server.assert_not_called()
        self.server.clean_pools.assert_called_once()
        self.gs_input.put.assert_not_called()
        return caught.exception

    def test_normal_nonprimary_order(self):
        self._assert_started([dmsg.TAPingLS(tag=3), dmsg.GSPingLS(tag=4)])
        self.assertEqual(
            self.ls_input.get.call_args_list[2:],
            [mock.call(timeout=10), mock.call(timeout=local_svc.dfacts.REMOTE_LAUNCH_TIMEOUT)],
        )

    def test_early_global_services_ping_uses_remaining_deadline(self):
        self._assert_started([dmsg.GSPingLS(tag=3), dmsg.TAPingLS(tag=4)])
        self.assertEqual(self.ls_input.get.call_args_list[2:], [mock.call(timeout=10), mock.call(timeout=7.0)])

    def test_normal_primary_order(self):
        self._assert_started([dmsg.TAPingLS(tag=3), dmsg.GSPingLS(tag=4)], primary=True)

    def test_normal_transport_test_order(self):
        self._assert_started([dmsg.TAPingLS(tag=3)], transport_test=True)

    def test_primary_rejects_early_global_services_ping(self):
        error = self._assert_rejected([dmsg.GSPingLS(tag=3), dmsg.TAPingLS(tag=4)], primary=True)
        self.assertIsInstance(error.__cause__, AssertionError)
        self.assertEqual(len(self.pending), 1)

    def test_transport_test_rejects_early_global_services_ping(self):
        error = self._assert_rejected([dmsg.GSPingLS(tag=3), dmsg.TAPingLS(tag=4)], transport_test=True)
        self.assertIsInstance(error.__cause__, AssertionError)
        self.assertEqual(len(self.pending), 1)

    def test_duplicate_global_services_ping_is_rejected(self):
        error = self._assert_rejected([dmsg.GSPingLS(tag=3), dmsg.GSPingLS(tag=4), dmsg.TAPingLS(tag=5)])
        self.assertIsInstance(error.__cause__, AssertionError)
        self.assertEqual(len(self.pending), 1)

    def test_duplicate_transport_ping_is_rejected(self):
        error = self._assert_rejected([dmsg.TAPingLS(tag=3), dmsg.TAPingLS(tag=4)])
        self.assertIsInstance(error.__cause__, AssertionError)

    def test_unexpected_message_is_rejected_before_transport_ping(self):
        for early_gs in (False, True):
            for invalid in (dmsg.BEPingLS(tag=4), dmsg.AbnormalTermination(tag=4, err_info="failure")):
                with self.subTest(early_gs=early_gs, invalid=type(invalid).__name__):
                    prefix = [dmsg.GSPingLS(tag=3)] if early_gs else []
                    error = self._assert_rejected([*prefix, invalid, dmsg.TAPingLS(tag=5)])
                    self.assertIsInstance(error.__cause__, AssertionError)
                    self.assertEqual(len(self.pending), 1)

    def _assert_transport_timeout(self, messages, **options):
        error = self._assert_rejected(messages, **options)
        self.assertIsInstance(error.__cause__, RuntimeError)
        self.assertIn("Unable to bring up Dragon transport agent: transport unavailable", str(error.__cause__))
        self.ta.communicate.assert_called_once_with()
        failures = [
            c.args[0] for c in self.la_input.put.call_args_list if isinstance(c.args[0], dmsg.AbnormalTermination)
        ]
        self.assertEqual(len(failures), 1)
        self.assertIn("transport unavailable", failures[0].err_info)

    def test_empty_initial_transport_wait_preserves_failure(self):
        self._assert_transport_timeout([queue.Empty()])
        self.assertEqual(self.ls_input.get.call_args_list[2:], [mock.call(timeout=10)])

    def test_empty_remaining_transport_wait_preserves_failure(self):
        self._assert_transport_timeout([dmsg.GSPingLS(tag=3), queue.Empty()])
        self.assertEqual(self.ls_input.get.call_args_list[2:], [mock.call(timeout=10), mock.call(timeout=7.0)])

    def test_expired_deadline_does_not_restart_transport_wait(self):
        for now in (110.0, 111.0):
            with self.subTest(now=now):
                self._assert_transport_timeout([dmsg.GSPingLS(tag=3), dmsg.TAPingLS(tag=4)], clock=(100.0, now))
                self.assertEqual(self.ls_input.get.call_args_list[2:], [mock.call(timeout=10)])
                self.assertEqual(len(self.pending), 1)


if __name__ == "__main__":
    unittest.main()
