"""Wire round-trip tests for external attach and detach messages."""

import json
import os
import sys
import unittest
import zlib

_repo_src = os.path.join(os.path.dirname(__file__), "..", "..", "src")
if _repo_src not in sys.path:
    sys.path.insert(0, _repo_src)

import dragon.infrastructure.messages as dmsg
import dragon.utils as du


class MessageRoundTripTest(unittest.TestCase):
    def assert_round_trip(self, message, message_type, fields):
        restored = message_type.from_sdict(message.get_sdict())
        parsed = dmsg.parse(message.serialize())

        self.assertIsInstance(parsed, message_type)
        for field in fields:
            self.assertEqual(getattr(restored, field), getattr(message, field))
            self.assertEqual(getattr(parsed, field), getattr(message, field))


class TestLSExternalRegister(MessageRoundTripTest):
    def test_round_trip(self):
        message = dmsg.LSExternalRegister(
            tag=2001,
            auth_token="secret-token-abc",
            pid=9876,
            proc_resp_sdesc="base64replyqueue==",
            runtime_incarnation="runtime-123",
            attempt_id="attempt-456",
        )

        self.assert_round_trip(
            message,
            dmsg.LSExternalRegister,
            (
                "tag",
                "auth_token",
                "pid",
                "proc_resp_sdesc",
                "runtime_incarnation",
                "attempt_id",
            ),
        )
        self.assertEqual(message.tc, dmsg.MessageTypes.LS_EXTERNAL_REGISTER)


class TestLSExternalResourcesReady(MessageRoundTripTest):
    def test_round_trip(self):
        message = dmsg.LSExternalResourcesReady(
            tag=3001,
            ref=2001,
            err=dmsg.LSExternalResourcesReady.Errors.SUCCESS,
            runtime_incarnation="runtime-123",
            attempt_id="attempt-456",
            node_index=2,
            p_uid=54321,
            ls_return_qd="base64lsreturn==",
            gs_return_qd="base64gsreturn==",
            num_gw_channels_per_node=2,
            gw_env={"DRAGON_GW1": "base64gw1==", "DRAGON_GW2": "base64gw2=="},
            default_pd="base64default==",
            inf_pd="base64infra==",
        )

        self.assert_round_trip(
            message,
            dmsg.LSExternalResourcesReady,
            (
                "tag",
                "ref",
                "err",
                "runtime_incarnation",
                "attempt_id",
                "node_index",
                "p_uid",
                "ls_return_qd",
                "gs_return_qd",
                "num_gw_channels_per_node",
                "gw_env",
                "default_pd",
                "inf_pd",
            ),
        )
        self.assertEqual(message.tc, dmsg.MessageTypes.LS_EXTERNAL_RESOURCES_READY)

    def test_error_round_trip(self):
        message = dmsg.LSExternalResourcesReady(
            tag=3002,
            ref=2001,
            err=dmsg.LSExternalResourcesReady.Errors.FAIL,
            errInfo="provisioning failed",
            runtime_incarnation="runtime-123",
            attempt_id="attempt-456",
            node_index=2,
        )

        parsed = dmsg.parse(message.serialize())
        self.assertEqual(parsed.err, message.err)
        self.assertEqual(parsed.errInfo, message.errInfo)


class TestGSExternalRegisterPrepared(MessageRoundTripTest):
    def test_round_trip(self):
        message = dmsg.GSExternalRegisterPrepared(
            tag=4001,
            node_index=2,
            p_uid=54321,
            pid=9876,
            process_start_time="1234.5",
            user_name="external.54321",
            gs_return_cuid=76543,
            gs_return_muid=42,
            gs_return_cd="base64channel==",
            gs_return_qd="base64queue==",
        )

        self.assert_round_trip(
            message,
            dmsg.GSExternalRegisterPrepared,
            (
                "tag",
                "node_index",
                "p_uid",
                "pid",
                "process_start_time",
                "user_name",
                "gs_return_cuid",
                "gs_return_muid",
                "gs_return_cd",
                "gs_return_qd",
            ),
        )
        self.assertEqual(message.tc, dmsg.MessageTypes.GS_EXTERNAL_REGISTER_PREPARED)


class TestGSExternalRegisterComplete(MessageRoundTripTest):
    def test_round_trip(self):
        message = dmsg.GSExternalRegisterComplete(
            tag=5001,
            ref=4001,
            err=dmsg.GSExternalRegisterComplete.Errors.SUCCESS,
            p_uid=54321,
        )

        self.assert_round_trip(
            message,
            dmsg.GSExternalRegisterComplete,
            ("tag", "ref", "err", "errInfo", "p_uid"),
        )
        self.assertEqual(message.tc, dmsg.MessageTypes.GS_EXTERNAL_REGISTER_COMPLETE)


class TestExternalRegisterCancel(MessageRoundTripTest):
    def test_client_cancel_round_trip(self):
        message = dmsg.LSExternalRegisterCancel(
            tag=5501,
            runtime_incarnation="runtime-123",
            attempt_id="attempt-456",
            p_uid=54321,
        )
        self.assert_round_trip(
            message,
            dmsg.LSExternalRegisterCancel,
            ("tag", "runtime_incarnation", "attempt_id", "p_uid"),
        )

    def test_internal_cancel_round_trip(self):
        message = dmsg.GSExternalRegisterCancel(
            tag=5502,
            node_index=2,
            p_uid=54321,
            attempt_id="attempt-456",
            pid=9876,
            process_start_time="1234.5",
            gs_return_cuid=76543,
            gs_return_muid=42,
            gs_return_cd="base64channel==",
            gs_return_qd="base64queue==",
        )
        self.assert_round_trip(
            message,
            dmsg.GSExternalRegisterCancel,
            (
                "tag",
                "node_index",
                "p_uid",
                "attempt_id",
                "pid",
                "process_start_time",
                "gs_return_cuid",
                "gs_return_muid",
                "gs_return_cd",
                "gs_return_qd",
            ),
        )


class TestGSProcessDetach(MessageRoundTripTest):
    def test_request_round_trip(self):
        message = dmsg.GSProcessDetach(tag=6001, p_uid=54321, r_c_uid=76543)
        self.assert_round_trip(message, dmsg.GSProcessDetach, ("tag", "p_uid", "r_c_uid"))

    def test_response_round_trip(self):
        message = dmsg.GSProcessDetachResponse(
            tag=6002,
            ref=6001,
            err=dmsg.GSProcessDetachResponse.Errors.SUCCESS,
        )
        self.assert_round_trip(message, dmsg.GSProcessDetachResponse, ("tag", "ref", "err", "errInfo"))

    def test_ack_round_trip(self):
        message = dmsg.GSProcessDetachAck(tag=6003, p_uid=54321, detach_ref=6001)
        self.assert_round_trip(message, dmsg.GSProcessDetachAck, ("tag", "p_uid", "detach_ref"))
        self.assertEqual(message.tc, dmsg.MessageTypes.GS_PROCESS_DETACH_ACK)


class TestAttachableRuntimeDescr(MessageRoundTripTest):
    FIELDS = ("tag", "node_index", "gs_qd", "ls_qd", "default_pd", "inf_pd", "rt_uid")

    def make_descr(self):
        return dmsg.AttachableRuntimeDescr(
            tag=7001,
            node_index=3,
            gs_qd="base64gsinput==",
            ls_qd="base64lsinput==",
            default_pd="base64default==",
            inf_pd="base64infra==",
            rt_uid="123456789",
        )

    def test_round_trip(self):
        message = self.make_descr()
        self.assert_round_trip(message, dmsg.AttachableRuntimeDescr, self.FIELDS)
        self.assertEqual(message.tc, dmsg.MessageTypes.ATTACHABLE_RUNTIME_DESCR)

    def test_base64_file_encoding_round_trip(self):
        message = self.make_descr()
        parsed = dmsg.parse(du.b64decode(du.b64encode(message.serialize())))

        self.assertIsInstance(parsed, dmsg.AttachableRuntimeDescr)
        for field in self.FIELDS:
            self.assertEqual(getattr(parsed, field), getattr(message, field))


class TestMessageTypeOrdering(unittest.TestCase):
    def test_existing_values_are_unchanged(self):
        self.assertEqual(dmsg.MessageTypes.LS_PROCESS_DETACH_RESPONSE.value, 271)

    def test_new_values_are_appended(self):
        self.assertEqual(dmsg.MessageTypes.LS_EXTERNAL_RESOURCES_READY.value, 276)
        self.assertEqual(dmsg.MessageTypes.GS_EXTERNAL_REGISTER_PREPARED.value, 277)
        self.assertEqual(dmsg.MessageTypes.GS_EXTERNAL_REGISTER_COMPLETE.value, 278)
        self.assertEqual(dmsg.MessageTypes.GS_PROCESS_DETACH_ACK.value, 279)
        self.assertEqual(dmsg.MessageTypes.LS_EXTERNAL_REGISTER_CANCEL.value, 280)
        self.assertEqual(dmsg.MessageTypes.GS_EXTERNAL_REGISTER_CANCEL.value, 281)
        self.assertEqual(dmsg.MessageTypes.ATTACHABLE_RUNTIME_DESCR.value, 282)


class TestParseUnknownTc(unittest.TestCase):
    def test_unknown_tc_raises(self):
        payload = zlib.compress(json.dumps({"_tc": 999999, "tag": 1}).encode("utf-8"))
        with self.assertRaises(TypeError):
            dmsg.parse(payload)


if __name__ == "__main__":
    unittest.main()
