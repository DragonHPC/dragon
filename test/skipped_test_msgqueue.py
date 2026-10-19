#!/usr/bin/env python3


"""Test script for PMsgQueue class"""

import unittest
import dragon.infrastructure.messages as dmsg
import dragon.pmsgqueue as pq


class PMsgQueueUnitTest(unittest.TestCase):
    def test_create_send_recv(self):
        sendh = pq.PMsgQueue("/test", write_intent=True)
        recvh = pq.PMsgQueue("/test", read_intent=True)

        orig = dmsg.SHPingBE(tag=0, shep_cd="", be_cd="", gs_cd="", default_pd="", inf_pd="")
        sendh.send(orig.serialize())

        msg = recvh.recv()
        msg = dmsg.parse(msg)

        recvh.close()
        sendh.close(destroy=True)

        self.assertEqual(repr(msg), repr(orig))

    def test_bad_create(self):
        with self.assertRaises(pq.PMsgQueueOpenException):
            pq.PMsgQueue("test")

    def test_bad_create2(self):
        with self.assertRaises(pq.PMsgQueueConfigException):
            pq.PMsgQueue("test", write_intent=True)

    def test_bad_create3(self):
        with self.assertRaises(pq.PMsgQueueConfigException):
            pq.PMsgQueue("test", read_intent=True)

    def test_recv_send(self):
        recvh = pq.PMsgQueue("/test", read_intent=True)
        sendh = pq.PMsgQueue("/test", write_intent=True)

        orig = dmsg.SHPingBE(tag=0, shep_cd="", be_cd="", gs_cd="", default_pd="", inf_pd="")
        sendh.send(orig.serialize())

        msg = recvh.recv()
        msg = dmsg.parse(msg)

        sendh.close()
        recvh.close(destroy=True)

        self.assertEqual(repr(msg), repr(orig))

    def test_exceed_msg_size(self):
        sendh = pq.PMsgQueue("/test", write_intent=True)
        recvh = pq.PMsgQueue("/test", read_intent=True)

        orig = "msg" * 10000
        with self.assertRaises(pq.PMsgQueueSendException):
            sendh.send(orig)

        recvh.close()
        sendh.close(destroy=True)

    def test_send_on_read_handle(self):
        sendh = pq.PMsgQueue("/test", write_intent=True)
        recvh = pq.PMsgQueue("/test", read_intent=True)

        orig = "msg" * 10000
        with self.assertRaises(pq.PMsgQueueSendException):
            recvh.send(orig)

        recvh.close()
        sendh.close(destroy=True)

    def test_recv_on_send_handle(self):
        sendh = pq.PMsgQueue("/test", write_intent=True)
        recvh = pq.PMsgQueue("/test", read_intent=True)

        with self.assertRaises(pq.PMsgQueueReceiveException):
            sendh.recv()

        recvh.close()
        sendh.close(destroy=True)

    def test_send_on_closed_handle(self):
        sendh = pq.PMsgQueue("/test", write_intent=True)
        sendh.close()

        orig = dmsg.SHPingBE(tag=0, shep_cd="", be_cd="", gs_cd="", default_pd="", inf_pd="").serialize()

        with self.assertRaises(pq.PMsgQueueSendException):
            sendh.send(orig)

    def test_recv_on_closed_handle(self):
        recvh = pq.PMsgQueue("/test", read_intent=True)
        recvh.close()

        with self.assertRaises(pq.PMsgQueueReceiveException):
            recvh.recv()

    def test_timeout(self):
        recvh = pq.PMsgQueue("/test", read_intent=True)

        with self.assertRaises(pq.PMsgQueueTimeoutException):
            recvh.recv(timeout=1)

        recvh.close(destroy=True)


if __name__ == "__main__":
    unittest.main()
