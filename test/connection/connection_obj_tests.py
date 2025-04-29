#!/usr/bin/env python3

"""Test script for basic Connection object"""
import os
import pickle
import random
import subprocess
import threading
import unittest
import multiprocessing

import dragon.channels as dch
import dragon.managed_memory as dmm

import dragon.infrastructure.connection as dconn


class ConnectionTest(unittest.TestCase):
    def setUp(self):
        self.pool_name = "conn_test_" + os.environ.get("USER", str(os.getuid()))
        self.pool_size = 2**28
        self.pool_prealloc_blocks = None
        self.pool_uid = 17
        self.mpool = dmm.MemoryPool(self.pool_size, self.pool_name, self.pool_uid, self.pool_prealloc_blocks)
        self.mpool_ser = self.mpool.serialize()

        self.first_channel_uid = 42
        self.first_chan = dch.Channel(self.mpool, self.first_channel_uid)
        self.first_chan_ser = self.first_chan.serialize()

        self.second_channel_uid = 43
        self.second_chan = dch.Channel(self.mpool, self.second_channel_uid)
        self.second_chan_ser = self.second_chan.serialize()

    def tearDown(self) -> None:
        self.second_chan.destroy()
        self.first_chan.destroy()
        self.mpool.destroy()

    def test_nothing(self):
        pass

    def pass_an_obj(self, obj):
        writer = dconn.Connection(outbound_initializer=self.second_chan)
        reader = dconn.Connection(inbound_initializer=self.second_chan)

        def send_obj():
            writer.send(obj)

        st = threading.Thread(target=send_obj)
        st.start()

        rec_obj = reader.recv()

        st.join()

        self.assertEqual(obj, rec_obj)

    def pass_some_bytes(self, some_bytes):
        writer = dconn.Connection(outbound_initializer=self.second_chan)
        reader = dconn.Connection(inbound_initializer=self.second_chan)

        def send_some_bytes():
            writer.send_bytes(some_bytes)

        st = threading.Thread(target=send_some_bytes)
        st.start()
        rec_bytes = reader.recv_bytes()
        st.join()
        self.assertEqual(some_bytes, rec_bytes)

    def test_bytes(self):
        self.pass_some_bytes(random.randbytes(17))

        for lg_size in range(25):
            self.pass_some_bytes(random.randbytes(2**lg_size))

    def test_objs(self):
        self.pass_an_obj("hyena")
        self.pass_an_obj({"hyena": "hyena"})

        self.pass_an_obj(bytes(100000))

        self.pass_an_obj(bytes(200000))

        for lg_size in range(25):
            self.pass_an_obj(bytearray(2**lg_size))

        for lg_size in range(25):
            self.pass_an_obj(random.randbytes(2**lg_size))

    def test_recv_bytes_from_send(self):
        # required for multiprocessing.Connection unit tests...
        writer = dconn.Connection(outbound_initializer=self.second_chan)
        reader = dconn.Connection(inbound_initializer=self.second_chan)

        my_obj = {"hyenas": "are awesome"}
        writer.send(my_obj)
        # would like this to raise, but....
        ser_obj = reader.recv_bytes()
        rec_obj = pickle.loads(ser_obj)
        self.assertEqual(my_obj, rec_obj)

    def test_recv_from_send_bytes(self):
        # again, weird usage but multiprocessing.Connection unit tests need
        # this functionality

        writer = dconn.Connection(outbound_initializer=self.second_chan)
        reader = dconn.Connection(inbound_initializer=self.second_chan)

        my_obj = {"hyenas": "are awesome"}
        ser_obj = pickle.dumps(my_obj)
        writer.send_bytes(ser_obj)
        rec_obj = reader.recv()
        self.assertEqual(my_obj, rec_obj)

    def origin(self, write_options={}):
        some_bytes = random.randbytes(2**24)

        try:
            writer = dch.Peer2PeerWritingChannelFile(self.first_chan, options=write_options)
            writer.open()
            writer.write_raw_header(len(some_bytes))
            writer.write(some_bytes)
        finally:
            writer.close()

        try:
            reader = dch.Peer2PeerReadingChannelFile(self.second_chan)
            reader.open()
            send_type, msg_len = reader.check_header()
            self.assertEqual(send_type, dch.ChannelAdapterMsgTypes.RAW_BYTES)
            self.assertEqual(msg_len, len(some_bytes))
            reader.advance_raw_header(msg_len)
            buf = bytearray(msg_len)
            reader.readinto(memoryview(buf))
            self.assertEqual(some_bytes, buf)
        finally:
            reader.close()

    def bounce(self, write_options={}):
        buf = bytearray(1)
        try:
            reader = dch.Peer2PeerReadingChannelFile(self.first_chan)
            reader.open()
            send_type, msg_len = reader.check_header()
            self.assertEqual(send_type, dch.ChannelAdapterMsgTypes.RAW_BYTES)
            reader.advance_raw_header(msg_len)
            buf = bytearray(msg_len)
            reader.readinto(memoryview(buf))
        finally:
            reader.close()

        try:
            writer = dch.Peer2PeerWritingChannelFile(self.second_chan, options=write_options)
            writer.open()
            writer.write_raw_header(len(buf))
            writer.write(buf)
        finally:
            writer.close()

    def test_channel_adapters_basic(self):

        bounce_th = threading.Thread(target=self.bounce)
        bounce_th.start()
        self.origin()
        bounce_th.join()

    def test_channel_adapters_tuned(self):

        options = {
            "write_options": {
                "small_blk_size": 2**8,
                "large_blk_size": 2**12,
                "huge_blk_size": 2**24,
                "buffer_pool": self.mpool,
            }
        }
        bounce_th = threading.Thread(target=self.bounce, args=(options))
        bounce_th.start()
        self.origin()
        bounce_th.join()

        broken_options = {"small_blk_size": 2**8, "large_blk_size": 2**24, "huge_blk_size": 2**12}
        with self.assertRaises(ValueError):
            dch.Peer2PeerWritingChannelFile(self.first_chan, options=broken_options)

        empty_pool = dmm.MemoryPool.empty_pool()
        broken_options = {"buffer_pool": empty_pool}
        with self.assertRaises(dch.ChannelError):
            dch.Peer2PeerWritingChannelFile(self.first_chan, options=broken_options)

    def test_many2many_basic(self):
        some_bytes = random.randbytes(2**24)

        try:
            writer = dch.Many2ManyWritingChannelFile(self.first_chan)
            writer.open()
            pickle.dump(some_bytes, file=writer, protocol=5)
        finally:
            writer.close()

        try:
            reader = dch.Many2ManyReadingChannelFile(self.first_chan)
            reader.open()
            buf = pickle.load(reader)
            self.assertEqual(some_bytes, buf)
        finally:
            reader.close()

    def _many2many_origin(self):
        some_bytes = random.randbytes(2**24)

        try:
            writer = dch.Many2ManyWritingChannelFile(self.first_chan)
            writer.open()
            pickle.dump(some_bytes, file=writer, protocol=5)
        finally:
            writer.close()

        try:
            reader = dch.Many2ManyReadingChannelFile(self.second_chan)
            reader.open()
            buf = pickle.load(reader)
            self.assertEqual(some_bytes, buf)
        finally:
            reader.close()

    def _many2many_bounce(self):
        try:
            reader = dch.Many2ManyReadingChannelFile(self.first_chan)
            reader.open()
            buf = pickle.load(reader)
        finally:
            reader.close()

        try:
            writer = dch.Many2ManyWritingChannelFile(self.second_chan)
            writer.open()
            pickle.dump(buf, file=writer, protocol=5)
        finally:
            writer.close()

    def test_many2many_p2p(self):
        th = threading.Thread(target=self._many2many_bounce)
        th.start()
        msg = self._many2many_origin()
        th.join()


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    unittest.main()
