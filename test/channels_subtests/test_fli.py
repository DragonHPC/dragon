#!/usr/bin/env python3

import unittest
import os
import multiprocessing as mp
from dragon.fli import FLInterface, DragonFLIError, DragonFLIEOT
from dragon.managed_memory import MemoryPool, MemoryAlloc
from dragon.channels import Channel
from dragon.localservices.options import ChannelOptions
import dragon.infrastructure.facts as dfacts

# cuids/muids below FIRST_CUID/FIRST_MUID are reserved for infrastructure objects
# (e.g. cuid 2 is the Global Services input channel), so user objects must not reuse them.
BASE_CUID = dfacts.FIRST_CUID + 15000
BASE_MUID = dfacts.FIRST_MUID + 1000


class FLICreateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pool_name = f"pydragon_fli_test_{os.getpid()}"
        pool_size = 1073741824  # 1GB
        pool_uid = BASE_MUID + 1
        cls.mpool = MemoryPool(pool_size, pool_name, pool_uid)
        cls.main_ch = Channel(cls.mpool, BASE_CUID + 1)

    @classmethod
    def tearDownClass(cls):
        cls.mpool.destroy()

    def test_create_destroy_buffered(self):
        # Basic test to make a FLI, no manager channel
        fli = FLInterface.create_buffered(main_ch=self.main_ch, pool=self.mpool)
        fli.destroy()

    def test_send_recv_mem_buffered(self):
        fli = FLInterface.create_buffered(main_ch=self.main_ch, pool=self.mpool)
        payload = b"buffered managed memory"
        hint = 42
        mem = self.mpool.alloc(len(payload))
        mem.get_memview()[:] = payload

        with fli.sendh() as sendh:
            sendh.send_mem(mem, arg=hint)

        with fli.recvh() as recvh:
            received_mem, received_hint = recvh.recv_mem()
            try:
                self.assertEqual(received_mem.get_memview().tobytes(), payload)
                self.assertEqual(received_hint, hint)
            finally:
                received_mem.free()

        fli.destroy()

    def test_send_mem_buffered_cannot_mix_byte_writes(self):
        fli = FLInterface.create_buffered(main_ch=self.main_ch, pool=self.mpool)
        payload = b"buffered managed memory"
        mem = self.mpool.alloc(len(payload))
        mem.get_memview()[:] = payload

        with fli.sendh() as sendh:
            sendh.send_mem(mem)
            with self.assertRaises(DragonFLIError):
                sendh.send_bytes(b"another message", buffer=True)

        with fli.recvh() as recvh:
            received_mem, _ = recvh.recv_mem()
            try:
                self.assertEqual(received_mem.get_memview().tobytes(), payload)
            finally:
                received_mem.free()

        fli.destroy()

    @unittest.skip("Needs a global default pool available")
    def test_create_destroy_buffered_no_pool(self):
        # Test class method to more easily make a simple buffered FLI
        fli = FLInterface.create_buffered(main_ch=self.main_ch)
        fli.destroy()

    def test_create_destroy_streaming(self):
        # Basic test to make a full FLI including manager and streaming channels
        num_streams = 5
        # Make manager channel
        manager_ch = Channel(self.mpool, BASE_CUID + 2, capacity=num_streams)
        # Make list of streaming channels
        streams = []
        for i in range(num_streams):
            strm = Channel(self.mpool, BASE_CUID + 3 + i)
            streams.append(strm)

        fli = FLInterface(main_ch=self.main_ch, manager_ch=manager_ch, pool=self.mpool, stream_channels=streams)

        self.assertEqual(fli.num_available_streams(), 5)

        fli.destroy()

        # Clean up excess channels
        manager_ch.destroy()
        for i in range(num_streams):
            streams[i].destroy()

    def test_create_serialize_attach(self):
        pass

    def test_create_serialize_attach_detach(self):
        pass


def worker_recv_fd(fli_serial, fli_pool, expected):
    try:
        fli = FLInterface.attach(fli_serial, fli_pool)
        recvh = fli.recvh()
        fdes = recvh.create_fd()
        r = os.fdopen(fdes, "r")
        s = ""
        x = " "
        while len(x) > 0:
            x = r.read()
            s += x

        r.close()

        if s != expected:
            print(f"The expected string as {expected} and received {s} instead!")
            return -1

        recvh.finalize_fd()

        recvh.close()

        return 0
    except Exception as ex:
        print(f"GOT EXCEPTION: {ex}")


def echo(fli_in, fli_out):
    # Opening a receive handle blocks until the sender deposits its stream channel into
    # the main channel, which only happens on the first sends or on send handle close.
    recvh = fli_in.recvh()

    (x, hint) = recvh.recv_bytes()  # recv_bytes returns a tuple, first the bytes then the message attribute
    try:
        _ = recvh.recv_bytes()
        print("Did not get EOT as expected", flush=True)
    except EOFError:
        pass
    recvh.close()

    sendh = fli_out.sendh()
    sendh.send_bytes(x, hint)
    sendh.close()


class FLISendRecvTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pool_name = f"pydragon_fli_test_{os.getpid()}"
        pool_size = 1073741824  # 1GB
        pool_uid = BASE_MUID + 1
        self.mpool = MemoryPool(pool_size, pool_name, pool_uid)
        self.main_ch = Channel(self.mpool, BASE_CUID + 1)
        self.manager_ch = Channel(self.mpool, BASE_CUID + 2)
        self.stream_chs = []
        for i in range(5):
            self.stream_chs.append(Channel(self.mpool, BASE_CUID + 3 + i))

        self.fli = FLInterface(
            main_ch=self.main_ch, manager_ch=self.manager_ch, pool=self.mpool, stream_channels=self.stream_chs
        )

        pool2_name = f"pydragon_fli_test2_{os.getpid()}"
        pool2_size = 1073741824  # 1GB
        pool2_uid = BASE_MUID + 2
        self.mpool2 = MemoryPool(pool2_size, pool2_name, pool2_uid)

    def tearDown(self):
        self.fli.destroy()
        for i in range(5):
            self.stream_chs[i].destroy()
        self.mpool.destroy()
        self.mpool2.destroy()

    def test_create_close_send_handle(self):
        sendh = self.fli.sendh()
        sendh.close()

    def test_zero_byte_recv(self):

        with self.fli.sendh() as sendh:
            b = b"Hello World"
            sendh.send_bytes(b)

        with self.fli.recvh(destination_pool=self.mpool2) as recvh:
            (x, _) = recvh.recv_mem()  # recv_bytes returns a tuple, first the bytes then the message attribute
            self.assertEqual(b, x.get_memview().tobytes())
            self.assertEqual(x.pool.muid, BASE_MUID + 2)

            with self.assertRaises(DragonFLIEOT):
                (x, _) = recvh.recv_bytes()  # We should get back an EOT here

    @unittest.skip("AICI-1537")
    def test_zero_byte_send(self):
        with self.fli.sendh(destination_pool=self.mpool2) as sendh:
            b = b"Hello World"
            sendh.send_bytes(b)

        with self.fli.recvh() as recvh:
            (x, _) = recvh.recv_mem()  # recv_bytes returns a tuple, first the bytes then the message attribute
            self.assertEqual(b, x.get_memview().tobytes())
            self.assertEqual(x.pool.muid, BASE_MUID + 2)

            with self.assertRaises(DragonFLIEOT):
                (x, _) = recvh.recv_bytes()  # We should get back an EOT here

    def test_send_recv_bytes(self):

        with self.fli.sendh() as sendh:
            b = b"Hello World"
            sendh.send_bytes(b)

        with self.fli.recvh() as recvh:
            (x, _) = recvh.recv_bytes()  # recv_bytes returns a tuple, first the bytes then the message attribute
            self.assertEqual(b, x)

            with self.assertRaises(DragonFLIEOT):
                (x, _) = recvh.recv_bytes()  # We should get back an EOT here

    def test_send_recv_mem(self):
        sendh = self.fli.sendh()


        mem = self.mpool.alloc(512)
        mview = mem.get_memview()
        mview[0:5] = b"Hello"

        sendh.send_mem(mem)
        sendh.close()
        recvh = self.fli.recvh()
        (recv_mem, _) = recvh.recv_mem()

        mview2 = recv_mem.get_memview()
        self.assertEqual(b"Hello", mview2[0:5])

        with self.assertRaises(DragonFLIEOT):
            _ = recvh.recv_mem()
            recvh.close()

    def test_recv_mem_empty_nowait(self):
        with self.assertRaises(DragonFLIEOT):
            with self.fli.recvh(timeout=0) as recvh:
                    recvh.recv_mem(timeout=0)

    def test_send_recv_bytes_buffer(self):
        pass

    def test_send_bytes_recv_mem(self):
        sendh = self.fli.sendh()

        b = b"Hello"
        sendh.send_bytes(b)
        sendh.close()
        recvh = self.fli.recvh()
        (x, _) = recvh.recv_mem()
        mview = x.get_memview()
        self.assertEqual(b"Hello", bytes(mview[0:5]))

        with self.assertRaises(DragonFLIEOT):
            _ - recvh.recv_mem()
            recvh.close()

    def test_send_recv_direct(self):
        stream = Channel(self.mpool, BASE_CUID + 9999)
        sendh = self.fli.sendh(stream_channel=stream)

        b = b"Hello World"
        sendh.send_bytes(b)
        sendh.close()

        recvh = self.fli.recvh()
        (x, _) = recvh.recv_bytes()
        self.assertEqual(b"Hello World", x)

        with self.assertRaises(DragonFLIEOT):
            _ = recvh.recv_bytes()
            recvh.close()

        stream.destroy()

    def test_create_close_write_file(self):
        sendh = self.fli.sendh()
        fdes = sendh.create_fd()

        f = os.fdopen(fdes, "w")
        f.write("Test")
        f.close()
        sendh.finalize_fd()
        sendh.close()

    def test_read_write_file(self):
        fli_ser = self.fli.serialize()
        test_string = "Hello World"
        p = mp.Process(target=worker_recv_fd, args=(fli_ser, self.mpool, test_string))
        p.start()
        sendh = self.fli.sendh()
        fdes = sendh.create_fd()
        f = os.fdopen(fdes, "w")
        f.write(test_string)
        f.close()
        sendh.finalize_fd()
        sendh.close()
        p.join()

    def test_pass_fli(self):
        main2_ch = Channel(self.mpool, BASE_CUID + 101)
        manager2_ch = Channel(self.mpool, BASE_CUID + 102)
        stream2_chs = []
        for i in range(5):
            stream2_chs.append(Channel(self.mpool, BASE_CUID + 103 + i))

        fli2 = FLInterface(main_ch=main2_ch, manager_ch=manager2_ch, pool=self.mpool, stream_channels=stream2_chs)

        proc = mp.Process(target=echo, args=(self.fli, fli2))
        proc.start()

        sendh = self.fli.sendh()
        b = b"Hello World"
        sendh.send_bytes(b, 42)
        sendh.close()

        recvh = fli2.recvh()
        x, hint = recvh.recv_bytes()
        recvh.close()
        self.assertEqual(x, b)
        self.assertEqual(42, hint)
        proc.join()


if __name__ == "__main__":
    unittest.main()
