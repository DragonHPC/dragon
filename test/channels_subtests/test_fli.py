#!/usr/bin/env python3

import unittest
import os
import multiprocessing as mp
from dragon.fli import FLInterface, DragonFLIError, FLIEOT
from dragon.managed_memory import MemoryPool, MemoryAlloc
from dragon.channels import Channel
from dragon.localservices.options import ChannelOptions

class FLICreateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pool_name = f"pydragon_fli_test_{os.getpid()}"
        pool_size = 1073741824  # 1GB
        pool_uid = 1
        cls.mpool = MemoryPool(pool_size, pool_name, pool_uid)
        cls.main_ch = Channel(cls.mpool, 1)

    @classmethod
    def tearDownClass(cls):
        cls.mpool.destroy()

    def test_create_destroy_buffered(self):
        # Basic test to make a FLI, no manager channel
        fli = FLInterface.create_buffered(main_ch=self.main_ch, pool=self.mpool)
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
        manager_ch = Channel(self.mpool, 2, capacity=num_streams)
        # Make list of streaming channels
        streams = []
        for i in range(num_streams):
            strm = Channel(self.mpool, 3+i)
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
        r = os.fdopen(fdes, 'r')
        s = ''
        x = ' '
        while len(x) > 0:
            x = r.read()
            s += x

        r.close()

        if s != expected:
            print(f'The expected string as {expected} and received {s} instead!')
            return -1

        recvh.finalize_fd()

        recvh.close()

        return 0
    except Exception as ex:
        print(f"GOT EXCEPTION: {ex}")

def echo(fli_in, fli_out):
    sendh = fli_out.sendh()
    recvh = fli_in.recvh()

    (x, hint) = recvh.recv_bytes() # recv_bytes returns a tuple, first the bytes then the message attribute
    try:
        _ = recvh.recv_bytes()
        print('Did not get EOT as expected', flush=True )
    except EOFError:
        pass
    recvh.close()
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
        pool_uid = 1
        self.mpool = MemoryPool(pool_size, pool_name, pool_uid)
        self.main_ch = Channel(self.mpool, 1)
        self.manager_ch = Channel(self.mpool, 2)
        self.stream_chs = []
        for i in range(5):
            self.stream_chs.append(Channel(self.mpool, 3+i))

        self.fli = FLInterface(main_ch=self.main_ch, manager_ch=self.manager_ch, pool=self.mpool, stream_channels=self.stream_chs)

    def tearDown(self):
        self.fli.destroy()
        for i in range(5):
            self.stream_chs[i].destroy()
        self.mpool.destroy()

    def test_create_close_send_handle(self):
        sendh = self.fli.sendh()
        sendh.close()

    @unittest.skip("Hangs indefinitely on close")
    def test_create_close_recv_handle(self):
        recvh = self.fli.recvh()
        recvh.close()

    def test_send_recv_bytes(self):

        with self.fli.sendh() as sendh:
            b = b'Hello World'
            sendh.send_bytes(b)

        with self.fli.recvh() as recvh:
            (x, _) = recvh.recv_bytes() # recv_bytes returns a tuple, first the bytes then the message attribute
            self.assertEqual(b, x)

            with self.assertRaises(FLIEOT):
                (x, _) = recvh.recv_bytes() # We should get back an EOT here

    def test_send_recv_mem(self):
        sendh = self.fli.sendh()
        recvh = self.fli.recvh()

        mem = self.mpool.alloc(512)
        mview = mem.get_memview()
        mview[0:5] = b'Hello'

        sendh.send_mem(mem)
        sendh.close()
        (recv_mem, _) = recvh.recv_mem()

        mview2 = recv_mem.get_memview()
        self.assertEqual(b'Hello', mview2[0:5])

        with self.assertRaises(FLIEOT):
            _ = recvh.recv_mem()
            recvh.close()

    def test_send_recv_bytes_buffer(self):
        pass

    def test_send_bytes_recv_mem(self):
        sendh = self.fli.sendh()
        recvh = self.fli.recvh()

        b = b'Hello'
        sendh.send_bytes(b)
        sendh.close()
        (x, _) = recvh.recv_mem()
        mview = x.get_memview()
        self.assertEqual(b'Hello', bytes(mview[0:5]))

        with self.assertRaises(FLIEOT):
            _ - recvh.recv_mem()
            recvh.close()

    def test_send_recv_direct(self):
        stream = Channel(self.mpool, 9999)
        sendh = self.fli.sendh(stream)
        recvh = self.fli.recvh()

        b = b'Hello World'
        sendh.send_bytes(b)
        sendh.close()

        (x, _) = recvh.recv_bytes()
        self.assertEqual(b'Hello World', x)

        with self.assertRaises(FLIEOT):
            _ = recvh.recv_bytes()
            recvh.close()

        stream.destroy()

    def test_create_close_write_file(self):
        sendh = self.fli.sendh()
        fdes = sendh.create_fd()

        f = os.fdopen(fdes, 'w')
        f.write("Test")
        f.close()
        sendh.finalize_fd()
        sendh.close()

    def test_read_write_file(self):
        fli_ser = self.fli.serialize()
        test_string = 'Hello World'
        p = mp.Process(target=worker_recv_fd, args=(fli_ser, self.mpool, test_string))
        p.start()
        sendh = self.fli.sendh()
        fdes = sendh.create_fd()
        f = os.fdopen(fdes, 'w')
        f.write(test_string)
        f.close()
        sendh.finalize_fd()
        sendh.close()
        p.join()

    def test_pass_fli(self):
        main2_ch = Channel(self.mpool, 101)
        manager2_ch = Channel(self.mpool, 102)
        stream2_chs = []
        for i in range(5):
            stream2_chs.append(Channel(self.mpool, 103+i))

        fli2 = FLInterface(main_ch=main2_ch, manager_ch=manager2_ch, pool=self.mpool, stream_channels=stream2_chs)

        proc = mp.Process(target=echo, args=(self.fli, fli2))
        proc.start()
        sendh = self.fli.sendh()
        recvh = fli2.recvh()
        b = b'Hello World'
        sendh.send_bytes(b, 42)
        sendh.close()
        x, hint = recvh.recv_bytes()
        self.assertEqual(x, b)
        self.assertEqual(42, hint)
        proc.join()

if __name__ == '__main__':
    unittest.main()