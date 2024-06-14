#!/usr/bin/env python3

import unittest
import os
import dragon
import multiprocessing as mp
from dragon.fli import FLInterface, DragonFLIError, FLIEOT
from dragon.managed_memory import MemoryPool, MemoryAlloc
from dragon.channels import Channel
from dragon.localservices.options import ChannelOptions
from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import parameters
from dragon.utils import B64
import socket

class Next:
    def __init__(self, initial_value):
        self._value = initial_value

    @property
    def next(self):
        val = self._value
        self._value += 1
        return val

def echo(fli_in, fli_out, main_host):
    host = socket.gethostname()
    # print(f'echo running on {host}', flush=True)
    # print('Made it into echo', flush=True)
    recvh = fli_in.recvh()
    sendh = fli_out.sendh()
    # print("Getting bytes in echo", flush=True)
    x, hint = recvh.recv_bytes()
    # print("Got bytes in echo", flush=True)


    try:
        # print("Getting EOT in echo", flush=True)
        recvh.recv_bytes()
        # print(f'ERROR in test: Did not get EOFError. Data was {x} and hint was {hint} in echo', flush=True)
        sendh.send_bytes(b'Error: EOF was not raised in echo', hint+1)
    except EOFError:
        # print("Got EOT in echo", flush=True)
        if host == main_host:
            sendh.send_bytes(b'The host for echo and host for main were the same and so is not testing cross node communication.', hint)
        else:
            sendh.send_bytes(x, hint)
        recvh.close()

    sendh.close()
    # print("Echo exiting", flush=True)

def nothing():
    # print(f'Doing nothing on node {socket.gethostname()}', flush=True)
    pass

def test_main(fli1, fli2, channel_host, same):
    host = socket.gethostname()
    # print(f'test_main running on {host}', flush=True)

    # For some reason, on a two node allocation, the first process started
    # does not run on a different node. If this test_main is already run on
    # a different node, then it will not need the dummy process created.
    if same:
        dummy = mp.Process(target=nothing, args=())
        dummy.start()
    proc = mp.Process(target=echo, args=(fli1, fli2, host))
    proc.start()
    # print("And here", flush=True)

    sendh = fli1.sendh()
    recvh = fli2.recvh()

    # print('Sending bytes', flush=True)
    sendh.send_bytes(b'hello', 42)
    sendh.close()
    x, hint = recvh.recv_bytes()

    try:
        # print("Getting EOT in main", flush=True)
        x, hint = recvh.recv_bytes()
        # print(f'ERROR in main: Did not get EOFError. Got {x=} and {hint=}', flush=True)
    except EOFError:
        # print("Got EOT in main", flush=True)
        recvh.close()

    proc.join()
    if same:
        dummy.join()

    sendh = fli1.sendh()

    if (same and host == channel_host) or (not same and host != channel_host):
        sendh.send_bytes(x)
        sendh.send_bytes(hint.to_bytes(8, byteorder='little'))
    else:
        msg = f'Required {same=} and {host} == {channel_host} is not {same}.'
        sendh.send_bytes(bytes(msg, 'utf-8'))
        sendh.send_bytes(b'0')

    sendh.close()

class FLITest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        mp.set_start_method('dragon')
        cls.host_name = socket.gethostname()
        # print(f'Created channels on node {cls.host_name}', flush=True)
        cls.default_muid = dfacts.default_pool_muid_from_index(parameters.this_process.index)
        cls._buffer_pool = MemoryPool.attach(B64.str_to_bytes(parameters.this_process.default_pd))

        cls.cuids = Next(dfacts.FIRST_CUID + 1000)
        cls.main_ch = Channel(cls._buffer_pool, cls.cuids.next)
        cls.mgr_ch = Channel(cls._buffer_pool, cls.cuids.next)
        cls.strm_chs = []

        for i in range(5):
            cls.strm_chs.append(Channel(cls._buffer_pool, cls.cuids.next))

        cls.main_ch2 = Channel(cls._buffer_pool, cls.cuids.next)
        cls.mgr_ch2 = Channel(cls._buffer_pool, cls.cuids.next)
        cls.strm_chs2 = []

        for i in range(5):
            cls.strm_chs2.append(Channel(cls._buffer_pool, cls.cuids.next))

        cls.fli1 = FLInterface(main_ch=cls.main_ch, manager_ch=cls.mgr_ch, stream_channels=cls.strm_chs)
        cls.fli2 = FLInterface(main_ch=cls.main_ch2, manager_ch=cls.mgr_ch2, stream_channels=cls.strm_chs2)

    @classmethod
    def tearDownClass(cls):
        cls.main_ch.destroy()
        for i in range(5):
            cls.strm_chs[i].destroy()

        cls.main_ch2.destroy()
        for i in range(5):
            cls.strm_chs2[i].destroy()

    def test_hints(self):
        test_main(self.fli1, self.fli2, self.host_name, True)
        recvh = self.fli1.recvh()
        x, _ = recvh.recv_bytes()
        hint_bytes, _ = recvh.recv_bytes()
        hint = int.from_bytes(hint_bytes, byteorder='little')
        # The following code should work with assertRaises, but does not.
        # Have no idea why.
        try:
            recvh.recv_bytes()
            self.assertTrue(False, "The recv_bytes should have raised EOFError")
        except EOFError:
            pass
        recvh.close()
        self.assertEqual(x,b'hello')
        self.assertEqual(hint, 42)
        # print('Completed test_hints test!', flush=True)

    def test_hints_reverse(self):
        # For some reason, on a two node allocation, the first process started
        # does not run on a different node. If this test_main is to be run on a
        # separate node from where the channels were created, then we must
        # start a dummy process first.
        dummy = mp.Process(target=nothing, args=())
        dummy.start()
        # print(f'HOST_NAME IS {self.host_name}')
        main_proc = mp.Process(target=test_main, args=(self.fli1, self.fli2, self.host_name, False))
        main_proc.start()
        main_proc.join()
        recvh = self.fli1.recvh()
        x, _ = recvh.recv_bytes()
        hint_bytes, _ = recvh.recv_bytes()
        hint = int.from_bytes(hint_bytes, byteorder='little')
        # The following code should work with assertRaises, but does not.
        # Have no idea why.
        try:
            recvh.recv_bytes()
            self.assertTrue(False, "The recv_bytes should have raised EOFError")
        except EOFError:
            pass

        recvh.close()
        self.assertEqual(x,b'hello')
        self.assertEqual(hint, 42)
        # print('Completed test_hints_reverse test!', flush=True)
        dummy.join()


if __name__ == '__main__':
    unittest.main()
