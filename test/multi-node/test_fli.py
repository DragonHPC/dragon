#!/usr/bin/env python3

import unittest
import socket
import random
import os

import dragon
import multiprocessing as mp

from dragon.fli import FLInterface
from dragon.managed_memory import MemoryPool
from dragon.channels import Channel
from dragon.utils import B64

from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import parameters
from dragon.infrastructure.policy import Policy

from dragon.native.process import Process
from dragon.native.machine import System, Node


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
    recvh = fli_in.recvh()
    sendh = fli_out.sendh()
    x, hint = recvh.recv_bytes()

    try:
        recvh.recv_bytes()
        sendh.send_bytes(b"Error: EOF was not raised in echo", hint + 1)
    except EOFError:
        if host == main_host:
            sendh.send_bytes(
                b"The host for echo and host for main were the same and so is not testing cross node communication.",
                hint,
            )
        else:
            sendh.send_bytes(x, hint)
        recvh.close()

    sendh.close()


def nothing():
    # print(f'Doing nothing on node {socket.gethostname()}', flush=True)
    pass


def test_main(fli1, fli2, channel_host, same):
    host = socket.gethostname()

    # Launch on some random other node
    my_alloc = System()
    node_list = my_alloc.nodes
    hostname_list = [Node(node).hostname for node in node_list if Node(node).hostname != host]
    echo_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=random.choice(hostname_list))
    proc = Process(target=echo, args=(fli1, fli2, host), policy=echo_policy)
    proc.start()

    sendh = fli1.sendh()
    recvh = fli2.recvh()

    sendh.send_bytes(b"hello", 42)
    sendh.close()
    x, hint = recvh.recv_bytes()

    try:
        x, hint = recvh.recv_bytes()
    except EOFError:
        recvh.close()

    proc.join()
    sendh = fli1.sendh()

    if (same and host == channel_host) or (not same and host != channel_host):
        sendh.send_bytes(x)
        sendh.send_bytes(hint.to_bytes(8, byteorder="little"))
    else:
        msg = f"Required {same=} and {host} == {channel_host} is not {same}."
        sendh.send_bytes(bytes(msg, "utf-8"))
        sendh.send_bytes(b"0")

    sendh.close()


@unittest.skipIf(bool(os.getenv("DRAGON_PROXY_ENABLED", False)), "Fails in proxy mode")
class FLITest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        mp.set_start_method("dragon")
        cls.host_name = socket.gethostname()

        cls.main_ch = Channel.make_process_local()
        cls.mgr_ch = Channel.make_process_local()
        cls.strm_chs = []

        for i in range(5):
            cls.strm_chs.append(Channel.make_process_local())

        cls.main_ch2 = Channel.make_process_local()
        cls.mgr_ch2 = Channel.make_process_local()
        cls.strm_chs2 = []

        for i in range(5):
            cls.strm_chs2.append(Channel.make_process_local())

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
        # We want to make sure we run main on the same node as the channels were created on
        # and then place echo elsewhere
        test_main(self.fli1, self.fli2, self.host_name, True)
        recvh = self.fli1.recvh()
        x, _ = recvh.recv_bytes()
        hint_bytes, _ = recvh.recv_bytes()
        hint = int.from_bytes(hint_bytes, byteorder="little")
        # The following code should work with assertRaises, but does not.
        # Have no idea why.
        try:
            recvh.recv_bytes()
            self.assertTrue(False, "The recv_bytes should have raised EOFError")
        except EOFError:
            pass
        recvh.close()
        self.assertEqual(x, b"hello")
        self.assertEqual(hint, 42)

    def test_hints_reverse(self):
        # We want to run the child process on a node different from the one the channels were created on

        # Get the hostnames and place the main process on any node but the one we're
        # currently on
        my_alloc = System()
        node_list = my_alloc.nodes
        my_hostname = socket.gethostname()
        hostname_list = [Node(node).hostname for node in node_list if Node(node).hostname != my_hostname]

        # Launch on some random other node
        main_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=random.choice(hostname_list))
        main_proc = Process(target=test_main, args=(self.fli1, self.fli2, self.host_name, False), policy=main_policy)
        main_proc.start()
        main_proc.join()
        recvh = self.fli1.recvh()
        x, _ = recvh.recv_bytes()
        hint_bytes, _ = recvh.recv_bytes()
        hint = int.from_bytes(hint_bytes, byteorder="little")

        # The following code should work with assertRaises, but does not.
        # Have no idea why.
        try:
            recvh.recv_bytes()
            self.assertTrue(False, "The recv_bytes should have raised EOFError")
        except EOFError:
            pass

        recvh.close()

        self.assertEqual(x, b"hello")
        self.assertEqual(hint, 42)


if __name__ == "__main__":
    unittest.main()
