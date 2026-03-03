#!/usr/bin/env python3

import unittest
import os
import dragon
import sys
import multiprocessing as mp
from dragon.fli import FLInterface, DragonFLIError, DragonFLIEOT
from dragon.managed_memory import MemoryPool, MemoryAlloc
from dragon.channels import Channel
from dragon.globalservices import channel
from dragon.localservices.options import ChannelOptions
from dragon.native.process import Popen
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.facts as facts
import dragon.infrastructure.parameters as parameters
import dragon.utils as du


class FLISendRecvTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        mp.set_start_method("dragon")

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self._default_muid = facts.default_pool_muid_from_index(parameters.this_process.index)

        cdesc = channel.create(self._default_muid)
        self.main_ch = Channel.attach(cdesc.sdesc)

        cdesc = channel.create(self._default_muid)
        self.manager_ch = Channel.attach(cdesc.sdesc)

        self.stream_chs = []
        for i in range(5):
            cdesc = channel.create(self._default_muid)
            strm_ch = Channel.attach(cdesc.sdesc)
            self.stream_chs.append(strm_ch)

        self.fli = FLInterface(main_ch=self.main_ch, manager_ch=self.manager_ch, stream_channels=self.stream_chs)

    def tearDown(self):
        self.fli.destroy()
        for i in range(5):
            channel.destroy(self.stream_chs[i].cuid)

    def test_receive_from_python(self):
        fli_ser = self.fli.serialize()
        b64fli_ser = du.b64encode(fli_ser)
        path = os.path.abspath("flimsgfrom")
        proc = Popen(executable=sys.executable, args=["flimsgfrom.py", b64fli_ser], stdout=Popen.PIPE)
        recvh = self.fli.recvh()
        ser_msg, arg = recvh.recv_bytes()
        msg = dmsg.parse(ser_msg)
        self.assertEqual(msg.tc, dmsg.MessageTypes.DD_REGISTER_CLIENT)
        self.assertEqual(msg.respFLI, "Hello World")
        status = proc.stdout.recv().strip()
        self.assertEqual(status, "OK")

    def test_receive_from_cpp(self):
        fli_ser = self.fli.serialize()
        b64fli_ser = du.b64encode(fli_ser)
        path = os.path.abspath("flimsgfrom")
        proc = Popen(executable=path, args=[b64fli_ser], stdout=Popen.PIPE)
        recvh = self.fli.recvh()
        ser_msg, arg = recvh.recv_bytes()
        msg = dmsg.parse(ser_msg)
        self.assertEqual(msg.tc, dmsg.MessageTypes.DD_REGISTER_CLIENT)
        self.assertEqual(msg.respFLI, "Hello World")
        self.assertEqual(msg.bufferedRespFLI, "Dragon is the best")
        status = proc.stdout.recv().strip()
        self.assertEqual(status, "OK")

    def test_send_to_cpp(self):
        fli_ser = self.fli.serialize()
        b64fli_ser = du.b64encode(fli_ser)
        path = os.path.abspath("flimsgto")
        proc = Popen(executable=path, args=[b64fli_ser], stdout=Popen.PIPE)
        sendh = self.fli.sendh()
        msg = dmsg.DDRegisterClient(5, "Hello World!", "Dragon is the best")
        sendh.send_bytes(msg.serialize())
        sendh.close()
        status = proc.stdout.recv().strip()
        self.assertEqual(status, "OK")

    def test_set_kv(self):
        du.set_local_kv("Hello", "World")
        value = du.get_local_kv("Hello")
        self.assertEqual(value, "World")
        du.set_local_kv("Hello", "")
        self.assertRaises(KeyError, du.get_local_kv, "Hello")
        self.assertRaises(KeyError, du.get_local_kv, "NoKey")
        du.set_local_kv("Dragon", "")
        self.assertRaises(KeyError, du.get_local_kv, "Dragon")

    def test_get_channel(self):
        ch = Channel.make_process_local()
        ch.destroy()


if __name__ == "__main__":
    unittest.main()
