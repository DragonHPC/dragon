#!/usr/bin/env python3

import unittest
import pickle
import sys
import ctypes
import random
import string
import zlib
import json
import time
import os
import cloudpickle

import dragon
import dragon.infrastructure.messages as dmsg
import dragon.channels as dch
from dragon.utils import b64encode, b64decode, host_id
from dragon.data.ddict import DDict
from dragon.native.machine import Node
import multiprocessing as mp
import traceback
from dragon.rc import DragonError


class TestDDict(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_local_channel(self):
        ch = dch.Channel.make_process_local()
        ch.detach()

    def test_infra_message(self):
        msg = dmsg.GSHalted(42)
        ser = msg.serialize()
        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.GSHalted)
        newser = "eJyrVoovSVayUjA21lFQKklMBzItawE+xQWS"
        from_str = dmsg.parse(newser)
        self.assertIsInstance(from_str, dmsg.GSHalted)
        newser = "eJyrVoovSVayUjA21lFQKklMBzItawE+xQWS\n"
        from_str = dmsg.parse(newser)
        self.assertIsInstance(from_str, dmsg.GSHalted)
        newline = b"\n\n\n\n"
        encoded = b64encode(newline)
        decoded = b64decode(encoded)
        self.assertEqual(newline, decoded)
        newline = "\n\n\n\n"
        encoded = b64encode(newline.encode("utf-8"))
        decoded = b64decode(encoded)
        self.assertEqual(newline, decoded.decode("utf-8"))

    def test_capnp_message(self):
        msg = dmsg.DDRegisterClient(42, "HelloWorld", "MiskaIsAdorable")
        ser = msg.serialize()

        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.DDRegisterClient)

    def test_ddict_client_response_message(self):
        manager_nodes = b64encode(cloudpickle.dumps([Node(ident=host_id()) for _ in range(2)]))
        msg = dmsg.DDRegisterClientResponse(
            42, 43, DragonError.SUCCESS, 0, 2, 3, manager_nodes, "this is name", 10, "this is dragon error info"
        )
        ser = msg.serialize()
        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.DDRegisterClientResponse)

    def test_bringup_teardown(self):
        d = DDict(2, 1, 3000000)
        d.destroy()

    def test_detach_client(self):
        d = DDict(2, 1, 3000000)
        d.detach()
        d.destroy()

    def test_put_and_get(self):
        d = DDict(2, 1, 3000000)

        d["abc"] = "def"
        x = d["abc"]
        self.assertEqual(d["abc"], "def")

        d[123] = "456"
        x = d[123]
        self.assertEqual(d[123], "456")

        d[(12, 34, 56)] = [1, 2, 3, 4, 5, 6]
        y = d[(12, 34, 56)]
        y1 = d[(12, 34, 56)]  # test if the key-value can be requested twice or more
        y2 = d[(12, 34, 56)]
        self.assertEqual(y, [1, 2, 3, 4, 5, 6])
        self.assertEqual(y1, [1, 2, 3, 4, 5, 6])
        self.assertEqual(y2, [1, 2, 3, 4, 5, 6])
        self.assertEqual(d[(12, 34, 56)], [1, 2, 3, 4, 5, 6])

        try:
            y = d["hello"]
            raise AttributeError("Expected KeyError not raised")
        except KeyError:
            pass

        d.destroy()

    def test_pop(self):
        d = DDict(2, 1, 3000000, trace=True)
        d["abc"] = "def"
        x = d.pop("abc")
        self.assertEqual(x, "def")
        self.assertRaises(KeyError, d.pop, "abc")

        d[123] = 456
        del d[123]
        self.assertRaises(KeyError, d.pop, 123)

        d[(12, 34, 56)] = [1, 2, 3, 4, 5, 6]
        x = d.pop((12, 34, 56))
        self.assertEqual(x, [1, 2, 3, 4, 5, 6])
        self.assertRaises(KeyError, d.pop, (12, 34, 56))

        d.destroy()

    def test_contains_key(self):
        d = DDict(2, 1, 3000000)
        d["abc"] = "def"
        self.assertTrue("abc" in d)  # test existence of the added key
        self.assertFalse(123 in d)  # test existence if the key is never added
        d[123] = 456
        self.assertTrue(123 in d)
        d.pop(123)
        self.assertFalse(123 in d)  # test existence of a poped key
        d.pop("abc")
        self.assertFalse("abc" in d)  # test existence of a poped key

        # test tuple key and value
        d[(1, 2, 3, 4, 5)] = [6, 7, 8, 9, 10]
        self.assertTrue((1, 2, 3, 4, 5) in d)
        del d[(1, 2, 3, 4, 5)]
        self.assertFalse((1, 2, 3, 4, 5) in d)

        d.destroy()

    def test_len(self):
        d = DDict(2, 1, 3000000)
        self.assertEqual(len(d), 0)
        d["abc"] = "def"
        self.assertEqual(len(d), 1)
        d[123] = 456
        self.assertEqual(len(d), 2)
        d[(1, 2, 3, 4, 5)] = [6, 7, 8, 9, 10]
        self.assertEqual(len(d), 3)
        d.pop("abc")
        self.assertEqual(len(d), 2)
        d.pop(123)
        self.assertEqual(len(d), 1)
        d.pop((1, 2, 3, 4, 5))
        self.assertEqual(len(d), 0)
        d.destroy()

    def test_clear(self):
        d = DDict(2, 1, 3000000)
        d["abc"] = "def"
        d[123] = 456
        d[(1, 2, 3, 4, 5)] = [6, 7, 8, 9, 10]
        self.assertEqual(len(d), 3)
        d.clear()
        self.assertEqual(len(d), 0)
        d.clear()  # test clearing an empty dictionary
        self.assertEqual(len(d), 0)
        d["hello"] = "world"
        d.clear()
        self.assertEqual(len(d), 0)
        d.destroy()

    @unittest.skip("Not yet implemented")
    def test_iter(self):
        try:
            d = DDict(2, 1, 3000000)
            k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
            v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
            for i, key in enumerate(k):
                d[key] = v[i]

            for i in d:
                if i == "abc":
                    self.assertEqual(d[i], "def")
                elif i == 98765:
                    self.assertEqual(d[i], 200)
                elif i == "hello":
                    self.assertEqual(d[i], "world")
                elif i == (1, 2, 3, 4, 5):
                    self.assertEqual(d[i], ["a", 1, 3, 5, "b"])
                else:
                    raise RuntimeError(f"Get the key which is not added by client: key={i}")

            iter_d = iter(d)
            ddict_keys = []
            while True:
                try:
                    ddict_keys.append(next(iter_d))
                except StopIteration:
                    del iter_d
                    break
            for key in k:
                self.assertTrue(key in ddict_keys)

            d.destroy()
        except Exception as e:
            tb = traceback.format_exc()
            raise Exception(f"Exception caught {e}\n Traceback: {tb}")

    def test_keys(self):
        d = DDict(2, 1, 3000000)
        k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
        v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
        for i, key in enumerate(k):
            d[key] = v[i]
        ddict_keys = d.keys()
        for key in k:
            self.assertTrue(key in ddict_keys)
        d.destroy()

    def test_attach_ddict(self):
        d = DDict(2, 1, 3000000)
        d["hello"] = "world"
        d_serialized = d.serialize()
        new_d = DDict.attach(d_serialized)
        self.assertEqual(new_d["hello"], "world")
        d.detach()
        new_d.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
