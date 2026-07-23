#!/usr/bin/env python3

import unittest
import random
import dragon
import multiprocessing as mp
import sys
import threading
import numpy as np
import queue as pyqueue


def setUpModule():
    mp.set_start_method("dragon", force=True)


class TestQueue(unittest.TestCase):
    def setUp(self):
        self.assertEqual(mp.get_start_method(), "dragon")

    def test_queue(self):
        q = dragon.mpbridge.queues.DragonQueue()
        y = [1, 2, 3]
        q.put(y)
        x = q.get()
        self.assertEqual(x, y, "The Queue implementation did not pass a list correctly through it.")

        yy = ["a", "b", "c"]
        q.put(yy)
        q.put(y)
        x = q.get()
        self.assertEqual(x, yy, "The Queue implementation did not pass a list correctly through it.")
        x = q.get()
        self.assertEqual(x, y, "The Queue implementation did not pass a list correctly through it.")

        q.close()

    @classmethod
    def _writer(self, id, q, nitems, msg_list, qver):
        for i in range(id * nitems, id * nitems + nitems):
            q.put((i, msg_list[i]))
            qver.put((i, msg_list[i]))
        q.close()
        qver.close()

    @classmethod
    def _reader(self, id, q, nitems, msg_list):
        for _ in range(nitems):
            item = q.get()
            _id, payload = item
            assert payload == msg_list[_id]
        q.close()

    def test_queue_multi_readers_multi_writers(self):
        nitems = 20  # multiple of num_readers and num_writers
        num_readers = 4
        num_writers = 2
        msg_list = {}  # key is the msg identifier, value is the payload
        for i in range(nitems):
            msg_list[i] = "Hello msg with id " + str(i)

        q = dragon.mpbridge.queues.DragonQueue()
        q_ver = dragon.mpbridge.queues.DragonQueue()  # used to verify that all messages are gotten from the queue

        writers = []
        for i in range(num_writers):
            p = mp.Process(target=self._writer, args=(i, q, int(nitems / num_writers), msg_list, q_ver))
            p.start()
            writers.append(p)

        readers = []
        for i in range(num_readers):
            p = mp.Process(target=self._reader, args=(i, q, int(nitems / num_readers), msg_list))
            readers.append(p)
            p.start()

        for p in writers:
            p.join()

        for p in readers:
            p.join()

        while not q_ver.empty():
            _id, payload = q_ver.get()
            if _id in msg_list:
                del msg_list[_id]

        self.assertEqual(len(msg_list), 0)
        self.assertTrue(q_ver.empty())

        self.assertTrue(q.empty())

        q.close()
        q_ver.close()

    def pass_an_obj(self, obj):
        q = dragon.mpbridge.queues.DragonQueue()
        q.put(obj)
        rec_obj = q.get()

        if isinstance(obj, np.ndarray):
            np.testing.assert_array_equal(obj, rec_obj)
        else:
            self.assertEqual(obj, rec_obj)

        q.close()

    def test_objs_queue(self):
        """Test different python objects and different msg sizes"""
        self.pass_an_obj("hyena")
        self.pass_an_obj({"hyena": "hyena"})
        self.pass_an_obj(bytes(100000))
        self.pass_an_obj(bytes(200000))
        self.pass_an_obj(np.random.rand(1000000))

        for lg_size in range(25):
            self.pass_an_obj(bytearray(2**lg_size))

        for lg_size in range(15):
            self.pass_an_obj(random.randbytes(2**lg_size))

        for lg_size in range(22):
            my_msg = []
            my_size = sys.getsizeof(my_msg)
            while my_size < 2**lg_size:
                my_msg.append("a")
                my_size = sys.getsizeof(my_msg)
            self.pass_an_obj(my_msg)

    def test_queue_timeout(self):
        q = dragon.mpbridge.queues.DragonQueue()

        # try to send an object which is large enough
        # such that pickle will call write() multiple
        # times and we want to make sure that the timeout
        # is updated/decremented at each call
        obj = bytes(10000000)

        # set a small value for the timeout for this obj
        # so that we make sure it raises ChannelTimeout
        self.assertRaises(pyqueue.Full, q.put, obj, True, 0.0001)

        q.put(obj, timeout=5)

        rec_obj = q.get()
        if isinstance(obj, np.ndarray):
            np.testing.assert_array_equal(obj, rec_obj)
        else:
            self.assertEqual(obj, rec_obj)

        q.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
