import unittest
import pickle
import time

import dragon
import multiprocessing as mp


def setUpModule():
    mp.set_start_method("dragon", force=True)


class TestMPBridgeProcess(unittest.TestCase):
    def setUp(self):
        self.assertEqual(mp.get_start_method(), "dragon")

    def test_active_children(self):

        self.assertTrue(len(mp.active_children()) == 0)

        processes = []

        for _ in range(0, 8):
            p = mp.Process(target=time.sleep, args=(100,))
            processes.append(p)

        pids = []

        for p in processes:
            p.start()
            active_pids = [c.pid for c in mp.active_children()]
            self.assertTrue(p.pid in active_pids)
            pids.append(p.pid)

        self.assertTrue({c.pid for c in mp.active_children()} == set(pids))

        for p in processes:
            p.kill()
            p.join()

        self.assertTrue(len(mp.active_children()) == 0)

    @staticmethod
    def _send_own_process_object(writer):

        p = mp.current_process()

        p.authkey = b""  # cannot pickle the authkey
        p._config["authkey"] = b""
        writer.send(p)

    def test_current_process(self):

        reader, writer = mp.Pipe()

        my_p = mp.Process(target=self._send_own_process_object, args=(writer,))
        my_p.start()

        my_p.authkey = b""  # make authkey equal
        my_p._config["authkey"] = b""

        their_p = reader.recv()

        # Note that the two process objects must be the same only in some
        # attributes. Other are different - this is true also for
        # standard multiprocessing

        self.assertTrue(their_p._closed == my_p._closed)
        self.assertTrue(their_p._identity == my_p._identity)
        self.assertTrue(their_p._name == my_p._name)
        self.assertTrue(their_p._parent_pid == my_p._parent_pid)
        self.assertTrue(their_p._parent_name == my_p._parent_name)

        self.assertTrue(their_p._p_p_uid == my_p._p_p_uid)

    @staticmethod
    def _send_parent_process_object(writer):

        pp = mp.parent_process()

        writer.send(pp)

    def test_parent_process(self):

        reader, writer = mp.Pipe()

        my_p = mp.current_process()

        cp = mp.Process(target=self._send_parent_process_object, args=(writer,))
        cp.start()

        their_p = reader.recv()

        self.assertTrue(their_p._closed == my_p._closed)
        self.assertTrue(their_p._identity == my_p._identity)
        self.assertTrue(their_p._name == my_p._name)
        self.assertTrue(their_p._popen == my_p._popen)
        self.assertTrue(their_p._parent_pid == my_p._parent_pid)


if __name__ == "__main__":
    unittest.main()
