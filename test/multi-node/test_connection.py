""" This file contains Dragon multi-node acceptance tests for the
`multiprocessing.Pipe` object.  The test scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.

The test is run with `dragon test_connection.py -f -v`
"""

import numpy as np
import unittest

import dragon
import multiprocessing as mp

import hashlib  # not using standard hash, as it is salted :-(


def ring_send_recv(id, bwd_recv, fwd_send, q, ev, sizes):
    """Send random bytes through a connection. Then recv the same."""

    data_all = np.random.bytes(max(sizes) * 1024**2)

    q.put(True)

    ev.wait()

    for size in sizes:

        idx = int(size * 1024**2)
        data = data_all[0:idx]

        m = hashlib.sha256()
        m.update(data)
        checksum = m.hexdigest()

        fwd_send.send((data, checksum))

        data, checksum = bwd_recv.recv()

        m = hashlib.sha256()
        m.update(data)
        digest = m.hexdigest()

        if digest != checksum:
            raise AssertionError("Data Corruption detected")

        q.put(len(data))

    fwd_send.close()
    bwd_recv.close()


def ring_recv_send(id, bwd_recv, fwd_send, q, ev, sizes):
    """Recv random bytes through a connection. Then send the same."""

    data_all = np.random.bytes(max(sizes) * 1024**2)

    q.put(True)

    ev.wait()

    for size in sizes:
        data, checksum = bwd_recv.recv()

        m = hashlib.sha256()
        m.update(data)
        digest = m.hexdigest()

        if digest != checksum:
            raise AssertionError("Data Corruption detected")

        q.put(len(data))

        idx = int(size * 1024**2)
        data = data_all[0:idx]

        m = hashlib.sha256()
        m.update(data)
        checksum = m.hexdigest()

        fwd_send.send((data, checksum))

    fwd_send.close()
    bwd_recv.close()


# @unittest.skip(f"Fails for larger CPU counts")
class TestConnectionMultiNode(unittest.TestCase):
    def test_ring_multi_node(self):
        """This test creates 2 processes per cpu. They send increasingly large
        messages in a ring to each other."""

        sizes = [0.1, 1, 10, 20]  # , 100, 500]  # MB !

        num_processes = max(2, mp.cpu_count() // 8)

        qs = [mp.Queue() for i in range(num_processes)]
        ev = mp.Event()

        first_recv, last_send = mp.Pipe(duplex=False)

        this_recv = first_recv

        processes = []
        for i in range(num_processes):

            if i == num_processes - 1:
                this_send = last_send
            else:
                next_recv, this_send = mp.Pipe(duplex=False)

            if i % 2 == 0:
                target = ring_send_recv
            else:
                target = ring_recv_send

            p = mp.Process(target=target, args=(i, this_recv, this_send, qs[i], ev, sizes))
            p.start()
            processes.append(p)

            this_recv = next_recv

        for q in qs:
            self.assertTrue(q.get())

        ev.set()  # start communication all at once

        for size in sizes:
            for q in qs:
                recv_size = q.get()
                self.assertTrue(recv_size == int(size * 1024**2))

        for p in processes:
            p.join()

        for q in qs:
            q.close()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
