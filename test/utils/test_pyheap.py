#/usr/bin/env python3

import unittest
import array
from dragon.pheap import PriorityHeap


class PHeapTest(unittest.TestCase):

    def setUp(self):
        self.cap = 15
        self.nvals = 1
        self.base = 3
        self.size = PriorityHeap.size(self.cap, self.nvals)
        # @MCB TODO: Parameterize this to test with multiprocessing
        self.mem = bytearray(self.size)

    def tearDown(self):
        del self.mem

    def test_init(self):
        """
        Make sure init works without throwing an exception
        """
        hdl = PriorityHeap.create(self.base, self.cap, self.nvals, self.mem)
        hdl.destroy()

    def test_simple_insert(self):
        hdl = PriorityHeap.create(self.base, self.cap, self.nvals, self.mem)

        arr = array.array('Q', [0] * self.cap)
        for i in range(self.cap):
            for j in range(self.nvals):
                arr[j] = i * self.nvals + j

            hdl.insert(arr)

        for i in range(self.cap):
            v = array.array('Q', [0] * self.nvals)
            p = int()
            hdl.extract(v, p)

            # Since nothing is inserted as urgent, everything should have equal value and priority
            for j in range(self.nvals):
                self.assertEqual(i * self.nvals + j, v[j])

        hdl.destroy()

    def test_urgent_insert(self):
        hdl = PriorityHeap.create(self.base, self.cap, self.nvals, self.mem)

        expected = [5, 0, 2, 3, 4, 1, 6, 7, 8, 9, 10, 11, 12, 13, 14]
        arr = array.array('Q', [0] * self.cap)
        for i in range(self.cap):
            for j in range(self.nvals):
                arr[j] = i * self.nvals + j

            if i != 5:
                hdl.insert(arr)
            else:
                hdl.insert(arr, urgent=True)

        for i in range(self.cap):
            v = array.array('Q', [0] * self.nvals)
            p = int()
            hdl.extract(v, p)

            if i == 0:
                for j in range(self.nvals):
                    self.assertEqual(5 * self.nvals + j, v[j])
            elif i <= 5:
                for j in range(self.nvals):
                    self.assertEqual((i-1) * self.nvals + j, v[j])
            else:
                for j in range(self.nvals):
                    self.assertEqual(i * self.nvals + j , v[j])

        hdl.destroy()


if __name__ == '__main__':
    unittest.main()
