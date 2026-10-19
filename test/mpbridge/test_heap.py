import unittest

import dragon
import multiprocessing
import multiprocessing.heap


class TestMultiprocessingAPI(unittest.TestCase):
    def test_arena(self):
        size = 1024
        fd = 12
        a = multiprocessing.heap.Arena(size, fd=fd)

        self.assertTrue(a.size == size)
        self.assertTrue(a.fd == fd)
        self.assertIsInstance(a.buffer, memoryview)

    def test_with_BufferWrapper(self):
        bw = multiprocessing.heap.BufferWrapper(8)
        memory = bw.create_memoryview()
        self.assertIsInstance(memory, memoryview)


if __name__ == "__main__":
    unittest.main()
