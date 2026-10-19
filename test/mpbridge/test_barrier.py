import unittest
import time
import threading

import dragon
from dragon.mpbridge.synchronize import Barrier, BrokenBarrierError


class TestBarrier(unittest.TestCase):
    def test_requirement_1_3(self):
        """Requirement 1.3: If the method is called with timeout <0, it has to assume timeout=0."""
        start = time.monotonic()
        Barrier(parties=2, timeout=-0.1)
        stop = time.monotonic()
        timeout = int(int(stop) - int(start))
        self.assertAlmostEqual(0, timeout, "The negative timeout for a barrier is 0.")

    def test_requirement_2_1(self):
        """If the wait is called with timeout <0, it has to assume timeout=0."""
        start = float(time.monotonic())
        barrier = Barrier(parties=2)
        try:
            barrier.wait(timeout=-1)
        except threading.BrokenBarrierError:
            pass
        stop = float(time.monotonic())
        timeout = round(float(float(stop) - float(start)), 1)
        self.assertAlmostEqual(0, timeout, "The negative timeout for a barrier is 0.")


if __name__ == "__main__":
    unittest.main()
