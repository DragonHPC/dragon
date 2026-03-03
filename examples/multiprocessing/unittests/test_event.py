""" Test mulitprocessing event
"""
import unittest
import time
from test.support import hashlib_helper

import dragon  # DRAGON import before multiprocessing

import multiprocessing

from common import (
    BaseTestCase,
    ProcessesMixin,
    ManagerMixin,
    ThreadsMixin,
    TimingWrapper,
    setUpModule,
    tearDownModule,
    TIMEOUT1,
    TIMEOUT2,
)


class WithProcessesTestEvent(BaseTestCase, ProcessesMixin, unittest.TestCase):
    @classmethod
    def _test_event(cls, event):
        time.sleep(TIMEOUT2)
        event.set()

    def test_event(self):
        event = self.Event()
        wait = TimingWrapper(event.wait)

        # Removed temporarily, due to API shear, this does not
        # work with threading._Event objects. is_set == isSet
        self.assertEqual(event.is_set(), False)

        # Removed, threading.Event.wait() will return the value of the __flag
        # instead of None. API Shear with the semaphore backed mp.Event
        self.assertEqual(wait(0.0), False)
        self.assertTimingAlmostEqual(wait.elapsed, 0.0)
        self.assertEqual(wait(TIMEOUT1), False)
        self.assertTimingAlmostEqual(wait.elapsed, TIMEOUT1)

        event.set()

        # See note above on the API differences
        self.assertEqual(event.is_set(), True)
        self.assertEqual(wait(), True)
        self.assertTimingAlmostEqual(wait.elapsed, 0.0)
        self.assertEqual(wait(TIMEOUT1), True)
        self.assertTimingAlmostEqual(wait.elapsed, 0.0)
        # self.assertEqual(event.is_set(), True)

        event.clear()

        # self.assertEqual(event.is_set(), False)

        p = self.Process(target=self._test_event, args=(event,))
        p.daemon = True
        p.start()
        self.assertEqual(wait(), True)
        p.join()


#


@hashlib_helper.requires_hashdigest("md5")
@unittest.skip("DRAGON: not implemented")
class WithManagerTestEvent(BaseTestCase, ManagerMixin, unittest.TestCase):
    @classmethod
    def _test_event(cls, event):
        time.sleep(TIMEOUT2)
        event.set()

    def test_event(self):
        event = self.Event()
        wait = TimingWrapper(event.wait)

        # Removed temporarily, due to API shear, this does not
        # work with threading._Event objects. is_set == isSet
        self.assertEqual(event.is_set(), False)

        # Removed, threading.Event.wait() will return the value of the __flag
        # instead of None. API Shear with the semaphore backed mp.Event
        self.assertEqual(wait(0.0), False)
        self.assertTimingAlmostEqual(wait.elapsed, 0.0)
        self.assertEqual(wait(TIMEOUT1), False)
        self.assertTimingAlmostEqual(wait.elapsed, TIMEOUT1)

        event.set()

        # See note above on the API differences
        self.assertEqual(event.is_set(), True)
        self.assertEqual(wait(), True)
        self.assertTimingAlmostEqual(wait.elapsed, 0.0)
        self.assertEqual(wait(TIMEOUT1), True)
        self.assertTimingAlmostEqual(wait.elapsed, 0.0)
        # self.assertEqual(event.is_set(), True)

        event.clear()

        # self.assertEqual(event.is_set(), False)

        p = self.Process(target=self._test_event, args=(event,))
        p.daemon = True
        p.start()
        self.assertEqual(wait(), True)
        p.join()


@unittest.skip("DRAGON: Threads not implemented")
class WithThreadsTestEvent(BaseTestCase, ThreadsMixin, unittest.TestCase):
    @classmethod
    def _test_event(cls, event):
        time.sleep(TIMEOUT2)
        event.set()

    def test_event(self):
        event = self.Event()
        wait = TimingWrapper(event.wait)

        # Removed temporarily, due to API shear, this does not
        # work with threading._Event objects. is_set == isSet
        self.assertEqual(event.is_set(), False)

        # Removed, threading.Event.wait() will return the value of the __flag
        # instead of None. API Shear with the semaphore backed mp.Event
        self.assertEqual(wait(0.0), False)
        self.assertTimingAlmostEqual(wait.elapsed, 0.0)
        self.assertEqual(wait(TIMEOUT1), False)
        self.assertTimingAlmostEqual(wait.elapsed, TIMEOUT1)

        event.set()

        # See note above on the API differences
        self.assertEqual(event.is_set(), True)
        self.assertEqual(wait(), True)
        self.assertTimingAlmostEqual(wait.elapsed, 0.0)
        self.assertEqual(wait(TIMEOUT1), True)
        self.assertTimingAlmostEqual(wait.elapsed, 0.0)
        # self.assertEqual(event.is_set(), True)

        event.clear()

        # self.assertEqual(event.is_set(), False)

        p = self.Process(target=self._test_event, args=(event,))
        p.daemon = True
        p.start()
        self.assertEqual(wait(), True)
        p.join()


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
