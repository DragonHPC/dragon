"""Test for our context wait.
When the wait is lowered into dragon.native, these tests should move along.
"""

import unittest
from test.support import socket_helper

import socket
import time
import random
import os
import random
from sys import platform

import dragon

import multiprocessing as mp
import threading

import dragon.mpbridge.context


def setUpModule():
    mp.set_start_method("dragon", force=True)


class TestDragonContextWait(unittest.TestCase):
    """Software tests for Dragons wait() working on PUIDs, Connections, CUIDs,
    DragonQueues, DragonQueue.Fakers and sockets.
    """

    def setUp(self):
        self.assertEqual(mp.get_start_method(), "dragon")

    @classmethod
    def _waitev_and_ready(self, ev, send_handle, msg):

        ev.wait(timeout=None)

        # when many processes are waiting at the same time, randomize the return here,
        # so that the parent can get one or several at the same time.
        time.sleep(random.uniform(0, 0.1))

        if send_handle is None:  # sentinel
            pass  # just exit
        elif isinstance(send_handle, dragon.infrastructure.connection.Connection):
            send_handle.send(msg)
        elif isinstance(send_handle, dragon.infrastructure.connection.CUID):
            send_handle.send(msg)
        elif isinstance(send_handle, dragon.mpbridge.queues.DragonQueue):
            send_handle.put(msg)
        elif isinstance(send_handle, dragon.mpbridge.queues.BaseImplQueue):
            send_handle.put(msg)
        elif isinstance(send_handle, dragon.mpbridge.queues.BaseImplJoinableQueue):
            send_handle.put(msg)
        elif isinstance(send_handle, dragon.mpbridge.queues.BaseImplSimpleQueue):
            send_handle.put(msg)
        elif isinstance(send_handle, dragon.mpbridge.queues.FakeConnectionHandle):
            send_handle.send(msg)
        elif isinstance(send_handle, tuple):  # socket address
            s = socket.socket()
            s.connect(send_handle)
            s.sendall(("%s" % msg).encode("ascii"))
            s.close()
        else:
            raise (TypeError(f"Don't know how to set ready for a handle of type: {type(send_handle)}"))

    def test_sentinel(self):

        num_workers = 4

        events = [dragon.native.event.Event() for _ in range(num_workers)]
        procs = []

        for i in range(num_workers):
            p = dragon.mpbridge.DragonProcess(target=self._waitev_and_ready, args=(events[i], None, ""))
            procs.append(p)

        for p in procs:
            p.start()

        sentinels = [p.sentinel for p in procs]

        # they should all be sleeping
        ready_sentinels = dragon.mpbridge.DragonContext.wait(sentinels, timeout=0)
        self.assertEqual(ready_sentinels, [])

        # they should wake up and exit roughly every 0.1 seconds
        while sentinels:

            events[0].set()

            ready_sentinels = dragon.mpbridge.DragonContext.wait(sentinels, timeout=None)

            self.assertNotEqual(ready_sentinels, [])
            self.assertEqual(len(ready_sentinels), 1)

            s = ready_sentinels[0]
            self.assertIsInstance(s, dragon.mpbridge.process.PUID)
            self.assertTrue(s == sentinels[0])

            sentinels.pop(0)
            events.pop(0)

        # make sure we shut down all service threads of WaitService class
        # before moving to the next test
        dragon.mpbridge.context._wait_service.__del__()
        # Reset global _wait_service so that init() is called again
        # and the garbage_collect thread initiated
        dragon.mpbridge.context._wait_service = None

    def test_connection(self):

        num_workers = 4

        impbridge_ctx = mp.get_context()  # Pipe() requires a context object

        pipes = [impbridge_ctx.Pipe(duplex=False) for _ in range(num_workers)]
        readers = [pipes[i][0] for i in range(num_workers)]
        writers = [pipes[i][1] for i in range(num_workers)]

        messages = [f"Test-Message-{i}" for i in range(num_workers)]
        events = [dragon.native.event.Event() for _ in range(num_workers)]

        procs = []

        for i in range(num_workers):
            p = dragon.mpbridge.DragonProcess(target=self._waitev_and_ready, args=(events[i], writers[i], messages[i]))
            procs.append(p)

        # there should be no message yet
        self.assertFalse(dragon.mpbridge.DragonContext.wait(readers, timeout=0))

        for p in procs:
            p.start()

        #  a new message should be ready every 0.1 sec
        while readers:
            events[0].set()
            ready_readers = dragon.mpbridge.DragonContext.wait(readers, timeout=10)

            self.assertNotEqual(ready_readers, [])

            self.assertEqual(len(ready_readers), 1)
            r = ready_readers[0]
            self.assertEqual(r, readers[0])
            msg = r.recv()  # this shouldn't block now
            self.assertEqual(msg, messages[0])
            messages.pop(0)
            readers.pop(0)
            events.pop(0)

        # make sure we shut down all service threads of WaitService class
        # before moving to the next test
        dragon.mpbridge.context._wait_service.__del__()
        # Reset global _wait_service so that init() is called again
        # and the garbage_collect thread initiated
        dragon.mpbridge.context._wait_service = None

    def test_queue(self):

        num_workers = 4

        impbridge_ctx = mp.get_context()  # Queue() requires a context object

        queues = [impbridge_ctx.Queue() for _ in range(num_workers)]
        items = [f"Test-Item-{i}" for i in range(num_workers)]
        events = [dragon.native.event.Event() for _ in range(num_workers)]

        procs = []

        for i in range(num_workers):
            p = dragon.mpbridge.DragonProcess(target=self._waitev_and_ready, args=(events[i], queues[i], items[i]))
            procs.append(p)

        # there should be no message yet
        self.assertFalse(dragon.mpbridge.DragonContext.wait(queues, timeout=0.0))

        for p in procs:
            p.start()

        # a queue should be ready every 0.1 sec
        while queues:
            events[0].set()
            ready_queues = dragon.mpbridge.DragonContext.wait(queues, timeout=10)

            self.assertNotEqual(ready_queues, [])  # cannot hang, delay is bound < 0.1 sec

            self.assertEqual(len(ready_queues), 1)
            q = ready_queues[0]
            self.assertEqual(q, queues[0])
            item = q.get()
            self.assertEqual(item, items[0])
            items.pop(0)
            queues.pop(0)
            events.pop(0)

        # make sure we shut down all service threads of WaitService class
        # before moving to the next test
        dragon.mpbridge.context._wait_service.__del__()
        # Reset global _wait_service so that init() is called again
        # and the garbage_collect thread initiated
        dragon.mpbridge.context._wait_service = None

    @unittest.skip(
        "This is broken, because dragon.mpbridge.queue.Faker() cannot be hashed. So Ben's solution actually "
        + "doesn't work either. The relevant MP unit tests seem to be:  WithProcessesTestPool.test_context,  "
        + "WithProcessesTestPool.test_empty_iterable"
    )
    def test_faker(self):

        num_workers = 4

        impbridge_ctx = mp.get_context()  # Queue() requires a context object

        queues = [impbridge_ctx.Queue() for _ in range(num_workers)]
        events = [dragon.native.event.Event() for _ in range(num_workers)]

        fake_writers = [q._reader for q in queues]
        fake_readers = [q._writer for q in queues]

        messages = [f"Test-Message-{i}" for i in range(num_workers)]

        procs = []

        for i in range(num_workers):
            p = dragon.mpbridge.DragonProcess(
                target=self._waitev_and_ready, args=(events[i], fake_writers[i], messages[i])
            )
            procs.append(p)

        for p in procs:
            p.start()

        # there should be no message yet
        self.assertFalse(dragon.mpbridge.DragonContext.wait(fake_readers, timeout=0.0))

        #  a new pipe should be ready every 0.1 sec
        while fake_readers:
            events[0].set()
            ready_readers = dragon.mpbridge.DragonContext.wait(fake_readers, timeout=10)

            self.assertNotEqual(ready_readers, [])  # cannot hang, delay is bound < 0.1 sec

            self.assertEqual(len(ready_readers), 1)
            r = ready_readers[0]
            self.assertEqual(r, fake_readers[0])
            msg = r.recv()  # this shouldn't block now
            self.assertEqual(msg, messages[0])
            messages.pop(0)
            fake_readers.pop(0)
            events.pop(0)

        # make sure we shut down all service threads of WaitService class
        # before moving to the next test
        dragon.mpbridge.context._wait_service.__del__()
        # Reset global _wait_service so that init() is called again
        # and the garbage_collect thread initiated
        dragon.mpbridge.context._wait_service = None

    # Make sure we can wait on a large list of different Dragon and multiprocessing objects with random return.
    # This also tests the capability of wait() to return several handles at once, as some delays will be
    # close to one another. We are using sockets to emulate a multiprocessing object that is certainly not Dragon.
    # TODO: Test this with a much large number of objects on a slow system. We should be able to handle 100s of
    # objects getting ready at very odd times.
    # JD: Add the sockets back in. This stopped working in the devcontainer and it is unclear to me why.
    def test_mixed(self):

        impbridge_ctx = mp.get_context()
        self.assertIsInstance(impbridge_ctx, dragon.mpbridge.DragonContext)

        num_pipes = num_queues = num_sentinels = num_fakers = num_channels = num_mp_objects = 4

        num_workers = num_sentinels + num_pipes + num_queues  # + num_mp_objects  # + num_channels + num_fakers

        items = [f"Test-Item-{i}" for i in range(num_workers)]  # wont need all of them

        # create handles for the processes to write to and if possible to read from
        pseudo_sentinels = [None for _ in range(num_sentinels)]  # don't have the real ones yet

        pipes = [impbridge_ctx.Pipe(duplex=False) for _ in range(num_pipes)]
        writers = [pipes[i][1] for i in range(num_pipes)]
        readers = [pipes[i][0] for i in range(num_pipes)]

        queues = [impbridge_ctx.Queue() for _ in range(num_queues)]

        faker_queues = [impbridge_ctx.Queue() for _ in range(num_fakers)]
        faker_writers = [q._reader for q in faker_queues]
        faker_readers = [q._writer for q in faker_queues]

        chan_pipes = [impbridge_ctx.Pipe(duplex=False) for _ in range(num_channels)]
        chan_writers = [chan_pipes[i][1] for i in range(num_channels)]
        chan_readers = [chan_pipes[i][0] for i in range(num_channels)]

        # socket_server = socket.create_server((socket_helper.HOST, 0))
        # addr = socket_server.getsockname()
        # mp_writers = [addr for _ in range(num_mp_objects)]
        # mp_readers = []

        proc_handles = pseudo_sentinels + writers + queues  # + mp_writers  # + chan_writers + faker_writers
        events = [dragon.native.event.Event() for _ in range(num_workers // 4)]  # that'll make it pretty messy ...

        # start the processes
        procs = []
        random.seed(a=42)

        for i in range(num_workers):
            p = dragon.mpbridge.DragonProcess(
                target=self._waitev_and_ready, args=(events[i % len(events)], proc_handles[i], items[i])
            )
            procs.append(p)

        for p in procs:
            p.start()

        # create rest of the read handles to wait on
        sentinels = [p.sentinel for p in procs[:num_sentinels]]
        # cuids = [r.fileno() for r in chan_readers]
        # for _ in range(num_mp_objects):
        #     r, __ = socket_server.accept()
        #     mp_readers.append(r)
        # socket_server.close()

        # we should be able to wait on all of those simultaneously
        # this works for connections and queues, so I am assuming the test itself is OK.
        wait_handles = sentinels + readers + queues  # + mp_readers  # + cuids + faker_readers

        while wait_handles:

            if events:  # notify groups of processes
                ev = events.pop(0)
                ev.set()

            ready_handles = dragon.mpbridge.DragonContext.wait(wait_handles, timeout=None)

            self.assertNotEqual(ready_handles, [])

            for h in ready_handles:

                wait_handles.remove(h)

                if isinstance(h, dragon.mpbridge.process.PUID):
                    self.assertIn(h, sentinels)
                    sentinels.remove(h)
                elif isinstance(h, dragon.infrastructure.connection.Connection):
                    self.assertIn(h, readers)
                    readers.remove(h)
                    item = h.recv()
                    self.assertIn(item, items)
                    items.remove(item)
                elif isinstance(h, dragon.infrastructure.connection.CUID):
                    self.assertIn(h, cuids)
                    cuids.remove(h)
                    item = h.conn.recv()
                    self.assertIn(item, items)
                    items.remove(item)
                elif isinstance(h, dragon.mpbridge.queues.DragonQueue):
                    self.assertIn(h, queues)
                    queues.remove(h)
                    item = h.get()
                    self.assertIn(item, items)
                    items.remove(item)
                elif isinstance(h, dragon.mpbridge.queues.BaseImplQueue):
                    self.assertIn(h, queues)
                    queues.remove(h)
                    item = h.get()
                    self.assertIn(item, items)
                    items.remove(item)
                elif isinstance(h, dragon.mpbridge.queues.BaseImplJoinableQueue):
                    self.assertIn(h, queues)
                    queues.remove(h)
                    item = h.get()
                    self.assertIn(item, items)
                    items.remove(item)
                elif isinstance(h, dragon.mpbridge.queues.BaseImplSimpleQueue):
                    self.assertIn(h, queues)
                    queues.remove(h)
                    item = h.get()
                    self.assertIn(item, items)
                    items.remove(item)
                elif isinstance(h, dragon.mpbridge.queues.FakeConnectionHandle):
                    self.assertIn(h, faker_readers)
                    item = h.recv()
                    self.assertIn(item, items)
                    items.remove(item)
                elif isinstance(h, socket.socket):
                    self.assertIn(h, mp_readers)
                    mp_readers.remove(h)
                    item = h.recv(128).decode("utf-8")
                    self.assertIn(item, items)
                    items.remove(item)
                    h.close()
                else:
                    raise (TypeError(f"Wait returned a type that was not in the wait list: {type(h)}"))

        # make sure we shut down all service threads of WaitService class
        # before moving to the next test
        dragon.mpbridge.context._wait_service.__del__()
        # Reset global _wait_service so that init() is called again
        # and the garbage_collect thread initiated
        dragon.mpbridge.context._wait_service = None

    @staticmethod
    def _get_linux_num_threads(pid):
        with open(f"/proc/{pid}/status", "r") as fp:
            txt = fp.read()
        pstatus = dict(x.split(":\t") for x in txt.splitlines())

        return int(pstatus["Threads"])

    @classmethod
    def _check_num_threads(self, max_num_threads) -> bool:

        max_wait_time = 5  # sec
        pid = os.getpid()
        num_threads = self._get_linux_num_threads(pid)
        start = time.monotonic()
        while (time.monotonic() - start) < max_wait_time:
            if num_threads == max_num_threads:
                break
            time.sleep(0.1)
            num_threads = self._get_linux_num_threads(pid)

        return num_threads == max_num_threads

    # Ensure that the number of threads used by wait is constant, if we issue multiple waits on
    # the same connection object. Check that no service threads are spawned if we wait only on a
    # single object.
    # NOTE: This will break, when we implement a multi-poll on channels - that's the intention.
    # @unittest.skipIf(platform == "win32" or platform == "darwin", "Works on Linux only")
    def test_thread_hoarding(self):

        # Create something to wait on
        num_queues = 6

        impbridge_ctx = mp.get_context()

        queues = [impbridge_ctx.Queue() for _ in range(num_queues)]
        items = [f"Test-Item-{i}" for i in range(num_queues)]
        events = [dragon.native.event.Event() for _ in range(num_queues)]

        # start evaluating thread behaviour of the wait implementation
        wait_threads = list()
        spawn_counter = list()

        # initial state, should really be 1
        pid = os.getpid()
        spawn_counter.append(self._get_linux_num_threads(pid))

        # start waiting on two queues
        t = threading.Thread(target=dragon.mpbridge.DragonContext.wait, args=(queues[0:2],))
        t.start()
        wait_threads.append(t)
        self.assertTrue(
            self._check_num_threads(spawn_counter[0] + 4)
        )  # + 2 service threads (conns) + 1 wait thread + 1 garbage_collect thread
        spawn_counter.append(self._get_linux_num_threads(pid))

        # have another thread wait on the same 2 queues
        t = threading.Thread(target=dragon.mpbridge.DragonContext.wait, args=(queues[0:2],))
        t.start()
        wait_threads.append(t)
        self.assertTrue(self._check_num_threads(spawn_counter[1] + 1))  # + 0 service threads + 1 wait thread
        spawn_counter.append(self._get_linux_num_threads(pid))

        # have a third thread wait on another 3 queues
        t = threading.Thread(target=dragon.mpbridge.DragonContext.wait, args=(queues[2:5],))
        t.start()
        wait_threads.append(t)
        self.assertTrue(self._check_num_threads(spawn_counter[2] + 4))  # + 3 service threads (conns) + 1 wait thread
        spawn_counter.append(self._get_linux_num_threads(pid))

        # have a fourth thread wait on a single queue
        t = threading.Thread(target=dragon.mpbridge.DragonContext.wait, args=([queues[5]],))
        t.start()
        wait_threads.append(t)
        self.assertTrue(self._check_num_threads(spawn_counter[3] + 2))  # + 1 service threads (conns) + 1 wait thread
        spawn_counter.append(self._get_linux_num_threads(pid))

        # have the third wait thread and 4 service thread exit
        queues[4].put(items[4])
        wait_threads[2].join()
        self.assertTrue(self._check_num_threads(spawn_counter[4] - 4))  # - 3 service threads (conns) - 1 wait thread
        spawn_counter.append(self._get_linux_num_threads(pid))

        # have the first 2 service threads + 2x1 wait threads exit
        queues[0].put(items[0])
        wait_threads[0].join()
        wait_threads[1].join()  # this one should wake up and exit too.
        self.assertTrue(self._check_num_threads(spawn_counter[5] - 4))  # - 2 service threads (conns) - 2 wait threads
        spawn_counter.append(self._get_linux_num_threads(pid))

        # have the single wait thread exit last
        queues[5].put(items[5])
        wait_threads[3].join()
        self.assertTrue(self._check_num_threads(spawn_counter[6] - 2))  # - 1 service threads (conns) - 1 wait thread

        # make sure we shut down all service threads of WaitService class
        # before moving to the next test
        dragon.mpbridge.context._wait_service.__del__()
        # Reset global _wait_service so that init() is called again
        # and the garbage_collect thread initiated
        dragon.mpbridge.context._wait_service = None

        # wait for the garbage_colloct thread to exit
        time.sleep(1)
        spawn_counter.append(self._get_linux_num_threads(pid))

        self.assertEqual(spawn_counter[7], spawn_counter[0])  # as we started

    # Test that we get the timeouts right.
    def test_timeout(self):

        # Create something to wait on
        num_queues = 6

        impbridge_ctx = mp.get_context()

        queues = [impbridge_ctx.Queue() for _ in range(num_queues)]
        items = [f"Test-Item-{i}" for i in range(num_queues)]

        # check that the timeout is reasonably correct, even when small
        timeout = 0.01
        t = threading.Thread(
            target=dragon.mpbridge.DragonContext.wait,
            args=(queues,),
            kwargs={
                "timeout": timeout,
            },
        )
        t.start()
        start = time.monotonic()
        t.join()
        end = time.monotonic()
        self.assertTrue(end - start < 1.5 * timeout)

        # check that waiting on the same objects a second time with a shorter timout works
        t0 = threading.Thread(
            target=dragon.mpbridge.DragonContext.wait,
            args=(queues,),
            kwargs={
                "timeout": 0.1,
            },
        )
        t1 = threading.Thread(
            target=dragon.mpbridge.DragonContext.wait,
            args=(queues,),
            kwargs={
                "timeout": 0.05,
            },
        )
        t0.start()
        t1.start()
        self.assertTrue(t1.is_alive())
        self.assertTrue(t0.is_alive())
        t1.join()
        self.assertTrue(t0.is_alive())
        self.assertFalse(t1.is_alive())
        t0.join()

        # check that waiting on the same objects a second time with a longer timeout works
        t0 = threading.Thread(
            target=dragon.mpbridge.DragonContext.wait,
            args=(queues,),
            kwargs={
                "timeout": 0.05,
            },
        )
        t1 = threading.Thread(
            target=dragon.mpbridge.DragonContext.wait,
            args=(queues,),
            kwargs={
                "timeout": 0.1,
            },
        )
        t0.start()
        t1.start()
        self.assertTrue(t1.is_alive())
        self.assertTrue(t0.is_alive())
        t0.join()
        self.assertTrue(t1.is_alive())
        self.assertFalse(t0.is_alive())
        t1.join()

        # the following is related to PE-44688
        # calling close() on the queues is useful when combined with
        # #define CHECK_POINTER on bcast.c
        # for q in queues:
        #     q.close()

        # make sure we shut down all service threads of WaitService class
        # before moving to the next test
        dragon.mpbridge.context._wait_service.__del__()
        # Reset global _wait_service so that init() is called again
        # and the garbage_collect thread initiated
        dragon.mpbridge.context._wait_service = None


if __name__ == "__main__":
    unittest.main()
