import unittest
import sys
import pickle
import time
import getpass
import queue
import numpy as np

import dragon
from dragon.native.queue import Queue
import multiprocessing as mp
from dragon.fli import DragonFLIError

from dragon.globalservices.process import create as process_create, join as process_join
from dragon.globalservices.channel import create
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.infrastructure.channel_desc import ChannelOptions
from dragon.managed_memory import MemoryPool
from dragon.channels import Channel
import dragon.utils as du


def _putter(env_str):

    bytes = du.B64.str_to_bytes(env_str)
    q, item = pickle.loads(bytes)

    q.put(item)

def _getter(q):
    q.get()

def _joiner(env_str):

    bytes = du.B64.str_to_bytes(env_str)
    q, item = pickle.loads(bytes)

    q.put(item)

    q.join()


def _attach(q):
    return


def _serialize(q, q_ser):
    assert q.serialize() == q_ser


def _fillit(q):
    x = 25
    while True:
        q.put(x, block=False)


def _poll(q):
    assert q.poll()


def _put_join(q, num_puts):
    for _ in range(num_puts):
        q.put(25)
    q.join()


def _put(q, num_puts):
    for i in range(num_puts):
        q.put(f"hello{i}")


def _get(q, num_puts):
    for i in range(num_puts):
        assert q.get() == f"hello{i}"


def _thread_wait(q, timeout, ev):
    received_obj = []
    q.thread_wait(timeout, ev, received_obj)
    assert len(received_obj) == 1
    assert received_obj[0] == q


class numPy2dPickler:
    def __init__(self, shape: tuple, data_type: np.dtype, chunk_size=0):
        self._shape = shape
        self._data_type = data_type
        self._chunk_size = chunk_size

    def dump(self, nparr, file) -> None:
        mv = memoryview(nparr)
        bobj = mv.tobytes()
        # print(f"Dumping {bobj=}", file=sys.stderr, flush=True)
        if self._chunk_size == 0:
            chunk_size = len(bobj)
        else:
            chunk_size = self._chunk_size

        for i in range(0, len(bobj), chunk_size):
            file.write(bobj[i : i + chunk_size])

    def load(self, file):
        obj = None
        try:
            while True:
                data = file.read(self._chunk_size)
                if obj is None:
                    # convert bytes to bytearray
                    view = memoryview(data)
                    obj = bytearray(view)
                else:
                    obj.extend(data)
        except EOFError:
            pass

        ret_arr = np.frombuffer(obj, dtype=self._data_type).reshape(self._shape)

        return ret_arr


class TestQueue(unittest.TestCase):

    def test_put_get(self):
        q = Queue()

        q.put(42)
        x = q.get()
        self.assertEqual(x, 42)

    def test_simple(self):
        """Test basic functionality"""

        q = Queue(joinable=False)
        item = "Queue-item-0"

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps((q, item), protocol=5))

        the_descr = process_create(cmd, wdir, [__file__, "putter", env_str], None, options=options)

        new_item = q.get()

        self.assertTrue(new_item == item)

        process_join(the_descr.p_uid)

    def test_joinable(self):
        """Test that the queue can be joinable"""

        q = Queue(joinable=True)

        self.assertTrue(q._joinable == True)

        item = "JQueue-item-0"

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps((q, item), protocol=5))

        the_descr = process_create(cmd, wdir, [__file__, "joiner", env_str], None, options=options)

        new_item = q.get()
        self.assertTrue(new_item == item)
        time.sleep(0.1)

        ready = process_join(the_descr.p_uid, timeout=0)  # worker remains blocked
        self.assertTrue(ready == None)

        q.task_done()  # unblock worker

        ready = process_join(the_descr.p_uid, timeout=None)  # must cleanly exit
        self.assertTrue(ready == 0)

    def test_attach_detach(self):
        q = Queue()
        proc = mp.Process(target=_attach, args=(q,))
        proc.start()
        proc.join()
        self.assertEqual(0, proc.exitcode)
        q.destroy()

    def test_create_with_pool(self):
        pool_name = f"queue_test_{getpass.getuser()}"
        pool_size = 1073741824  # 1GB
        mpool = MemoryPool.make_process_local(name=pool_name, size=pool_size)
        q = Queue(pool=mpool)
        q.put(25)
        q.put(45)
        self.assertEqual(q.get(), 25)
        self.assertEqual(q.get(), 45)
        q.destroy()
        mpool.deregister()
        mpool.destroy()

    def test_create_delete_mem_leak(self):
        pool_name = f"queue_test_{getpass.getuser()}"
        pool_size = 1073741824  # 1GB
        mpool = MemoryPool.make_process_local(name=pool_name, size=pool_size)
        before_space = mpool.free_space
        q = Queue(pool=mpool)
        q.put(42)

        proc = mp.Process(target=_getter, args=(q,))
        proc.start()
        proc.join()
        del q # This is non-blocking, hence the need for the loop below.
        after_space = 0
        iter = 0
        while after_space != before_space:
            time.sleep(0.1) # Make sure we yield when we are running with a single process for Dragon.
            after_space = mpool.free_space
            self.assertTrue(iter < 100, "We timed out waiting for the channel allocation to be freed. Memory leak or other freeing issue.")
            iter += 1

        mpool.deregister()
        mpool.destroy()

    def test_create_with_num_strms(self):
        num_strm = 5
        num_blocked_put = 2
        q = Queue(buffered=False, num_streams=num_strm)
        proc = mp.Process(target=_put, args=(q, num_strm + num_blocked_put))
        proc.start()
        proc.join(timeout=0)
        self.assertEqual(proc.exitcode, None)
        for i in range(num_blocked_put - 1):
            q.get()
            proc.join(timeout=0)
            self.assertEqual(proc.exitcode, None)
        q.get()
        proc.join(timeout=None)
        self.assertEqual(proc.exitcode, 0)
        q.destroy()

    def test_create_with_main_channel(self):
        main_ch = Channel.make_process_local()
        q = Queue(main_channel=main_ch)
        for i in range(8):
            q.put(f"hello_{i}")
        for i in range(8):
            self.assertEqual(q.get(), f"hello_{i}")
        q.destroy()
        main_ch.destroy_process_local()

    def test_create_with_mgr_channel(self):
        mgr_ch = Channel.make_process_local()
        q = Queue(buffered=False, mgr_channel=mgr_ch)
        q.destroy()
        mgr_ch.destroy_process_local()

    def test_create_with_sem_channel(self):
        pool_name = f"queue_test_{getpass.getuser()}"
        pool_size = 1073741824  # 1GB
        mpool = MemoryPool.make_process_local(name=pool_name, size=pool_size)
        local_opts = {"semaphore": True, "bounded_semaphore": False, "initial_sem_value": 0}
        the_options = ChannelOptions(ref_count=True, local_opts=local_opts)
        sem_ch_descr = create(m_uid=mpool.muid, options=the_options)
        sem_ch = Channel.attach(sem_ch_descr.sdesc)
        q = Queue(sem_channel=sem_ch, joinable=True)
        for i in range(8):
            q.put(f"hello_{i}")
        for i in range(8):
            self.assertEqual(q.get(), f"hello_{i}")
            q.task_done()
        q.destroy()
        sem_ch.destroy()
        mpool.deregister()
        mpool.destroy()

    def test_create_with_strm_channel(self):
        strm_chs = []
        num_strms = 8
        for _ in range(num_strms):
            ch = Channel.make_process_local()
            strm_chs.append(ch)
        q = Queue(buffered=False, strm_channels=strm_chs)
        for i in range(num_strms):
            q.put(f"hello_{i}")
        for i in range(num_strms):
            self.assertEqual(q.get(), f"hello_{i}")
        q.destroy()
        for ch in strm_chs:
            ch.destroy_process_local()

    # def test_create_with_policy(self)

    def test_serialize(self):
        q = Queue()
        proc = mp.Process(target=_serialize, args=(q, q.serialize()))
        proc.start()
        proc.join()
        self.assertEqual(0, proc.exitcode)
        q.destroy()

    def test_single_put_get(self):
        q = Queue()
        q.put("hello")
        self.assertEqual(q.get(), "hello")
        q.destroy()

    def test_single_multiple_put_get(self):
        q = Queue()
        for i in range(8):
            q.put(f"hello_{i}")
        for i in range(8):
            self.assertEqual(q.get(), f"hello_{i}")
        q.destroy()

    def test_put_get_with_custom_pickler(self):
        q = Queue(pickler=numPy2dPickler((2, 3), np.double))
        arr = [[0.12, 0.31, 3.4], [4.579, 5.98, 6.54]]
        value = np.array(arr)
        q.put(value)
        get_val = q.get()
        self.assertTrue(np.array_equal(value, get_val))
        q.destroy()

    def test_put_in_full_queue(self):
        q = Queue(maxsize=5)
        self.assertRaises(queue.Full, _fillit, q)
        q.destroy()

    def test_get_from_empty_queue(self):
        q = Queue()
        with self.assertRaises(queue.Empty):
            q.get(timeout=0.5)
        q.destroy()

    def test_put_nowait(self):
        q = Queue(maxsize=2)
        q.put(25)
        q.put(45)
        self.assertRaises(queue.Full, q.put_nowait, 5)
        q.destroy()

    def test_get_nowait(self):
        q = Queue()
        self.assertRaises(queue.Empty, q.get_nowait)
        q.destroy()

    def test_poll_empty_queue(self):
        q = Queue()
        res = q.poll(timeout=0.5)
        self.assertFalse(res)
        q.destroy()

    def test_poll_block(self):
        q = Queue()
        proc = mp.Process(target=_poll, args=(q,))
        proc.start()
        q.put(42)
        proc.join()
        self.assertEqual(proc.exitcode, 0)
        q.destroy()

    def test_full(self):
        q = Queue(maxsize=5)
        q.put(25)
        self.assertFalse(q.full())
        q.destroy()

    def test_full_from_filled_queue(self):
        q = Queue(maxsize=5)
        self.assertRaises(queue.Full, _fillit, q)
        self.assertTrue(q.full())
        q.destroy()

    def test_empty(self):
        q = Queue()
        self.assertTrue(q.empty())
        q.put(25)
        self.assertFalse(q.empty())
        q.destroy()

    def test_size(self):
        q = Queue()
        self.assertEqual(q.size(), 0)
        num_items = 10
        for i in range(num_items):
            q.put(26)
            self.assertEqual(q.size(), i + 1)
        for i in range(num_items, 0, -1):
            q.get()
            self.assertEqual(q.size(), i - 1)
        q.destroy()

    def test_task_done(self):
        q = Queue(joinable=True)
        proc = mp.Process(target=_put_join, args=(q, 1))
        proc.start()
        proc.join(timeout=0)
        self.assertEqual(proc.exitcode, None)
        q.task_done()
        proc.join(timeout=None)
        self.assertEqual(proc.exitcode, 0)
        q.destroy()

    def test_multiple_task_done(self):
        q = Queue(joinable=True)
        num_puts = 10
        proc = mp.Process(target=_put_join, args=(q, num_puts))
        proc.start()
        proc.join(timeout=0)
        self.assertEqual(proc.exitcode, None)
        for _ in range(num_puts):
            q.task_done()
        proc.join(timeout=None)
        self.assertEqual(proc.exitcode, 0)
        q.destroy()

    def test_join(self):
        q = Queue(joinable=True)
        q.put(25)
        with self.assertRaises(DragonFLIError):
            q.join(timeout=0)
        q.task_done()
        q.join()
        q.destroy()

    def test_thread_wait(self):
        ev = mp.Event()
        timeout = None
        q = Queue()
        proc = mp.Process(
            target=_thread_wait,
            args=(q, timeout, ev),
        )
        proc.start()

        proc.join(timeout=0)
        self.assertEqual(proc.exitcode, None)
        q.put(4)
        proc.join(timeout=None)
        self.assertEqual(proc.exitcode, 0)
        q.destroy()

    def test_create_without_main_mgr_str_channels(self):
        with self.assertRaises(DragonFLIError) as ex:
            q = Queue(buffered=False)
        self.assertEqual(ex.exception.lib_err, "DRAGON_INVALID_ARGUMENT")
        self.assertIn(
            "The main channel and the manager channel cannot both be null when the number of stream channels is 0.",
            ex.exception.lib_msg,
            "Expected message not found in the exception.",
        )

    def test_unbuffered_queue_with_mgr_channel(self):
        mgr_ch = Channel.make_process_local()
        strm_ch = Channel.make_process_local()
        num_puts = 8
        q = Queue(buffered=False, mgr_channel=mgr_ch)
        proc = mp.Process(target=_put, args=(q, num_puts))  # Hange to wait for stream channel.
        # ^ This is doing a blocking receive of a serialized stream channel from the manager channel, and perform
        # a put into the stream channel.
        proc.start()
        proc.join(timeout=0)
        self.assertEqual(proc.exitcode, None)
        for i in range(num_puts):
            self.assertEqual(
                f"hello{i}", q.get(stream_channel=strm_ch)
            )  # This will put the serialized stream channel into the manager channel, and do a blocking receive on the stream channel.
        proc.join()
        self.assertEqual(proc.exitcode, 0)
        q.destroy()
        mgr_ch.destroy_process_local()
        strm_ch.destroy_process_local()

    def test_unbuffered_queue_with_main_channel(self):
        main_ch = Channel.make_process_local()
        strm_ch = Channel.make_process_local()
        num_puts = 8
        q = Queue(buffered=False, main_channel=main_ch)
        proc = mp.Process(target=_get, args=(q, num_puts))  # Hange to wait for stream channel.
        proc.start()

        for i in range(num_puts):
            q.put(f"hello{i}", stream_channel=strm_ch)

        proc.join()
        self.assertEqual(proc.exitcode, 0)
        q.destroy()
        main_ch.destroy_process_local()
        strm_ch.destroy_process_local()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "putter":
        _putter(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "joiner":
        _joiner(sys.argv[2])
    else:
        mp.set_start_method("dragon")
        unittest.main()
