#!/usr/bin/env python3

import time
import unittest
import os
import multiprocessing as mp
import dragon.infrastructure.parameters as dparms
from dragon.utils import B64
from dragon.channels import (
    Channel,
    Message,
    ChannelSendH,
    ChannelRecvH,
    ChannelError,
    ChannelSendError,
    ChannelRecvError,
    OwnershipOnSend,
    LockType,
    FlowControl,
    ChannelFull,
    ChannelEmpty,
    ChannelHandleNotOpenError,
    ChannelRecvTimeout,
    ChannelSendTimeout,
    EventType,
    ChannelBarrierBroken,
    ChannelBarrierReady,
    ChannelFlags,
    ChannelSet,
    ChannelSetTimeout,
    EventType,
    SPIN_WAIT,
    ADAPTIVE_WAIT,
)
from dragon.managed_memory import MemoryPool, DragonMemoryError
import sys

BARRIER_CHANNEL_CAPACITY = 5
MAX_SPINNERS = 5


def worker_attach_detach(ch_ser, pool_ser):
    mpool = MemoryPool.attach(pool_ser)
    ch = Channel.attach(ch_ser, mpool)
    sendh = ch.sendh()
    sendh.open()

    msg = Message.create_alloc(mpool, 512)
    mview = msg.bytes_memview()
    mview[0:5] = b"Hello"
    sendh.send(msg)

    sendh.close()
    msg.destroy()
    mpool.detach()
    ch.detach()


def worker_pickled_attach(ch, mpool):
    sendh = ch.sendh()
    sendh.open()

    msg = Message.create_alloc(mpool, 512)
    mview = msg.bytes_memview()
    mview[:5] = b"Howdy"
    sendh.send(msg)

    sendh.close()
    msg.destroy()
    mpool.detach()
    ch.detach()


def worker_send_recv(id, ch_ser, ch2_ser, pool_ser):
    try:
        mpool = MemoryPool.attach(pool_ser)
        ch = Channel.attach(ch_ser, mpool)
        ch2 = Channel.attach(ch2_ser, mpool)
        sendh = ch.sendh(wait_mode=SPIN_WAIT)
        sendh.open()

        msg = Message.create_alloc(mpool, 32)
        mview = msg.bytes_memview()
        mview[0:5] = b"Hello"
        sendh.send(msg)

        recvh = ch2.recvh(wait_mode=ADAPTIVE_WAIT)
        recvh.open()

        msg = recvh.recv()
        msg.destroy()
        recvh.close()
        ch.detach()
        sys.exit(0)

    except Exception as ex:
        print(ex)
        sys.exit(1)


def worker_fill_poll_empty(ch_ser, pool_ser):
    try:
        mpool = MemoryPool.attach(pool_ser)
        ch = Channel.attach(ch_ser, mpool)
        sendh = ch.sendh()
        sendh.open()

        for j in range(3):
            for i in range(BARRIER_CHANNEL_CAPACITY):
                msg = Message.create_alloc(mpool, 512)
                mview = msg.bytes_memview()
                mview[0:5] = b"Hello"
                sendh.send(msg)

            if not ch.poll(event_mask=EventType.POLLEMPTY, timeout=30):
                print("Error in polling for empty")
                sys.exit(1)

    except Exception as ex:
        print(ex)
        sys.exit(1)


def worker_empty_poll_full(ch_ser):
    try:
        ch = Channel.attach(ch_ser)
        recvh = ch.recvh()
        recvh.open()

        for j in range(3):
            if not ch.poll(event_mask=EventType.POLLFULL, timeout=30):
                print("Error in polling for full")
                sys.exit(1)

            for i in range(BARRIER_CHANNEL_CAPACITY):
                msg = recvh.recv()
                mview = msg.bytes_memview()
                if mview[0:5] != b"Hello":
                    print("Error in received message")
                    sys.exit(1)

    except Exception as ex:
        print(ex)
        sys.exit(1)


def worker_barrier_wait(ch_ser):
    try:
        ch = Channel.attach(ch_ser)
        recvh = ch.recvh()
        recvh.open()

        try:
            if not ch.poll(event_mask=EventType.POLLBARRIER, timeout=30):
                print("Error calling Barrier Poll", flush=True)
                sys.exit(1)
        except ChannelBarrierReady:
            ch.poll(event_mask=EventType.POLLBARRIER_RELEASE, timeout=30)

        msg = recvh.recv(timeout=30)
        x = int.from_bytes(msg.bytes_memview(), byteorder=sys.byteorder, signed=True)
        if x < -1 or x > BARRIER_CHANNEL_CAPACITY:
            print(f"Error: Got Barrier Value {x}", flush=True)
            sys.exit(1)

        if x == -1:
            # 2 is used to communicate an abort on the barrier back to caller.
            sys.exit(2)

        sys.exit(0)

    except ChannelBarrierBroken as ex:
        # return a 2 on the exit to indicate a barrier broken exception.
        sys.exit(2)

    except Exception as ex:
        print("There was an exception while running the barrier wait process.", flush=True)
        print(ex, flush=True)
        sys.exit(1)


class ChannelCreateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.pool_name = f"pydragon_channel_test_{os.getpid()}"
        pool_size = 1073741824  # 1GB
        pool_uid = 1
        cls.mpool = MemoryPool(pool_size, cls.pool_name, pool_uid)

    @classmethod
    def tearDownClass(cls):
        cls.mpool.destroy()

    def test_pool_create_destroy(self):
        pool = pool_name = "pydragon_pool_test"
        pool_size = 1073741824  # 1GB
        pool_uid = 42
        mpool = MemoryPool(pool_size, pool_name, pool_uid)
        mpool.destroy()
        mpool = MemoryPool(pool_size, pool_name, pool_uid)
        mpool.destroy()

    def test_create_destroy(self):
        ch = Channel(self.mpool, 1)
        ch.destroy()
        with self.assertRaises(ChannelError):
            ch.serialize()

    def test_with_stmt(self):
        msg = Message.create_alloc(self.mpool, 1)
        mview = msg.bytes_memview()
        mview[0] = 1

        with Channel(self.mpool, 1) as ch:
            with ch.recvh() as recvh:
                with self.assertRaises(TimeoutError):
                    recvh.recv(timeout=1.5)

            with ch.sendh() as sendh:
                sendh.send(msg)

            with ch.recvh() as recvh:
                msg2 = recvh.recv(timeout=1.5)
                mview2 = msg2.bytes_memview()
                self.assertEqual(1, mview2[0])

    def test_with_stmt_on_attach(self):
        msg = Message.create_alloc(self.mpool, 1)
        mview = msg.bytes_memview()
        mview[0] = 1

        # Make sure we're getting back a copy of the serializer, not a reference
        ch = Channel(self.mpool, 1)
        ch_ser = ch.serialize()  # Will return a pre-loaded copy of the serializer based on the above detach call
        with ch.sendh() as sendh:
            sendh.send(msg)

        with Channel.attach(ch_ser) as ch2:
            with ch2.recvh() as recvh:
                msg2 = recvh.recv(timeout=1.5)
                mview2 = msg2.bytes_memview()
                self.assertEqual(1, mview2[0])

        ch = Channel.attach(ch_ser)
        ch.destroy()

    def test_create_destroy_repeat(self):
        for i in range(10):
            ch = Channel(self.mpool, 1)
            ch.destroy()
            with self.assertRaises(ChannelError):
                ch.serialize()

    def test_negative_uid(self):
        # Causes overflow error since the parameter type is uint64_t, can't cast from negative
        with self.assertRaises(OverflowError):
            Channel(self.mpool, -1)

    def test_zero_uid(self):
        ch = Channel(self.mpool, 0)
        ch.destroy()

    def test_string_uid(self):
        with self.assertRaises(TypeError):
            Channel(self.mpool, "0")

    def test_small_message(self):
        ch = Channel(self.mpool, 1)
        sendh = ch.sendh()
        recvh = ch.recvh()
        msg = Message.create_alloc(self.mpool, 1)
        mview = msg.bytes_memview()
        mview[0] = 1

        sendh.open()
        sendh.send(msg)
        sendh.close()
        msg.destroy()

        recvh.open()
        msg = recvh.recv()
        mview = msg.bytes_memview()
        self.assertEqual(1, mview[0])
        recvh.close()
        msg.destroy()
        ch.destroy()

    @unittest.skip("AICI-1537")
    def test_landing_pad_serialized(self):
        ch = Channel(self.mpool, 1)

        msg = Message.create_alloc(self.mpool, 1500)
        mview = msg.bytes_memview()
        for i in range(1500):
            mview[i] = 1

        recv_msg = Message.create_alloc(self.mpool, 2000)

        sendh = ch.sendh()
        recvh = ch.recvh()

        sendh.open()
        sendh.send(msg)
        sendh.close()

        recvh.open()
        recv_msg = recvh.recv(recv_msg)
        recvh.close()

        recv_mview = recv_msg.bytes_memview()
        self.assertEqual(len(mview), len(recv_mview))

        msg.destroy()
        recv_msg.destroy()
        ch.destroy()

    def test_attr_channel(self):
        with self.assertRaises(ChannelError):
            ch = Channel(self.mpool, 1, block_size=10)

        with self.assertRaises(ChannelError):
            ch = Channel(self.mpool, 1, block_size=256, capacity=0)

        with self.assertRaises(TypeError):
            ch = Channel()
            ch.recvh()

        ch = Channel(self.mpool, 1, block_size=256, capacity=10)
        sendh = ch.sendh()
        recvh = ch.recvh()
        msg = Message.create_alloc(self.mpool, 1)
        mview = msg.bytes_memview()
        mview[0] = 1

        sendh.open()
        sendh.send(msg)
        sendh.close()
        msg.destroy()

        recvh.open()
        msg = recvh.recv()
        mview = msg.bytes_memview()
        self.assertEqual(1, mview[0])
        recvh.close()

        self.assertEqual(ch.capacity, 10)
        self.assertEqual(ch.block_size, 256)

        msg.destroy()
        ch.destroy()

    def test_channel_full(self):
        ch = Channel(self.mpool, 1, block_size=256, capacity=1)
        sendh = ch.sendh()
        recvh = ch.recvh()
        msg = Message.create_alloc(self.mpool, 1)
        mview = msg.bytes_memview()
        mview[0] = 1
        sendh.open()
        recvh.open()
        sendh.send(msg)

        msg2 = Message.create_alloc(self.mpool, 1)
        mview2 = msg2.bytes_memview()
        mview2[0] = 1

        with self.assertRaises(TimeoutError):
            sendh.send(msg2, blocking=True, timeout=1)

        msg = recvh.recv()
        msg.destroy()
        msg2.destroy()
        sendh.close()
        recvh.close()
        ch.destroy()

    def test_attr_channel2(self):
        ch = Channel(self.mpool, 1, fc_type=FlowControl.RESOURCES_FLOW_CONTROL, lock_type=LockType.GREEDY)
        sendh = ch.sendh()
        recvh = ch.recvh()
        msg = Message.create_alloc(self.mpool, 1)
        mview = msg.bytes_memview()
        mview[0] = 1

        sendh.open()
        sendh.send(msg)
        sendh.close()
        msg.destroy()

        recvh.open()
        msg = recvh.recv()
        mview = msg.bytes_memview()
        self.assertEqual(1, mview[0])
        recvh.close()

        msg.destroy()
        ch.destroy()

    def test_multiple_channels(self):
        channels = []
        for i in range(0, 5):
            ch = Channel(self.mpool, i)
            channels.append(ch)
            sendh = ch.sendh()
            msg = Message.create_alloc(self.mpool, 512)
            mview = msg.bytes_memview()
            mview[1] = i
            sendh.open()
            sendh.send(msg)
            sendh.close()
            msg.destroy()

        for i in range(0, 5):
            ch = channels[i]
            recvh = ch.recvh()
            recvh.open()
            msg = recvh.recv()
            mview = msg.bytes_memview()
            self.assertEqual(i, mview[1])
            recvh.close()
            msg.destroy()
            ch.destroy()

    @unittest.skip("Cython auto-truncates floats to integer types where possible.  Need to determine behavior.")
    def test_uid_float(self):
        with self.assertRaises(TypeError):
            Channel(self.mpool, 1.5)

    def test_serialize_after_detach(self):
        # Make sure we can store a serializer after detaching
        ch = Channel(self.mpool, 1)
        ch.detach(serialize=True)
        ch_ser = ch.serialize()
        ch.attach(ch_ser)
        ch.destroy()

    @unittest.skip("Not valid yet. Need to do some investigation as to similar problem.")
    def test_destroy_after_detach(self):
        # Make sure we can store a serializer after detaching
        ch = Channel(self.mpool, 1, flags=ChannelFlags.MASQUERADE_AS_REMOTE)
        ch.detach()
        ch.destroy()

    def test_serialize_lifecycle(self):
        # Make sure we're getting back a copy of the serializer, not a reference
        ch = Channel(self.mpool, 1)
        ch.detach(serialize=True)  # Stores a serializer in the object and sets a flag indicating such
        ch_ser = ch.serialize()  # Will return a pre-loaded copy of the serializer based on the above detach call
        del ch  # the __del__ call will check a flag to see if serialize was ever called, and free if necessary
        ch = Channel.attach(ch_ser)
        ch.destroy()

    def test_get_pool(self):
        # Simple test to see if we get a usable memorypool object
        ch = Channel(self.mpool, 1)
        mpool2 = ch.get_pool()
        mem = mpool2.alloc(512)
        mem.free()
        ch.destroy()

    def test_message_count(self):
        ch = Channel(self.mpool, 1)
        sh = ch.sendh()
        rh = ch.recvh()
        sh.open()
        rh.open()

        x = bytearray("Hello", "utf-8")
        sh.send_bytes(x, len(x))

        self.assertEqual(1, ch.num_msgs)

        rmsg = rh.recv()
        self.assertEqual(0, ch.num_msgs)
        rmsg.destroy()

        ch.destroy()

    def test_serialized_info_no_attach(self):
        cuid = 90210
        ch = Channel(self.mpool, cuid)
        ch_ser = ch.serialize()

        s_cuid = Channel.serialized_uid(ch_ser)
        self.assertEqual(s_cuid, cuid, "ID's do not match")

        ch.destroy()

    def test_serialized_pool_info_no_attach(self):
        cuid = 90210
        ch = Channel(self.mpool, cuid)
        ch_ser = ch.serialize()

        (pool_uid, pool_fname) = Channel.serialized_pool_uid_fname(ch_ser)
        self.assertEqual(pool_uid, 1, "Pool IDs don't match, expected 1")
        self.assertEqual(pool_fname, "/_dragon_" + self.pool_name + "_manifest")

        ch.destroy()


class ChannelTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pool_name = f"pydragon_channel_test_{os.getpid()}"
        pool_size = 1073741824  # 1GB
        pool_uid = 1
        cls.mpool = MemoryPool(pool_size, pool_name, pool_uid)

        # This is wrong below. There appears to be a problem where the check for whether a pool
        # is local in a channel is not returning true when it should. This may be based on hostid.
        # # This must be done because the channels attach expects to find
        # # an environment where it can find a default pool to attach to.
        # pool_ser = B64.bytes_to_str(cls.mpool.serialize())
        # os.environ[dparms.this_process.default_pd] = pool_ser

    @classmethod
    def tearDownClass(cls):
        cls.mpool.destroy()

    def setUp(self):
        self.ch = Channel(self.mpool, c_uid=1, max_spinners=MAX_SPINNERS)
        self.ch2 = Channel(self.mpool, c_uid=2, capacity=BARRIER_CHANNEL_CAPACITY, max_spinners=MAX_SPINNERS)

    def tearDown(self):
        self.ch.destroy()
        self.ch2.destroy()

    def test_poll_timeout(self):
        timeout = 0.4
        start = time.monotonic()
        result = self.ch.poll(timeout=timeout)
        delta = time.monotonic() - start

        self.assertFalse(result)
        self.assertGreater(delta, timeout, "delta should be bigger than timeout")

    def test_unopened_recv_hdl(self):
        recvh = self.ch.recvh()
        with self.assertRaises(ChannelHandleNotOpenError):
            recvh.recv()

    def test_poll_time(self):
        # Without a message it times out
        self.assertEqual(self.ch.poll(timeout=1.1), False)

    def test_poll(self):
        recvh = self.ch.recvh()
        recvh.open()

        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLIN), False)
        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLIN | EventType.POLLOUT), True)

        sendh = self.ch.sendh()
        sendh.open()
        msg = Message.create_alloc(self.mpool, 32)
        mview = msg.bytes_memview()
        mview[0:5] = b"Hello"
        sendh.send(msg, timeout=0)

        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLIN), True)

        # There is a message
        self.assertEqual(self.ch.poll(), True)
        recvh.recv()

        # Without a message it times out
        self.assertEqual(self.ch.poll(timeout=1.5), False)

        msg.destroy()
        sendh.close()
        recvh.close()

    def test_poll_empty_full(self):
        ch_ser = self.ch2.serialize()
        pool_ser = self.mpool.serialize()

        proc1 = mp.Process(target=worker_fill_poll_empty, args=(ch_ser, pool_ser))
        proc1.start()

        proc2 = mp.Process(target=worker_empty_poll_full, args=(ch_ser,))
        proc2.start()

        proc1.join()
        proc2.join()
        self.assertTrue(proc1.exitcode == 0, "The fill and poll for empty process did not complete successfully")
        self.assertTrue(proc2.exitcode == 0, "The empty and poll for full process did not complete successfully")

    def test_poll_barrier(self):
        ch_ser = self.ch2.serialize()
        proc_list = []

        for i in range(BARRIER_CHANNEL_CAPACITY):
            proc = mp.Process(target=worker_barrier_wait, args=(ch_ser,))
            proc.start()
            proc_list.append(proc)

        for i in range(BARRIER_CHANNEL_CAPACITY):
            proc_list[i].join()
            self.assertEqual(proc_list[i].exitcode, 0, "Non-zero exitcode from barrier proc")

    def test_poll_barrier2(self):
        ch_ser = self.ch2.serialize()
        try:
            for j in range(5):
                proc_list = []

                for i in range(BARRIER_CHANNEL_CAPACITY * 2):
                    proc = mp.Process(target=worker_barrier_wait, args=(ch_ser,))
                    proc.start()
                    proc_list.append(proc)

                for i in range(BARRIER_CHANNEL_CAPACITY * 2):
                    proc_list[i].join()
                    self.assertEqual(proc_list[i].exitcode, 0, "Non-zero exitcode from barrier proc")
        except Exception as ex:
            print(ex)
            print("**** Error in test_poll_barrier2")
            print(f"*** Receivers={self.ch2.blocked_receivers}")
            print(f"*** Number of messages={self.ch2.num_msgs}")
            print(f"*** Barrier Count is {self.ch2.barrier_count}")

    def test_poll_barrier3_with_abort(self):
        ch_ser = self.ch2.serialize()
        success = 0
        aborted = 0
        try:

            proc_list = []

            for i in range(BARRIER_CHANNEL_CAPACITY):
                proc = mp.Process(target=worker_barrier_wait, args=(ch_ser,))
                proc.start()
                proc_list.append(proc)

            for i in range(BARRIER_CHANNEL_CAPACITY):
                proc_list[i].join()
                ec = proc_list[i].exitcode
                if ec == 0:
                    success += 1
                elif ec == 2:
                    aborted += 1

            proc_list = []

            for i in range(BARRIER_CHANNEL_CAPACITY * 2):
                proc = mp.Process(target=worker_barrier_wait, args=(ch_ser,))
                if i == 4:
                    self.ch2.poll(event_mask=EventType.POLLBARRIER_ABORT)
                proc.start()
                proc_list.append(proc)

            for i in range(BARRIER_CHANNEL_CAPACITY * 2):
                proc_list[i].join()
                ec = proc_list[i].exitcode
                if ec == 0:
                    success += 1
                elif ec == 2:
                    aborted += 1

            self.assertEqual(success, 5, f"Five should have succeeded. Instead there were {success}")
            self.assertEqual(aborted, 10, f"Ten should have aborted, instead there were {aborted}")
            self.ch2.poll(event_mask=EventType.POLLRESET)

        except Exception as ex:
            print(ex)
            print("**** Error in test_poll_barrier3_with_abort")
            print(f"*** Receivers={self.ch2.blocked_receivers}")
            print(f"*** Number of messages={self.ch2.num_msgs}")
            print(f"*** Barrier Count is {self.ch2.barrier_count}")

    def test_poll_barrier_abort(self):
        ch_ser = self.ch2.serialize()
        proc_list = []

        for i in range(BARRIER_CHANNEL_CAPACITY - 1):
            proc = mp.Process(target=worker_barrier_wait, args=(ch_ser,))
            proc.start()
            proc_list.append(proc)

        time.sleep(1)
        self.ch2.poll(event_mask=EventType.POLLBARRIER_ABORT, timeout=5)

        for i in range(BARRIER_CHANNEL_CAPACITY - 1):
            proc_list[i].join()
            self.assertEqual(proc_list[i].exitcode, 2, "Non-zero exitcode from barrier proc")

        self.ch2.poll(event_mask=EventType.POLLRESET, timeout=5)

        proc_list = []

        for i in range(BARRIER_CHANNEL_CAPACITY):
            proc = mp.Process(target=worker_barrier_wait, args=(ch_ser,))
            proc.start()
            proc_list.append(proc)

        for i in range(BARRIER_CHANNEL_CAPACITY):
            proc_list[i].join()
            self.assertEqual(proc_list[i].exitcode, 0, "Non-zero exitcode from barrier proc")

    def test_blocking_recv(self):
        recvh = self.ch.recvh()
        with self.assertRaises(ChannelHandleNotOpenError):
            recvh.recv(timeout=1)

        recvh.open()

        with self.assertRaises(ChannelRecvTimeout) as ex:
            recvh.recv(timeout=1.5)

        with self.assertRaises(TimeoutError) as ex:
            recvh.recv(timeout=1)

        with self.assertRaises(ChannelEmpty) as ex:
            recvh.recv(timeout=0)

        sendh = self.ch.sendh()
        sendh.open()
        msg = Message.create_alloc(self.mpool, 32)
        mview = msg.bytes_memview()
        mview[0:5] = b"Hello"
        sendh.send(msg, timeout=0)

        msg = recvh.recv()

        self.assertNotEqual(msg, None)

        msg.destroy()

        with self.assertRaises(ChannelEmpty) as ex:
            recvh.recv(timeout=0)

        sendh.close()
        recvh.close()

    def test_empty_buffer(self):
        recvh = self.ch.recvh()
        recvh.open()

        with self.assertRaises(ChannelEmpty) as ex:
            recvh.recv(blocking=False)

        recvh.close()

    def test_blocking_spin_wait_to_idle_wait(self):
        proc_list = []
        ser_ch = self.ch.serialize()
        ser_ch2 = self.ch2.serialize()
        ser_pool = self.mpool.serialize()

        for k in range(MAX_SPINNERS * 2):
            proc = mp.Process(target=worker_send_recv, args=(k, ser_ch, ser_ch2, ser_pool))
            proc_list.append(proc)
            proc.start()

        recvh = self.ch.recvh(wait_mode=SPIN_WAIT)
        recvh.open()

        for _ in range(MAX_SPINNERS * 2):
            msg = recvh.recv()
            msg.destroy()

        recvh.close()

        sendh = self.ch2.sendh(wait_mode=SPIN_WAIT)
        sendh.open()

        for _ in range(MAX_SPINNERS * 2):
            msg = Message.create_alloc(self.mpool, 32)
            mview = msg.bytes_memview()
            mview[0:5] = b"Hello"
            sendh.send(msg, timeout=0)

        for proc in proc_list:
            proc.join()
            self.assertEqual(proc.exitcode, 0)

    def test_fill_buffer_exception(self):
        sendh = self.ch.sendh()
        sendh.open()
        msg = Message.create_alloc(self.mpool, 32)
        mview = msg.bytes_memview()
        mview[0:5] = b"Hello"
        i = 0
        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLOUT), True)
        try:
            while True:
                sendh.send(msg, timeout=0)
                i += 1
        except ChannelFull:
            pass
        except:
            msg.destroy()
            sendh.close()
            self.fail("Expected ChannelFull exception")

        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLOUT), False)

        try:
            while True:
                sendh.send(msg, timeout=1.1)
                i += 1
        except ChannelSendTimeout:
            pass
        except:
            msg.destroy()
            sendh.close()
            self.fail("Expected ChannelSendTimeout exception")

        msg.destroy()
        sendh.close()

    def test_fill_then_empty_buffer(self):
        sendh = self.ch.sendh()
        sendh.open()
        msg = Message.create_alloc(self.mpool, 32)
        mview = msg.bytes_memview()
        mview[0:5] = b"Hello"
        i = 0
        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLOUT), True)
        try:
            while True:
                sendh.send(msg, timeout=0)
                i += 1
        except ChannelFull:
            pass
        except:
            msg.destroy()
            sendh.close()
            self.fail("Expected ChannelFull exception")

        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLOUT), False)
        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLOUT | EventType.POLLIN), True)
        msg.destroy()
        sendh.close()
        recvh = self.ch.recvh()
        recvh.open()

        j = 0
        try:
            while True:
                msg = recvh.recv(blocking=False)
                mview = msg.bytes_memview()
                self.assertEqual(b"Hello", mview[0:5])
                msg.destroy()
                j += 1
        except ChannelEmpty:
            pass
        except:
            recvh.close()
            self.fail("Expected ChannelEmpty exception")

        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLIN), False)
        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLOUT), True)
        self.assertEqual(self.ch.poll(timeout=0, event_mask=EventType.POLLOUT | EventType.POLLIN), True)

        self.assertEqual(i, j)
        recvh.close()

    def test_create_attach_detach_destroy(self):
        # Create a channel, serialize it to a new process
        # Send a message from the new process
        # Receive the message from this process
        # Tests attach functionality from differing processes
        ch_ser = self.ch.serialize()
        pool_ser = self.mpool.serialize()

        proc = mp.Process(target=worker_attach_detach, args=(ch_ser, pool_ser))
        proc.start()
        proc.join()

        recvh = self.ch.recvh()
        recvh.open()
        msg = recvh.recv()
        mview = msg.bytes_memview()
        self.assertEqual(b"Hello", mview[0:5])
        msg.destroy()

    def test_create_attach_via_pickle_detach_destroy(self):
        # Create a channel, pickle it to a new process
        # Send a message from the new process
        # Receive the message from this process
        # Tests attach functionality from differing processes

        proc = mp.Process(target=worker_pickled_attach, args=(self.ch, self.mpool))
        proc.start()
        proc.join()

        recvh = self.ch.recvh()
        recvh.open()
        msg = recvh.recv()
        mview = msg.bytes_memview()
        self.assertEqual(b"Howdy", mview[:5])
        msg.destroy()

    def test_send_bytes(self):
        sendh = self.ch.sendh()
        recvh = self.ch.recvh()
        sendh.open()
        recvh.open()
        x = bytearray(1024)
        x[0:5] = b"Hello"
        sendh.send_bytes(x, 1024)
        msg = recvh.recv()
        mview = msg.bytes_memview()
        self.assertEqual(mview[0:5], b"Hello")
        msg.destroy()

    def test_recv_bytes_timeout(self):
        recvh = self.ch.recvh()
        with self.assertRaises(ChannelHandleNotOpenError):
            recvh.recv_bytes(timeout=1)

        recvh.open()

        with self.assertRaises(ChannelRecvTimeout) as ex:
            recvh.recv_bytes(timeout=1.5)

        with self.assertRaises(TimeoutError) as ex:
            recvh.recv_bytes(timeout=1)

        with self.assertRaises(ChannelEmpty) as ex:
            recvh.recv_bytes(timeout=0)

        sendh = self.ch.sendh()
        sendh.open()
        my_bytes = b"Hello"
        sendh.send_bytes(my_bytes, timeout=0)

        msg = recvh.recv_bytes()

        self.assertEqual(msg, my_bytes)

        with self.assertRaises(ChannelEmpty) as ex:
            recvh.recv_bytes(timeout=0)

        sendh.close()
        recvh.close()

    def test_send_to_dest(self):
        # Assert messages can be delivered to specific payload objects
        sendh = self.ch.sendh()
        recvh = self.ch.recvh()
        sendh.open()
        recvh.open()

        s_msg = Message.create_alloc(self.mpool, 512)
        r_msg = Message.create_alloc(self.mpool, 512)
        s_mview = s_msg.bytes_memview()
        s_mview[0:5] = b"Hello"
        sendh.send(s_msg, r_msg)
        s_msg.destroy()

        r_msg = recvh.recv()
        r_mview = r_msg.bytes_memview()

        self.assertEqual(b"Hello", r_mview[0:5])
        r_msg.destroy()

    def test_send_transfer(self):
        # Assert messages can be delivered by transferring ownership.
        sendh = self.ch.sendh()
        recvh = self.ch.recvh()
        sendh.open()
        recvh.open()

        s_msg = Message.create_alloc(self.mpool, 512)
        s_mview = s_msg.bytes_memview()
        s_mview[0:5] = b"Hello"
        sendh.send(s_msg, ownership=sendh.transfer_ownership_on_send)

        r_msg = recvh.recv()
        r_mview = r_msg.bytes_memview()

        self.assertEqual(b"Hello", r_mview[0:5])
        r_msg.destroy()

    def test_send_copy(self):
        sendh = self.ch.sendh()
        recvh = self.ch.recvh()
        sendh.open()
        recvh.open()

        s_msg = Message.create_alloc(self.mpool, 512)
        s_mview = s_msg.bytes_memview()
        s_mview[0:5] = b"Hello"
        sendh.send(s_msg, ownership=OwnershipOnSend.copy_on_send)
        s_msg.destroy()

        r_msg = recvh.recv()
        r_mview = r_msg.bytes_memview()

        self.assertEqual(b"Hello", r_mview[0:5])
        r_msg.destroy()

    def test_recv_to_dest(self):
        # Assert messages can be received into specific payload objects
        sendh = self.ch.sendh()
        recvh = self.ch.recvh()
        sendh.open()
        recvh.open()

        s_msg = Message.create_alloc(self.mpool, 512)
        s_mview = s_msg.bytes_memview()
        s_mview[0:5] = b"Hello"
        sendh.send(s_msg)
        s_msg.destroy()

        r_msg = Message.create_alloc(self.mpool, 512)
        recvh.recv(r_msg)
        r_mview = r_msg.bytes_memview()

        self.assertEqual(b"Hello", r_mview[0:5])
        r_msg.destroy()

    def test_send_to_dest_and_recv_to_dest(self):
        # Make sure that if we receive a message to the same place we sent it
        #    that it works correctly
        sendh = self.ch.sendh()
        recvh = self.ch.recvh()
        sendh.open()
        recvh.open()

        s_msg = Message.create_alloc(self.mpool, 512)
        r_msg = Message.create_alloc(self.mpool, 512)
        s_mview = s_msg.bytes_memview()
        s_mview[0:5] = b"Hello"
        sendh.send(s_msg, r_msg)
        s_msg.destroy()

        recvh.recv(r_msg)
        r_mview = r_msg.bytes_memview()

        self.assertEqual(b"Hello", r_mview[0:5])
        r_msg.destroy()

        sendh.close()
        recvh.close()

    def test_recv_bytes(self):
        # Assert that messages can be received as python byte objects
        sendh = self.ch.sendh()
        recvh = self.ch.recvh()
        sendh.open()
        recvh.open()

        x = bytearray(1024)
        x[0:5] = b"Hello"
        sendh.send_bytes(x, 1024)
        recv_x = recvh.recv_bytes()
        self.assertEqual(recv_x[0:5], b"Hello")

        sendh.close()
        recvh.close()

    def test_send_bytes_sliced(self):
        # Assert that slices get sent with correct sizes
        sendh = self.ch.sendh()
        recvh = self.ch.recvh()
        sendh.open()
        recvh.open()

        x = bytearray(1024)
        x[0:11] = b"Hello World"
        sendh.send_bytes(x[0:5])
        recv_x = recvh.recv_bytes()
        self.assertEqual(recv_x[0:5], b"Hello")

        sendh.close()
        recvh.close()


class MessageTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pool_name = "pydragon_channel_test"
        pool_size = 1073741824  # 1GB
        pool_uid = 1
        cls.mpool = MemoryPool(pool_size, pool_name, pool_uid)

    @classmethod
    def tearDownClass(cls):
        cls.mpool.destroy()

    def test_create_destroy(self):
        msg = Message.create_alloc(self.mpool, 512)
        msg.destroy()
        with self.assertRaises(ChannelError):
            msg.bytes_memview()

    @unittest.skip("This is not currently supported.")
    def test_zero_size(self):
        msg = Message.create_alloc(self.mpool, 0)
        self.assertEqual(len(msg.tobytes()), 0)
        self.assertEqual(len(msg.bytes_memview()), 0)

    def test_negative_size(self):
        with self.assertRaises(OverflowError):
            Message.create_alloc(self.mpool, -1)

    def test_memview(self):
        msg = Message.create_alloc(self.mpool, 512)
        mview = msg.bytes_memview()
        mview[0:5] = b"Hello"
        self.assertEqual(b"Hello", mview[0:5])
        msg.destroy()
        # Assert our memory is dead
        self.assertNotEqual(b"Hello", mview[0:5])


def worker_send_to_chset(ch_ser, pool_ser):
    # give some time to the head process to start
    # polling on the channel set

    try:
        mpool = MemoryPool.attach(pool_ser)
        ch = Channel.attach(ch_ser, mpool)
        sendh = ch.sendh()
        sendh.open()

        msg = Message.create_alloc(mpool, 32)
        mview = msg.bytes_memview()
        mview[0:5] = b"Hello"
        sendh.send(msg)

        msg.destroy()
        sendh.close()

    except Exception as ex:
        print(ex)


class ChannelSetTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pool_name = f"pydragon_channelset_test_{os.getpid()}"
        pool_size = 1073741824  # 1GB
        pool_uid = 1
        cls.mpool = MemoryPool(pool_size, pool_name, pool_uid)

    @classmethod
    def tearDownClass(cls):
        cls.mpool.destroy()

    def setUp(self):
        self.channel_list = []
        for i in range(3):
            self.channel_list.append(Channel(self.mpool, c_uid=i))
        self.ch_set = ChannelSet(self.mpool, self.channel_list)

    def tearDown(self):
        del self.ch_set
        for ch in self.channel_list:
            ch.destroy()

    def test_poll_timeout(self):
        timeout = 0.4
        start = time.monotonic()

        with self.assertRaises(ChannelSetTimeout):
            self.ch_set.poll(timeout=timeout)

        delta = time.monotonic() - start
        self.assertGreater(delta, timeout, "delta should be bigger than timeout")

    def test_poll(self):
        ch_ser = self.channel_list[0].serialize()
        pool_ser = self.mpool.serialize()

        recvh = self.channel_list[0].recvh()
        recvh.open()

        # polling on an empty channelset
        with self.assertRaises(ChannelSetTimeout):
            self.ch_set.poll(timeout=0)

        # we are on DRAGON_SYNC mode
        # this process blocks on polling the channel set
        # while another process will send a message to a channel
        proc = mp.Process(target=worker_send_to_chset, args=(ch_ser, pool_ser))
        proc.start()
        # now, the first channel should have a message
        result, revent = self.ch_set.poll()
        self.assertEqual((result, revent), (self.channel_list[0], EventType.POLLIN))
        self.assertEqual(revent, EventType.POLLIN)

        # receive the message to empty the channel
        recvh.recv()

        # # Without a message it times out
        with self.assertRaises(ChannelSetTimeout):
            self.ch_set.poll(timeout=0.5)

        recvh.close()
        proc.join()

    def test_poll_more(self):
        # empty channelset for now
        with self.assertRaises(ChannelSetTimeout):
            self.ch_set.poll(timeout=0)

        pool_ser = self.mpool.serialize()

        for i in range(len(self.channel_list)):
            # send a message to channel i, poll, and
            # then receive
            recvh = self.channel_list[i].recvh()
            recvh.open()

            ch_ser = self.channel_list[i].serialize()

            proc = mp.Process(target=worker_send_to_chset, args=(ch_ser, pool_ser))
            proc.start()

            # there is a message in channel i
            result, revent = self.ch_set.poll()
            self.assertEqual((result, revent), (self.channel_list[i], EventType.POLLIN))

            # receive the message and poll again
            recvh.recv()
            # Without a message it times out
            with self.assertRaises(ChannelSetTimeout):
                self.ch_set.poll(timeout=0.5)

            recvh.close()
            proc.join()

        # verify we have an empty channel set
        with self.assertRaises(ChannelSetTimeout):
            self.ch_set.poll(timeout=0)

    def test_poll_with_dragon_no_sync(self):
        # Create a separate channel set with DRAGON_NO_SYNC
        # by initializing the set with capture_all_events=False
        channel_list = []
        for i in range(3):
            channel_list.append(Channel(self.mpool, c_uid=i + 10))
        ch_set = ChannelSet(self.mpool, channel_list, capture_all_events=False)

        # poll an empty channel set
        with self.assertRaises(ChannelSetTimeout):
            self.ch_set.poll(timeout=0)

        # send a message to one of the channels
        sendh = channel_list[1].sendh()
        sendh.open()
        msg = Message.create_alloc(self.mpool, 32)
        mview = msg.bytes_memview()
        mview[0:5] = b"Hello"
        sendh.send(msg)

        # since the message was sent before polling
        # we should not be able to catch the message
        # and so the poll should timeout
        with self.assertRaises(ChannelSetTimeout):
            self.ch_set.poll(timeout=1)

        msg.destroy()
        sendh.close()
        for ch in channel_list:
            ch.destroy()

    def test_poll_when_set_is_full(self):
        # empty channelset for now
        with self.assertRaises(ChannelSetTimeout):
            self.ch_set.poll(timeout=0)

        pool_ser = self.mpool.serialize()

        proc_list = []
        # send a message to every channel
        for i in range(len(self.channel_list)):
            ch_ser = self.channel_list[i].serialize()
            proc = mp.Process(target=worker_send_to_chset, args=(ch_ser, pool_ser))
            proc.start()
            proc_list.append(proc)

        # Poll on a non-empty channel set.
        # Since the channel set is initialized with DRAGON_SYNC
        # we need to poll as many times as the sends, otherwise
        # the send operation will block
        for i in range(len(self.channel_list)):
            # this should return one of the channels
            # does not matter which and we cannot know which
            result, _ = self.ch_set.poll()
            self.assertNotEqual(result, False)

        for proc in proc_list:
            proc.join()


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    unittest.main(verbosity=2)
