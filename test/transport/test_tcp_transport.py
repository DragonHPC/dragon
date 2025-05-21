import unittest
import time
import os
import random
import pickle
import logging

logging.captureWarnings(True)

import multiprocessing as mp
from dragon.channels import (
    Channel,
    Message,
    ChannelSendH,
    ChannelRecvH,
    register_gateways_from_env,
    discard_gateways,
    ChannelFlags,
    EventType,
)
from dragon.managed_memory import MemoryPool
from dragon.utils import B64
import asyncio
from collections import defaultdict

from dragon.mpbridge.queues import DragonQueue
import dragon.infrastructure.connection as dconn

SUCCESS = 0
FAILURE = 1
TIMEOUT = 2

SILENT = True


def status(*args, **kwargs):
    if not SILENT:
        print(*args, **kwargs)


from dragon.transport.tcp.transport import Transport
from dragon.transport.tcp.transport import LOOPBACK_ADDRESS_IPv4
from dragon.transport.tcp.server import Server
from dragon.transport.tcp.client import Client
from dragon.transport.tcp.task import cancel_all_tasks

GW_ENV_PREFIX = "DRAGON_GW"


def mk_msg(pool, nbytes):
    msg = Message.create_alloc(pool, nbytes)
    mview = msg.bytes_memview()
    for i in range(nbytes):
        mview[i] = i % 256

    return msg


def validate_msg(msg):
    # Here we can do any message validation that needs
    # to be done.
    mview = msg.bytes_memview()

    for i in range(len(mview)):
        if mview[i] != i % 256:
            print(f"The message contents were incorrectly {mview[i]} at location {i}")
            return i

    return len(mview)


async def agent(channels, control):
    # Create a Transport instance to handle Request/Response I/O.
    # By default, Transport._handle_send() implements a loopback
    # strategy by calling Tranport._handle_recv().
    transport = Transport(LOOPBACK_ADDRESS_IPv4)

    # Create a Server instance to process Requests (i.e., remote
    # channel operations).
    server = Server(transport)

    # Create Client instances for each gateway channel to send
    # Requests to the Server and process Responses in order to
    # complete gateway messages.
    clients = {}

    # Using the default transport, the node mapping should
    # ensure every address is the transport's address, for
    # consistency.
    nodes = defaultdict(lambda: transport.addr)

    # Assuming the gateway channels have been created and
    # CHANNELS is a list of the corresponding names...
    for name in channels:
        gw_ser_str = os.environ[name]
        gw_ser = B64.from_str(gw_ser_str).decode()
        clients[name] = Client(gw_ser, transport, nodes)

    # Note that Transport is not a TaskMixin as there is nothing that
    # requires a separate thread of execution, so there is no need to
    # call transport.start() or await transport.wait_started() as
    # is the case with StreamTransport.

    # Start the server
    server.start(name=f"Transport-server")

    status("Transport service started.")

    # Start each client
    for name, client in clients.items():
        client.start(name=f"Transport-client-{name}")

    # At this point, the server and client tasks are running and you
    # can do whatever, but if you need a control loop...
    try:
        while control.value == 1:
            await asyncio.sleep(5.0)
    finally:
        # Shut down clients and server
        status("Transport service is shutting down")
        await asyncio.gather(*[c.stop() for c in clients.values()], return_exceptions=True)
        try:
            await server.stop()
        except:
            pass
        # Cancel all remaining tasks for good measure
        await cancel_all_tasks()


def start_transport(control):
    done = False
    id = 1
    channels = []
    while not done:
        id_str = GW_ENV_PREFIX + str(id)
        if id_str in os.environ:
            channels.append(id_str)
            id += 1
        else:
            done = True

    status("Added gateway channels to environment. Now starting transport service.")

    logging.disable(logging.ERROR)
    asyncio.run(agent(channels, control))


def add_gw_to_env(gw_ch, id):
    ser_gw = gw_ch.serialize()
    encoded_ser_gw = B64(ser_gw)
    os.environ[GW_ENV_PREFIX + str(id)] = str(encoded_ser_gw)


def echo_gw_and_attach():
    gw_channels = []
    id = 1
    id_str = GW_ENV_PREFIX + str(id)
    while id_str in os.environ:
        ser_gw = B64.from_str(os.environ[id_str])
        status("Found", id_str, "in environment")
        gw_ch = Channel.attach(ser_gw.decode())
        status("Attached to gateway channel", id)
        gw_channels.append(gw_ch)
        id += 1
        id_str = GW_ENV_PREFIX + str(id)


def get_msg_from_channel(ch_ser, msg_nbytes, ret_value, timeout=None):
    ch = Channel.attach(ch_ser)
    recvh = ChannelRecvH(ch)
    recvh.open()
    try:
        msg = recvh.recv(timeout=timeout)
        # Here we can do any message validation that needs
        # to be done.
        mview = msg.bytes_memview()
        if validate_msg(msg) == msg_nbytes:
            ret_value.value = SUCCESS
            status("Received message sent to masquerading remote channel and verified its contents.")
        else:
            ret_value.value = FAILURE
        recvh.close()
        ch.detach()
    except Exception as ex:
        print("Exception getting message from channel.")
        print(ex)
        ret_value.value = FAILURE


def send_msg_to_channel(ch_ser, msg_nbytes, ret_value, timeout=None, sleep_secs=0):
    time.sleep(sleep_secs)
    ch = Channel.attach(ch_ser)
    pool = ch.get_pool()
    sendh = ChannelSendH(ch)
    sendh.open()
    msg = mk_msg(pool, msg_nbytes)
    try:
        sendh.send(msg, timeout=timeout)
        sendh.close()
        ch.detach()
        ret_value.value = SUCCESS
    except Exception as ex:
        print("Exception sending message to channel.")
        print(ex)
        ret_value.value = FAILURE


class SingleNodeTransportBench(unittest.TestCase):
    """Unit tests for the Single Node emulation of a transport service."""

    @classmethod
    def setUpClass(cls):
        pool_name = f"dragon_transport_test_{os.getpid()}"
        pool_size = 1073741824  # 1GB Pool
        pool_uid = 1
        cls.mpool = MemoryPool(pool_size, pool_name, pool_uid)
        cls.gw_ch1 = Channel(cls.mpool, c_uid=1, block_size=256)
        add_gw_to_env(cls.gw_ch1, 1)

        cls.transport_running = mp.Value("i", 1, lock=False)
        cls.transport_proc = mp.Process(target=start_transport, args=(cls.transport_running,))
        cls.transport_proc.start()

        register_gateways_from_env()

        cls.opts = dconn.ConnectionOptions(default_pool=cls.mpool)

    @classmethod
    def tearDownClass(cls):
        cls.transport_running.value = 0
        cls.transport_proc.join()
        discard_gateways()
        cls.gw_ch1.destroy()
        cls.mpool.destroy()

    def setUp(self):
        self.user_ch1 = Channel(self.mpool, 3, flags=ChannelFlags.MASQUERADE_AS_REMOTE)
        self.user_ch2 = Channel(self.mpool, 4, flags=ChannelFlags.MASQUERADE_AS_REMOTE)

    def tearDown(self):
        self.user_ch1.destroy()
        self.user_ch2.destroy()

    def test_setup_and_teardown(self):
        pass

    def test_encoding_decoding(self):
        x = b"Hello World"
        y = B64(x)
        self.assertEqual(str(y), "SGVsbG8gV29ybGQ=")
        self.assertEqual(y.decode(), x)
        x = b"How are you?"
        y = B64(x)
        self.assertEqual(str(y), "SG93IGFyZSB5b3U/")
        self.assertEqual(y.decode(), x)

    def test_environment_attach(self):
        proc = mp.Process(target=echo_gw_and_attach)
        proc.start()
        proc.join()
        self.assertEqual(proc.exitcode, 0)

    def test_send(self):
        sendh = ChannelSendH(self.user_ch1)
        sendh.open()
        msg = mk_msg(self.mpool, 1)
        sendh.send(msg)
        # Pass the serialized original self.user_ch1
        # to get_msg_from_channel to verify that the
        # transport service forwarded the sent message
        # to the channel. Then that, combined with the
        # completion of the send in this test, will
        # verify the complete path from bootstrapping the
        # gw channels, through channels, the gw, and the
        # transport service and back out the other end.
        ch_ser = self.user_ch1.serialize()
        ret_value = mp.Value("i", FAILURE, lock=False)
        proc = mp.Process(target=get_msg_from_channel, args=(ch_ser, 1, ret_value))
        proc.start()
        proc.join()
        self.assertEqual(proc.exitcode, 0)
        self.assertEqual(ret_value.value, SUCCESS)

    def test_send_serialized(self):
        sendh = ChannelSendH(self.user_ch1)
        sendh.open()
        msg = mk_msg(self.mpool, 512)
        sendh.send(msg)
        # Pass the serialized original self.user_ch1
        # to get_msg_from_channel to verify that the
        # transport service forwarded the sent message
        # to the channel. Then that, combined with the
        # completion of the send in this test, will
        # verify the complete path from bootstrapping the
        # gw channels, through channels, the gw, and the
        # transport service and back out the other end.
        ch_ser = self.user_ch1.serialize()
        ret_value = mp.Value("i", FAILURE, lock=False)
        proc = mp.Process(target=get_msg_from_channel, args=(ch_ser, 512, ret_value))
        proc.start()
        proc.join()
        self.assertEqual(proc.exitcode, 0)
        self.assertEqual(ret_value.value, SUCCESS)

    def test_receive_timeout(self):
        recvh = ChannelRecvH(self.user_ch1)
        recvh.open()
        # No message is sent to the channel. This forces a timeout within
        # the transport agent since here the channel looks like a remote
        # channel.
        try:
            self.assertRaises(TimeoutError, recvh.recv, timeout=2)
        finally:
            recvh.close()

    def test_receive(self):
        recvh = ChannelRecvH(self.user_ch1)
        recvh.open()
        ch_ser = self.user_ch1.serialize()
        ret_value = mp.Value("i", FAILURE, lock=False)
        proc = mp.Process(target=send_msg_to_channel, args=(ch_ser, 1, ret_value))
        proc.start()
        msg = recvh.recv()
        # Here we can do any message validation that needs
        # to be done.
        mview = msg.bytes_memview()
        self.assertEqual(validate_msg(msg), 1, "The message did not have the correct contents")
        status("Received message from masquerading remote channel via remote receive and verified its contents.")
        recvh.close()
        proc.join()
        self.assertEqual(proc.exitcode, 0)
        self.assertEqual(ret_value.value, SUCCESS)

    def test_poll(self):
        status("Starting test_poll")
        # try:
        recvh = ChannelRecvH(self.user_ch1)
        recvh.open()
        ch_ser = self.user_ch1.serialize()
        ret_value = mp.Value("i", FAILURE, lock=False)
        proc = mp.Process(target=send_msg_to_channel, args=(ch_ser, 1, ret_value, None, 2))
        proc.start()
        rc = self.user_ch1.poll(event_mask=EventType.POLLIN)
        self.assertEqual(rc, True)
        msg = recvh.recv()
        # Here we can do any message validation that needs
        # to be done.
        mview = msg.bytes_memview()
        self.assertEqual(validate_msg(msg), 1, "The message did not have the correct contents")
        status(
            "Polled for and received message from masquerading remote channel via remote receive and verified its contents."
        )
        recvh.close()
        proc.join()
        self.assertEqual(proc.exitcode, 0)
        self.assertEqual(ret_value.value, SUCCESS)
        # except Exception as ex:
        #    self.assertTrue(False, "There was an exception in the test code.")

    @classmethod
    def _queue_proc(self, q, obj1, obj2):
        msg = q.get()
        assert obj1 == msg
        q.put(obj2)

        q.close()

    def test_queue_with_external_channel(self):
        # In the case of an externally managed channel, we need to
        # provide a memory pool which will be used as the buffer_pool
        # for the Many2ManyWriting adapter.
        # This is because we don't call the Dragon start method and
        # dparms.this_process.default_pd that we use in case of a
        # remote channel is not set in Queue.
        q = DragonQueue(m_uid=self.mpool, _ext_channel=self.user_ch1)

        y = [1, 2, 3]
        q.put(y)
        self.assertEqual(q.qsize(), 1, "We could not get the proper size from a queue on a remote channel.")
        x = q.get()
        self.assertEqual(x, y, "The Queue implementation did not pass a list correctly through it.")
        y = ["a", "b", "c"]
        q.put(y)
        x = q.get()
        self.assertEqual(x, y, "The Queue implementation did not pass a list correctly through it.")

        obj1 = {"hyena": "hyena"}
        obj2 = bytes(10000)
        q.put(obj1)
        proc = mp.Process(
            target=self._queue_proc,
            args=(
                q,
                obj1,
                obj2,
            ),
        )
        proc.start()
        proc.join()
        self.assertEqual(proc.exitcode, 0)
        msg = q.get()
        self.assertEqual(msg, obj2)

        q.close()

    def test_queue_ordering_with_external_channel(self):
        # In the case of an externally managed channel, we need to
        # provide a memory pool which will be used as the buffer_pool
        # for the Many2ManyWriting adapter.
        # This is because we don't call the Dragon start method and
        # dparms.this_process.default_pd that we use in case of a
        # remote channel is not set in Queue.
        q = DragonQueue(m_uid=self.mpool, _ext_channel=self.user_ch1)

        # This test assumes ordering on queue messages and tests that
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

    @staticmethod
    def _connection_proc(some_msg, connin, connout):
        if connin:
            msg = connin.recv()
            assert some_msg == msg
            connin.ghost_close()
        if connout:
            connout.send(some_msg)
            connout.ghost_close()

    def test_connection_with_external_channels(self):
        msg = random.randbytes(1024)

        connout = dconn.Connection(outbound_initializer=self.user_ch1, options=self.opts)
        connin = dconn.Connection(inbound_initializer=self.user_ch2, options=self.opts)
        pconnout = dconn.Connection(outbound_initializer=self.user_ch2, options=self.opts)
        pconnin = dconn.Connection(inbound_initializer=self.user_ch1, options=self.opts)
        proc = mp.Process(
            target=self._connection_proc,
            args=(
                msg,
                pconnin,
                pconnout,
            ),
        )
        proc.start()

        connout.send(msg)
        msg_back = connin.recv()
        self.assertEqual(msg, msg_back)

        proc.join()
        self.assertEqual(proc.exitcode, 0)
        connout.ghost_close()
        connin.ghost_close()
        pconnin.ghost_close()
        pconnout.ghost_close()

    @staticmethod
    def _connection_proc_bytes(some_bytes, connin, connout):
        if connout:
            connout.send_bytes(some_bytes)
            connout.ghost_close()
        if connin:
            rec_bytes = connin.recv_bytes()
            assert some_bytes == rec_bytes
            connin.ghost_close()

    def test_connection_with_external_channels_test_proc_send_bytes(self):
        connin = dconn.Connection(inbound_initializer=self.user_ch2, options=self.opts)
        pconnout = dconn.Connection(outbound_initializer=self.user_ch2, options=self.opts)

        for lg_size in range(25):
            some_bytes = random.randbytes(2**lg_size)
            proc = mp.Process(
                target=self._connection_proc_bytes,
                args=(
                    some_bytes,
                    None,
                    pconnout,
                ),
            )
            proc.start()

            rec_bytes = connin.recv_bytes()
            self.assertEqual(some_bytes, rec_bytes)
            proc.join()
            self.assertEqual(proc.exitcode, 0)

        connin.ghost_close()
        pconnout.ghost_close()

    def test_connection_with_external_channels_test_proc_recv_bytes(self):
        connout = dconn.Connection(outbound_initializer=self.user_ch1, options=self.opts)
        pconnin = dconn.Connection(inbound_initializer=self.user_ch1, options=self.opts)

        # The following for loop fails and we have added the above lines to at least test one case of
        # having the process receiving bytes
        for lg_size in range(25):
            some_bytes = random.randbytes(2**lg_size)
            proc = mp.Process(
                target=self._connection_proc_bytes,
                args=(
                    some_bytes,
                    pconnin,
                    None,
                ),
            )
            proc.start()
            connout.send_bytes(some_bytes)
            proc.join()
            self.assertEqual(proc.exitcode, 0)

        connout.ghost_close()
        pconnin.ghost_close()

    def pass_an_obj(self, obj):
        writer = dconn.Connection(outbound_initializer=self.user_ch1, options=self.opts)
        reader = dconn.Connection(inbound_initializer=self.user_ch1, options=self.opts)

        proc = mp.Process(
            target=self._connection_proc,
            args=(
                obj,
                None,
                writer,
            ),
        )
        proc.start()

        rec_obj = reader.recv()
        proc.join()
        self.assertEqual(proc.exitcode, 0)
        self.assertEqual(obj, rec_obj)

        writer.ghost_close()
        reader.ghost_close()

    def test_connection_with_external_channels_test_objs(self):
        self.pass_an_obj("hyena")
        self.pass_an_obj({"hyena": "hyena"})

        self.pass_an_obj(bytes(100000))

        self.pass_an_obj(bytes(200000))

        self.pass_an_obj(bytearray(2**20))

        for lg_size in range(25):
            self.pass_an_obj(bytearray(2**lg_size))

        for lg_size in range(25):
            self.pass_an_obj(random.randbytes(2**lg_size))

    def test_connection_with_external_channels_test_recv_bytes_from_send(self):
        # required for multiprocessing.Connection unit tests...
        writer = dconn.Connection(outbound_initializer=self.user_ch1, options=self.opts)
        reader = dconn.Connection(inbound_initializer=self.user_ch1, options=self.opts)

        my_obj = {"hyenas": "are awesome"}
        writer.send(my_obj)
        # would like this to raise, but....
        ser_obj = reader.recv_bytes()
        rec_obj = pickle.loads(ser_obj)
        self.assertEqual(my_obj, rec_obj)

        writer.ghost_close()
        reader.ghost_close()

    def test_connection_with_external_channels_test_recv_from_send_bytes(self):
        # again, weird usage but multiprocessing.Connection unit tests need
        # this functionality

        writer = dconn.Connection(outbound_initializer=self.user_ch1, options=self.opts)
        reader = dconn.Connection(inbound_initializer=self.user_ch1, options=self.opts)

        my_obj = {"hyenas": "are awesome"}
        ser_obj = pickle.dumps(my_obj)
        writer.send_bytes(ser_obj)
        rec_obj = reader.recv()
        self.assertEqual(my_obj, rec_obj)

        writer.ghost_close()
        reader.ghost_close()

    @staticmethod
    def _connection_proc_poll(msg, conn):
        rc = conn.poll(timeout=20.0)
        assert rc == True
        msg_recv = conn.recv()
        assert msg == msg_recv

    def test_connection_with_external_channels_test_poll(self):
        msg = random.randbytes(1024)
        writer = dconn.Connection(outbound_initializer=self.user_ch1, options=self.opts)
        reader = dconn.Connection(inbound_initializer=self.user_ch1, options=self.opts)

        proc = mp.Process(
            target=self._connection_proc_poll,
            args=(
                msg,
                reader,
            ),
        )
        proc.start()
        time.sleep(3)
        writer.send(msg)
        proc.join()
        self.assertEqual(proc.exitcode, 0)

        writer.ghost_close()
        reader.ghost_close()


if __name__ == "__main__":
    mp.set_start_method("spawn")
    unittest.main()
