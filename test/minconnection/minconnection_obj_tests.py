#!/usr/bin/env python3

"""Test script for basic MinConnection object"""

import multiprocessing
import subprocess
import unittest

import dragon.infrastructure.minconnection as dimc
import dragon.infrastructure.messages as dmsg
import dragon.channels as dch
import dragon.managed_memory as dmm


# tolerate double-detach on a channel
def soft_detach_chan(the_chan):
    try:
        the_chan.detach()
    except dch.ChannelError as dche:
        if dche.Errors.FAIL == dche.ex_code:
            if not dche.args[0].startswith("Could not detach"):
                raise dche


# tolerate double-detach on a pool
def soft_detach_pool(the_pool):
    try:
        the_pool.detach()
    except dmm.DragonPoolDetachFail:
        # Expect DetachFail, don't raise
        pass


def parse_echo(the_msgstr):
    msg = dmsg.parse(the_msgstr)
    return msg.serialize()


def echo_end_last(input_sc, input_sp, output_sc, output_sp, last_str, mutator=lambda x: x):
    """Echos messages coming from the input to the output.

    Exits when last_str is seen (and echoed)
    """

    last_str = last_str.strip()

    input_chan = dch.Channel.attach(input_sc)
    output_chan = dch.Channel.attach(output_sc)
    input_pool = dmm.MemoryPool.attach(input_sp)
    output_pool = dmm.MemoryPool.attach(output_sp)

    input_mc = dimc.MinConnection(input_pool, input_chan, reading=True)
    output_mc = dimc.MinConnection(output_pool, output_chan, reading=False)

    rec_str = None
    while rec_str != last_str:
        rec_str = input_mc.recv().strip()

        if rec_str != last_str:
            out_str = mutator(rec_str)
        else:
            out_str = rec_str

        output_mc.send(out_str)

    input_mc.close()
    output_mc.close()

    soft_detach_chan(input_chan)
    soft_detach_chan(output_chan)
    soft_detach_pool(input_pool)
    soft_detach_pool(output_pool)


def worker_pickling_echo_end_last(writer_mc, reader_mc, last_str, mutator=lambda x: x):
    last_str = last_str.strip()

    rec_str = None
    while rec_str != last_str:
        rec_str = reader_mc.recv().strip()

        if rec_str != last_str:
            out_str = mutator(rec_str)
        else:
            out_str = rec_str

        writer_mc.send(out_str)

    writer_mc.close()
    reader_mc.close()

    soft_detach_pool(writer_mc.the_channel)
    soft_detach_pool(reader_mc.the_channel)
    soft_detach_pool(writer_mc.the_pool)
    soft_detach_pool(reader_mc.the_pool)


class MinConnectionTest(unittest.TestCase):
    def setUp(self):
        username = subprocess.check_output("whoami").decode().strip()
        self.pool_name = "mctest_" + username
        self.pool_size = 2**30
        self.pool_uid = 17
        self.mpool = dmm.MemoryPool(self.pool_size, self.pool_name, self.pool_uid)
        self.mpool_ser = self.mpool.serialize()

    def tearDown(self) -> None:
        self.mpool.destroy()

    def make_in_and_out_channels(self):
        in_channel_uid = 42
        in_chan = dch.Channel(self.mpool, in_channel_uid)
        in_chan_ser = in_chan.serialize()

        out_channel_uid = 43
        out_chan = dch.Channel(self.mpool, out_channel_uid)
        out_chan_ser = out_chan.serialize()

        return in_chan, in_chan_ser, out_chan, out_chan_ser

    def test_nothing(self):
        in_chan, _, out_chan, _ = self.make_in_and_out_channels()
        in_chan.destroy()
        out_chan.destroy()

    def test_echo(self):
        in_chan, in_chan_ser, out_chan, out_chan_ser = self.make_in_and_out_channels()

        inbound_mc = dimc.MinConnection(self.mpool, in_chan, reading=True)
        outbound_mc = dimc.MinConnection(self.mpool, out_chan, reading=False)

        words = "hyena hyena hyena hyena"
        lastword = "nomorehyena"

        echo_process = multiprocessing.Process(
            target=echo_end_last, args=(out_chan_ser, self.mpool_ser, in_chan_ser, self.mpool_ser, lastword)
        )
        echo_process.start()

        for k in range(10):
            outbound_mc.send(words)
            rec = inbound_mc.recv()
            self.assertEqual(words.strip(), rec.strip())

        outbound_mc.send(lastword)
        rec = inbound_mc.recv()
        self.assertEqual(lastword.strip(), rec.strip())

        inbound_mc.close()
        outbound_mc.close()

        echo_process.join()
        in_chan.destroy()
        out_chan.destroy()

    def test_pickled_path_echo(self):
        in_chan, in_chan_ser, out_chan, out_chan_ser = self.make_in_and_out_channels()

        their_reader_mc = dimc.MinConnection(self.mpool, in_chan, reading=True)
        my_writer_mc = dimc.MinConnection(self.mpool, in_chan, reading=False)
        their_writer_mc = dimc.MinConnection(self.mpool, out_chan, reading=False)
        my_reader_mc = dimc.MinConnection(self.mpool, out_chan, reading=True)

        words = "hyena Hyena hyena Hyena"
        lastword = "no more Hyena"

        echo_process = multiprocessing.Process(
            target=worker_pickling_echo_end_last, args=(their_writer_mc, their_reader_mc, lastword)
        )
        echo_process.start()

        for k in range(10):
            my_writer_mc.send(words)
            rec = my_reader_mc.recv()
            self.assertEqual(words.strip(), rec.strip())

        my_writer_mc.send(lastword)
        rec = my_reader_mc.recv()
        self.assertEqual(lastword.strip(), rec.strip())

        their_writer_mc.close()
        their_reader_mc.close()
        my_writer_mc.close()
        my_reader_mc.close()

        echo_process.join()

        in_chan.destroy()
        out_chan.destroy()

    def test_msg_echo(self):
        in_chan, in_chan_ser, out_chan, out_chan_ser = self.make_in_and_out_channels()

        inbound_mc = dimc.MinConnection(self.mpool, in_chan, reading=True)
        outbound_mc = dimc.MinConnection(self.mpool, out_chan, reading=False)

        lastword = "nomorehyena"

        echo_process = multiprocessing.Process(
            target=echo_end_last, args=(out_chan_ser, self.mpool_ser, in_chan_ser, self.mpool_ser, lastword, parse_echo)
        )
        echo_process.start()

        for msg in (dmsg.GSIsUp(1), dmsg.BEHalted(2), dmsg.GSHalted(3)):
            outbound_mc.send(msg.serialize())
            rec = inbound_mc.recv()
            return_msg = dmsg.parse(rec)
            self.assertEqual(type(msg), type(return_msg))
            self.assertEqual(msg.get_sdict(), return_msg.get_sdict())

        outbound_mc.send(lastword)
        rec = inbound_mc.recv()
        self.assertEqual(lastword.strip(), rec.strip())

        inbound_mc.close()
        outbound_mc.close()

        echo_process.join()
        in_chan.destroy()
        out_chan.destroy()


if __name__ == "__main__":
    unittest.main()
