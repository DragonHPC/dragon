#!/usr/bin/env python3

import unittest
import sys
import os
import time

import dragon
import dragon.channels as dch
import dragon.globalservices.channel as dgchan
import dragon.globalservices.process as dgprocess

from dragon.infrastructure.channel_desc import ChannelOptions
from dragon.infrastructure.parameters import this_process
from dragon.infrastructure.facts import default_pool_muid_from_index
from dragon.infrastructure.process_desc import ProcessOptions
import dragon.utils as du


def attach_proc(ser_desc_str):
    sdesc = du.B64.str_to_bytes(ser_desc_str)
    channel = dch.Channel.attach(sdesc)
    dgchan.get_refcnt(channel.cuid)
    dgchan.release_refcnt(channel.cuid)
    channel.detach()


@unittest.skip(f"PE-42136 : Fails if run with other GS unit tests.")
class TestGSRefcounting(unittest.TestCase):

    def test_refcnt(self):

        m_uid = default_pool_muid_from_index(this_process.index)

        the_options = ChannelOptions(ref_count=True)
        the_options.local_opts.capacity = 1

        descr = dgchan.create(m_uid=m_uid, user_name=f"test_refcnt_{os.getpid()}", options=the_options)

        # refcount == 1

        cname = descr.name
        cuid = descr.c_uid

        channel = dch.Channel.attach(descr.sdesc)

        channel.detach()

        descr = dgchan.query(cuid)  # channel should be alive
        self.assertTrue(descr.state == descr.State.ACTIVE)

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        puids = []

        for _ in range(0, 4):
            p = dgprocess.create(
                cmd,
                wdir,
                [os.path.basename(__file__), "attach_proc", du.B64.bytes_to_str(descr.sdesc)],
                None,
                options=options,
            )
            puids.append(p.p_uid)

        for puid in puids:
            dgprocess.join(puid, timeout=None)
            descr = dgprocess.query(puids[0])
            self.assertTrue(descr.state == descr.State.DEAD)

        # refcount == 1

        descr = dgchan.query(cuid)  # channel is alive
        self.assertTrue(descr.state == descr.State.ACTIVE)

        dgchan.release_refcnt(cuid)  # channel needs to die now

        for i in range(0, 10):
            descr = dgchan.query(cuid)  # channel is not alive
            if descr.state == descr.State.DEAD:
                break
            time.sleep(0.1)

        self.assertTrue(descr.state == descr.State.DEAD)


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "attach_proc":
        attach_proc(sys.argv[2])
    else:
        unittest.main()
