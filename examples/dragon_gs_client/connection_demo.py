"""Single producer and single consumer communication scheme with Dragon Channels.

This example demonstrates the optimal way to communicate data that will be serialized
with Pickle or other serializers which interact with a file-like interface. It shows
how large payloads (larger than the Dragon Managed Memory Pool) can be communicated.

* Create a single Dragon Channel.
* Create one process that acts as the producer and one as the consumer. The producer
    sends a large size message, in this example 8GB is used, and the consumer waits
    until receiving the message.
"""

import sys
import os
import random
import pickle

import dragon.globalservices.channel as dgchan
import dragon.globalservices.process as dgprocess

import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.facts as dfacts
from dragon.infrastructure.process_desc import ProcessOptions

from dragon.channels import Channel, Peer2PeerWritingChannelFile, Peer2PeerReadingChannelFile


def producer(chan_name, msg):
    print(f"I am the producer.", end=" ")

    channel_desc = dgchan.query(chan_name)
    chan = Channel.attach(channel_desc.sdesc)

    my_size = sys.getsizeof(msg)
    print(f"Message size to be sent: {my_size} bytes.", flush=True)

    try:
        adapter = Peer2PeerWritingChannelFile(chan)
        adapter.open()
        pickle.dump(msg, file=adapter, protocol=5)
    finally:
        adapter.close()


def consumer(chan_name, msg):
    channel_desc = dgchan.query(chan_name)
    chan = Channel.attach(channel_desc.sdesc)

    try:
        adapter = Peer2PeerReadingChannelFile(chan)
        adapter.open()
        rcv_msg = pickle.load(adapter)
    finally:
        adapter.close()

    assert rcv_msg == msg, "The msg received is not correct. Something went wrong."
    print(f"I am the consumer. Successful communication.", flush=True)


def main():
    default_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)

    # ask GS to create a dragon channel
    channel = dgchan.create(default_muid, user_name="InputChannel")

    # create processes
    cmd = sys.executable
    wdir = "."
    options = ProcessOptions(make_inf_channels=True)
    proc1 = dgprocess.create(
        cmd, wdir, [os.path.basename(__file__), "sub", "producer", channel.name], None, options=options
    )
    proc2 = dgprocess.create(
        cmd, wdir, [os.path.basename(__file__), "sub", "consumer", channel.name], None, options=options
    )

    dgprocess.join(proc1.p_uid)
    dgprocess.join(proc2.p_uid)

    dgchan.destroy(channel.c_uid)


if __name__ == "__main__":

    # get size of default pool
    pool_size = int(os.getenv("DRAGON_DEFAULT_SEG_SZ"))

    print(f"{pool_size=}", flush=True)

    msg_size = 2 * pool_size  # message does not fit into pool
    msg = bytearray(msg_size)

    if len(sys.argv) > 1 and sys.argv[1] == "sub":
        if sys.argv[2] == "producer":
            producer(sys.argv[3], msg)
        else:
            consumer(sys.argv[3], msg)
    else:
        print(f"Memory Pool Size is {pool_size} bytes", flush=True)

        main()
