#!/usr/bin/env python3

"""An example target for start_single...
Two scripts in one - a 'driver' and a 'worker'.

The 'driver' makes a couple channels thru the interface and uses them to
communicate with a worker it launches.
"""

import sys
import time

import dragon.channels as dchan

import dragon.globalservices.api_setup as dga
import dragon.globalservices.process as dgp
import dragon.globalservices.channel as dgch
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.parameters as dparm
import pickle


def fake_blocking_recv_bytes(handle):
    while True:
        try:
            rv = handle.recv_bytes()
            break
        except dchan.ChannelRecvError as cre:
            if cre.ex_code == dchan.ChannelRecvEx.Errors.CH_EMPTY:
                time.sleep(0.001)
            else:
                raise cre
    return rv


def driver():
    print("hello from the driver, p_uid {}".format(dparm.this_process.my_puid))
    dga.connect_to_infrastructure()
    print("connected to the infrastructure!")

    default_pool_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)
    channel_1_desc = dgch.create(default_pool_muid)
    channel_2_desc = dgch.create(default_pool_muid)

    print("made a couple channels")
    print("channel 1: {}".format(channel_1_desc))
    print("channel 2: {}".format(channel_2_desc))

    channel_1 = dchan.Channel.attach(channel_1_desc.sdesc)
    channel_2 = dchan.Channel.attach(channel_2_desc.sdesc)

    print("attached to cuid: {}".format(channel_1.cuid))
    print("attached to cuid: {}".format(channel_2.cuid))

    # gonna send out the ids because that's more multi-nodey
    target_input_id = channel_1.cuid
    target_output_id = channel_2.cuid

    our_argdata = pickle.dumps((target_input_id, target_output_id))
    print("starting target process")
    proc_desc = dgp.create_with_argdata(
        exe=sys.executable, run_dir="", env={}, args=[sys.argv[0], "t"], argdata=our_argdata
    )

    sender = channel_1.sendh()
    message = "hyenas".encode()
    sender.open()
    sender.send_bytes(message)
    sender.close()

    recvr = channel_2.recvh()
    recvr.open()
    rec_msg = fake_blocking_recv_bytes(recvr)
    recvr.close()

    if rec_msg != message:
        print("sent {} but got back {}".format(message, rec_msg))
    else:
        print("messages matched")

    print("joining target process")
    dgp.join(proc_desc.p_uid)

    dgch.destroy(channel_1.cuid)
    dgch.destroy(channel_2.cuid)
    print("destroyed the channels")
    exit(0)


def target():
    print("+++hello from target p_uid {}".format(dparm.this_process.my_puid))
    dga.connect_to_infrastructure()
    print("+++connected to infrastructure")
    args = pickle.loads(dga._ARG_PAYLOAD)
    inbound_cuid, outbound_cuid = args
    print(f"+++inbound cuid: {inbound_cuid}\t outbound cuid {outbound_cuid}")

    input_desc = dgch.query(inbound_cuid)
    output_desc = dgch.query(outbound_cuid)

    print("+++target channels queried")

    input_chan = dchan.Channel.attach(input_desc.sdesc)
    output_chan = dchan.Channel.attach(output_desc.sdesc)

    print("+++target channels attached to")

    recvr = input_chan.recvh()
    sender = output_chan.sendh()
    recvr.open()
    sender.open()
    msg = fake_blocking_recv_bytes(recvr)
    print("+++got: {}".format(msg))
    sender.send_bytes(msg)
    sender.close()
    recvr.close()
    input_chan.detach()
    output_chan.detach()
    print("+++target channels detached from")
    exit(0)


def usage_exit():
    print("usage: [d|t]")
    exit(1)


if __name__ == "__main__":
    if 1 == len(sys.argv):
        driver()

    if 2 != len(sys.argv):
        usage_exit()

    if "d" == sys.argv[1]:
        driver()
    elif "t" == sys.argv[1]:
        target()
    else:
        usage_exit()
