#!/usr/bin/env python3

import time
import sys
from dragon.channels import Channel, Message, ChannelSendH, ChannelRecvH
from dragon.managed_memory import MemoryPool

POOL_NAME = "pydragon_channel_test"
POOL_SIZE = 1073741824  # 1GB
POOL_UID = 0

N_REPEATS = 10000
N_MSGS = 100
MSG_WORDS = 4096
C_UID = 0
B_PER_MBLOCK = 1024
BILL = 1000000000
MSG_SIZE = 4 * MSG_WORDS  # memoryviews in integer format take 4 byte ints

mpool = MemoryPool(POOL_SIZE, POOL_NAME, POOL_UID)
ch = Channel(mpool, C_UID)

ch_ser = ch.serialize()

send_hdl = ch.sendh()
send_hdl.open()

recv_hdl = ch.recvh()
recv_hdl.open()

msg = Message().create_alloc(mpool, MSG_SIZE)
msg_view = msg.bytes_memview()
msg_view = msg_view.cast("i")

for i in range(0, MSG_WORDS):
    msg_view[i] = i

print(f"Measuring performance for {MSG_SIZE} byte messages")

start_time = time.monotonic_ns()

for j in range(N_REPEATS):
    for i in range(N_MSGS):
        send_hdl.send(msg)

    for i in range(N_MSGS):
        recv_msg = recv_hdl.recv()

        if i == (N_MSGS - 1):
            recv_view = recv_msg.bytes_memview()
            recv_view = recv_view.cast("i")
            for k in range(0, MSG_WORDS):
                if recv_view[k] != k:
                    print(f"Expected {k} at index {k} but got {recv_view[k]}")
                    sys.exit(0)

        recv_msg.destroy()

end_time = time.monotonic_ns()
total = 1.0 * (end_time - start_time) * 1e-9
mrate = N_REPEATS * N_MSGS / total
uni_lat = 5e5 * total / (N_REPEATS * N_MSGS)
bandwidth = MSG_SIZE * N_REPEATS * N_MSGS / (1024 * 1024 * total)
print(
    "Perf: pt2pt message rate = {:0.3f} msgs/sec, uni-directional latency = {:0.3f} usec, BW = {:0.3f} MiB/s".format(
        mrate, uni_lat, bandwidth
    )
)

recv_hdl.close()
send_hdl.close()
ch.destroy()
mpool.destroy()
