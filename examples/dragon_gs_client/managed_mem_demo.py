""" The serialized descriptor for the managed memory allocation is passed from the input channel to the output channel to access the allocation where the pids
for the processes are written and stored.

    * Coordinating process creates two channels (input, output) and gets serialized descriptors of each
    * Coordinating process creates an allocation from the default Pool of 32 B and writes its PID into the first 8 byes and 0 everywhere else.
    * For the three processes started by the coordinating process, the pid and any payload is written to the first 0s found in the 32 bytes.
    * Once all the descriptors and payload are written to the message, the message is printed and the GS objects are cleaned up.

"""
import sys
import os

import dragon.infrastructure.parameters as dparm
import dragon.utils as du
import dragon.infrastructure.facts as dfacts
from dragon.infrastructure.process_desc import ProcessOptions
import dragon.globalservices.channel as dgchan
import dragon.globalservices.process as dgprocess
from dragon.managed_memory import MemoryPool, MemoryAlloc
from dragon.channels import Channel


def worker(input_channel_sdesc, output_channel_sdesc):
    """The workers write their pids to the managed memory allocation."""

    input_channel_desc = du.B64.str_to_bytes(input_channel_sdesc)
    output_channel_desc = du.B64.str_to_bytes(output_channel_sdesc)

    ch_in = Channel.attach(input_channel_desc)
    ch_out = Channel.attach(output_channel_desc)

    recv_handle = ch_in.recvh()
    send_handle = ch_out.sendh()

    recv_handle.open()
    send_handle.open()

    serialized_mem_descr = recv_handle.recv_bytes()

    # Worker process attaches to the memory allocation created by coordinating process

    mem = MemoryAlloc.attach(serialized_mem_descr)
    memview = mem.get_memview()

    i = 0
    found = False
    while i < 32 and not found:

        # write pid of process to first set of 8 bytes equal to 0

        if int.from_bytes(memview[i : i + 8], "little") == 0:
            memview[i : i + 8] = os.getpid().to_bytes(8, "little")
            found = True
        i += 8

    send_handle.send_bytes(serialized_mem_descr)

    mem.detach()

    recv_handle.close()
    send_handle.close()

    ch_in.detach()
    ch_out.detach()


def main():
    """The coordinating process creates the managed memory allocation, the two channels, and the processes.
    The first 8 bytes of the managed memory are dedicated to the coordinating process's pid."""

    default_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)

    input_channel = dgchan.create(default_muid, user_name="input-channel")
    output_channel = dgchan.create(default_muid, user_name="output-channel")

    # The coord_in channel is where the coordinating process waits for the message on output. The coord_out channel is the input channel where the coordinating process sends the serialized memory descriptor for the memory allocation to the worker processes.
    coord_in = Channel.attach(output_channel.sdesc)
    coord_out = Channel.attach(input_channel.sdesc)

    recv_handle = coord_in.recvh()
    send_handle = coord_out.sendh()

    recv_handle.open()
    send_handle.open()

    # Coordinating process attaches to the default memory pool

    mpool = MemoryPool.attach(coord_in.get_pool().serialize())

    # Memory allocation of 32 bytes is created in the default memory pool

    mem = mpool.alloc(32)
    memview = mem.get_memview()

    # Write coordinating process PID to first 8 bytes

    memview[:8] = os.getpid().to_bytes(8, "little")

    cmd = sys.executable
    wdir = "."
    cmd_params = [
        os.path.basename(__file__),
        "worker",
        du.B64.bytes_to_str(input_channel.sdesc),
        du.B64.bytes_to_str(output_channel.sdesc),
    ]

    options = ProcessOptions(make_inf_channels=True)
    processes = []

    for _ in range(3):
        p = dgprocess.create(cmd, wdir, cmd_params, None, options=options)
        processes.append(p.p_uid)

    for _ in range(len(processes)):
        send_handle.send_bytes(mem.serialize())
        recv_handle.recv_bytes()

    memview = mem.get_memview()

    # Convert memory allocation data from bytes to integers/pids

    pid_list = [int.from_bytes(memview[i : i + 8], "little") for i in range(0, len(memview), 8)]
    print("Completed message with all pids:", pid_list)

    recv_handle.close()
    send_handle.close()

    coord_in.detach()
    coord_out.detach()

    dgprocess.multi_join(processes, join_all=True)

    dgchan.destroy(input_channel.c_uid)
    dgchan.destroy(output_channel.c_uid)

    mpool.destroy()


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            worker(sys.argv[2], sys.argv[3])
        else:
            sys.stdout.flush()
            main()
    except Exception as e:
        print(e)
