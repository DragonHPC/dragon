""" Pass a message between two Dragon processes using one Dragon channel. Uses Global Services and Channels
from Dragon API.
    * Create a single channel and process on default Dragon pool.  In the new process, attach to the Channel
    * passed as an argument via the serialized descriptor.  In the new process, use subprocess.run to execute the
    * shell command and get the output. Write that output into the attached Channel.  In the original process in
    * main, output the message.  Join the channel and the process with Global Services.
"""

import dragon.globalservices.channel as channel
import dragon.globalservices.process as process
from dragon.infrastructure.process_desc import ProcessOptions
import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.facts as dfacts

from dragon.channels import Channel, Message

import sys
import subprocess


def main():
    """The channel dragon is created in main. The data from dragon is printed in main."""

    try:

        if len(sys.argv) != 2:
            print("You must pass a shell command as the first argument")
            return

        shell_cmd = sys.argv[1]
        default_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)
        c_desc = channel.create(default_muid, user_name="dragon")

        p = process.create(
            "python3",
            ".",
            ["dragon_run_api.py", "sub", c_desc.name, shell_cmd],
            None,
            options=ProcessOptions(make_inf_channels=True),
        )

        ch = Channel.attach(c_desc.sdesc)

        recv_h = ch.recvh()
        recv_h.open()
        recv_msg = recv_h.recv()
        recv_msgview = recv_msg.bytes_memview()
        data = recv_msgview[:].tobytes().decode("utf-8")
        print(data)
        recv_msg.destroy()

        process.join(p.p_uid)
        ch.detach()
        channel.destroy(c_desc.c_uid)
        print("Exiting main")

    except Exception as e:
        print(e)


def submain():
    """The output from the shell command passed from main to submain is written in the Dragon channel by the
    submain process.
    """

    try:

        shell_cmd = sys.argv[3]
        channel_name = sys.argv[2]

        c_desc = channel.join(channel_name)
        serialized_channel = c_desc.sdesc
        ch = Channel.attach(serialized_channel)
        mpool = ch.get_pool()

        send_handle = ch.sendh()
        send_handle.open()

        # Subprocess.run captures the output of the shell command after the program exits.
        proc = subprocess.run([shell_cmd], stdout=subprocess.PIPE)
        data = proc.stdout
        msg = Message.create_alloc(mpool, len(data))
        msg_view = msg.bytes_memview()
        msg_view[:] = data
        send_handle.send(msg)
        send_handle.close()

        msg.destroy()
        ch.detach()
        mpool.detach()

    except Exception as e:
        print(e)


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "sub":
        submain()
    else:
        main()
