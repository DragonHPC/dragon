""" Pass a message between two Dragon processes using one Dragon channel. Uses Global Services and Channels
from Dragon API.
    * Create a single channel and process on default Dragon pool.  In the new process, attach to the Channel
    * passed as an argument via the serialized descriptor.  In the new process, use subprocess.Popen to execute
    * the shell command and get the output. Write that output into the attached Channel.  In the original process
    * in main, output the message.  Join the channel and the process with Global Services.
"""


import dragon.globalservices.channel as channel
import dragon.globalservices.process as process

from dragon.infrastructure.process_desc import ProcessOptions
import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.facts as dfacts

from dragon.channels import Channel, Message

import subprocess
import sys


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
            ["dragon_popen_api.py", "sub", c_desc.name, shell_cmd],
            None,
            options=ProcessOptions(make_inf_channels=True),
        )

        ch = Channel.attach(c_desc.sdesc)
        recv_h = ch.recvh()
        recv_h.open()

        reading_output = True
        while reading_output:
            recv_msg = recv_h.recv()
            recv_msgview = recv_msg.bytes_memview()
            data_flag = recv_msgview[:4].tobytes().decode("utf-8")

            if data_flag == "EOT ":
                reading_output = False

            else:
                data = recv_msgview[4:].tobytes().decode("utf-8")
                print(data)

            recv_msg.destroy()

        process.join(p.p_uid)
        ch.detach()
        channel.destroy(c_desc.c_uid)
        print("Exiting main")

    except Exception as e:
        print(e)


def submain():
    """The output from the shell command passed from main to submain is written in the Dragon channel by the submain process."""

    try:

        shell_cmd = sys.argv[3]
        channel_name = sys.argv[2]

        c_desc = channel.join(channel_name)
        serialized_channel = c_desc.sdesc
        ch = Channel.attach(serialized_channel)
        mpool = ch.get_pool()

        send_handle = ch.sendh()
        send_handle.open()

        # Subprocess.Popen captures the output of the shell command while the program runs.
        proc = subprocess.Popen([shell_cmd], stdout=subprocess.PIPE)
        data = proc.stdout.read()

        while data != None and len(data) != 0:
            msg = Message.create_alloc(mpool, len(data) + 4)
            msg_view = msg.bytes_memview()
            msg_view[:4] = bytes("data", "utf-8")
            msg_view[4:] = data
            send_handle.send(msg)
            data = proc.stdout.read()

        msg = Message.create_alloc(mpool, 4)
        msg_view = msg.bytes_memview()
        msg_view[:] = bytes("EOT ", "utf-8")
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
