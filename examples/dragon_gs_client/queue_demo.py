"""Multiple producers multiple consumers communication scheme with Dragon
Channels.

* Create one Dragon Channel for the communication.
* Start up two processes that act as producers and two processes as consumers.
* Each producer generates a python object consisting of random data (a set of
    random strings) and a set of random functions to operate on the data (string
    operations such as upper()). Then, it uses the Channel to send the object as
    a message.
* Each consumer blocks until they receive a message on the channel. Then they apply
    the functions on the data they received and print the result in the output.
"""

import sys
import os
import random
import cloudpickle
import string

import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.facts as dfacts
import dragon.globalservices.channel as dgchan
import dragon.globalservices.process as dgprocess
from dragon.infrastructure.process_desc import ProcessOptions

from dragon.channels import Channel, Many2ManyWritingChannelFile, Many2ManyReadingChannelFile


def producer(chan_name, funcs):
    data = ""
    for i in range(5):
        data = data + " " + "".join(random.choices(string.ascii_lowercase, k=i + 3))

    print(f'I am producer {os.getpid()} and I\'m sending data: "{data}" and string ops:', end=" ")

    channel_desc = dgchan.query(chan_name)
    _chan = Channel.attach(channel_desc.sdesc)

    n = random.randint(1, len(funcs))

    chosen = random.sample(list(funcs.items()), n)  # random selection without replacement

    for item in chosen:
        print(item[0], end=" ")
    print(flush=True)

    obj = (chosen, data)
    try:
        adapter = Many2ManyWritingChannelFile(_chan)
        adapter.open()
        cloudpickle.dump(obj, file=adapter, protocol=5)
    finally:
        adapter.close()


def consumer(chan_name, funcs):
    print(f"I am consumer {os.getpid()} --", end=" ")

    channel_desc = dgchan.query(chan_name)
    _chan = Channel.attach(channel_desc.sdesc)

    try:
        adapter = Many2ManyReadingChannelFile(_chan)
        adapter.open()
        funcs, data = cloudpickle.load(adapter)
    finally:
        adapter.close()

    for identifier, f in funcs:
        print(f'{identifier}: "{f(data)}"', end=" ")
        data = f(data)
    print(flush=True)


def main():
    # use default memory pool of this process
    default_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)

    # ask GS to create a dragon channel
    _channel = dgchan.create(default_muid, user_name="InputChannel")

    # create processes
    cmd = sys.executable
    wdir = "."
    options = ProcessOptions(make_inf_channels=True)
    processes = []
    for _ in range(2):
        p = dgprocess.create(
            cmd, wdir, [os.path.basename(__file__), "sub", "producer", _channel.name], None, options=options
        )
        processes.append(p.p_uid)

    for _ in range(2):
        p = dgprocess.create(
            cmd, wdir, [os.path.basename(__file__), "sub", "consumer", _channel.name], None, options=options
        )
        processes.append(p.p_uid)

    # join a set of processes at once; wait until all the processes are finished
    dgprocess.multi_join(processes, join_all=True)

    dgchan.destroy(_channel.c_uid)


if __name__ == "__main__":
    # predefined functions for the producers to choose from
    # the dict is also used to verify the correctness of send/receive ops
    funcs = {
        "upper": lambda a: a.upper(),
        "lower": lambda a: a.lower(),
        "strip": lambda a: a.strip(),
        "capitalize": lambda a: a.capitalize(),
        'replace(" ", "")': lambda a: a.replace(" ", ""),
    }

    if len(sys.argv) > 1 and sys.argv[1] == "sub":
        if sys.argv[2] == "producer":
            producer(sys.argv[3], funcs)
        else:
            consumer(sys.argv[3], funcs)
    else:
        main()
