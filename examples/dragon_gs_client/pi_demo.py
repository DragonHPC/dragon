""" Calculate pi in parallel using the Dragon GS Client interface.

    * Create two Dragon Channels to be used as an in-queue and out-queue for work
    * Start up a collection of processes (number is an argument to the script) each of
       which will execute some function based on input data. Let's have that function do the simple thing of
       helping to estimate Pi by taking two input random number and checking if the sum of the squares is less
       than 1. If so, have the function return True. Pass the descriptors to the 2 Channels to each process as
       startup information.
    * Have all processes attach to the Channels and use one of them to pass the two numbers and the other to
       return True or False.
    * The coordinating process then generates random number pairs in the range of [0 .. 1] and passes pairs of
       them as messages into one of the Channels.
    * The worker then should wait for messages on that Channel as work and return the result in the other
       Channel.
    * The coordinator will need to generate random pairs and accumulate results at the same time and use the ratio
       of True results to the total number of pairs to compute Pi
    * The coordinator then should send some terminate message into the work queue Channel and clean everything up.
"""

import sys
import os
import random
import pickle
import math

# Dragon GS Client API
import dragon.globalservices.pool as dgpool
import dragon.globalservices.channel as dgchan
import dragon.globalservices.process as dgprocess

# Dragon infrastructure API
import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.facts as dfacts
from dragon.infrastructure.process_desc import ProcessOptions

# Dragon Core API
from dragon.managed_memory import MemoryPool
from dragon.channels import Channel


def pi_helper(output_channel_name, input_channel_name):

    # get serialized channel descriptors from channel names
    input_channel_desc = dgchan.query(input_channel_name)
    output_channel_desc = dgchan.query(output_channel_name)

    ch_in = Channel.attach(input_channel_desc.sdesc)
    ch_out = Channel.attach(output_channel_desc.sdesc)

    # we are using the message interface to channels here, not
    # the file-like interface.
    recv_handle = ch_in.recvh()
    send_handle = ch_out.sendh()

    recv_handle.open()
    send_handle.open()

    while True:  # keep working

        msg = recv_handle.recv_bytes()
        x, y = pickle.loads(msg)

        if x == -1 and y == -1:  # we're done here
            break

        r = x * x + y * y

        if r > 1.0:
            msg = pickle.dumps(False)
        else:
            msg = pickle.dumps(True)

        send_handle.send_bytes(msg)

    recv_handle.close()
    send_handle.close()

    ch_in.detach()
    ch_out.detach()


def main(num_workers):

    # use default memory pool of this process
    default_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)

    # ask GS to create 2 dragon channels
    input_channel = dgchan.create(default_muid, user_name="InputChannel")
    output_channel = dgchan.create(default_muid, user_name="OutputChannel")

    # create processes
    cmd = sys.executable
    wdir = "."
    cmd_params = [
        os.path.basename(__file__),
        "sub",
        input_channel.name,
        output_channel.name,
    ]
    options = ProcessOptions(make_inf_channels=True)
    processes = []
    for _ in range(num_workers):
        p = dgprocess.create(cmd, wdir, cmd_params, None, options=options)
        processes.append(p.p_uid)

    # attach to channels
    ch_in = Channel.attach(input_channel.sdesc)
    ch_out = Channel.attach(output_channel.sdesc)

    recv_handle = ch_in.recvh()
    send_handle = ch_out.sendh()

    recv_handle.open()
    send_handle.open()

    # start calculation
    random.seed(a=42)

    num_true = 0
    num_send = 0
    delta = 1
    count = 0

    while abs(delta) > 1e-6:

        # send new values to everyone
        for _ in range(num_workers):
            x = random.uniform(0.0, 1.0)
            y = random.uniform(0.0, 1.0)

            msg = pickle.dumps((x, y))

            send_handle.send_bytes(msg)  # easy but slow way to send 2 x 8 bytes
            num_send += 1

        # receive results from everyone
        for _ in range(num_workers):
            msg = recv_handle.recv_bytes()

            is_inside = pickle.loads(msg)

            if is_inside:
                num_true += 1

        # calculate result of this iteration
        value = 4.0 * float(num_true) / float(num_send)
        delta = (value - math.pi) / math.pi

        if count % 512 == 0:
            print(f"{count}: pi={value}, error={delta}")
            sys.stdout.flush()

        count += 1

    print(f"Final value after {count} iterations: pi={value}, error={delta}")

    # shut down all managed processes
    for _ in range(num_workers):
        msg = pickle.dumps((-1.0, -1.0))  # termination message
        send_handle.send_bytes(msg)

    recv_handle.close()
    send_handle.close()

    ch_in.detach()
    ch_out.detach()

    # join a set of processes at once; wait until all the processes are finished
    dgprocess.multi_join(processes, join_all=True)

    dgchan.destroy(input_channel.c_uid)
    dgchan.destroy(output_channel.c_uid)


def usage():
    print(f"USAGE: dragon pi_demo.py $N")
    print(f"  N : number of worker processes to start")


if __name__ == "__main__":

    if len(sys.argv) == 1:
        usage()
        sys.exit(f"")

    if sys.argv[1] == "sub":
        pi_helper(sys.argv[2], sys.argv[3])
    else:

        print(f"\npi-demo: Calculate π = 3.1415 ... in parallel using the Dragon native API.\n")

        try:
            num_workers = int(sys.argv[1])
        except:
            usage()
            sys.exit(f"Wrong argument '{sys.argv[1]}'")

        print(f"Got num_workers = {num_workers}")
        sys.stdout.flush()
        main(num_workers)
