import random
import string
import sys
import traceback
import time
import pickle
import multiprocessing as mp

from dragon.channels import Channel, register_gateways_from_env, EventType
from dragon.utils import B64

import dragon.infrastructure.parameters as dparms
from dragon.infrastructure.connection import Connection


def send_to(ch_descr, message):
    send_ch = Channel.attach(ch_descr)
    send_h = send_ch.sendh()
    send_h.open()
    send_h.send_bytes(message)
    send_h.close()
    send_ch.detach()


def recv_from(ch_descr):
    recv_ch = Channel.attach(ch_descr)
    recv_h = recv_ch.recvh()
    recv_h.open()
    message = recv_h.recv_bytes()
    recv_h.close()
    recv_ch.detach()
    return message


def send_message_around_ring_of_nodes(this_node_index, send_to_node_index, recv_from_node_index, send_node, recv_node):
    """Send a random secret message around the ring of allocated nodes. Each node has an allocated channel that
    can be sent to/received from.
    Depending on the values of send_node and recv_node, this function can send the message or clockwise,
    counter clockwise. Also depending on the values of send_node and recv_node, either the send or recv will
    be remote.
     Clockwise Local Send / Remote Recv    │    Clockwise Remote Send / Local Recv
                 ┌────────┐                │                ┌────────┐
                 │        │                │                │        │
         ┌─recv──┤ Node 0 ◄─recv─┐         │        ┌─send──► Node 0 ├─send─┐
         │       │        │      │         │        │       │        │      │
         │       └──▲───┬─┘     msg        │        │       └──▲───┬─┘     msg
         │          └─msg        │         │        │          └───┘        │
     ┌───▼────┐     send     ┌───┴────┐    │    ┌───┴────┐     recv     ┌───▼────┐
     │        ├─┐          ┌─►        │    │    │        ├─┐          ┌─►        │
     │ Node 4 │ │send  send│ │ Node   │    │    │ Node 4 │ │recv  recv│ │ Node   │
     │        ◄─┘          └─┤        │    │    │        ◄─┘         msg┤        │
     └┬───────┘              └───────▲┘    │    └▲───────┘              └───────┬┘
      │    send              send    │     │     │    recv              recv    │
    recv   ┌───┐             ┌───┐  recv   │   send   ┌───┐             ┌───┐  send
      │ ┌──▼───┴─┐        ┌──▼───┴─┐ │     │     │ ┌──▼───┴─┐        ┌──▼───┴─┐ │
      │ │        │        │        │ │     │     │ │        │        │        │ │
      └─► Node 3 ├──recv──► Node 2 ├─┘     │     └─┤ Node 3 ◄──send──┤ Node 2 ◄─┘
        │        │        │        │       │       │        │        │        │
        └────────┘        └────────┘       │       └────────┘        └────────┘
    """
    try:
        if this_node_index == 0:
            my_secret_message = "".join(random.choice(string.ascii_letters) for _ in range(64)).encode()

            print(
                f"{this_node_index}: Sending node {send_to_node_index} my secret message {my_secret_message}",
                flush=True,
            )
            send_to(send_node, my_secret_message)

            print(f"{this_node_index}: Waiting to receive my message from node {recv_from_node_index}", flush=True)
            check_message = recv_from(recv_node)

            print(f"{this_node_index}: Received {check_message}", flush=True)
            if check_message == my_secret_message:
                print(f"{this_node_index}: SUCCESS. I received the same message that I sent.", flush=True)
            else:
                print(f"{this_node_index}: ERROR. I did not receive the same message that I sent.", flush=True)
        else:
            print(
                f"{this_node_index}: Waiting to receive a secret message from node {recv_from_node_index}", flush=True
            )
            the_secret_message = recv_from(recv_node)

            print(f"{this_node_index}: Received {the_secret_message}", flush=True)
            print(f"{this_node_index}: Forwarding to node {send_to_node_index}", flush=True)

            send_to(send_node, the_secret_message)

        print(f"{this_node_index}: Done", flush=True)
    except Exception:
        print("!!!!There was an EXCEPTION!!!!!")
        ex_type, ex_value, ex_tb = sys.exc_info()
        tb_str = "".join(traceback.format_exception(ex_type, ex_value, ex_tb))
        print(tb_str)


def poll_ch(ch_descr):
    _ch = Channel.attach(ch_descr)
    rc = _ch.poll(event_mask=EventType.POLLIN)
    return rc


def test_remote_polling(this_node_index, node_to_poll_index, node_to_poll):
    """Test remote polling via the transport agent.
    From node 0, attach to the first channel from the channel list on node 1.
    From node 0, call poll with the POLLIN constant. This is defined in pydragon_channels.pyx.
    On node 0, if poll returns without raising an exception and return True, then the test succeeded.
    On node 1, sleep for five seconds, then attach to the first channel on its own node.
    On node 1, send a message into the channel you just attached to.
    On node 1, sleep for five more seconds to make sure that node 0 gets to successful completion.
    If the node index is not 0 or 1, then the program should sleep for 10 seconds and then exit.
    """
    try:
        rc = True
        if this_node_index == 0:
            print(f"{this_node_index}: Ready to poll on node {node_to_poll_index}", flush=True)
            rc = poll_ch(node_to_poll)
            if rc:
                print(f"{this_node_index}: SUCCESS. The poll completed successfully.", flush=True)
                recv_from(node_to_poll)  # receive the message to clean the channel
            else:
                print(f"{this_node_index}: ERROR. Something went wrong with poll.", flush=True)
        elif this_node_index == 1:
            time.sleep(5)
            my_secret_message = "".join(random.choice(string.ascii_letters) for _ in range(64)).encode()
            print(
                f"{this_node_index}: Sending node {node_to_poll_index} my secret message {my_secret_message}",
                flush=True,
            )
            send_to(node_to_poll, my_secret_message)
        else:
            print(f"{this_node_index}: I am doing nothing. Moving on.", flush=True)

        print(f"{this_node_index}: Done with test_remote_polling", flush=True)
        return rc
    except Exception:
        print("!!!!There was an EXCEPTION on test_remote_polling!!!!!", flush=True)
        ex_type, ex_value, ex_tb = sys.exc_info()
        tb_str = "".join(traceback.format_exception(ex_type, ex_value, ex_tb))
        print(tb_str)


def _connection_comm(this_node_index, nodes_to_channel_map, some_msg, isconnin, isconnout, isbytes):
    if this_node_index == 0:
        if isconnout:
            ch_descr = B64.from_str(nodes_to_channel_map[0][0]).decode()
            _ch = Channel.attach(ch_descr)
            connout = Connection(outbound_initializer=_ch)

            if isbytes:
                connout.send_bytes(some_msg)
            else:
                connout.send(some_msg)

            connout.ghost_close()
        if isconnin:
            ch_descr = B64.from_str(nodes_to_channel_map[0][1]).decode()
            _ch = Channel.attach(ch_descr)
            connin = Connection(inbound_initializer=_ch)

            print(f"{this_node_index}: Waiting to receive message from node 1", flush=True)
            if isbytes:
                msg = connin.recv_bytes()
            else:
                msg = connin.recv()
            assert some_msg == msg

            connin.ghost_close()

    elif this_node_index == 1:
        if isconnin:
            ch_descr = B64.from_str(nodes_to_channel_map[0][0]).decode()
            _ch = Channel.attach(ch_descr)
            connin = Connection(inbound_initializer=_ch)

            print(f"{this_node_index}: Waiting to receive a message from node 0", flush=True)
            if isbytes:
                msg = connin.recv_bytes()
            else:
                msg = connin.recv()
            assert some_msg == msg

            connin.ghost_close()
        if isconnout:
            ch_descr = B64.from_str(nodes_to_channel_map[0][1]).decode()
            _ch = Channel.attach(ch_descr)
            connout = Connection(outbound_initializer=_ch)

            if isbytes:
                connout.send_bytes(some_msg)
            else:
                connout.send(some_msg)

            connout.ghost_close()


def test_connection_basic(nodes_to_channel_map):
    """Basic communication between nodes 0 and 1, through Connection objects.
    Node 0 sends some data to node 1 and then waits to receive it back.
    Node 1 is doing remote operations, both send and recv,
    whereas Node 0 is doing local operations only.
    :param nodes_to_channel_map: map of pre-created channels on all the nodes of the allocation
    :type nodes_to_channel_map: dict, with key as the node index and value a list of channels
    """
    this_node_index = dparms.this_process.index
    some_msg = ["a", "b", "c", 1, 2, 3]
    try:
        if this_node_index == 0 or this_node_index == 1:
            _connection_comm(this_node_index, nodes_to_channel_map, some_msg, True, True, False)
        else:
            print(f"{this_node_index}: I am doing nothing for test_connection_basic.", flush=True)
    except Exception:
        print("!!!!There was an EXCEPTION on test_connection_basic!!!!!")
        ex_type, ex_value, ex_tb = sys.exc_info()
        tb_str = "".join(traceback.format_exception(ex_type, ex_value, ex_tb))
        print(tb_str)


def test_connection_different_msg_sizes(nodes_to_channel_map):
    """Remote receive of different message sizes.
    :param nodes_to_channel_map: map of pre-created channels on all the nodes of the allocation
    :type nodes_to_channel_map: dict, with key as the node index and value a list of channels
    """
    this_node_index = dparms.this_process.index
    for lg_size in range(25):
        some_msg = bytes(2**lg_size)
        try:
            if this_node_index == 0:
                _connection_comm(0, nodes_to_channel_map, some_msg, isconnin=False, isconnout=True, isbytes=False)
            elif this_node_index == 1:
                _connection_comm(1, nodes_to_channel_map, some_msg, isconnin=True, isconnout=False, isbytes=False)
            else:
                print(f"{this_node_index}: I am doing nothing for test_connection_different_msg_sizes.", flush=True)
        except Exception:
            print("!!!!There was an EXCEPTION on test_connection_different_msg_sizes!!!!!")
            ex_type, ex_value, ex_tb = sys.exc_info()
            tb_str = "".join(traceback.format_exception(ex_type, ex_value, ex_tb))
            print(tb_str)


def test_connection_different_objs(nodes_to_channel_map):
    """Remote receive of different object types.
    :param nodes_to_channel_map: map of pre-created channels on all the nodes of the allocation
    :type nodes_to_channel_map: dict, with key as the node index and value a list of channels
    """
    this_node_index = dparms.this_process.index
    objs = [
        "hyena",
        {"hyena": "hyena"},
        bytes(100000),
        bytearray(200000),
        bytes("this is a test", "utf-8"),
        b"\x01\x02\x03\x04\x05" * 1000,
    ]
    for some_msg in objs:
        try:
            if this_node_index == 0:
                _connection_comm(0, nodes_to_channel_map, some_msg, isconnin=False, isconnout=True, isbytes=False)
            elif this_node_index == 1:
                _connection_comm(1, nodes_to_channel_map, some_msg, isconnin=True, isconnout=False, isbytes=False)
            else:
                print(f"{this_node_index}: I am doing nothing for test_connection_different_objs.", flush=True)
        except Exception:
            print("!!!!There was an EXCEPTION on test_connection_different_objs!!!!!")
            ex_type, ex_value, ex_tb = sys.exc_info()
            tb_str = "".join(traceback.format_exception(ex_type, ex_value, ex_tb))
            print(tb_str)


def test_connection_poll(nodes_to_channel_map):
    """Remote Connection poll.
    :param nodes_to_channel_map: map of pre-created channels on all the nodes of the allocation
    :type nodes_to_channel_map: dict, with key as the node index and value a list of channels
    """
    this_node_index = dparms.this_process.index
    some_msg = b"\x01\x02\x03\x04\x05"
    try:
        if this_node_index == 0:
            ch_descr = B64.from_str(nodes_to_channel_map[0][0]).decode()
            _ch = Channel.attach(ch_descr)
            connout = Connection(outbound_initializer=_ch)
            time.sleep(3)
            connout.send(some_msg)
            connout.ghost_close()
        elif this_node_index == 1:
            ch_descr = B64.from_str(nodes_to_channel_map[0][0]).decode()
            _ch = Channel.attach(ch_descr)
            connin = Connection(inbound_initializer=_ch)
            rc = connin.poll(timeout=20.0)
            assert rc == True
            msg = connin.recv()
            assert msg == some_msg
            connin.ghost_close()
        else:
            print(f"{this_node_index}: I am doing nothing for test_connection_poll.", flush=True)
    except Exception:
        print("!!!!There was an EXCEPTION on test_connection_poll!!!!!")
        ex_type, ex_value, ex_tb = sys.exc_info()
        tb_str = "".join(traceback.format_exception(ex_type, ex_value, ex_tb))
        print(tb_str)


def test_connection_remote_recv_bytes_from_send(nodes_to_channel_map):
    """Local send and remote recv_bytes. Edge case.
    :param nodes_to_channel_map: map of pre-created channels on all the nodes of the allocation
    :type nodes_to_channel_map: dict, with key as the node index and value a list of channels
    """
    this_node_index = dparms.this_process.index
    some_msg = {"hyenas": "are awesome"}
    try:
        if this_node_index == 0:
            ch_descr = B64.from_str(nodes_to_channel_map[0][0]).decode()
            _ch = Channel.attach(ch_descr)
            connout = Connection(outbound_initializer=_ch)
            connout.send(some_msg)
            connout.ghost_close()
        elif this_node_index == 1:
            ch_descr = B64.from_str(nodes_to_channel_map[0][0]).decode()
            _ch = Channel.attach(ch_descr)
            connin = Connection(inbound_initializer=_ch)
            ser_msg = connin.recv_bytes()
            msg = pickle.loads(ser_msg)
            assert msg == some_msg
            connin.ghost_close()
        else:
            print(f"{this_node_index}: I am doing nothing for test_connection_remote_recv_bytes_from_send.", flush=True)
    except Exception:
        print("!!!!There was an EXCEPTION on test_connection_remote_recv_bytes_from_send!!!!!")
        ex_type, ex_value, ex_tb = sys.exc_info()
        tb_str = "".join(traceback.format_exception(ex_type, ex_value, ex_tb))
        print(tb_str)


def test_connection_recv_from_remote_send_bytes(nodes_to_channel_map):
    """Remote send_bytes and local recv. Edge case.
    :param nodes_to_channel_map: map of pre-created channels on all the nodes of the allocation
    :type nodes_to_channel_map: dict, with key as the node index and value a list of channels
    """
    this_node_index = dparms.this_process.index
    some_msg = {"hyenas": "are awesome"}
    try:
        if this_node_index == 0:
            ch_descr = B64.from_str(nodes_to_channel_map[1][0]).decode()
            _ch = Channel.attach(ch_descr)
            connout = Connection(outbound_initializer=_ch)
            ser_msg = pickle.dumps(some_msg)
            connout.send_bytes(ser_msg)
            connout.ghost_close()
        elif this_node_index == 1:
            ch_descr = B64.from_str(nodes_to_channel_map[1][0]).decode()
            _ch = Channel.attach(ch_descr)
            connin = Connection(inbound_initializer=_ch)
            msg = connin.recv()
            assert msg == some_msg
            connin.ghost_close()
        else:
            print(f"{this_node_index}: I am doing nothing for test_connection_recv_from_remote_send_bytes.", flush=True)
    except Exception:
        print("!!!!There was an EXCEPTION on test_connection_recv_from_remote_send_bytes!!!!!")
        ex_type, ex_value, ex_tb = sys.exc_info()
        tb_str = "".join(traceback.format_exception(ex_type, ex_value, ex_tb))
        print(tb_str)


def send_stuff(nnodes, this_node_index, all_connouts, msg):
    register_gateways_from_env()
    for i in range(nnodes):
        if i is not this_node_index:
            # print(f"Node {this_node_index} sending to node {i}")
            all_connouts[this_node_index, i].send(msg)


def recv_stuff(nnodes, connin, msg):
    register_gateways_from_env()
    for i in range(nnodes - 1):
        # print(f"Node {this_node_index} receiving msg {i}")
        msg_recv = connin.recv()
        assert msg_recv == msg


def test_connection_alltoall(nodes_to_channel_map):
    this_node_index = dparms.this_process.index
    nnodes = len(nodes_to_channel_map)

    all_connouts = {}
    for i in range(nnodes):
        if i is not this_node_index:
            # connection for remote sending on node i
            ch_descr = B64.from_str(nodes_to_channel_map[i][0]).decode()
            _ch = Channel.attach(ch_descr)
            all_connouts[this_node_index, i] = Connection(outbound_initializer=_ch)

    # connection object for local receiving on this node
    ch_descr = B64.from_str(nodes_to_channel_map[this_node_index][0]).decode()
    _ch = Channel.attach(ch_descr)
    connin = Connection(inbound_initializer=_ch)

    msg = b"\x01\x02\x03\x04\x05"

    try:
        send_proc = mp.Process(target=send_stuff, args=(nnodes, this_node_index, all_connouts, msg))
        recv_proc = mp.Process(target=recv_stuff, args=(nnodes, connin, msg))
        send_proc.start()
        recv_proc.start()
        send_proc.join()
        recv_proc.join()
    except Exception:
        print("!!!!There was an EXCEPTION on test_connection_alltoall!!!!!")
        ex_type, ex_value, ex_tb = sys.exc_info()
        tb_str = "".join(traceback.format_exception(ex_type, ex_value, ex_tb))
        print(tb_str)


def main():
    mp.set_start_method("spawn")

    register_gateways_from_env()
    nodes_to_channel_map = eval(input())
    this_node_index = dparms.this_process.index

    next_node_index = (this_node_index + 1) % len(nodes_to_channel_map)
    prev_node_index = (this_node_index - 1) % len(nodes_to_channel_map)
    print(f"{this_node_index}: prev_node_index={prev_node_index} next_node_index={next_node_index}", flush=True)
    print(f"{this_node_index}: Got node to channel map={nodes_to_channel_map}", flush=True)

    next_node = B64.from_str(nodes_to_channel_map[next_node_index][0]).decode()
    this_node = B64.from_str(nodes_to_channel_map[this_node_index][0]).decode()
    prev_node = B64.from_str(nodes_to_channel_map[prev_node_index][0]).decode()

    print("Initiating clockwise remote send/local recv ring test:", flush=True)
    send_message_around_ring_of_nodes(
        this_node_index,
        send_to_node_index=next_node_index,
        recv_from_node_index=this_node_index,
        send_node=next_node,
        recv_node=this_node,
    )

    print("Initiating clockwise local send/remote recv ring test:", flush=True)
    send_message_around_ring_of_nodes(
        this_node_index,
        send_to_node_index=this_node_index,
        recv_from_node_index=prev_node_index,
        send_node=this_node,
        recv_node=prev_node,
    )

    # this is a connection test including all nodes
    print("\ntest_connection_alltoall", flush=True)
    test_connection_alltoall(nodes_to_channel_map)

    # Beginning of tests including only 2 nodes
    # this is a channel test including only two nodes
    node_to_poll_index = 1
    sync_index = 0
    sync_channel = B64.from_str(nodes_to_channel_map[sync_index][0]).decode()
    node_to_poll = B64.from_str(nodes_to_channel_map[node_to_poll_index][0]).decode()
    print("Initiating remote polling:", flush=True)
    rc = test_remote_polling(this_node_index, node_to_poll_index=node_to_poll_index, node_to_poll=node_to_poll)
    if rc:
        print(f"The poll test was a SUCCESS on node {this_node_index}!", flush=True)
    else:
        print(f"The poll test failed on node {this_node_index}!", flush=True)

    # these are connection tests including only 2 nodes
    print("\ntest_connection_basic", flush=True)
    test_connection_basic(nodes_to_channel_map)
    print("\ntest_connection_different_msg_sizes", flush=True)
    test_connection_different_msg_sizes(nodes_to_channel_map)
    print("\ntest_connection_different_objs", flush=True)
    test_connection_different_objs(nodes_to_channel_map)
    print("\ntest_connection_recv_bytes_from_send", flush=True)
    test_connection_remote_recv_bytes_from_send(nodes_to_channel_map)
    print("\ntest_connection_recv_from_remote_send_bytes", flush=True)
    test_connection_recv_from_remote_send_bytes(nodes_to_channel_map)
    print("\ntest_connection_poll", flush=True)
    test_connection_poll(nodes_to_channel_map)


if __name__ == "__main__":
    main()
