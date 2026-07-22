#!/usr/bin/env python3

import multiprocessing
import time

import dragon.channels as dch
import dragon.managed_memory as dmm
import dragon.infrastructure.connection as dic


def recv_n_objs(conn_or_init, num):
    if isinstance(conn_or_init, multiprocessing.connection.Connection):
        my_conn = conn_or_init
    else:
        input_chan = dch.Channel.attach(conn_or_init)
        my_conn = dic.Connection(inbound_initializer=input_chan)

    for _ in range(num):
        _ = my_conn.recv()


def send_n_obj(conn_or_init, num, obj_size):
    if isinstance(conn_or_init, multiprocessing.connection.Connection):
        my_conn = conn_or_init
    else:
        output_chan = dch.Channel.attach(conn_or_init)
        my_conn = dic.Connection(outbound_initializer=output_chan)

    start_obj = bytearray(obj_size)

    for _ in range(num):
        my_conn.send(start_obj)


def cheesy_measure_speed(num, obj_size, new_obj=True):
    if new_obj:
        mpool = dmm.MemoryPool(2**30, "hyenas", 17, None)
        chan = dch.Channel(mpool, 42)
        read_init = chan.serialize()
        write_init = read_init
    else:
        read_init, write_init = multiprocessing.Pipe(duplex=False)

    recv_proc = multiprocessing.Process(target=recv_n_objs, args=(read_init, num))
    send_proc = multiprocessing.Process(target=send_n_obj, args=(write_init, num, obj_size))

    recv_proc.start()

    st_time = time.time()
    send_proc.start()
    recv_proc.join()
    time_took = time.time() - st_time
    send_proc.join()

    if new_obj:
        chan.destroy()
        mpool.destroy()

    return time_took


if __name__ == "__main__":

    num_objs = 1000
    print("sending {} objects between 2 processes".format(num_objs))

    for lg_size in range(10, 24):
        size = 2**lg_size
        existing = cheesy_measure_speed(num_objs, size, False)
        new = cheesy_measure_speed(num_objs, size)
        fmt = "size 2**{}: old {:.3}\tnew {:.3}\tratio {:.3}"
        print(fmt.format(lg_size, existing, new, existing / new))
