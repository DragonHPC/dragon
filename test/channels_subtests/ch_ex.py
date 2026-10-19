import multiprocessing as mp
from multiprocessing import Process
from dragon.channels import Channel, Message, ChannelSendH, ChannelRecvH
from dragon.managed_memory import MemoryPool


def recv_worker():
    # read the serialized channel from a file
    ser_file = open("channel.dat", "rb")
    ser = ser_file.read()
    ser_file.close()

    # attach to a Channel
    ch = Channel.attach(ser)

    # create and open a recv handle to Channel
    rh = ch.recvh()
    rh.open()

    # run a loop where we will keep receiving the message
    for i in range(26):
        msg = rh.recv()
        rb = msg.bytes_memview()
        rb = rb.cast("c")  # Cast to char to spit out letters instead of ascii values
        print(f"Value at 1 is {rb[1]}")
        msg.destroy()

    rh.close()
    ch.detach()


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)

    pool_name = "pydragon_channel_test"
    pool_size = 1073741824  # 1GB
    pool_uid = 1
    mpool = MemoryPool(pool_size, pool_name, pool_uid)

    # create a Channel
    ch = Channel(mpool, 1)

    # serialze the Channel and write that to a file
    ser = ch.serialize()
    ser_file = open("channel.dat", "wb")
    ser_file.write(ser)
    ser_file.close()

    # create and open a send handle to Channel 1 and a recv handle to Channel 2
    sh = ch.sendh()
    sh.open()

    # create a message we will keep sending over and over again
    sm = Message.create_alloc(mpool, 512)
    mb = sm.bytes_memview()
    mb = mb.cast("c")  # Cast to char to directly assign letters in

    alpha = "abcdefghijklmnopqrstuvwxyz"
    # run a loop where we will keep sending the message
    for i in range(26):
        # Because messages are buffered by default we can reuse the same message allocation multiple times
        mb[1] = alpha[i].encode()
        sh.send(sm)

    # destroy the message and by default the underlying managed memory allocation
    # This does not release all buffered messages
    sm.destroy()
    # Close the send handle
    sh.close()

    worker = mp.Process(target=recv_worker, args=())
    worker.start()
    worker.join()

    # wait for user input
    input("Enter to continue: ")

    # cleanup the channel
    ch.destroy()
    mpool.destroy()
