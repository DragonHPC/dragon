from dragon.channels import Channel, Message, ChannelSendH, ChannelRecvH

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
for i in range(100):
    msg = rh.recv()
    rb = msg.bytes_memview()
    print(f"Value at 1 is {rb[1]}")
    msg.destroy()

# wait for user input
input("Enter to continue: ")
