from dragon.channels import Channel, Message, ChannelSendH, ChannelRecvH

# create a Channel
ch = Channel(1)

# serialze the Channel and write that to a file
ser = ch.serialize()
ser_file = open("channel.dat", "wb")
ser_file.write(ser)
ser_file.close()

# create and open a send handle to Channel 1 and a recv handle to Channel 2
sh = ch.sendh()
sh.open()

# create a message we will keep sending over and over again
sm=Message().create_alloc(512)
mb=sm.bytes_memview()
mb[1] = b"K"
mb[511] = b"b"

# run a loop where we will keep sending the message
for i in range(100):
    sh.send(sm)

# destroy the message and by default the underlying managed memory allocation
sm.destroy()

# wait for user input
input("Enter to continue: ")

# cleanup the channel
ch.destroy()
