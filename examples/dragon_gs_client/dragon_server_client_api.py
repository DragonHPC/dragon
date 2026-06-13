""" Pass a message between two Dragon processes using one Dragon channel. Uses Global Services and Channels
from Dragon API.
    * Create a server process that communicates with the client.
    * The Dragon channel is created on the default memory pool.
    * The client handles user input from the requests.txt file.
    * The client passes the request information on the Dragon channel to the server process.
    * The server creates a server:client channel to pass the messages to the client.
    * After all the requests are processed or the stop request is processed, the server stops listening for requests.
"""


import dragon.globalservices.channel as channel
import dragon.globalservices.process as process

import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.facts as dfacts
from dragon.infrastructure.process_desc import ProcessOptions

from dragon.channels import Channel, Message

import sys
import os


def response_message_array(request):
    """This function creates the response for the request to be placed in the response channel."""

    messages = []
    request_types = ["bananas", "apples", "rodents", "stop"]

    if request not in request_types:
        print("Request value ", request, " is not supported.")
    if request == "apples":
        messages = ["honeycrisp", "red delicious", "braeburn"]
    if request == "bananas":
        messages = ["plantain", "cavendish", "java"]
    if request == "rodents":
        messages = ["squirrels", "mice", "capybaras"]
    if request == "stop":
        messages = "stop"

    output = "The request: " + request + " has the messages " + str(messages)
    return output


def create_requests(filename):
    """This function will be replaced once input() works with Dragon. The request textfile is read into an
    array of request."""
    request_array = []

    with open(filename, "r") as f:
        for line in f:
            request_array.append(line.strip())
    f.close()

    return request_array


def server(channel_name):
    """The server creates the request channel and the response channel. It provides the response messages for
    the requests from the user."""
    receiving = True

    # Join the server channel that receives the request from user
    server_descriptor = channel.join(channel_name)
    server_serialized_channel = server_descriptor.sdesc
    server_channel = Channel.attach(server_serialized_channel)
    mpool = server_channel.get_pool()
    server_receive_handle = server_channel.recvh()
    server_receive_handle.open()

    while receiving:
        request_msg = server_receive_handle.recv()
        request_msgview = request_msg.bytes_memview()
        request_puid_channel_name = request_msgview[:].tobytes().decode("utf-8")
        request = request_puid_channel_name.split("-")[0]
        request_msg.destroy()

        if "stop" == request:
            receiving = False

        # Create the response channel to send the messages of the responses to the request
        response_channel_name = request_puid_channel_name
        default_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)
        response_c_desc = channel.create(default_muid, user_name=response_channel_name)
        response_serialized_channel = response_c_desc.sdesc
        response_channel = Channel.attach(response_serialized_channel)

        response_send_handle = response_channel.sendh()
        response_send_handle.open()
        response = response_message_array(request)
        response_msg = Message.create_alloc(mpool, len(response))
        response_msg_view = response_msg.bytes_memview()
        response_msg_view[:] = bytes(response, "utf-8")
        response_send_handle.send(response_msg)
        response_send_handle.close()

        response_msg.destroy()
        response_channel.detach()

    server_receive_handle.close()


def main():
    """The main provides an interface for the user to communicate to the server. This is where the requests
    are sent and the responses are printed.
    """

    default_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)
    server_channel_create = channel.create(default_muid, user_name="dragon")
    channel_name = server_channel_create.name

    requests = create_requests("requests.txt")

    # Create the server process
    cmd = sys.executable
    wdir = "."
    options = ProcessOptions(make_inf_channels=True)
    server_process = process.create(
        cmd, wdir, [os.path.basename(__file__), "server", channel_name], None, options=options
    )

    # Create the server channel where the user passes the requests to the server
    server_cdesc = channel.join(channel_name)
    server_serialized_channel = server_cdesc.sdesc
    server_channel = Channel.attach(server_serialized_channel)
    mpool = server_channel.get_pool()
    server_send_handle = server_channel.sendh()
    server_send_handle.open()

    for request in requests:

        # Send the request to the server. The server will create the request channel named after the request.
        puid = dparm.this_process.my_puid
        response_channel = str(request) + "-" + str(puid)
        msg = Message.create_alloc(mpool, len(response_channel))
        msg_view = msg.bytes_memview()
        msg_view[:] = bytes(response_channel, "utf-8")
        server_send_handle.send(msg)
        msg.destroy()

        # Output the messages from the response channel.
        response_desc = channel.join(response_channel)
        response_serialized_channel = response_desc.sdesc
        response_channel = Channel.attach(response_serialized_channel)
        response_recv_h = response_channel.recvh()
        response_recv_h.open()
        response_msg = response_recv_h.recv()
        response_msgview = response_msg.bytes_memview()
        response_data = response_msgview[:].tobytes().decode("utf-8")

        if response_data != "stop":
            print(response_data, flush=True)

        response_msg.destroy()
        response_recv_h.close()
        channel.destroy(response_desc.c_uid)

    process.join(server_process.p_uid)
    server_send_handle.close()
    server_channel.destroy()
    mpool.detach()


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            server(sys.argv[2])
        else:
            main()
    except Exception as e:
        print(e)
