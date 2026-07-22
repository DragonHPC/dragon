import re
import socket
import argparse
from os import environ
from time import sleep, monotonic


def get_my_friends_ip(hostname):
    """Use SLURM environment variables and DNS to get my IP address. This is hacky, and I hate it."""

    # Use slurm's nodelist to get my friend's IP courtest of the DNS I'm hoping is configured.
    nodelist = environ.get('SLURM_JOB_NODELIST')

    # Make sure there isn't some annoying truncation that I have to regex to usability
    expanded_nodelist = re.sub(r"([0-9,a-z,A-Z]*)\[([0-9]*)-([0-9]*)\]", r'\1\2, \1\3', nodelist)

    # Split the list up
    nodes = re.split(r',\s', expanded_nodelist)

    # And find the node that's not mine
    pal_node = [node for node in nodes if node != hostname][0]
    print(f'getting IP address for {pal_node}', flush=True)

    # And finally use DNS to get their IP
    pal_ip = socket.gethostbyname(pal_node)

    return pal_ip


def looped_function(port, is_a_restart, trigger_restart):
    """Function that will be ProcessTemplate we launch many of"""

    # Glean as much info as I can from things I would do if I didn't have dragon. It is hacky, and I hate it.
    myrank = int(environ.get('SLURM_NODEID'))  # I love relying on slurm to implement hacky distributed communication. That never goes wrong.
    hostname = socket.gethostname()  # Hoping this matches with DNS. Also, hoping DNS is configured on this system.
    my_ip = socket.gethostbyname(hostname)  # This is going to give me the management IP which is slow, but I don't know that.
                                            # Well, actually I do, but you, user, probably don't. Dragon knows too (I helped program it)
                                            # and will instead give me a high performance network address.
    pal_ip = get_my_friends_ip(hostname)  # ibid.
    print(f"rank {myrank} has ip {my_ip} and am going to talk to {pal_ip}", flush=True)

    # Now try to establish connections
    if myrank == 0:
        # Now open a socket to allow my friend to connect to me
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((my_ip, port))
        print("server listening")
        server.listen(1)

        conn, addr = server.accept()
        server.close()
        print(f"Server connected to {addr} and closing its listening port", flush=True)

        timeout = 10
        start_time = monotonic()

        while timeout > (monotonic() - start_time):
            msg = conn.recv(1024)
            print(f"Server recv'd {msg}", flush=True)

            if (((monotonic() - start_time) > (0.5 * timeout)) and trigger_restart) and not is_a_restart:
                exit(21)
            sleep(0.1)

        conn.close()
        print("server closed all connections")

    elif myrank == 1:
        # Connect to my pal:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        break_loop = False
        while not break_loop:
            try:
                client.connect((pal_ip, port))
                break_loop = True
            except Exception as e:
                print(f"client didn't connect: {e}", flush=True)
                sleep(0.1)

        timeout = 10
        start_time = monotonic()
        while timeout > (monotonic() - start_time):
            try:
                client.sendall(b'hello, friend')
            except BrokenPipeError as e:
                print(f'client sender hit error: {type(e)}: {e}.', flush=True)
                exit(33)
            sleep(0.1)
        client.close()
        print("client close its connection")


def main():
    """Function that will execute the ProcessGroup"""

    parser = argparse.ArgumentParser(prog="Resiliency Demo",
                                     description="Demo restart triggered by failure of user application")
    parser.add_argument('--trigger-restart', action='store_true')
    parser.add_argument('--port', type=int, default=7421)

    args = parser.parse_args()
    is_a_restart = bool(environ.get('BASH_RESILIENT_RESTART', False))
    port = args.port

    looped_function(port, is_a_restart, args.trigger_restart)


if __name__ == '__main__':
    main()
