import socket
import re
import argparse
from os import environ
from time import sleep, monotonic

from dragon.native.process_group import ProcessGroup, ExitErrorCodes
from dragon.native.process import ProcessTemplate
from dragon.native.queue import Queue

from dragon.transport.ifaddrs import getifaddrs, InterfaceAddressFilter
from dragon.infrastructure.facts import DEFAULT_TRANSPORT_NETIF


def get_ip_address(myrank, network_prefix):
    ifaddr_filter = InterfaceAddressFilter()
    ifaddr_filter.af_inet(inet6=False)  # Disable IPv6 for now
    ifaddr_filter.up_and_running()
    try:
        ifaddrs = list(filter(ifaddr_filter, getifaddrs()))
    except OSError:
        raise
    if not ifaddrs:
        _msg = 'No network interface with an AF_INET address was found'
        raise RuntimeError(_msg)

    try:
        re_prefix = re.compile(network_prefix)
    except (TypeError, NameError):
        _msg = "expected a string regular expression for network interface network_prefix"
        raise RuntimeError(_msg)

    ifaddr_filter.clear()
    ifaddr_filter.name_re(re.compile(re_prefix))
    ip_addrs = [ifa['addr']['addr'] for ifa in filter(ifaddr_filter, ifaddrs)]
    if not ip_addrs:
        _msg = f'No IP addresses found for rank {myrank} matching regex pattern: {network_prefix}'
        raise ValueError(_msg)

    return ip_addrs


def get_my_friends_ip(myrank, my_ip, ip_queue):
    """Manipulate the queue to get the IP of someone to talk to"""

    print(f"Got my ip address: {my_ip} for rank {myrank}", flush=True)

    # This will be weird, but here we go. If I'm rank 0, put my IP in there
    if myrank == 0:
        print('rank 0 putting ip in queue', flush=True)
        ip_queue.put(my_ip)

        # Now wait until someone grabs mine and puts theirs in the queue
        while (pal_ip := ip_queue.get(timeout=None)) == my_ip:
            ip_queue.put(my_ip)
            sleep(0.01)

    # Otherwise, grab the first IP I can, then drop mine in the queue for someone
    # else to grab
    elif myrank == 1:
        print(f"rank {myrank} get an IP address", flush=True)
        pal_ip = ip_queue.get(timeout=None)
        ip_queue.put(my_ip)
    else:
        raise RuntimeError("example only designed for 2 workers")

    return pal_ip


def looped_function(rank_queue, ip_addrs, network_prefix, port, trigger_restart, is_a_restart):
    """Function that will be ProcessTemplate we launch many of"""

    # Grab a personal rank
    myrank = rank_queue.get(timeout=None)

    # Get my IP address
    my_ip = get_ip_address(myrank, network_prefix)[0]
    pal_ip = get_my_friends_ip(myrank, my_ip, ip_addrs)
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
                print(f'client sender hit error: {type(e)}: {e}. Exiting with {ExitErrorCodes.NONROOT_FAILURE.value}', flush=True)
                exit(ExitErrorCodes.NONROOT_FAILURE.value)
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
    is_a_restart = bool(environ.get('DRAGON_RESILIENT_RESTART', False))

    pg = ProcessGroup(restart=False, ignore_error_on_exit=False)

    # Use a queue as a quick and dirty way to get IP addresses to everyone
    n_nodes = 2
    port = args.port

    ip_addrs = Queue()
    rank_queue = Queue()
    for rank in range(n_nodes):
        rank_queue.put(rank)

    net_prefix = DEFAULT_TRANSPORT_NETIF
    pg.add_process(nproc=n_nodes,
                   template=ProcessTemplate(target=looped_function,
                                            args=(rank_queue, ip_addrs, net_prefix, port, args.trigger_restart, is_a_restart)))

    pg.init()
    pg.start()

    pg.join()
    pg.close()


if __name__ == '__main__':
    main()
