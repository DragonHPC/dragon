import logging
import os
import select
import signal
import socket
import subprocess
import time
from pathlib import Path

from ..util import create_hsta_env
from ... import channels as dch
from ... import utils as dutils
from ...dlogging.util import DragonLoggingServices as dls
from ...infrastructure import facts as dfacts
from ...infrastructure import messages as dmsg
from ...infrastructure import parameters as dparm
from ...infrastructure.connection import Connection
from ...launcher.util import next_tag

import threading


LOGGER = logging.getLogger(str(dls.TA)).getChild('dragon.transport.hsta.__main__')
sending_ta_halted = False


def logger_thread(handle, ls_in_wh):
    global sending_ta_halted
    transport_stdout = handle.stdout
    poll_obj = select.poll()
    poll_obj.register(transport_stdout, select.POLLIN)
    poll_timeout = 10
    try:
        while True:
            if poll_obj.poll(poll_timeout):
                line = transport_stdout.readline()
                if not line:
                    break
                # TODO: the call to decode fails occasionally
                #LOGGER.debug(line.decode('utf-8').strip())
                LOGGER.debug(line.strip())
            elif handle.poll() is not None and not sending_ta_halted:
                sending_ta_halted = True
                ls_in_wh.send(dmsg.TAHalted(tag=next_tag()).serialize())
                LOGGER.info('detected HSTA exit, transmitted TAHalted')
    except EOFError:
        pass
    except Exception as ex:
        LOGGER.debug(f'Got exception while handling HSTA output. Exception is {ex}')


def get_pid_by_name_and_parent(proc_name, parent_pid):
    pid_str = subprocess.check_output(['pgrep', f'--parent={parent_pid}', proc_name], encoding='utf-8')
    return int(pid_str)


def start_hsta_job(node_index, num_threads_per_node):
    hsta_binary = dfacts.HSTA_BINARY

    try:
        enable_valgrind = bool(int(os.environ['DRAGON_HSTA_ENABLE_VALGRIND']))
    except:
        enable_valgrind = False

    try:
        enable_perf = bool(int(os.environ['DRAGON_HSTA_ENABLE_PERF']))
    except:
        enable_perf = False

    try:
        enable_gdb = os.environ['DRAGON_HSTA_ENABLE_GDB']
    except:
        enable_gdb = None

    started_indirectly = False

    if enable_valgrind and node_index == 0:
        args = ['valgrind', '--track-origins=yes', '--leak-check=yes', str(hsta_binary), f'--num-threads={num_threads_per_node}']
    elif enable_perf and node_index == 0:
        args = ['perf', 'record', '-F', '99', '-g', '--user-callchains', '--all-user', '-v', str(hsta_binary), f'--num-threads={num_threads_per_node}']
        started_indirectly = True
    else:
        args = [str(hsta_binary), f'--num-threads={num_threads_per_node}']

    handle = subprocess.Popen(
        args,
        env=create_hsta_env(num_threads_per_node),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    if enable_gdb is not None:
        gdb_nids = [nid.strip() for nid in enable_gdb.split(',')]
    else:
        gdb_nids = []

    if started_indirectly:
        time.sleep(1)
        pid = get_pid_by_name_and_parent('dragon-hsta', handle.pid)
    elif str(node_index) in gdb_nids:
        # TODO: this needs work to fully automate the solution
        args_gdb = ['screen', '-dmS', f'"name=dragon-hsta-dbg:node={node_index}"', 'gdb', 'attach', f'{handle.pid}']
        handle_gdb = subprocess.Popen(args_gdb)
        pid = handle.pid
    else:
        pid = handle.pid

    return handle, pid


def kill_hsta_job(handle, pid):
    # kill local procs
    os.kill(pid, signal.SIGTERM)

    # wait for procs to complete since os.kill() can return
    # before the process has completed
    handle.wait()


def send_startup_info(la_channels_info, this_node_idx, num_threads_per_node, hsta_handle):
    # we're only using 4 bytes to store some of the sizes, since their
    # values never really get that big
    sizeof_uint64_t = 8
    sizeof_int = 4

    num_nodes = len(la_channels_info.nodes_desc)
    hsta_handle.stdin.write(num_nodes.to_bytes(sizeof_int, 'little'))
    hsta_handle.stdin.write(this_node_idx.to_bytes(sizeof_int, 'little'))
    hsta_handle.stdin.flush()

    for node_idx in range(num_nodes):
        logging_enabled = (LOGGER.getEffectiveLevel() == logging.DEBUG)
        node_idx_str = str(node_idx)
        host_id = la_channels_info.nodes_desc[node_idx_str].host_id

        try:
            no_net_config = bool(os.environ['DRAGON_HSTA_NO_NET_CONFIG'])
        except:
            no_net_config = True

        fabric_ep_addrs_available = la_channels_info.nodes_desc[node_idx_str].fabric_ep_addrs_available
        fabric_ep_addrs_available = fabric_ep_addrs_available and not no_net_config

        fabric_ep_addrs = la_channels_info.nodes_desc[node_idx_str].fabric_ep_addrs
        fabric_ep_addr_lens = la_channels_info.nodes_desc[node_idx_str].fabric_ep_addr_lens

        ip_addr = la_channels_info.nodes_desc[node_idx_str].ip_addrs[0].encode()
        ip_addr_len = len(ip_addr)
        num_nics = num_threads_per_node

        LOGGER.debug(f'{node_idx=}: {host_id=}, {ip_addr=}, {ip_addr_len=}, {num_nics=}, {fabric_ep_addrs_available=}')

        hsta_handle.stdin.write(logging_enabled.to_bytes(sizeof_int, 'little'))
        hsta_handle.stdin.write(host_id.to_bytes(sizeof_uint64_t, 'little'))
        hsta_handle.stdin.write(ip_addr_len.to_bytes(sizeof_int, 'little'))
        hsta_handle.stdin.write(ip_addr)
        hsta_handle.stdin.write(num_nics.to_bytes(sizeof_int, 'little'))
        hsta_handle.stdin.write(fabric_ep_addrs_available.to_bytes(sizeof_int, 'little'))
        hsta_handle.stdin.flush()

        if fabric_ep_addrs_available:
            for nic_idx in range(num_nics):
                # the libfabric endpoint name is encoded as a string in
                # the network config, so we need to convert it to bytes
                ep_addr = dutils.B64.str_to_bytes(fabric_ep_addrs[nic_idx])
                ep_addr_len = fabric_ep_addr_lens[nic_idx]

                LOGGER.debug(f'ep addr str={fabric_ep_addrs[nic_idx]}, {ep_addr_len=}')

                hsta_handle.stdin.write(ep_addr_len.to_bytes(sizeof_int, 'little'))
                hsta_handle.stdin.write(ep_addr)
                hsta_handle.stdin.flush()


def hsta(node_index: str , ch_in_sdesc: bytes):
    LOGGER.info(f'HSTA starting: nodeid={node_index}, host={socket.gethostname()}, pid={os.getpid()}')

    ta_in_ch = dch.Channel.attach(ch_in_sdesc)
    LOGGER.info('attached to transport agent infrastructure channel')
    ta_in_rh = Connection(inbound_initializer=ta_in_ch, policy=dparm.POLICY_INFRASTRUCTURE)

    # Connect to my stdin and stdout to receive messages from ls
    la_channels_info = dmsg.parse(ta_in_rh.recv())
    assert isinstance(la_channels_info, dmsg.LAChannelsInfo), 'HSTA not receiving LAChannelsInfo'
    LOGGER.info(f'received LAChannelsInfo - m5.3 ')
    LOGGER.info(repr(la_channels_info))

    # Attach to the shepherd channel
    ls_ch_sdesc = dutils.B64.from_str(la_channels_info.nodes_desc[node_index].shep_cd).decode()
    ls_in_ch = dch.Channel.attach(ls_ch_sdesc)
    LOGGER.info('attached to local services channels - pre a11.1')
    ls_in_wh = Connection(outbound_initializer=ls_in_ch, policy=dparm.POLICY_INFRASTRUCTURE)

    num_nodes = len(la_channels_info.nodes_desc)

    if (num_nodes > 1):
        # Set up gateways
        num_gws = dparm.this_process.num_gw_channels_per_node
        err_str = f'{num_gws=}, but must be a multiple of {dfacts.NUM_GW_TYPES} when using HSTA'
        assert (num_gws % dfacts.NUM_GW_TYPES) == 0, err_str

        num_threads_per_node = int(num_gws / dfacts.NUM_GW_TYPES)
        handle, pid = start_hsta_job(int(node_index), num_threads_per_node)

        # Start logging thread
        log_thread = threading.Thread(target=logger_thread, args=(handle, ls_in_wh))
        log_thread.start()

        send_startup_info(la_channels_info, int(node_index), num_threads_per_node, handle)

        LOGGER.info('HSTA ready and waiting for teardown message.')

    ls_in_wh.send(dmsg.TAPingSH(tag=next_tag()).serialize())
    LOGGER.info('transmitted TAPingSH - m7')

    # Necessary for local services teardown
    sh_ta_halt = dmsg.parse(ta_in_rh.recv())
    assert isinstance(sh_ta_halt, dmsg.SHHaltTA), 'HSTA expected SHHaltTA, but receieved wrong message'
    LOGGER.info(f'HSTA recvd SHHaltTA')

    if (num_nodes > 1):
        # Terminate the transport agent
        kill_hsta_job(handle, pid)
        #hsta_handle.wait()
        LOGGER.info('HSTA exiting')

    global sending_ta_halted
    if not sending_ta_halted:
        sending_ta_halted = True
        ls_in_wh.send(dmsg.TAHalted(tag=next_tag()).serialize())
        LOGGER.info('transmitted TAHalted')


def main(args=None):
    import argparse
    from distutils.util import strtobool

    from ...dlogging.util import DragonLoggingServices, setup_BE_logging
    from ...infrastructure.facts import DRAGON_LOGGER_SDESC
    from ...infrastructure.util import range_expr
    from ...utils import B64, set_procname

    set_procname(f'{dfacts.PROCNAME_RDMA_TA}-if')

    parser = argparse.ArgumentParser(description='Runs Dragon High-Speed transport agent (HSTA)')

    def get_log_level(level):
        x = str(level).upper()
        if x not in logging._nameToLevel:
            raise argparse.ArgumentTypeError(f'Unknown log level: {level}')
        return x

    parser.add_argument(
        '--log-level',
        choices=logging._nameToLevel.keys(),
        type=get_log_level,
        help="Set log level (default: %(default)s)",
    )
    parser.add_argument(
        '--dragon-logging',
        type=lambda x: bool(strtobool(x)),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        '--no-dragon-logging',
        dest='dragon_logging',
        action='store_false',
        help="Disable Dragon logging",
    )
    parser.add_argument(
        '--log-sdesc',
        type=B64.from_str,
        help="Serialized descriptor for Dragon logs",
    )

    if hasattr(os, 'sched_setaffinity'):
        max_cpu = os.cpu_count() - 1
        parser.add_argument(
            '--cpu-list',
            type=lambda s: set(range_expr(s)),
            default=f'0-{max_cpu}' if max_cpu > 0 else '0',
            help="List of processors separated by commas and may include ranges (default: %(default)s)",
        )

    parser.add_argument(
        'node_index',
        help="Node index in LAChannelsInfo message of this agent",
    )
    parser.add_argument(
        '--ch-in-sdesc',
        type=B64.from_str,
        help="Base64 encoded serialized input channel descriptor",
    )

    parser.set_defaults(
        log_level='NOTSET',
        dragon_logging=True,
        log_sdesc=os.environ.get(DRAGON_LOGGER_SDESC)
    )

    args = parser.parse_args()

    if args.dragon_logging:
        setup_BE_logging(
            service=DragonLoggingServices.TA,
            logger_sdesc=args.log_sdesc
        )
        LOGGER = logging.getLogger(str(dls.TA)).getChild('dragon.transport.hsta.__main__')
    else:
        logging.basicConfig(
            format='%(asctime)s %(levelname)-9s %(process)d %(name)s:%(filename)s:%(lineno)d %(message)s',
            level=args.log_level,
        )

    LOGGER.debug(f'Command line arguments: {args}')

    if hasattr(os, 'sched_setaffinity'):
        try:
            # Set CPU affinity to be inherited by HSTA executable
            os.sched_setaffinity(os.getpid(), args.cpu_list)
        except:
            LOGGER.exception('Failed to set CPU affinity')
            raise

    try:
        hsta(args.node_index, args.ch_in_sdesc.decode())
    except:
        LOGGER.exception('Uncaught exception')
        raise


if __name__ == '__main__':
    main()

