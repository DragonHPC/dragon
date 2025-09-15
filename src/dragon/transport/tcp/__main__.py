import asyncio
import logging
import os
import socket
import ssl
from collections import defaultdict
from typing import Union

from ...channels import Channel, register_gateways_from_env, GatewayMessage
from ...infrastructure import messages as dmsg
from ...infrastructure.connection import Connection, ConnectionOptions
from ...infrastructure.facts import GW_ENV_PREFIX, DEFAULT_TRANSPORT_PORT, FRONTEND_HOSTID
from ...infrastructure.facts import DEFAULT_OVERLAY_NETWORK_PORT, DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE
from ...launcher.util import next_tag
from ...utils import B64
from ...utils import host_id as get_host_id, set_host_id
from ...dtypes import DEFAULT_WAIT_MODE, IDLE_WAIT

from .agent import Agent
from .task import cancel_all_tasks
from .transport import Address, StreamTransport

FRONTEND_HELP = """FRONTEND_IP specifies the single IP address for the frontend of
the dragon runtime"""

BACKEND_HELP = """BACKEND_IP specifies the list of backend compute node IP addresses
for execution of the dragon runtime backend"""

PORT_HELP = f"""PORT specifies the port to be used for infrastructure communication. By
default, {DEFAULT_OVERLAY_NETWORK_PORT} is used for runtime infrastructure
or {DEFAULT_TRANSPORT_PORT} for backend communiction"""

LOGGER = logging.getLogger("dragon.transport.tcp.__main__")

user_initiated = False
infrastructure = False
out_of_band_connect = False
out_of_band_accept = False


def single_recv(ch_sdesc: bytes) -> Union[tuple(dmsg.all_message_classes)]:
    ch = Channel.attach(ch_sdesc)
    try:
        with Connection(inbound_initializer=ch) as conn:
            return dmsg.parse(conn.recv())
    finally:
        ch.detach()


def open_connection(ch_in_sdesc: bytes = None, ch_out_sdesc: bytes = None) -> Connection:
    if ch_in_sdesc is None and ch_out_sdesc is None:
        raise ValueError("Requires at least one serialized channel descriptor")
    ch_in = ch_out = conn = None
    if ch_in_sdesc is not None:
        ch_in = Channel.attach(ch_in_sdesc)
    if ch_out_sdesc is not None:
        ch_out = Channel.attach(ch_out_sdesc)
    conn = Connection(
        inbound_initializer=ch_in,
        outbound_initializer=ch_out,
        options=ConnectionOptions(creation_policy=ConnectionOptions.CreationPolicy.EXTERNALLY_MANAGED),
    )
    conn.ghost = True
    conn.open()
    return conn


def close_connection(conn: Connection) -> None:
    conn.close()
    if conn.inbound_chan is not None:
        conn.inbound_chan.detach()
    if conn.outbound_chan is not None:
        conn.outbound_chan.detach()


async def tcp_transport_agent(
    node_index: str = None,
    ch_in_sdesc: B64 = None,
    ch_out_sdesc: B64 = None,
    default_port: int = DEFAULT_TRANSPORT_PORT,
    tls_enabled: bool = True,
    cafile: str = None,
    certfile: str = None,
    keyfile: str = None,
    tls_verify: bool = True,
    max_threads: int = None,
    host_ids: list[str] = None,
    ip_addrs: list[str] = None,
    oob_ip_addr: str = None,
    oob_port: str = None,
    frontend: bool = False,
) -> None:

    # TODO: get rid of globals from proxy-api work (when possible)
    global user_initiated
    global infrastructure
    global out_of_band_connect
    global out_of_band_accept

    from ...dlogging.util import DragonLoggingServices

    if infrastructure:
        LOGGER = logging.getLogger(DragonLoggingServices.ON).getChild("transport.tcp.__main__")
        if frontend:
            up_msg = dmsg.OverlayPingLA(next_tag())
            halt_msg = dmsg.LAHaltOverlay
        else:
            up_msg = dmsg.OverlayPingBE(next_tag())
            halt_msg = dmsg.BEHaltOverlay
    elif user_initiated:
        LOGGER = logging.getLogger(DragonLoggingServices.TA).getChild("transport.tcp.__main__")
        up_msg = dmsg.TAPingSH(next_tag())
        halt_msg = dmsg.SHHaltTA
    else:
        LOGGER = logging.getLogger(DragonLoggingServices.OOB).getChild("transport.tcp.__main__")
        halt_msg = dmsg.UserHaltOOB

    # This tells the library to be silent about any transport completion timeouts.
    GatewayMessage.silence_transport_timeouts()

    if max_threads is not None:
        # Set a new default executor that limits the maximum number of worker
        # threads to use.
        from concurrent.futures import ThreadPoolExecutor

        executor = ThreadPoolExecutor(
            max_workers=int(max_threads),
            thread_name_prefix="asyncio",
        )
        loop = asyncio.get_running_loop()
        loop.set_default_executor(executor)

    # Initial receive from input channel to get LAChannelsInfo message
    if user_initiated:
        la_channels_info = await asyncio.to_thread(single_recv, ch_in_sdesc.decode())
        assert isinstance(la_channels_info, dmsg.LAChannelsInfo), "Did not receive LAChannelsInfo from local services"

        LOGGER.debug(f"Node {node_index}: LAChannelsInfo: {la_channels_info.get_sdict()}")

        try:  # Validate LAChannelsInfo nodes_desc structure
            # Verify given node index in LAChannelInfo node descriptors
            assert node_index in la_channels_info.nodes_desc, f"Invalid node index: {node_index}"
        except AssertionError:
            LOGGER.critical(f"Invalid LAChannelsInfo={la_channels_info.get_sdict()}")
            raise

        # Get my node descriptor
        node_desc = la_channels_info.nodes_desc[node_index]

        # TODO Add support for multiple addresses per host ID
        # Create host ID => address mapping from LAChannelsInfo node descriptors
        nodes = {}
        for x in la_channels_info.nodes_desc.values():
            try:
                # XXX Use the first IP address until multiple addresses per host ID
                # XXX are supported.
                addr = Address.from_netloc(str(x.ip_addrs[0]))
                nodes[int(x.host_id)] = Address(addr.host, addr.port or default_port)
            except Exception:
                LOGGER.critical(
                    f"Failed to build node-address mapping from LAChannelsInfo.nodes_desc: {la_channels_info.nodes_desc}"
                )
                raise
    elif out_of_band_connect:
        # If this agent is connecting to another dragon instance to allow out-of-band
        # communication, then all host_ids should map to the same target address
        addr = Address.from_netloc(f"{oob_ip_addr}:{oob_port}")
        nodes = defaultdict(lambda: addr)
    elif out_of_band_accept:
        # If this agent is accepting connections from remote dragon instances to allow
        # out-of-band communication, then it can have an empty nodes dict
        nodes = {}
    else:
        # If infrastructure, create same data structures as above, but use information input
        # since there's no LAChannelsInfo for us
        nodes = {}
        for ip_addr, host_id in zip(ip_addrs, host_ids):
            try:
                # XXX Use the first IP address until multiple addresses per host ID
                # XXX are supported.
                addr = Address.from_netloc(str(ip_addr))
                nodes[int(host_id)] = Address(addr.host, addr.port or default_port)

            except Exception:
                LOGGER.critical("Failed to build node-address mapping")
                raise

    try:
        # Establish connection for command-and-control
        if user_initiated:
            ch_out_sdesc = B64.from_str(node_desc.shep_cd)
        control = await asyncio.to_thread(open_connection, ch_in_sdesc.decode(), ch_out_sdesc.decode())
    except Exception:
        if user_initiated:
            LOGGER.critical(
                f"Failed to initialize the control connection: Requires valid input and output channel descriptors for node index {node_index}: input_channel={ch_in_sdesc}, output_channel={ch_out_sdesc}, LAChannelsInfo={la_channels_info.get_sdict()}"
            )
        elif infrastructure:
            LOGGER.critical(
                f"Failed to initialize the control connection: Requires valid input and output channel descriptors: input_channel={ch_in_sdesc}, output_channel={ch_out_sdesc}"
            )
        raise

    try:
        # Create transport
        if user_initiated:
            transport = StreamTransport(nodes[int(node_desc.host_id)])
            wait_mode = DEFAULT_WAIT_MODE
        elif infrastructure:
            transport = StreamTransport(nodes[int(get_host_id())])
            wait_mode = IDLE_WAIT
        else:
            hostname = socket.gethostname()
            ip_addr = socket.gethostbyname(hostname)
            local_addr = Address.from_netloc(f"{ip_addr}:{oob_port}")
            transport = StreamTransport(local_addr)
            if out_of_band_connect:
                transport._oob_connect = True
            else:
                transport._oob_accept = True
            wait_mode = IDLE_WAIT

        LOGGER.info(f"Created transport: {transport.addr} with wait mode {wait_mode}")

        if tls_enabled:
            client_ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            server_ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            client_ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_3
            server_ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_3
            if cafile is not None:
                server_ssl_ctx.load_verify_locations(cafile)
                client_ssl_ctx.load_verify_locations(cafile)
            if certfile is not None or keyfile is not None:
                server_ssl_ctx.load_cert_chain(certfile, keyfile)
                client_ssl_ctx.load_cert_chain(certfile, keyfile)
            if not tls_verify:
                client_ssl_ctx.check_hostname = False
                client_ssl_ctx.verify_mode = ssl.CERT_NONE
                server_ssl_ctx.verify_mode = ssl.CERT_NONE
            transport.server_options.update(ssl=server_ssl_ctx)
            transport.default_connection_options.update(ssl=client_ssl_ctx)

        # Create agent
        async with Agent(transport, nodes, wait_mode=wait_mode) as agent:
            LOGGER.info("Created agent")

            n_gw = 0

            # Create clients for each gateway channel
            if user_initiated:
                n_gw = la_channels_info.num_gw_channels
            elif infrastructure:
                n_gw = DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE
            elif out_of_band_connect:
                rt_uid = os.environ["DRAGON_REMOTE_RT_UID"]
                encoded_channel_sdesc = os.environ[f"DRAGON_RT_UID__{rt_uid}"]
                channel_sdesc = B64.from_str(encoded_channel_sdesc).decode()
                agent.new_client(channel_sdesc)
                LOGGER.debug(f"Created client for OOB gateway channel")

            for i in range(n_gw):
                encoded_channel_sdesc = os.environ[GW_ENV_PREFIX + str(i + 1)]
                channel_sdesc = B64.from_str(encoded_channel_sdesc).decode()
                agent.new_client(channel_sdesc)
                LOGGER.debug(f"Created client for gateway channel {i}")

            # Send TAPingSH to local services to acknowledge transport is active
            # XXX What is tag?
            if user_initiated or infrastructure:
                await asyncio.to_thread(control.send, up_msg.serialize())
                LOGGER.info(f"Sent {type(up_msg)} reply")

            while agent.is_running():
                LOGGER.debug("Agent is running, polling control")
                # XXX Should we sleep in between polls to avoid having a
                # XXX thread constantly blocking waiting for control input?
                # await asyncio.sleep(5.0)

                # Wait for control message
                ready = await asyncio.to_thread(control.poll, timeout=5.0)
                if ready:
                    # Receive control message
                    data = await asyncio.to_thread(control.recv)
                    msg = dmsg.parse(data)
                    # Process control message
                    if isinstance(msg, halt_msg):
                        LOGGER.info(f"Received {type(msg)}")
                        break
                    elif isinstance(msg, dmsg.TAUpdateNodes):
                        agent.update_nodes(msg.nodes)
                        LOGGER.info(f"Received {type(msg)}")
                        continue
                    LOGGER.warning(f"Received unsupported control message: {msg}")

            LOGGER.debug("Agent is not running, terminating")

            # Stop the Stream Transport first because of ordering issues that
            # appeared beginning in python 3.12 and then cancel anything remaining
            await transport.stop()
            LOGGER.debug("Stopped transport. Cancelling any remaining tasks")
            await cancel_all_tasks()

    except Exception as e:
        LOGGER.exception(f"Unable to run TCP agent: {e}")
        await asyncio.to_thread(close_connection, control)
        raise e

    # Hand back our connection to local services so we can signal our exit
    return control


def main(args=None):
    import argparse
    from distutils.util import strtobool

    from ...dlogging.util import DragonLoggingServices, setup_BE_logging
    from ...infrastructure.facts import PROCNAME_OVERLAY_TA, PROCNAME_TCP_TA, PROCNAME_OOB_TA
    from ...infrastructure.util import range_expr
    from ...utils import set_procname

    parser = argparse.ArgumentParser(description="Runs Dragon TCP socket transport agent (TSTA)")

    parser.add_argument(
        "--infrastructure",
        action="store_true",
        help="Set to use for runtime infrastructure rather than backend communiction",
    )
    parser.add_argument("--ip-addrs", metavar="FRONTEND_IP", dest="ip_addrs", nargs="+", type=str, help=FRONTEND_HELP)
    parser.add_argument("--oob-ip-addr", type=str, help="Target IP address for out-of-band communication")
    parser.add_argument("--oob-port", type=str, help="Listening port at target for out-of-band communication")
    parser.add_argument("--ch-in-sdesc", type=B64.from_str, help="Base64 encoded serialized input channel descriptor")
    parser.add_argument("--ch-out-sdesc", type=B64.from_str, help="Base64 encoded serialized output channel descriptor")
    parser.add_argument("--host-ids", dest="host_ids", type=str, nargs="+")

    parser.add_argument("-p", "--port", type=int, help="Listening port (default: %(default)s)")

    parser.add_argument("--tls", dest="tls_enabled", type=lambda x: bool(strtobool(x)), help=argparse.SUPPRESS)
    parser.add_argument("--no-tls", dest="tls_enabled", action="store_false", help="Disable TLS")
    parser.add_argument("--cafile", metavar="FILE", help="CA certificate for verifying clients")
    parser.add_argument("--certfile", metavar="FILE", help="X509 certificate for server and client auth")
    parser.add_argument("--keyfile", metavar="FILE", help="Private key corresponding to --certfile")
    parser.add_argument("--tls-verify", dest="tls_verify", type=lambda x: bool(strtobool(x)), help=argparse.SUPPRESS)
    parser.add_argument("--no-tls-verify", dest="tls_verify", action="store_false", help="Disable TLS verficiation")

    parser.add_argument("--max-threads", type=int, help="Maximum number of threads to use")
    parser.add_argument("--frontend", action="store_true", help="Whether this is being started on the frontend")

    def get_log_level(level):
        x = str(level).upper()
        if x not in logging._nameToLevel:
            raise argparse.ArgumentTypeError(f"Unknown log level: {level}")
        return x

    parser.add_argument(
        "--log-level",
        choices=logging._nameToLevel.keys(),
        type=get_log_level,
        help="Set log level (default: %(default)s)",
    )
    parser.add_argument(
        "--dragon-logging",
        type=lambda x: bool(strtobool(x)),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--no-dragon-logging",
        dest="dragon_logging",
        action="store_false",
        help="Disable Dragon logging",
    )
    parser.add_argument(
        "--log-sdesc",
        type=B64.from_str,
        help="Serialized descriptor for Dragon logs",
    )

    if hasattr(os, "sched_setaffinity"):
        max_cpu = os.cpu_count() - 1
        parser.add_argument(
            "--cpu-list",
            type=lambda s: set(range_expr(s)),
            default=f"0-{max_cpu}" if max_cpu > 0 else "0",
            help="List of processors separated by commas and may include ranges (default: %(default)s)",
        )

    parser.add_argument(
        "node_index",
        help="Node index in LAChannelsInfo message of this agent",
    )

    parser.set_defaults(
        port=os.environ.get("DRAGON_TRANSPORT_TCP_PORT", DEFAULT_TRANSPORT_PORT),
        tls_enabled=os.environ.get("DRAGON_TRANSPORT_TCP_TLS_ENABLED", True),
        cafile=os.environ.get("DRAGON_TRANSPORT_TCP_CAFILE"),
        certfile=os.environ.get("DRAGON_TRANSPORT_TCP_CERTFILE"),
        keyfile=os.environ.get("DRAGON_TRANSPORT_TCP_KEYFILE"),
        tls_verify=os.environ.get("DRAGON_TRANSPORT_TCP_TLS_VERIFY", True),
        max_threads=os.environ.get("DRAGON_TRANSPORT_TCP_MAX_THREADS"),
        log_level=os.environ.get("DRAGON_TRANSPORT_LOG_LEVEL", "WARNING"),
        dragon_logging=True,
        log_sdesc=None,
    )

    args = parser.parse_args()

    global user_initiated
    global infrastructure
    global out_of_band_connect
    global out_of_band_accept

    if args.oob_ip_addr != None:
        out_of_band_connect = True
    elif args.oob_port != None:
        out_of_band_accept = True
    elif args.infrastructure:
        infrastructure = True
    else:
        user_initiated = True

    if args.frontend:
        set_host_id(FRONTEND_HOSTID)

    if user_initiated:
        set_procname(PROCNAME_TCP_TA)
    elif infrastructure:
        set_procname(PROCNAME_OVERLAY_TA)
    else:
        set_procname(PROCNAME_OOB_TA)

    # In the OOB accept case, the sendmsg/getmsg/poll used for the local
    # channel operation can (and frequently will) target an off-node channel
    if out_of_band_accept:
        register_gateways_from_env()

    if args.dragon_logging and args.log_sdesc is not None:
        if user_initiated:
            service = DragonLoggingServices.TA
        elif infrastructure:
            service = DragonLoggingServices.ON
        else:
            service = DragonLoggingServices.OOB

        log_level, _ = setup_BE_logging(service=service, logger_sdesc=args.log_sdesc)
        # We will ignore the argument level because we want a unified setting
        # defined by user arguments to the launcher
        LOGGER.setLevel(log_level)
    else:
        logging.basicConfig(
            format="%(asctime)s %(levelname)-9s %(process)d %(name)s:%(filename)s:%(lineno)d %(message)s",
            level=args.log_level,
        )

    LOGGER.info(f"Command line arguments: {args}")

    if hasattr(os, "sched_setaffinity"):
        try:
            os.sched_setaffinity(os.getpid(), args.cpu_list)
        except Exception:
            LOGGER.exception("Failed to set CPU affinity")
            raise

    try:
        control = asyncio.run(
            tcp_transport_agent(
                args.node_index,
                args.ch_in_sdesc,
                args.ch_out_sdesc,
                args.port,
                args.tls_enabled,
                args.cafile,
                args.certfile,
                args.keyfile,
                args.tls_verify,
                args.max_threads,
                args.host_ids,
                args.ip_addrs,
                args.oob_ip_addr,
                args.oob_port,
                args.frontend,
            )
        )
    except Exception:
        LOGGER.exception("")
        raise

    # Send exit control message
    if user_initiated:
        down_msg = dmsg.TAHalted(tag=next_tag())
    elif infrastructure:
        down_msg = dmsg.OverlayHalted(tag=next_tag())

    try:
        if user_initiated or infrastructure:
            control.send(down_msg.serialize())
    except Exception:
        LOGGER.exception("Failed to send TAHalted")
        raise
    else:
        LOGGER.info("Sent TAHalted and exiting")
    finally:
        close_connection(control)


if __name__ == "__main__":
    main()
