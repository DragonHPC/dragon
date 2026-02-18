import logging
from typing import Optional, TYPE_CHECKING
from multiprocessing import Process, connection

import msgspec
import zmq

if TYPE_CHECKING:
    from vllm.config import CacheConfig, ParallelConfig
    from vllm.v1.engine.utils import (
        CoreEngine,
        CoreEngineProcManager,
        EngineZmqAddresses,
        EngineHandshakeMetadata,
    )

logger = logging.getLogger(__name__)

# Timeout for polling during startup
STARTUP_POLL_PERIOD_MS = 10000


def dragon_wait_for_engine_startup(
    handshake_socket: zmq.Socket,
    addresses: "EngineZmqAddresses",
    core_engines: list["CoreEngine"],
    parallel_config: "ParallelConfig",
    cache_config: "CacheConfig",
    proc_manager: Optional["CoreEngineProcManager"],
    coord_process: Optional[Process],
):
    """Patched version of wait_for_engine_startup with enhanced logging.

    Updated for vLLM 0.12 compatibility - uses connection.wait for sentinel monitoring.
    """
    from vllm.v1.engine.utils import CoreEngineState, EngineHandshakeMetadata

    # Wait for engine core process(es) to send ready messages.
    local_count = parallel_config.data_parallel_size_local
    remote_count = len(core_engines) - local_count
    # [local, remote] counts
    conn_pending, start_pending = [local_count, remote_count], [0, 0]
    sentinels_list = []

    remote_should_be_headless = (
        not parallel_config.data_parallel_hybrid_lb
        and not parallel_config.data_parallel_external_lb
    )

    if proc_manager is not None:
        sentinels_list.extend(proc_manager.sentinels())
    if coord_process is not None:
        sentinels_list.append(coord_process.sentinel)

    logger.debug("Waiting for core engine processes to start...")
    logger.debug("List of sentinels being monitored: %s", sentinels_list)

    while any(conn_pending) or any(start_pending):
        try:
            logger.debug(
                "Polling handshake socket... conn_pending=%s, start_pending=%s",
                conn_pending,
                start_pending,
            )
            events = handshake_socket.poll(
                timeout=STARTUP_POLL_PERIOD_MS, flags=zmq.POLLIN
            )
            logger.debug("Poll returned events=%s", events)

            if events == 0:
                logger.debug("No handshake events, checking process sentinels...")
                ready = connection.wait(sentinels_list, timeout=0)
                logger.debug("Sentinel wait returned: %s", ready)

                if ready:
                    # One of the local core processes exited.
                    finished = proc_manager.finished_procs() if proc_manager else {}
                    if coord_process is not None and coord_process.exitcode is not None:
                        finished[coord_process.name] = coord_process.exitcode
                    logger.debug("Process failure detected: %s", finished)
                    raise RuntimeError(
                        "Engine core initialization failed. "
                        "See root cause above. "
                        f"Failed core proc(s): {finished}"
                    )

                if any(conn_pending):
                    logger.debug(
                        "Waiting for %d local, %d remote core engine proc(s) "
                        "to connect.",
                        *conn_pending,
                    )
                if any(start_pending):
                    logger.debug(
                        "Waiting for %d local, %d remote core engine proc(s) "
                        "to start.",
                        *start_pending,
                    )
                continue

            # Receive HELLO and READY messages from the input socket.
            logger.debug("Receiving handshake message from engine...")
            eng_identity, ready_msg_bytes = handshake_socket.recv_multipart()
            eng_index = int.from_bytes(eng_identity, "little")
            logger.debug("Received message from engine with index %s", eng_index)

            engine = next((e for e in core_engines if e.identity == eng_identity), None)
            if engine is None:
                logger.debug("ERROR: Unknown engine index %s", eng_index)
                raise RuntimeError(
                    f"Message from engine with unexpected data "
                    f"parallel rank: {eng_index}"
                )

            msg = msgspec.msgpack.decode(ready_msg_bytes)
            status, local, headless = msg["status"], msg["local"], msg["headless"]
            logger.debug(
                "Engine %s: status=%s, local=%s, headless=%s, current_state=%s",
                eng_index,
                status,
                local,
                headless,
                engine.state,
            )

            if local != engine.local:
                logger.debug(
                    "ERROR: Engine %s local mismatch: got %s, expected %s",
                    eng_index,
                    local,
                    engine.local,
                )
                raise RuntimeError(
                    f"{status} message from "
                    f"{'local' if local else 'remote'} "
                    f"engine {eng_index}, expected it to be "
                    f"{'local' if engine.local else 'remote'}"
                )

            # Remote engines must be headless iff we aren't in hybrid dp lb mode.
            if not local and headless != remote_should_be_headless:
                if headless:
                    raise RuntimeError(
                        f"Remote engine {eng_index} must not use "
                        f"--headless in external or hybrid dp lb "
                        f"mode"
                    )
                else:
                    raise RuntimeError(
                        f"Remote engine {eng_index} must use "
                        f"--headless unless in external or hybrid "
                        f"dp lb mode"
                    )

            if status == "HELLO" and engine.state == CoreEngineState.NEW:
                # Send init message with DP config info (vLLM 0.12 format).
                init_message = msgspec.msgpack.encode(
                    EngineHandshakeMetadata(
                        addresses=addresses,
                        parallel_config={
                            "data_parallel_master_ip": parallel_config.data_parallel_master_ip,
                            "data_parallel_master_port": parallel_config.data_parallel_master_port,
                            "_data_parallel_master_port_list": parallel_config._data_parallel_master_port_list,
                            "data_parallel_size": parallel_config.data_parallel_size,
                        },
                        parallel_config_hash=(
                            parallel_config.compute_hash()
                            if parallel_config.data_parallel_size > 1
                            else None
                        ),
                    )
                )
                handshake_socket.send_multipart(
                    (eng_identity, init_message), copy=False
                )
                conn_pending[0 if local else 1] -= 1
                start_pending[0 if local else 1] += 1
                engine.state = CoreEngineState.CONNECTED
            elif status == "READY" and engine.state == CoreEngineState.CONNECTED:
                # Setup KV cache config with initialization state from engine core process.
                num_gpu_blocks = cache_config.num_gpu_blocks or 0
                num_gpu_blocks += msg["num_gpu_blocks"]
                cache_config.num_gpu_blocks = num_gpu_blocks

                # In external DP LB mode, get coordinator address from rank 0
                if addresses.frontend_stats_publish_address is None:
                    addresses.frontend_stats_publish_address = msg.get(
                        "dp_stats_address"
                    )

                # Validate parallel config hash for DP workers (vLLM 0.12+)
                if parallel_config.data_parallel_size > 1:
                    worker_config_hash = msg.get("parallel_config_hash")
                    expected_hash = parallel_config.compute_hash()
                    if (
                        worker_config_hash is not None
                        and worker_config_hash != expected_hash
                    ):
                        logger.warning(
                            f"Worker config hash mismatch for engine {eng_index}. "
                            f"Worker: {worker_config_hash}, Expected: {expected_hash}"
                        )

                start_pending[0 if local else 1] -= 1
                engine.state = CoreEngineState.READY
                logger.debug(
                    "Engine %s transitioned to READY. conn_pending=%s, start_pending=%s",
                    eng_index,
                    conn_pending,
                    start_pending,
                )
            else:
                raise RuntimeError(
                    f"Unexpected {status} message for "
                    f"{'local' if local else 'remote'} engine "
                    f"{eng_index} in {engine.state} state."
                )

            logger.debug(
                "%s from %s core engine process %s.",
                status,
                "local" if local else "remote",
                eng_index,
            )
        except zmq.Again:
            logger.debug("ZMQ timeout, retrying...")
            continue

    logger.debug("All engines connected and ready!")
