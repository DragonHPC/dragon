"""Dragon-compatible vLLM worker process helpers for vLLM 0.12.x.

These functions replace selected methods from
``vllm.v1.executor.multiproc_executor.WorkerProc``.  They keep vLLM's worker
protocol intact while adapting readiness pipes, shutdown handling, and response
message-queue handle processing for Dragon-managed processes and channels.
"""

import logging
import signal
import threading
from threading import Thread
from typing import Any, Optional, cast

logger = logging.getLogger(__name__)
from vllm.v1.executor.multiproc_executor import (
    WorkerProcHandle,
    UnreadyWorkerProcHandle,
)
from vllm.distributed.device_communicators.shm_broadcast import MessageQueue, Handle


def dragon_worker_main(*args, **kwargs):
    """Run a vLLM worker process with Dragon-aware shutdown handling.

    The function mirrors vLLM's worker entry point but adds a parent-death
    monitor, handles Dragon channel cleanup during teardown, sends the worker
    response message-queue handles back over the ready pipe, and enters the
    worker busy loop until cancellation.
    """

    from vllm.v1.executor.multiproc_executor import WorkerProc

    # Signal handler used for graceful termination.
    # SystemExit exception is only raised once to allow this and worker
    # processes to terminate without error
    shutdown_requested = False

    def signal_handler(signum, frame):
        """Request one graceful worker shutdown when SIGTERM or SIGINT arrives."""
        nonlocal shutdown_requested
        if not shutdown_requested:
            shutdown_requested = True
            raise SystemExit()

    # Either SIGTERM or SIGINT will terminate the worker
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    worker = None
    # tuple[Connection, Connection]
    reader, ready_writer = kwargs.pop("ready_pipe")
    death_pipe = kwargs.pop("death_pipe", None)
    shutdown_event = threading.Event()
    # Start death monitoring thread if death_pipe is provided
    if death_pipe is not None:

        def monitor_parent_death():
            """Set the local shutdown event when the parent death pipe closes."""
            try:
                # This will block until parent process exits (pipe closes)
                death_pipe.recv()
            except EOFError:
                # Parent process has exited, terminate this worker
                logger.info("Parent process exited, terminating worker")
                # Send signal to self to trigger clean shutdown
                shutdown_event.set()
            except Exception as e:
                logger.warning("Death monitoring error: %s", e)

        death_monitor = Thread(
            target=monitor_parent_death, daemon=True, name="WorkerDeathMonitor"
        )
        death_monitor.start()

    try:
        reader.close()
        worker = WorkerProc(*args, **kwargs)

        # Send READY once we know everything is loaded
        ready_writer.send(
            {
                "status": WorkerProc.READY_STR,
                "handle": worker.worker_response_mq.export_handle(),
                "peer_response_handles": worker.peer_response_handles,
            }
        )

        # Ensure message queues are ready. Will deadlock if re-ordered.
        # Must be kept consistent with the Executor
        if worker.rpc_broadcast_mq is not None:
            worker.rpc_broadcast_mq.wait_until_ready()
        worker.worker_response_mq.wait_until_ready()

        # Close ready_writer safely - Dragon channels may already be destroyed
        # by parent process, so we ignore errors here
        try:
            ready_writer.close()
        except Exception:
            pass
        ready_writer = None

        worker.worker_busy_loop(cancel=shutdown_event)

    except Exception:
        # NOTE: if an Exception arises in busy_loop, we send
        # a FAILURE message over the MQ RPC to notify the Executor,
        # which triggers system shutdown.
        # TODO(rob): handle case where the MQ itself breaks.

        if ready_writer is not None:
            logger.exception("WorkerProc failed to start.")
        elif shutdown_event.is_set():
            logger.info("WorkerProc shutting down.")
        else:
            logger.exception("WorkerProc failed.")

        # The parent sends a SIGTERM to all worker processes if
        # any worker dies. Set this value so we don't re-throw
        # SystemExit() to avoid zmq exceptions in __del__.
        shutdown_requested = True

    finally:
        # Close connections safely - Dragon channels may already be destroyed
        if ready_writer is not None:
            try:
                ready_writer.close()
            except Exception:
                pass
        if death_pipe is not None:
            try:
                death_pipe.close()
            except Exception:
                pass
        # Clean up once worker exits busy loop
        if worker is not None:
            worker.shutdown()


def dragon_wait_for_response_handle_ready(
    handles: dict[str, Any], proc_handle: UnreadyWorkerProcHandle
) -> WorkerProcHandle:
    """Process the response handles from a worker and create WorkerProcHandle.

    This is the Dragon-compatible version of WorkerProc.wait_for_response_handle_ready.

    :param handles: Ready-pipe payload containing the local response handle and
        optional peer response handles.
    :type handles: dict[str, Any]
    :param proc_handle: Worker process handle that has not yet been marked ready.
    :type proc_handle: UnreadyWorkerProcHandle
    :returns: Ready worker handle used by vLLM's multiprocess executor.
    :rtype: WorkerProcHandle
    """
    response_handle: Handle = handles["handle"]
    worker_response_mq: MessageQueue | None = None

    # Check if there are local readers (single-node case)
    if len(response_handle.local_reader_ranks) > 0:
        worker_response_mq = MessageQueue.create_from_handle(response_handle, 0)

    # Handle peer response handles for multi-node support
    peer_response_handles = handles.get("peer_response_handles", [])
    peer_worker_response_mqs = [
        (
            MessageQueue.create_from_handle(handle, -1)
            if handle.remote_subscribe_addr is not None
            else None
        )
        for handle in peer_response_handles
    ]

    return WorkerProcHandle.from_unready_handle(
        proc_handle,
        worker_response_mq,
        peer_worker_response_mqs=peer_worker_response_mqs,
    )


def dragon_wait_for_ready(
    unready_proc_handles: list[UnreadyWorkerProcHandle],
) -> list[WorkerProcHandle]:
    """Wait for all worker processes to be ready.

    This is the Dragon-compatible version of WorkerProc.wait_for_ready.
    Updated for vLLM 0.12 compatibility with peer_response_handles support.

    :param unready_proc_handles: Worker handles with ready pipes to monitor.
    :type unready_proc_handles: list[UnreadyWorkerProcHandle]
    :returns: Worker handles after their response queues have been attached.
    :rtype: list[WorkerProcHandle]
    :raises Exception: If any worker reports a non-ready status or exits before
        sending a ready message.
    """
    import multiprocessing

    e = Exception(
        "WorkerProc initialization failed due to "
        "an exception in a background process. "
        "See stack trace for root cause."
    )

    pipes = {handle.ready_pipe: handle for handle in unready_proc_handles}
    ready_proc_handles: list[Optional[WorkerProcHandle]] = [None] * len(
        unready_proc_handles
    )

    while pipes:
        ready = multiprocessing.connection.wait(pipes.keys())
        for pipe in ready:
            # Dragon uses its own Connection class, so we use duck-typing
            # instead of strict isinstance check
            try:
                # Wait until the WorkerProc is ready.
                unready_proc_handle = pipes.pop(pipe)
                response: dict[str, Any] = pipe.recv()
                if response["status"] != "READY":
                    raise e

                # Calculate the index for this worker
                idx = unready_proc_handle.rank % len(ready_proc_handles)

                # Use the new response handle processing
                ready_proc_handles[idx] = dragon_wait_for_response_handle_ready(
                    response, unready_proc_handle
                )

            except EOFError:
                e.__suppress_context__ = True
                raise e from None

            finally:
                # Close connection.
                pipe.close()

    return cast(list[WorkerProcHandle], ready_proc_handles)
