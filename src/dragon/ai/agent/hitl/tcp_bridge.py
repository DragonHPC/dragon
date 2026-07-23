"""TCP bridge: orchestrator-side relay between Dragon Queue and TCP client.

The :class:`HitlTcpBridge` runs as a daemon thread in the orchestrator
process.  Dragon primitives (Queue, Channel, DDict) stay intra-runtime;
the bridge relays HITL requests to an external TCP client as
newline-delimited JSON and forwards responses back.
"""

from __future__ import annotations

import json
import queue as _queue
import socket
import threading
from typing import Any

from .models import HumanApprovalResponse
from ..utils.errors import HITLBridgeError
from ..utils.logging import get_agent_logger

_log = get_agent_logger("hitl_tcp_bridge")


class HitlTcpBridge(threading.Thread):
    """Bridge between the intra-runtime Dragon HITL Queue and an external TCP client.

    Runs as a **daemon thread** in the orchestrator process.  Lifecycle:

    1. Binds a TCP server socket on a free port.
    2. Waits for a single HITL client to connect.
    3. In a loop, gets from ``hitl_queue``, sends the
         ``HumanApprovalRequest`` as newline-delimited JSON, reads the client's
         ``HumanApprovalResponse``, and puts it on the response queue.
    4. On :meth:`stop`, sends a ``{"type": "shutdown"}`` message and closes
       the socket.

    The external HITL client needs **no Dragon runtime, proxy, or
    cloudpickle** — it is a plain TCP client that receives/sends JSON.

    Args:

    hitl_queue:
        The Dragon Queue carrying ``(request, response_queue)`` tuples
        from agent processes (intra-runtime).
    host:
        Interface to bind to (default: all interfaces).
    port:
        Port to bind to (default: 0 = OS-assigned free port).
    """

    def __init__(self, hitl_queue: Any, host: str = "0.0.0.0", port: int = 0) -> None:
        super().__init__(daemon=True, name="HitlTcpBridge")
        self._hitl_queue = hitl_queue
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((host, port))
        self._server_sock.listen(1)
        self._addr = self._server_sock.getsockname()
        self._shutdown = threading.Event()

    @property
    def address(self) -> tuple[str, int]:
        """Return ``(host, port)`` the TCP server is listening on."""
        return self._addr

    def run(self) -> None:  # noqa: D401 — Thread.run override
        """Main loop — accept client, relay requests, relay responses."""
        self._server_sock.settimeout(1.0)
        conn = None
        rfile = wfile = None

        try:
            while not self._shutdown.is_set():
                # --- Accept a client if not yet connected -----------------
                if conn is None:
                    try:
                        conn, addr = self._server_sock.accept()
                        conn.settimeout(None)
                        rfile = conn.makefile("r")
                        wfile = conn.makefile("w")
                        _log.info("HITL Bridge: Client connected from %s", addr)
                    except socket.timeout:
                        continue

                # --- Read from Dragon Queue (intra-runtime) ---------------
                try:
                    request, response_queue = self._hitl_queue.get(timeout=1)
                except (TimeoutError, OSError, _queue.Empty):
                    # Timeout or queue destroyed — loop back and check shutdown
                    continue
                except Exception as exc:
                    _log.error("HITL queue read failed with unexpected error: %s", exc)
                    raise HITLBridgeError(
                        f"HITL queue read failed with unexpected error: {exc}"
                    ) from exc

                # --- Forward request over TCP as JSON ---------------------
                msg = json.dumps({
                    "tool_name": request.tool_name,
                    "tool_args": request.tool_args,
                    "context": request.context,
                    "agent_id": request.agent_id,
                    "task_id": request.task_id,
                    "dispatch_id": request.dispatch_id,
                    "timestamp": request.timestamp,
                })
                try:
                    wfile.write(msg + "\n")
                    wfile.flush()
                except (BrokenPipeError, ConnectionError, OSError):
                    _log.warning("HITL Bridge: Client disconnected. Waiting for reconnect...")
                    conn = rfile = wfile = None
                    # Re-queue the request so it isn't lost.
                    try:
                        self._hitl_queue.put((request, response_queue))
                    except Exception as exc:
                        _log.error(
                            "Failed to re-queue HITL request after client disconnect: %s", exc,
                        )
                        raise HITLBridgeError(
                            f"Failed to re-queue HITL request after client "
                            f"disconnect: {exc}.  The request for tool "
                            f"'{request.tool_name}' is lost."
                        ) from exc
                    continue

                # --- Read response from TCP client ------------------------
                try:
                    line = rfile.readline()
                    if not line:
                        raise ConnectionError("client closed connection")
                    data = json.loads(line)
                except (json.JSONDecodeError, ConnectionError, OSError) as exc:
                    _log.error("HITL Bridge: Error reading response: %s", exc, exc_info=True)
                    conn = rfile = wfile = None
                    # Unblock the agent's request_human_approval() coroutine
                    # with a rejection so it doesn't hang forever.
                    try:
                        response_queue.put(HumanApprovalResponse(
                            approved=False,
                            reason=f"HITL client disconnected: {exc}",
                        ))
                    except Exception as exc:
                        _log.debug("HITL Bridge: best-effort rejection enqueue failed (queue destroyed): %s", exc)
                    continue

                response = HumanApprovalResponse(
                    approved=data.get("approved", False),
                    reason=data.get("reason", ""),
                    is_feedback=data.get("is_feedback", False),
                )

                # --- Relay response back to agent (intra-runtime Queue) ---
                # Guard the put: the agent may have timed out and already
                # destroyed the per-request response queue.  An unhandled
                # exception here would kill the bridge thread, breaking ALL
                # future HITL approvals for the entire pipeline.
                try:
                    response_queue.put(response)
                except Exception as exc:
                    _log.warning(
                        "HITL Bridge: response enqueue failed (agent may have "
                        "timed out and destroyed the queue): %s", exc,
                    )

        finally:
            # Send shutdown notice to the client
            if conn and wfile:
                try:
                    wfile.write(json.dumps({"type": "shutdown"}) + "\n")
                    wfile.flush()
                except Exception as exc:
                    _log.warning("HITL Bridge: Shutdown write failed: %s", exc)
                try:
                    conn.close()
                except Exception as exc:
                    _log.warning("HITL Bridge: Connection close failed: %s", exc)
            self._server_sock.close()

    def stop(self) -> None:
        """Signal the bridge to shut down and wait for the thread to finish."""
        self._shutdown.set()
        # Close the server socket to unblock a potential accept() call,
        # matching the pattern used by TraceTcpBridge.stop().
        try:
            self._server_sock.close()
        except Exception as exc:
            _log.warning("HITL Bridge: server_sock.close in stop() failed: %s", exc)
        self.join(timeout=5)
