"""TCP bridge: orchestrator-side relay from DDict trace data to external viewer.

The :class:`TraceTcpBridge` runs as a daemon thread in the orchestrator
process, polling the DDict for trace spans **and per-event content**
(LLM calls, tool calls, HITL decisions) and streaming them as newline-
delimited JSON to an external :mod:`trace_viewer` TCP client.

**Why a TCP bridge?**

Dragon constraint: only one Dragon program can run per runtime.  DDict is
only accessible from inside that runtime (shared memory).  The trace viewer
cannot be a second ``dragon`` command.

This pattern is identical to :class:`~dragon.ai.agent.hitl.tcp_bridge.HitlTcpBridge`
— a daemon thread inside the runtime relays data over TCP to a plain-Python
external client.

Message types streamed:

* ``{"type": "trace_start", "task_id": ..., "name": ..., "start_time": ...}``
* ``{"type": "span_start", "span": {...}}``
* ``{"type": "span_end", "span": {...}}``    (end_time just set)
* ``{"type": "span_update", "span": {...}}``  (attribute change, no end_time transition)
* ``{"type": "llm_event", "agent_id": ..., "index": ..., "event": {...}}``
* ``{"type": "tool_event", "agent_id": ..., "index": ..., "event": {...}}``
* ``{"type": "hitl_event", "agent_id": ..., "index": ..., "event": {...}}``
* ``{"type": "memory_event", "agent_id": ..., "index": ..., "event": {...}}``
* ``{"type": "trace_end", "task_id": ..., "end_time": ...}``
* ``{"type": "shutdown"}``

All ``type`` values are members of :class:`~.tracer.MsgType` (a ``str`` enum).
"""

from __future__ import annotations

import json
import socket
import threading
import time
from typing import Any

from ..config import (
    EVENT_CURSOR_KEY,
    LLM_EVENT_KEY, LLM_EVENT_COUNT_KEY,
    TOOL_EVENT_KEY, TOOL_EVENT_COUNT_KEY,
    HITL_EVENT_KEY, HITL_EVENT_COUNT_KEY,
    MEMORY_EVENT_KEY, MEMORY_EVENT_COUNT_KEY,
)
from .trace_protocol import MsgType
from ..utils.logging import get_agent_logger

_log = get_agent_logger("trace_tcp_bridge")


class TraceTcpBridge(threading.Thread):
    """Bridge between intra-runtime DDict trace data and external TCP viewer.

    Runs as a **daemon thread** in the orchestrator process.  Lifecycle:

    1. Binds a TCP server socket on a free port.
    2. Waits for a viewer client to connect.
    3. In a loop:
       a. For each known agent_id, polls its slot key in DDict to discover
          the agent's trace key (written by the dispatcher).
       b. For each discovered trace key, reads the span list and detects
          new/updated spans.
       c. Streams newline-delimited JSON to the connected client.
    4. On :meth:`stop`, sends a ``{"type": "shutdown"}`` message and closes.

    Unlike a shared index list, per-agent slot keys avoid read-modify-write
    races during fan-out (parallel dispatchers write to their own unique keys).

    The external viewer needs **no Dragon runtime** — it is a plain TCP
    client that receives JSON.

    Parameters
    ----------
    ddict:
        Attached DDict instance.
    task_id:
        Pipeline run task_id (used to format slot keys).
    agent_ids:
        List of agent_id values from the pipeline (used to poll slot keys).
    host:
        Interface to bind to (default: all interfaces).
    port:
        Port to bind to (default: 0 = OS-assigned free port).
    poll_interval:
        Seconds between DDict polls (default: 0.5).
    """

    def __init__(
        self,
        ddict: Any,
        task_id: str,
        agent_ids: list[str] | None = None,
        host: str = "0.0.0.0",
        port: int = 0,
        poll_interval: float = 0.5,
    ) -> None:
        super().__init__(daemon=True, name="TraceTcpBridge")
        self._ddict = ddict
        self._task_id = task_id
        self._agent_ids = agent_ids or []
        self._poll_interval = poll_interval
        # Pre-format slot keys for each agent
        self._slot_keys = {
            aid: f"{task_id}:trace:slot:{aid}" for aid in self._agent_ids
        }

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

    def run(self) -> None:
        """Main loop — accept client, poll DDict, stream spans."""
        self._server_sock.settimeout(1.0)
        conn = None

        # Accept a viewer connection (non-blocking check for shutdown)
        while not self._shutdown.is_set():
            try:
                conn, _ = self._server_sock.accept()
                break
            except socket.timeout:
                continue
            except OSError:
                return

        if conn is None:
            return

        conn.settimeout(2.0)

        _log.info(
            "Viewer connected. Polling %d agent slot(s): %s",
            len(self._slot_keys), list(self._slot_keys.keys()),
        )

        # Send trace_start so the viewer knows the pipeline name immediately
        self._send(conn, {
            "type": MsgType.TRACE_START,
            "task_id": self._task_id,
            "name": self._task_id,
            "start_time": time.time(),
        })

        # Polling state
        # Maps trace_key → number of spans already sent (cursor)
        key_cursors: dict[str, int] = {}
        # Maps span_id → last sent span dict (for detecting updates)
        seen_versions: dict[str, dict] = {}
        # Discovered trace keys in the order agents' slots appeared
        discovered_keys: list[str] = []
        discovered_set: set[str] = set()
        # Per-event cursors: maps (agent_id, dispatch_id, event_type) → count
        event_cursors: dict[str, int] = {}
        # Maps trace_key → (agent_id, dispatch_id) extracted from key format
        key_to_ids: dict[str, tuple[str, str]] = {}

        try:
            while not self._shutdown.is_set():
                try:
                    self._discover_trace_keys(discovered_keys, discovered_set)
                    self._poll_and_stream(
                        conn, discovered_keys, key_cursors, seen_versions
                    )
                    self._poll_and_stream_events(
                        conn, discovered_keys, key_to_ids, event_cursors
                    )
                except (BrokenPipeError, ConnectionResetError, OSError):
                    break
                except Exception:
                    _log.warning("Poll error", exc_info=True)

                self._shutdown.wait(self._poll_interval)
        finally:
            # Final poll: the orchestrator calls stop() immediately after
            # results are collected.  The last span_end and event data may
            # still be sitting in DDict unread.  Do one last sweep so the
            # viewer receives everything before we disconnect.
            try:
                self._discover_trace_keys(discovered_keys, discovered_set)
                self._poll_and_stream(
                    conn, discovered_keys, key_cursors, seen_versions
                )
                self._poll_and_stream_events(
                    conn, discovered_keys, key_to_ids, event_cursors
                )
            except Exception as exc:
                _log.warning("Final poll failed: %s", exc)

            # Send trace_end + shutdown messages
            try:
                self._send(conn, {
                    "type": MsgType.TRACE_END,
                    "task_id": self._task_id,
                    "end_time": time.time(),
                })
            except Exception as exc:
                _log.warning("trace_end send failed: %s", exc)
            try:
                self._send(conn, {"type": MsgType.SHUTDOWN})
            except Exception as exc:
                _log.warning("shutdown send failed: %s", exc)
            try:
                conn.close()
            except Exception as exc:
                _log.warning("conn.close failed: %s", exc)
            try:
                self._server_sock.close()
            except Exception as exc:
                _log.warning("server_sock.close failed: %s", exc)

    def _discover_trace_keys(
        self,
        discovered_keys: list[str],
        discovered_set: set[str],
    ) -> None:
        """Read per-agent slot keys in DDict to discover trace keys.

        Each dispatcher writes its agent's trace key to a unique slot:
        ``{task_id}:trace:slot:{agent_id}``.  Because each agent writes to
        its own slot, there is no read-modify-write race during fan-out.
        """
        for agent_id, slot_key in self._slot_keys.items():
            try:
                raw = self._ddict[slot_key]
                trace_key = raw if isinstance(raw, str) else raw.decode() if isinstance(raw, bytes) else str(raw)
            except KeyError:
                continue
            except Exception as exc:
                _log.warning("Error reading slot key %r: %s", slot_key, exc)
                continue
            if trace_key not in discovered_set:
                discovered_keys.append(trace_key)
                discovered_set.add(trace_key)
                _log.info("Discovered trace key for %s: %s", agent_id, trace_key)

    def _poll_and_stream(
        self,
        conn: socket.socket,
        discovered_keys: list[str],
        key_cursors: dict[str, int],
        seen_versions: dict[str, dict],
    ) -> None:
        """One poll cycle: read DDict, detect changes, stream to client."""
        for key in discovered_keys:
            try:
                spans = list(self._ddict[key])
            except KeyError:
                continue
            except Exception as exc:
                _log.warning("Span read failed for key %r: %s", key, exc)
                continue

            cursor = key_cursors.get(key, 0)

            # New spans (after cursor)
            for span_dict in spans[cursor:]:
                span_id = span_dict.get("span_id", "")
                self._send(conn, {"type": MsgType.SPAN_START, "span": span_dict})
                if span_dict.get("end_time") is not None:
                    # Span already completed before we polled — send end too
                    self._send(conn, {"type": MsgType.SPAN_END, "span": span_dict})
                seen_versions[span_id] = span_dict

            # Updated spans (before cursor — check for end_time changes)
            for span_dict in spans[:cursor]:
                span_id = span_dict.get("span_id", "")
                prev = seen_versions.get(span_id)
                if prev and span_dict != prev:
                    # Differentiate: end_time transition → span_end, else update
                    if prev.get("end_time") is None and span_dict.get("end_time") is not None:
                        msg_type = MsgType.SPAN_END
                    else:
                        msg_type = MsgType.SPAN_UPDATE
                    self._send(conn, {"type": msg_type, "span": span_dict})
                    seen_versions[span_id] = span_dict

            key_cursors[key] = len(spans)

    def _poll_and_stream_events(
        self,
        conn: socket.socket,
        discovered_keys: list[str],
        key_to_ids: dict[str, tuple[str, str]],
        event_cursors: dict[str, int],
    ) -> None:
        """Poll per-event count keys and stream new LLM/tool/HITL events.

        For each discovered trace key, extract (agent_id, dispatch_id) from
        the key format ``{task_id}:{agent_id}:{dispatch_id}:trace``, then
        check each event type's count key for new entries.
        """
        for key in discovered_keys:
            if key not in key_to_ids:
                # Parse: {task_id}:{agent_id}:{dispatch_id}:trace
                parts = key.rsplit(":", 3)
                if len(parts) >= 4:
                    # parts = [...task_id_parts..., agent_id, dispatch_id, "trace"]
                    # But task_id itself may contain colons.  Use the known
                    # task_id prefix to extract agent_id and dispatch_id.
                    suffix = key[len(self._task_id) + 1:]  # "agent_id:dispatch_id:trace"
                    suffix_parts = suffix.rsplit(":", 2)
                    if len(suffix_parts) == 3:
                        key_to_ids[key] = (suffix_parts[0], suffix_parts[1])
                    else:
                        continue
                else:
                    continue

            agent_id, dispatch_id = key_to_ids[key]
            fmt = dict(task_id=self._task_id, agent_id=agent_id, dispatch_id=dispatch_id)

            # Poll each event type
            for event_type, evt_key_tmpl, count_key_tmpl in (
                (MsgType.LLM_EVENT, LLM_EVENT_KEY, LLM_EVENT_COUNT_KEY),
                (MsgType.TOOL_EVENT, TOOL_EVENT_KEY, TOOL_EVENT_COUNT_KEY),
                (MsgType.HITL_EVENT, HITL_EVENT_KEY, HITL_EVENT_COUNT_KEY),
                (MsgType.MEMORY_EVENT, MEMORY_EVENT_KEY, MEMORY_EVENT_COUNT_KEY),
            ):
                cursor_id = EVENT_CURSOR_KEY.format(
                    agent_id=agent_id, dispatch_id=dispatch_id, event_type=event_type,
                )
                cursor = event_cursors.get(cursor_id, 0)

                try:
                    count = int(self._ddict[count_key_tmpl.format(**fmt)])
                except KeyError:
                    continue
                except Exception as exc:
                    _log.warning(
                        "Event count read failed for %s agent=%s: %s",
                        event_type, agent_id, exc,
                    )
                    continue

                for idx in range(cursor, count):
                    try:
                        event_data = self._ddict[evt_key_tmpl.format(index=idx, **fmt)]
                        self._send(conn, {
                            "type": event_type,
                            "agent_id": agent_id,
                            "dispatch_id": dispatch_id,
                            "index": idx,
                            "event": event_data,
                        })
                    except KeyError:
                        pass  # event not yet written — expected during streaming
                    except Exception as exc:
                        _log.warning(
                            "Event read failed for %s index=%d agent=%s: %s",
                            event_type, idx, agent_id, exc,
                        )

                event_cursors[cursor_id] = count

    @staticmethod
    def _send(conn: socket.socket, msg: dict) -> None:
        """Send a newline-delimited JSON message."""
        data = json.dumps(msg, default=str) + "\n"
        conn.sendall(data.encode("utf-8"))

    def stop(self) -> None:
        """Signal the bridge to shut down and wait for the thread to finish."""
        self._shutdown.set()
        # Close server socket to unblock accept()
        try:
            self._server_sock.close()
        except Exception as exc:
            _log.warning("server_sock.close in stop() failed: %s", exc)
        self.join(timeout=5)
