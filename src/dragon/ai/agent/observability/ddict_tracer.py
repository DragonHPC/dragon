"""DDict-backed tracing processor.

Writes trace spans to the Dragon Distributed Dictionary as ordered lists,
enabling real-time visibility from within the Dragon runtime.  The
:class:`TraceTcpBridge` reads these lists and streams them over TCP to the
external :mod:`trace_viewer`.

DDict keys used (defined in :mod:`~dragon.ai.agent.config.ddict_keys`):

* ``TRACE_KEY`` (``{task_id}:{agent_id}:{dispatch_id}:trace``): ordered
  list of span dicts for a single agent invocation.
* ``TRACE_INDEX_SLOT_KEY`` (``{task_id}:trace:slot:{agent_id}``): per-agent
  slot written by the dispatcher so the TCP bridge can discover trace keys.

The slot key is maintained by the **dispatcher closure** (not this
processor) because each dispatcher writes to its own unique slot, avoiding
fan-out races.
"""

from __future__ import annotations

from typing import Any

from ..utils.logging import get_agent_logger
from ..config import TRACE_KEY
from ..ddict import DDictAccessor
from .tracer import Span, TracingProcessor

_log = get_agent_logger("ddict_tracer")


class DictTracingProcessor(TracingProcessor):
    """Write trace spans to a Dragon Distributed Dictionary.

    Parameters
    ----------
    ddict:
        An attached :class:`~dragon.data.ddict.DDict` instance.
    task_id:
        The current pipeline run's task_id (used for key formatting).
    agent_id:
        The agent_id for this processor instance.
    dispatch_id:
        The dispatch_id for this invocation.
    """

    def __init__(
        self,
        ddict: Any,
        task_id: str,
        agent_id: str,
        dispatch_id: str,
    ) -> None:
        self._accessor = DDictAccessor(ddict, agent_id=agent_id, task_id=task_id)
        self._task_id = task_id
        self._agent_id = agent_id
        self._dispatch_id = dispatch_id

        # No lock needed for the read-modify-write in on_span_start/end:
        #  1. trace_span is an async context manager — callbacks only fire
        #     on the event loop thread, never from a to_thread() worker.
        #  2. The get_list → mutate → put sequence is fully synchronous
        #     (no await), so asyncio cannot interleave another task.
        #  3. Each task creates its own DictTracingProcessor with a unique
        #     _trace_key (scoped by dispatch_id), so concurrent tasks
        #     write to different keys.

        # Pre-format the trace key for this agent/dispatch
        self._trace_key = TRACE_KEY.format(
            task_id=task_id, agent_id=agent_id, dispatch_id=dispatch_id
        )

        # Initialize the span list in DDict
        self._accessor.put(self._trace_key, [])

    def on_span_start(self, span: Span) -> None:
        """Append a span (with ``end_time=None``) to the DDict span list.

        Makes in-progress spans visible to the TCP bridge immediately.
        """
        spans: list = self._accessor.get_list(self._trace_key)
        spans.append(span.to_dict())
        self._accessor.put(self._trace_key, spans)

    def on_span_end(self, span: Span) -> None:
        """Update the existing span entry in the DDict span list.

        Finds the entry by ``span_id`` and replaces it with the completed
        version (``end_time`` set, final attributes, error).
        """
        spans: list = self._accessor.get_list(self._trace_key)
        for i, s in enumerate(spans):
            if s.get("span_id") == span.span_id:
                spans[i] = span.to_dict()
                break
        else:
            # span_start was missed — append the completed span
            spans.append(span.to_dict())
        self._accessor.put(self._trace_key, spans)
