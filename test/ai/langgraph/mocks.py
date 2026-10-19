"""Reusable mock objects for Dragon LangGraph integration tests.

Mocks LangGraph dependencies and transport internals so that the
dragon.ai.langgraph module logic can be tested.  Dragon primitives
(Queue, Process) are used directly when possible.
"""

import cloudpickle
from typing import Any, Optional
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# MockDragonQueue — drop-in for dragon.native.queue.Queue
# ---------------------------------------------------------------------------

class MockDragonQueue:
    """In-memory queue simulating dragon.native.queue.Queue for unit tests.

    Usage::

        queue = MockDragonQueue()
        queue.put({"task_id": "123", "node_name": "writer"})
        msg = queue.get()  # returns the dict
    """

    def __init__(self) -> None:
        self._items: list = []
        self._serialized: bytes = b"mock_queue_handle"

    def put(self, item: Any) -> None:
        self._items.append(item)

    def get(self, timeout: Optional[float] = None) -> Any:
        if not self._items:
            import queue
            raise queue.Empty()
        return self._items.pop(0)

    def serialize(self) -> bytes:
        return self._serialized


# ---------------------------------------------------------------------------
# MockProcess — drop-in for dragon.native.process.Process
# ---------------------------------------------------------------------------

class MockProcess:
    """Mock Dragon Process for unit tests."""

    def __init__(
        self,
        target: Any = None,
        kwargs: Optional[dict] = None,
        policy: Any = None,
    ) -> None:
        self.target = target
        self.kwargs = kwargs or {}
        self.policy = policy
        self._started = False
        self._killed = False
        self._joined = False

    def start(self) -> None:
        self._started = True

    def join(self, timeout: Optional[float] = None) -> None:
        self._joined = True

    def kill(self) -> None:
        self._killed = True


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def create_task_message(
    task_id: str = "task_123",
    node_name: str = "test_node",
    state: Any = None,
    serialized_done_queue: Optional[bytes] = None,
) -> dict:
    """Build a task message as sent to an AgentHost."""
    if state is None:
        state = {"messages": ["test"]}
    if serialized_done_queue is None:
        # An opaque serialized queue descriptor; the host passes it to
        # Queue.attach().
        serialized_done_queue = b"mock_sdesc"
    return {
        "task_id": task_id,
        "node_name": node_name,
        "state": state,
        "serialized_done_queue": serialized_done_queue,
    }


def create_done_envelope(
    task_id: str = "task_123",
    result: Any = None,
) -> bytes:
    """Build a success completion envelope."""
    from dragon.ai.langgraph.constants import (
        ENV_STATUS, ENV_PAYLOAD, STATUS_DONE, pack_envelope,
    )
    if result is None:
        result = {"messages": ["result"]}
    return pack_envelope(task_id, cloudpickle.dumps({
        ENV_STATUS: STATUS_DONE,
        ENV_PAYLOAD: result,
    }))


def create_error_envelope(
    task_id: str = "task_123",
    error: Optional[Exception] = None,
) -> bytes:
    """Build an error completion envelope."""
    from dragon.ai.langgraph.constants import (
        ENV_STATUS, ENV_PAYLOAD, STATUS_ERROR, pack_envelope,
    )
    if error is None:
        error = RuntimeError("test error")
    return pack_envelope(task_id, cloudpickle.dumps({
        ENV_STATUS: STATUS_ERROR,
        ENV_PAYLOAD: error,
    }))
