"""Completion-envelope constants shared between the AgentHost (worker side)
and the DragonWatcher (coordinator side).

Agent results travel *inline* on the shard's completion Queue as a small
cloudpickle'd envelope.  This layer does **not** transport bulk scientific
data: tensors, arrays and datasets belong in a user-managed Dragon DDict
(passed into the agent functions by the user).  Agents store such artifacts
in their own DDict and return only a lightweight handle/key through LangGraph
state, so the single watcher thread stays a pure signal router and the
coordinator never materializes bulk payloads.
"""

import struct
from typing import Any

import cloudpickle

from .logging import get_langgraph_logger

logger = get_langgraph_logger("envelope")

# -- Completion status ---------------------------------------------------------

STATUS_DONE = "done"
STATUS_ERROR = "error"

# -- Completion envelope -------------------------------------------------------
#
# Every completion Queue message is framed as:
#
#   [2-byte big-endian length][utf-8 task_id][cloudpickle'd body dict]
#
# where the body is:
#   {
#     "status":  STATUS_DONE | STATUS_ERROR,
#     "payload": <result_dict | exception>,
#   }
#
# The task_id rides outside the pickle so that a body which fails to
# deserialize can still be attributed to its task, and that task's Future
# failed, instead of the task hanging until its timeout.
#
# Field-name constants so host + watcher never drift.
ENV_STATUS = "status"
ENV_PAYLOAD = "payload"

_TASK_ID_HEADER = struct.Struct("!H")


def pack_envelope(task_id: str, body: bytes) -> bytes:
    """Frame a serialized envelope body behind its ``task_id``.

    :param task_id: Identifier of the task this envelope completes.
    :type task_id: str
    :param body: The cloudpickle'd envelope body.
    :type body: bytes
    :returns: The framed message to send on the completion Queue.
    :rtype: bytes
    """
    ident = task_id.encode("utf-8")
    return _TASK_ID_HEADER.pack(len(ident)) + ident + body


def unpack_envelope(data: bytes) -> tuple[str, bytes]:
    """Split a framed message into its ``task_id`` and still-serialized body.

    Deliberately does no unpickling, so the caller learns which task a message
    belongs to even when its body is undecodable.

    :param data: A message produced by :func:`pack_envelope`.
    :type data: bytes
    :returns: The task id and the undecoded body.
    :rtype: tuple[str, bytes]
    :raises ValueError: If the framing header is missing or truncated.
    """
    try:
        (length,) = _TASK_ID_HEADER.unpack_from(data, 0)
    except struct.error as exc:
        raise ValueError("completion envelope is too short to be framed") from exc
    start = _TASK_ID_HEADER.size
    end = start + length
    if end > len(data):
        raise ValueError("completion envelope header claims a truncated task_id")
    return data[start:end].decode("utf-8"), data[end:]


def _safe_str(obj: Any) -> str:
    """``str(obj)`` for an object whose own ``__str__`` may raise."""
    try:
        return str(obj)
    except Exception:  # noqa: BLE001
        return f"<unprintable {type(obj).__name__}>"


def _describe_unpicklable(envelope: Any, exc: BaseException) -> str:
    """Explain why a body would not serialize, without trusting it to behave.

    Keeps the original payload's own text, which for an unpicklable exception
    is usually the whole diagnosis, while never risking a second failure inside
    the handler that is already recovering from one.

    :param envelope: The body that failed to serialize.
    :param exc: The failure raised while serializing it.
    :type exc: BaseException
    :returns: A message safe to carry in the substituted error envelope.
    :rtype: str
    """
    payload = envelope
    if isinstance(envelope, dict):
        payload = envelope.get(ENV_PAYLOAD, envelope)
    return (
        f"agent result of type {type(payload).__name__} could not be "
        f"serialized: {_safe_str(exc)} -- {_safe_str(payload)}"
    )


class EnvelopePickler:
    """Framing codec for the per-shard completion Queue.

    A :class:`~dragon.native.queue.Queue` delegates serialization to its
    ``pickler``, which is what lets the ``task_id`` ride *outside* the pickle,
    so a body the coordinator cannot decode still names the task whose Future
    must be failed.

    Neither direction raises on a payload it cannot handle.  ``Queue.put``
    closes its send handle even when ``dump`` raises, and a handle that closes
    with nothing buffered still emits an empty message — unattributable, so no
    Future could be failed and the task would hang until its timeout.  ``dump``
    therefore substitutes an error envelope for a body it cannot serialize, and
    ``load`` hands a decode failure back alongside its ``task_id``.
    """

    def dump(self, obj: tuple[str, Any], file: Any) -> None:
        """Serialize and frame one completion onto the Queue's send stream.

        A body that will not pickle is replaced by an error envelope carrying
        the reason, so the task's Future fails with a diagnosis rather than
        waiting out its timeout.

        :param obj: The task id and its envelope body.
        :type obj: tuple[str, dict]
        :param file: The Queue-supplied write adapter.
        :type file: dragon.fli.PickleWriteAdapter
        """
        task_id, envelope = obj
        try:
            body = cloudpickle.dumps(envelope)
        except Exception as exc:  # noqa: BLE001
            detail = _describe_unpicklable(envelope, exc)
            logger.error(
                "[envelope] task %s: %s; reporting it as an error instead",
                task_id, detail,
            )
            body = cloudpickle.dumps(
                {
                    ENV_STATUS: STATUS_ERROR,
                    ENV_PAYLOAD: RuntimeError(detail),
                }
            )
        # Single write: the buffered FLI mallocs and copies once per write().
        file.write(pack_envelope(task_id, body))

    def load(self, file: Any) -> tuple[str, Any, BaseException | None]:
        """Read one completion and decode its body.

        Returns a failed decode rather than raising, so the caller can fail
        that task's Future instead of losing it: ``Queue.get`` maps only
        channel-level errors to :exc:`queue.Empty`, so anything raised here
        would reach the watcher stripped of its ``task_id``.

        :param file: The Queue-supplied read adapter.
        :type file: dragon.fli.PickleReadAdapter
        :returns: The task id, the decoded envelope (``None`` on failure), and
            the decode exception (``None`` on success).
        :rtype: tuple[str, dict | None, BaseException | None]
        :raises ValueError: If the framing header is missing or truncated.
        """
        task_id, body = unpack_envelope(file.read())
        try:
            return task_id, cloudpickle.loads(body), None
        except Exception as exc:  # noqa: BLE001
            return task_id, None, exc
