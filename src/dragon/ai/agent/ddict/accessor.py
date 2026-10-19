"""DDictAccessor — centralised, error-handled DDict access.

Every DDict read and write in the agent framework goes through this class,
providing a single seam for error handling, retry logic, circuit breaking,
metrics, or a future ``TaskContext`` facade.

Usage::

    accessor = DDictAccessor(ddict, agent_id="researcher", task_id="abc123")
    accessor.put(STATUS_KEY.format(...), TaskStatus.PROCESSING)
    result = accessor.get(RESULT_KEY.format(...))
"""

from __future__ import annotations

from typing import Any

from ..utils.logging import get_agent_logger

_log = get_agent_logger("ddict_accessor")


class DDictAccessor:
    """Thin, error-handled wrapper around a raw Dragon DDict instance.

    Parameters
    ----------
    ddict:
        An attached :class:`~dragon.data.ddict.DDict` instance.
    agent_id:
        Identifier of the agent using this accessor (for log messages).
    task_id:
        Identifier of the current pipeline run (for log messages).
        May be empty when used by the orchestrator (no single task scope).
    """

    def __init__(self, ddict: Any, agent_id: str = "", task_id: str = "") -> None:
        self._ddict = ddict
        self._agent_id = agent_id
        self._task_id = task_id

    # -- core operations -----------------------------------------------------

    def get(self, key: str) -> Any:
        """Read a value from DDict.

        Raises
        ------
        Exception
            If the DDict read fails for any reason.  The original exception
            is logged at ERROR level and re-raised.
        """
        try:
            return self._ddict[key]
        except KeyError:
            raise  # let callers handle missing keys explicitly
        except Exception as exc:
            _log.error(
                "DDict read failed for key %r (agent='%s', task=[%s]): %s",
                key, self._agent_id, self._task_id[:8] if self._task_id else "?",
                exc, exc_info=True,
            )
            raise

    def put(self, key: str, value: Any) -> None:
        """Write a value to DDict.

        Raises
        ------
        Exception
            If the DDict write fails.  The original exception is logged at
            ERROR level and re-raised.
        """
        try:
            self._ddict[key] = value
        except Exception as exc:
            _log.error(
                "DDict write failed for key %r (agent='%s', task=[%s]): %s",
                key, self._agent_id, self._task_id[:8] if self._task_id else "?",
                exc, exc_info=True,
            )
            raise

    def delete(self, key: str) -> None:
        """Remove a key from DDict.

        Raises
        ------
        Exception
            If the DDict delete fails.  The original exception is logged at
            ERROR level and re-raised.
        """
        try:
            del self._ddict[key]
        except KeyError:
            pass  # deleting a missing key is not an error
        except Exception as exc:
            _log.error(
                "DDict delete failed for key %r (agent='%s', task=[%s]): %s",
                key, self._agent_id, self._task_id[:8] if self._task_id else "?",
                exc, exc_info=True,
            )
            raise

    # -- convenience ---------------------------------------------------------

    def get_or_default(self, key: str, default: Any = None) -> Any:
        """Read a value, returning *default* if the key does not exist.

        Unlike :meth:`get`, a ``KeyError`` is **not** raised — the caller
        receives *default* instead.  Other DDict failures are still raised.
        """
        try:
            return self.get(key)
        except KeyError:
            return default

    def get_list(self, key: str) -> list:
        """Read a list from DDict, returning a mutable copy.

        Ensures the caller gets a Python ``list`` that is safe to mutate
        without affecting the DDict entry.
        """
        return list(self.get(key))

    # -- event helpers -------------------------------------------------------

    def write_event(self, key_template: str, count_template: str,
                    event_data: Any, index: int, **fmt) -> None:
        """Write a numbered event and update the companion count key.

        This deduplicates the identical pattern used for LLM, tool, HITL,
        and memory events in ``ToolDispatcher.chat()``.

        Parameters
        ----------
        key_template:
            Event key template (e.g. ``LLM_EVENT_KEY``).
        count_template:
            Count key template (e.g. ``LLM_EVENT_COUNT_KEY``).
        event_data:
            The event payload to write.
        index:
            Zero-based event index.
        **fmt:
            Format kwargs (``task_id``, ``agent_id``, ``dispatch_id``).
        """
        self.put(key_template.format(index=index, **fmt), event_data)
        self.put(count_template.format(**fmt), index + 1)

    # -- raw access (escape hatch) -------------------------------------------

    @property
    def raw(self) -> Any:
        """Return the underlying raw DDict instance.

        Use sparingly — prefer :meth:`get` / :meth:`put` for error handling.
        Needed for operations like ``DDict.attach()``, ``DDict.detach()``,
        ``DDict.serialize()``, ``DDict.destroy()`` that operate on the DDict
        object itself rather than a key-value pair.
        """
        return self._ddict
