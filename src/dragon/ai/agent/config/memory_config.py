"""MemoryConfig — conversation history management for the agentic loop."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Union


class MemoryStrategy(str, Enum):
    """Memory management strategy for the agentic tool-call loop.

    Used as the ``strategy`` field of :class:`MemoryConfig`.

    Since ``MemoryStrategy`` extends ``str``, enum values compare equal to
    their string representations (e.g.
    ``MemoryStrategy.SLIDING_WINDOW == "sliding_window"`` is ``True``).
    """

    FULL = "full"
    """Keep every message — no pruning (default when ``memory=None``)."""

    SLIDING_WINDOW = "sliding_window"
    """Drop older tool-call exchanges, keep the most recent turns."""

    SUMMARIZE = "summarize"
    """Summarize old turns via the LLM instead of dropping them."""


@dataclass
class MemoryConfig:
    """Configures how an agent manages its conversation history (work memory).

    The ``ToolDispatcher`` accumulates messages (system prompt, user task,
    assistant tool-call requests, tool results) in a growing list and sends
    the **entire list** to the LLM on every iteration.  Without memory
    management, this list grows without bound and can exceed the model's
    context window.

    ``MemoryConfig`` provides three strategies:

    * ``MemoryStrategy.FULL`` — keep everything (current default behavior,
      no pruning).
    * ``MemoryStrategy.SLIDING_WINDOW`` — drop older tool-call exchanges,
      keep the most recent ``max_kept_turns`` turn-pairs in full.  The LLM
      sees **at most** ``max_kept_turns`` turn-pairs on every call.
    * ``MemoryStrategy.SUMMARIZE`` — like sliding_window, but instead of
      dropping old turns, use the LLM to generate a condensed summary.  Old
      turns are **not** pruned until the summarization threshold is reached,
      so the LLM may temporarily see up to
      ``max_kept_turns + summarize_after_turns - 1`` turn-pairs before
      summarization fires and compresses them back down.

    Usage::

        # No memory management (default — keep everything)
        AgentConfig(..., memory=None)

        # Sliding window with explicit parameters
        AgentConfig(..., memory=MemoryConfig(
            strategy=MemoryStrategy.SLIDING_WINDOW,
            max_kept_turns=8,
        ))

        # Summarization with explicit parameters
        AgentConfig(..., memory=MemoryConfig(
            strategy=MemoryStrategy.SUMMARIZE,
            max_kept_turns=8,
            summarize_after_turns=6,
        ))

    Parameters
    ----------
    strategy:
        Memory management strategy.
    max_kept_turns:
        Maximum number of recent tool-call turn-pairs to keep in full.
        A turn-pair is one assistant tool-call request + its corresponding
        tool result(s).  System prompt and user task are **never** counted.

        .. note:: For ``strategy=MemoryStrategy.SLIDING_WINDOW``, the LLM
           sees at most ``max_kept_turns`` turn-pairs on every call.  For
           ``strategy=MemoryStrategy.SUMMARIZE``, old turns are allowed to accumulate
           until ``summarize_after_turns`` pruneable turns exist, so the
           LLM may temporarily see up to
           ``max_kept_turns + summarize_after_turns - 1`` turn-pairs
           before summarization fires.
    max_tool_result_chars:
        Maximum characters per individual tool result ``content`` field.
    summarize_after_turns:
        Only used when ``strategy=MemoryStrategy.SUMMARIZE``.  Trigger summarization when
        this many pruneable turns (turns outside the ``max_kept_turns``
        window) have accumulated.  Until the threshold is reached, old turns
        remain in the message list — meaning the LLM temporarily sees up to
        ``max_kept_turns + summarize_after_turns - 1`` turn-pairs.  Once
        triggered, all pruneable turns (and any prior summary) are replaced
        with a single summary message, bringing the count back down to
        ``max_kept_turns`` + 1 summary message.
    summarizer_max_tool_chars:
        Maximum characters to keep per tool-result message when building
        the summarizer input.  ``None`` (default) means **no truncation** —
        the full tool result is sent to the summarizer.  Set to an integer
        (e.g. 500) if the summarizer model has a very small context window.
    summarizer_max_content_chars:
        Maximum characters to keep per non-tool message (user / assistant)
        when building the summarizer input.  ``None`` (default) means
        **no truncation**.  Set to an integer (e.g. 300) for small models.
    """

    strategy: MemoryStrategy = MemoryStrategy.SLIDING_WINDOW
    max_kept_turns: int = 8
    max_tool_result_chars: int = 5000
    summarize_after_turns: int = 6
    summarizer_max_tool_chars: int | None = None
    summarizer_max_content_chars: int | None = None

    @staticmethod
    def resolve(
        value: Union["MemoryConfig", None],
    ) -> "MemoryConfig | None":
        """Normalize ``MemoryConfig | None`` into a validated config or ``None``.

        Resolution rules:

        * ``None`` → ``None`` (no memory management).
        * ``MemoryConfig(strategy=FULL)`` → ``None`` (FULL means no pruning).
        * ``MemoryConfig(...)`` → returned as-is.
        """
        if value is None:
            return None
        if isinstance(value, MemoryConfig):
            if value.strategy is MemoryStrategy.FULL:
                return None
            return value
        raise TypeError(
            f"memory must be MemoryConfig | None, "
            f"got {type(value).__name__}"
        )
