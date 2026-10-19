"""ContextManager — agent work-memory management for ToolDispatcher.

This module implements turn-based memory management for the agentic tool-call
loop.  It operates directly on the ``copy_prompts`` message list inside
``ToolDispatcher.chat()``, compressing tool results at insertion time, pruning
old turns before each LLM call, and optionally summarizing them via the LLM.

Three strategies are supported:

* **sliding_window** — drop older tool-call turn-pairs once ``max_kept_turns``
  is exceeded.  A synthetic note replaces the dropped turns.
* **summarize** — like sliding_window, but instead of dropping old turns,
  call the LLM to produce a condensed summary.
* **full** — keep everything (handled by not creating a ``ContextManager``
  at all; ``MemoryConfig.resolve()`` returns ``None`` for ``"full"``).

Usage inside ``ToolDispatcher.chat()``::


    # Before each llm_engine.chat() call:
    context_manager.enforce_window(copy_prompts, len_initial_prompt)
    await context_manager.maybe_summarize(
        copy_prompts, len_initial_prompt, llm_engine
    )
"""

from __future__ import annotations

from typing import Any, Dict, List

from ..config import MemoryConfig, MemoryStrategy
from ..utils.logging import get_agent_logger

log = get_agent_logger("context_manager")


class ContextManager:
    """Turn-based memory manager for the ToolDispatcher agentic loop.

    Parameters
    ----------
    config:
        A resolved :class:`MemoryConfig` instance (never ``None`` — the
        caller should check ``MemoryConfig.resolve()`` first).
    summarizer_engine:
        Optional **separate** LLM engine dedicated to summarization.
        When provided, ``maybe_summarize()`` calls this engine instead of
        the main agent LLM engine, keeping the primary inference queue
        free for reasoning and tool-calling.

        Typical HPC setup: main agent uses a 70B model on 4 GPUs;
        summarizer uses an 8B instruct model on 1 GPU — faster, no
        contention with the agentic loop.

        When ``None`` (default), summarization reuses the agent's own
        ``llm_engine`` — same behavior as LangChain / LlamaIndex defaults.
    """

    def __init__(self, config: MemoryConfig, summarizer_engine: Any = None) -> None:
        self._config = config
        self._summarizer_engine = summarizer_engine

    # ------------------------------------------------------------------
    # Strategy 1: Sliding Window Pruning
    # ------------------------------------------------------------------

    def enforce_window(
        self,
        messages: List[Dict[str, Any]],
        num_initial: int,
    ) -> None:
        """In-place prune old tool-call turns from the message list.

        Classifies messages into three zones:

        * **Zone A** (indices ``0..num_initial-1``): System prompt(s) and
          original user task.  Never pruned.
        * **Zone B**: The most recent ``max_kept_turns`` turn-pairs at the
          tail.  Never pruned.
        * **Zone C**: Everything between Zone A and Zone B — older tool-call
          exchanges.  Replaced with a synthetic note (or summarized if
          ``strategy="summarize"`` — handled by ``maybe_summarize()``).

        A **turn-pair** is one assistant message with ``tool_calls`` plus all
        following ``role: "tool"`` messages until the next assistant message.

        Parameters
        ----------
        messages:
            The ``copy_prompts`` list, mutated in-place.
        num_initial:
            Number of messages in the protected initial zone (system prompts
            + user task).
        """
        if self._config.strategy in (MemoryStrategy.FULL, MemoryStrategy.SUMMARIZE):
            # FULL: no pruning at all.
            # SUMMARIZE: let turns accumulate so maybe_summarize() can
            #   see them.  enforce_window must NOT prune first, otherwise
            #   maybe_summarize() always sees 0 pruneable turns and never
            #   triggers.  The trade-off is that the LLM may temporarily
            #   see up to (max_kept_turns + summarize_after_turns - 1)
            #   turn-pairs until the summarization threshold is reached.
            return

        max_kept = self._config.max_kept_turns

        # Parse turns after the initial zone
        turns = self._parse_turns(messages, num_initial)

        if len(turns) <= max_kept:
            # Nothing to prune
            return

        # Zone C = turns to prune (oldest), Zone B = turns to keep (newest)
        num_to_prune = len(turns) - max_kept
        zone_c_turns = turns[:num_to_prune]
        # zone_b_turns = turns[num_to_prune:]  # kept in place

        # Determine the index range to remove
        # Zone C spans from the first message of the first pruneable turn
        # to the last message of the last pruneable turn
        prune_start = zone_c_turns[0]["start_idx"]
        prune_end = zone_c_turns[-1]["end_idx"]  # inclusive

        # Silently drop Zone C — no synthetic note.  The pruned messages
        # are not stored anywhere, so a note would only mislead the LLM
        # into making assumptions about content it can never recover.
        del messages[prune_start:prune_end + 1]

        log.debug(
            "ContextManager: pruned %d turn(s), kept %d recent turn(s).",
            num_to_prune, max_kept,
        )

    # ------------------------------------------------------------------
    # Strategy 3: Summarization
    # ------------------------------------------------------------------

    def should_summarize(
        self,
        messages: List[Dict[str, Any]],
        num_initial: int,
    ) -> bool:
        """Cheap check: would :meth:`maybe_summarize` actually trigger?

        Returns ``True`` when strategy is ``MemoryStrategy.SUMMARIZE`` and the number
        of pruneable turns has reached the threshold.  This lets callers
        gate expensive trace spans so that no span is emitted when
        summarization is a no-op.
        """
        if self._config.strategy is not MemoryStrategy.SUMMARIZE:
            return False
        turns = self._parse_turns(messages, num_initial)
        if len(turns) <= self._config.max_kept_turns:
            return False
        return (len(turns) - self._config.max_kept_turns) >= self._config.summarize_after_turns

    async def maybe_summarize(
        self,
        messages: List[Dict[str, Any]],
        num_initial: int,
        llm_engine: Any,
    ) -> dict | None:
        """Summarize old turns via the LLM if conditions are met.

        Only active when ``strategy=MemoryStrategy.SUMMARIZE``.  Triggers when the number
        of pruneable turns (Zone C) reaches ``summarize_after_turns``.

        When triggered:

        1. Extract Zone C messages as text.
        2. Call the LLM with a summarization prompt (single-shot, no tools).
        3. Replace Zone C messages with a summary system message.

        Parameters
        ----------
        messages:
            The ``copy_prompts`` list, mutated in-place.
        num_initial:
            Number of messages in the protected initial zone.
        llm_engine:
            The agent's main LLM engine (fallback).  Used for
            summarization only when no dedicated ``summarizer_engine``
            was provided at construction time.

        Returns
        -------
        dict | None
            When summarization fires, returns
            ``{"input": <context_text>, "output": <summary_text>}``
            so callers (e.g. tracing) can record what was summarized.
            Returns ``None`` when summarization did not trigger.
        """
        if self._config.strategy is not MemoryStrategy.SUMMARIZE:
            return None

        max_kept = self._config.max_kept_turns
        threshold = self._config.summarize_after_turns

        turns = self._parse_turns(messages, num_initial)

        if len(turns) <= max_kept:
            return None

        num_pruneable = len(turns) - max_kept
        if num_pruneable < threshold:
            return None

        # Extract pruneable turns' messages
        zone_c_turns = turns[:num_pruneable]
        prune_start = zone_c_turns[0]["start_idx"]
        prune_end = zone_c_turns[-1]["end_idx"]

        # Check for existing summary/memory note
        has_existing_note = (
            len(messages) > num_initial
            and messages[num_initial].get("role") == "system"
            and isinstance(messages[num_initial].get("content", ""), str)
            and (
                messages[num_initial]["content"].startswith("[Memory:")
                or messages[num_initial]["content"].startswith("Summary of prior work:")
            )
        )

        if has_existing_note:
            prune_start = num_initial

        # Build text from the pruneable zone.
        # Separate the prior rolling summary (if any) from new interactions
        # so the summarization prompt can instruct the LLM to preserve all
        # facts from the prior summary while integrating new data.
        zone_c_msgs = messages[prune_start:prune_end + 1]
        prior_summary = None
        new_interaction_parts = []
        for msg in zone_c_msgs:
            role = msg.get("role", "unknown")
            content = msg.get("content") or ""
            tool_calls = msg.get("tool_calls")

            # Detect prior summary message (first system msg starting with
            # "Summary of prior work:" or "[Memory:")
            if (
                role == "system"
                and prior_summary is None
                and (content.startswith("Summary of prior work:")
                     or content.startswith("[Memory:"))
            ):
                prior_summary = content
                continue

            if tool_calls:
                # Format tool calls compactly
                calls_text = ", ".join(
                    f"{tc.get('function', {}).get('name', '?')}(...)"
                    for tc in tool_calls
                )
                new_interaction_parts.append(f"[assistant called: {calls_text}]")
            elif role == "tool":
                name = msg.get("name", "?")
                _tool_limit = self._config.summarizer_max_tool_chars
                if _tool_limit is not None and len(content) > _tool_limit:
                    preview = content[:_tool_limit]
                    preview += f" ... [truncated, {len(content) - _tool_limit} chars omitted]"
                else:
                    preview = content
                new_interaction_parts.append(f"[tool {name} returned: {preview}]")
            elif content:
                _content_limit = self._config.summarizer_max_content_chars
                if _content_limit is not None and len(content) > _content_limit:
                    new_interaction_parts.append(f"[{role}: {content[:_content_limit]} ... [truncated, {len(content) - _content_limit} chars omitted]]")
                else:
                    new_interaction_parts.append(f"[{role}: {content}]")

        new_interactions_text = "\n".join(new_interaction_parts)

        # Build context_text for DDict event (includes both sections)
        if prior_summary:
            context_text = f"[PRIOR SUMMARY]\n{prior_summary}\n\n[NEW INTERACTIONS]\n{new_interactions_text}"
        else:
            context_text = new_interactions_text

        # Build the summarization prompt — explicitly instruct the LLM to
        # preserve ALL facts from the prior summary when one exists.
        #
        # Strip the "Summary of prior work:\n" prefix from the prior
        # summary before embedding it in the prompt — the output will be
        # re-wrapped with that prefix anyway, and leaving it causes the
        # small model to echo "Summary of prior work: Summary of prior work:".
        _prior_body = prior_summary
        if _prior_body:
            for _pfx in ("Summary of prior work:\n", "Summary of prior work:"):
                if _prior_body.startswith(_pfx):
                    _prior_body = _prior_body[len(_pfx):].lstrip()
                    break

        if prior_summary:
            prompt_text = (
                "You are updating a rolling summary of an agent's work.\n\n"
                "PRIOR SUMMARY:\n"
                f"{_prior_body}\n\n"
                "NEW INTERACTIONS:\n"
                f"{new_interactions_text}\n\n"
                "Write an UPDATED SUMMARY that:\n"
                "- Keeps EVERY fact, number, and decision from the prior summary.\n"
                "- Integrates key results from the new interactions.\n"
                "- Is written in plain English sentences (NOT tool call syntax).\n"
                "- NEVER copies tool calls or JSON verbatim.\n\n"
                "EXAMPLE of good output:\n"
                "The agent ran tool_A twice with parameters X=100 and X=500. "
                "Operator feedback requested doubling the target value "
                "and adjusting parameter Y by 0.002.\n\n"
                "Now write the updated summary:"
            )
        else:
            prompt_text = (
                "Summarize the following tool interactions in plain English sentences. "
                "Preserve all key facts, data values, and decisions. "
                "Do NOT copy tool calls or JSON verbatim.\n\n"
                "EXAMPLE of good output:\n"
                "The agent called tool_A twice with different parameters. "
                "Both calls received operator feedback requesting adjustments.\n\n"
                "INTERACTIONS:\n"
                f"{new_interactions_text}\n\n"
                "SUMMARY:"
            )

        summarize_prompt = [
            {"role": "user", "content": prompt_text}
        ]

        # Use the dedicated summarizer if available; fall back to main engine.
        engine = self._summarizer_engine or llm_engine
        try:
            summary = await engine.chat(summarize_prompt)
            summary = summary.strip()
        except Exception as exc:
            log.warning(
                "ContextManager: summarization LLM call failed (%s). "
                "Falling back to sliding_window pruning.",
                exc,
            )
            # Fall back to simple pruning
            note = {
                "role": "system",
                "content": (
                    f"[Memory: {num_pruneable} earlier tool-call exchange(s) "
                    f"were completed and removed (summarization failed). "
                    f"Key context is preserved in the recent turns below.]"
                ),
            }
            messages[prune_start:prune_end + 1] = [note]
            _tool_lim = self._config.summarizer_max_tool_chars
            _cont_lim = self._config.summarizer_max_content_chars
            return {"input": context_text, "output": note["content"],
                    "truncated": _tool_lim is not None or _cont_lim is not None,
                    "zone_c_messages": len(zone_c_msgs),
                    "summarizer_max_tool_chars": _tool_lim,
                    "summarizer_max_content_chars": _cont_lim}

        # Replace zone C with the summary
        summary_msg = {
            "role": "system",
            "content": f"Summary of prior work:\n{summary}",
        }
        messages[prune_start:prune_end + 1] = [summary_msg]

        log.debug(
            "ContextManager: summarized %d turn(s) into %d chars.",
            num_pruneable, len(summary),
        )
        _tool_lim = self._config.summarizer_max_tool_chars
        _cont_lim = self._config.summarizer_max_content_chars
        _was_truncated = any(
            (_tool_lim is not None and msg.get("role") == "tool" and len(msg.get("content", "")) > _tool_lim)
            or (_cont_lim is not None and msg.get("role") != "tool" and len(msg.get("content", "")) > _cont_lim)
            for msg in zone_c_msgs
        )
        return {"input": context_text, "output": summary_msg["content"],
                "truncated": _was_truncated,
                "zone_c_messages": len(zone_c_msgs),
                "summarizer_max_tool_chars": _tool_lim,
                "summarizer_max_content_chars": _cont_lim}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_turns(
        messages: List[Dict[str, Any]],
        num_initial: int,
    ) -> list[dict]:
        """Parse the message list into turn-pair descriptors.

        Each turn-pair starts with an assistant message that has ``tool_calls``
        and includes all following ``role: "tool"`` messages.

        Returns a list of dicts::

            [
                {"start_idx": 5, "end_idx": 8},   # turn 0: assistant + 3 tool results
                {"start_idx": 9, "end_idx": 11},   # turn 1: assistant + 2 tool results
                ...
            ]
        """
        turns = []
        i = num_initial

        while i < len(messages):
            msg = messages[i]

            # Skip non-assistant messages that aren't part of a turn
            # (e.g., a memory note inserted by a previous prune, or a
            # standalone assistant final-answer message without tool_calls)
            if msg.get("role") != "assistant" or not msg.get("tool_calls"):
                i += 1
                continue

            # Found a turn start — assistant with tool_calls
            start = i
            i += 1

            # Collect all following tool result messages
            while i < len(messages) and messages[i].get("role") == "tool":
                i += 1

            turns.append({"start_idx": start, "end_idx": i - 1})

        return turns
