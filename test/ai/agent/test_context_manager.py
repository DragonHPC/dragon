"""Tests for ContextManager — memory strategies (sliding window, summarize)."""

import dragon
import multiprocessing as mp

from unittest import TestCase, IsolatedAsyncioTestCase, main

from dragon.ai.agent.config import MemoryConfig, MemoryStrategy
from dragon.ai.agent.memory.context_manager import ContextManager


# ---------------------------------------------------------------------------
# Helpers — build realistic message lists
# ---------------------------------------------------------------------------

def _initial_messages() -> list[dict]:
    """System prompt + user task (2 messages = num_initial)."""
    return [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Analyze the data."},
    ]


def _tool_turn(tool_name: str = "search", result: str = "found it") -> list[dict]:
    """One assistant tool-call + tool result (a single turn-pair)."""
    return [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {"id": "tc1", "type": "function",
                 "function": {"name": tool_name, "arguments": "{}"}}
            ],
        },
        {"role": "tool", "name": tool_name, "content": result, "tool_call_id": "tc1"},
    ]


# ========================================================================
# enforce_window — SLIDING_WINDOW
# ========================================================================

class TestEnforceWindowSlidingWindow(TestCase):
    """Verify enforce_window prunes oldest turns for SLIDING_WINDOW strategy."""

    def _make_mgr(self, max_kept: int = 2) -> ContextManager:
        cfg = MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW,
                           max_kept_turns=max_kept)
        return ContextManager(cfg)

    def test_no_pruning_when_under_limit(self):
        """Messages below max_kept_turns are left unchanged."""
        mgr = self._make_mgr(max_kept=5)
        msgs = _initial_messages() + _tool_turn("a") + _tool_turn("b")
        original_len = len(msgs)
        mgr.enforce_window(msgs, num_initial=2)
        self.assertEqual(len(msgs), original_len)  # unchanged

    def test_prunes_oldest_turns(self):
        """Oldest turns are silently removed."""
        mgr = self._make_mgr(max_kept=1)
        msgs = (
            _initial_messages()
            + _tool_turn("old1")
            + _tool_turn("old2")
            + _tool_turn("kept")
        )
        mgr.enforce_window(msgs, num_initial=2)

        # Should have: 2 initial + 2 kept turn msgs = 4
        self.assertEqual(len(msgs), 4)
        self.assertEqual(msgs[0]["role"], "system")
        self.assertEqual(msgs[1]["role"], "user")
        # Kept turn
        self.assertEqual(msgs[2]["role"], "assistant")
        self.assertEqual(msgs[2]["tool_calls"][0]["function"]["name"], "kept")

    def test_replaces_existing_memory_note(self):
        """Repeated pruning silently drops old turns without accumulating notes."""
        mgr = self._make_mgr(max_kept=1)
        msgs = _initial_messages() + _tool_turn("a") + _tool_turn("b")
        mgr.enforce_window(msgs, num_initial=2)
        # Add another turn and prune again
        msgs.extend(_tool_turn("c"))
        mgr.enforce_window(msgs, num_initial=2)

        # No memory notes should exist
        notes = [m for m in msgs if "[Memory:" in m.get("content", "")]
        self.assertEqual(len(notes), 0)
        # Should have: 2 initial + 2 kept turn msgs = 4
        self.assertEqual(len(msgs), 4)


# ========================================================================
# enforce_window — FULL / SUMMARIZE (no-op)
# ========================================================================

class TestEnforceWindowNoOp(TestCase):
    """Verify enforce_window is a no-op for FULL and SUMMARIZE strategies."""

    def test_full_strategy_does_nothing(self):
        """FULL strategy does not prune any messages."""
        cfg = MemoryConfig(strategy=MemoryStrategy.FULL)
        # resolve() returns None for FULL, but let's test the manager directly
        # if someone creates one explicitly
        mgr = ContextManager(cfg)
        msgs = _initial_messages() + _tool_turn() * 10
        original_len = len(msgs)
        mgr.enforce_window(msgs, num_initial=2)
        self.assertEqual(len(msgs), original_len)

    def test_summarize_strategy_does_not_prune(self):
        """SUMMARIZE strategy leaves messages unchanged (pruning deferred)."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SUMMARIZE,
                           max_kept_turns=1, summarize_after_turns=1)
        mgr = ContextManager(cfg)
        msgs = _initial_messages() + _tool_turn("a") + _tool_turn("b")
        original_len = len(msgs)
        mgr.enforce_window(msgs, num_initial=2)
        self.assertEqual(len(msgs), original_len)


# ========================================================================
# should_summarize
# ========================================================================

class TestShouldSummarize(TestCase):
    """Verify should_summarize trigger conditions."""

    def test_false_for_sliding_window(self):
        """SLIDING_WINDOW strategy never triggers summarization."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW)
        mgr = ContextManager(cfg)
        msgs = _initial_messages() + _tool_turn() * 20
        self.assertFalse(mgr.should_summarize(msgs, 2))

    def test_false_when_below_threshold(self):
        """Below summarize_after_turns threshold, returns False."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SUMMARIZE,
                           max_kept_turns=2, summarize_after_turns=3)
        mgr = ContextManager(cfg)
        # 3 turns, 2 kept => 1 pruneable, threshold=3 => no trigger
        msgs = _initial_messages() + _tool_turn("a") + _tool_turn("b") + _tool_turn("c")
        self.assertFalse(mgr.should_summarize(msgs, 2))

    def test_true_when_at_threshold(self):
        """At summarize_after_turns threshold, returns True."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SUMMARIZE,
                           max_kept_turns=1, summarize_after_turns=2)
        mgr = ContextManager(cfg)
        # 3 turns, 1 kept => 2 pruneable, threshold=2 => trigger
        msgs = _initial_messages() + _tool_turn("a") + _tool_turn("b") + _tool_turn("c")
        self.assertTrue(mgr.should_summarize(msgs, 2))


# ========================================================================
# maybe_summarize
# ========================================================================

class TestMaybeSummarize(IsolatedAsyncioTestCase):
    """Verify maybe_summarize with different strategies and LLM scenarios."""

    async def test_returns_none_for_sliding_window(self):
        """SLIDING_WINDOW strategy returns None (no summarization)."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW)
        mgr = ContextManager(cfg)
        result = await mgr.maybe_summarize([], 0, None)
        self.assertIsNone(result)

    async def test_returns_none_when_below_threshold(self):
        """Below summarize_after_turns, returns None."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SUMMARIZE,
                           max_kept_turns=5, summarize_after_turns=3)
        mgr = ContextManager(cfg)
        msgs = _initial_messages() + _tool_turn("a")
        result = await mgr.maybe_summarize(msgs, 2, None)
        self.assertIsNone(result)

    async def test_summarizes_when_threshold_reached(self):
        """At threshold, calls summarizer LLM and replaces old turns with summary."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SUMMARIZE,
                           max_kept_turns=1, summarize_after_turns=1)

        class FakeLLM:
            async def chat(self, prompts):
                return "Summary: two searches were performed."

        mgr = ContextManager(cfg, summarizer_engine=FakeLLM())
        msgs = _initial_messages() + _tool_turn("a") + _tool_turn("b")
        result = await mgr.maybe_summarize(msgs, 2, None)

        self.assertIsNotNone(result)
        self.assertIn("Summary", result["output"])
        # The old turns should be replaced with a summary message
        self.assertTrue(any("Summary of prior work:" in m.get("content", "")
                            for m in msgs))

    async def test_falls_back_to_main_llm_engine(self):
        """Without a dedicated summarizer, the main LLM engine is used."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SUMMARIZE,
                           max_kept_turns=1, summarize_after_turns=1)

        class FakeLLM:
            async def chat(self, prompts):
                return "Fallback summary."

        mgr = ContextManager(cfg)  # no summarizer_engine
        msgs = _initial_messages() + _tool_turn("a") + _tool_turn("b")
        result = await mgr.maybe_summarize(msgs, 2, FakeLLM())
        self.assertIsNotNone(result)
        self.assertIn("Fallback summary", result["output"])

    async def test_llm_failure_falls_back_to_pruning(self):
        """When the summarizer LLM fails, context falls back to sliding-window pruning."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SUMMARIZE,
                           max_kept_turns=1, summarize_after_turns=1)

        class FailingLLM:
            async def chat(self, prompts):
                raise RuntimeError("LLM exploded")

        mgr = ContextManager(cfg, summarizer_engine=FailingLLM())
        msgs = _initial_messages() + _tool_turn("a") + _tool_turn("b")
        result = await mgr.maybe_summarize(msgs, 2, None)

        self.assertIsNotNone(result)
        self.assertIn("summarization failed", result["output"])
        # Should still have a memory note (fallback to pruning)
        self.assertTrue(any("[Memory:" in m.get("content", "") for m in msgs))


# ========================================================================
# _parse_turns (static method)
# ========================================================================

class TestParseTurns(TestCase):
    """Verify _parse_turns extracts tool-call turn boundaries."""

    def test_empty(self):
        """No turns beyond initial messages."""
        turns = ContextManager._parse_turns(_initial_messages(), 2)
        self.assertEqual(turns, [])

    def test_single_turn(self):
        """One assistant tool-call + tool result pair is one turn."""
        msgs = _initial_messages() + _tool_turn("a")
        turns = ContextManager._parse_turns(msgs, 2)
        self.assertEqual(len(turns), 1)
        self.assertEqual(turns[0]["start_idx"], 2)
        self.assertEqual(turns[0]["end_idx"], 3)

    def test_multiple_turns(self):
        """Multiple tool-call pairs are counted as separate turns."""
        msgs = _initial_messages() + _tool_turn("a") + _tool_turn("b")
        turns = ContextManager._parse_turns(msgs, 2)
        self.assertEqual(len(turns), 2)

    def test_skips_assistant_without_tool_calls(self):
        """Plain assistant messages (no tool_calls) are not counted as turns."""
        msgs = _initial_messages() + [
            {"role": "assistant", "content": "Final answer."},
        ]
        turns = ContextManager._parse_turns(msgs, 2)
        self.assertEqual(turns, [])


# ========================================================================
# ContextManager — additional edge cases
# ========================================================================

class TestContextManagerEdgeCases(TestCase):
    """Verify ContextManager edge cases."""

    def test_enforce_window_empty_messages(self):
        """enforce_window with empty list does not crash."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW, max_kept_turns=2)
        mgr = ContextManager(cfg)
        msgs = []
        mgr.enforce_window(msgs, num_initial=0)
        self.assertEqual(msgs, [])

    def test_enforce_window_only_initial_messages(self):
        """enforce_window with only initial messages (no turns) does nothing."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW, max_kept_turns=2)
        mgr = ContextManager(cfg)
        msgs = _initial_messages()
        original_len = len(msgs)
        mgr.enforce_window(msgs, num_initial=2)
        self.assertEqual(len(msgs), original_len)

    def test_enforce_window_single_turn_under_limit(self):
        """Single turn within max_kept_turns is not pruned."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW, max_kept_turns=2)
        mgr = ContextManager(cfg)
        msgs = _initial_messages() + _tool_turn("a")
        original_len = len(msgs)
        mgr.enforce_window(msgs, num_initial=2)
        self.assertEqual(len(msgs), original_len)

    def test_large_max_kept_turns(self):
        """Very large max_kept_turns never triggers pruning."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW, max_kept_turns=1000)
        mgr = ContextManager(cfg)
        msgs = _initial_messages()
        for i in range(50):
            msgs.extend(_tool_turn(f"tool{i}"))
        original_len = len(msgs)
        mgr.enforce_window(msgs, num_initial=2)
        self.assertEqual(len(msgs), original_len)


class TestContextManagerConstruction(TestCase):
    """Verify ContextManager construction."""

    def test_construction_with_summarizer(self):
        """ContextManager stores the summarizer engine."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SUMMARIZE)

        class FakeLLM:
            pass

        mgr = ContextManager(cfg, summarizer_engine=FakeLLM())
        self.assertIsNotNone(mgr._summarizer_engine)

    def test_construction_without_summarizer(self):
        """ContextManager works without a summarizer engine."""
        cfg = MemoryConfig(strategy=MemoryStrategy.SLIDING_WINDOW)
        mgr = ContextManager(cfg)
        self.assertIsNone(mgr._summarizer_engine)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
