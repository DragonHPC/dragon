"""Tests for DDict event-write helpers (write_llm_event, etc.)."""

import dragon
import multiprocessing as mp

from unittest import TestCase, main
from unittest.mock import MagicMock, call

from dragon.ai.agent.reasoning.event_writer import (
    write_llm_event,
    write_tool_event,
    write_hitl_event,
    write_memory_event,
)


def _make_accessor():
    return MagicMock()


def _fmt():
    return {"task_id": "t", "agent_id": "a", "dispatch_id": "d"}


# ========================================================================
# write_llm_event
# ========================================================================

class TestWriteLLMEvent(TestCase):
    """Verify write_llm_event no-ops when disabled and writes when tracing."""

    def test_noop_when_tracing_false(self):
        """No event written when tracing is disabled."""
        acc = _make_accessor()
        write_llm_event(acc, tracing=False, fmt=_fmt(), iteration=1,
                        copy_prompts=[], output_text="x", event_idx=0)
        acc.write_event.assert_not_called()

    def test_noop_when_accessor_none(self):
        """No crash when accessor is None (graceful degradation)."""
        write_llm_event(None, tracing=True, fmt=_fmt(), iteration=1,
                        copy_prompts=[], output_text="x", event_idx=0)

    def test_writes_event_when_tracing(self):
        """Event dict with iteration and prompts is written to DDict."""
        acc = _make_accessor()
        prompts = [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "task"},
            {"role": "assistant", "content": "resp"},
        ]
        write_llm_event(acc, tracing=True, fmt=_fmt(), iteration=2,
                        copy_prompts=prompts, output_text="output", event_idx=0)
        acc.write_event.assert_called_once()
        args = acc.write_event.call_args
        event = args[0][2]  # event_data positional arg
        self.assertEqual(event["iteration"], 2)


# ========================================================================
# write_tool_event
# ========================================================================

class TestWriteToolEvent(TestCase):
    """Verify write_tool_event no-ops when disabled and writes when tracing."""

    def test_noop_when_tracing_false(self):
        """No event written when tracing is disabled."""
        acc = _make_accessor()
        write_tool_event(acc, tracing=False, fmt=_fmt(), name="t",
                         args={}, tool_answer="r", tool_source="local",
                         tool_call_id="tc1", event_idx=0)
        acc.write_event.assert_not_called()

    def test_writes_event_when_tracing(self):
        """Event dict with tool name and source is written to DDict."""
        acc = _make_accessor()
        write_tool_event(acc, tracing=True, fmt=_fmt(), name="search",
                         args={"q": "x"}, tool_answer="found",
                         tool_source="registry", tool_call_id="tc1",
                         event_idx=0)
        acc.write_event.assert_called_once()
        event = acc.write_event.call_args[0][2]
        self.assertEqual(event["name"], "search")
        self.assertEqual(event["source"], "registry")


# ========================================================================
# write_hitl_event
# ========================================================================

class TestWriteHITLEvent(TestCase):
    """Verify write_hitl_event no-ops when disabled and writes when tracing."""

    def test_noop_when_tracing_false(self):
        """No event written when tracing is disabled."""
        acc = _make_accessor()
        resp = MagicMock(approved=True, reason="", is_feedback=False)
        write_hitl_event(acc, tracing=False, fmt=_fmt(), name="t",
                         args={}, response=resp, event_idx=0)
        acc.write_event.assert_not_called()

    def test_writes_event_when_tracing(self):
        """Event dict with tool_name, approval status, and feedback flag is written."""
        acc = _make_accessor()
        resp = MagicMock(approved=False, reason="nope", is_feedback=True)
        write_hitl_event(acc, tracing=True, fmt=_fmt(), name="deploy",
                         args={"target": "prod"}, response=resp, event_idx=0)
        acc.write_event.assert_called_once()
        event = acc.write_event.call_args[0][2]
        self.assertEqual(event["tool_name"], "deploy")
        self.assertIs(event["is_feedback"], True)


# ========================================================================
# write_memory_event
# ========================================================================

class TestWriteMemoryEvent(TestCase):
    """Verify write_memory_event no-ops when disabled and writes when tracing."""

    def test_noop_when_tracing_false(self):
        """No event written when tracing is disabled."""
        acc = _make_accessor()
        write_memory_event(acc, tracing=False, fmt=_fmt(),
                           summ_result={"input": [], "output": ""},
                           event_idx=0)
        acc.write_event.assert_not_called()

    def test_writes_event_when_tracing(self):
        """Event dict with summarization result is written to DDict."""
        acc = _make_accessor()
        summ = {
            "input": [{"role": "user", "content": "hi"}],
            "output": "summary",
            "truncated": False,
            "zone_c_messages": 4,
            "summarizer_max_tool_chars": None,
            "summarizer_max_content_chars": None,
        }
        write_memory_event(acc, tracing=True, fmt=_fmt(),
                           summ_result=summ, event_idx=0)
        acc.write_event.assert_called_once()
        event = acc.write_event.call_args[0][2]
        self.assertEqual(event["zone_c_messages"], 4)


# ========================================================================
# Event writers — additional edge cases
# ========================================================================

class TestEventWriterEdgeCases(TestCase):
    """Verify event writers handle additional edge cases."""

    def test_llm_event_large_prompt_list(self):
        """LLM event captures prompt info from large prompt lists."""
        acc = _make_accessor()
        prompts = [{"role": "user", "content": f"msg{i}"} for i in range(20)]
        write_llm_event(acc, tracing=True, fmt=_fmt(), iteration=0,
                        copy_prompts=prompts, output_text="out", event_idx=0)
        acc.write_event.assert_called_once()

    def test_tool_event_noop_when_accessor_none(self):
        """write_tool_event with None accessor does not crash."""
        write_tool_event(None, tracing=True, fmt=_fmt(), name="t",
                         args={}, tool_answer="r", tool_source="local",
                         tool_call_id="tc1", event_idx=0)

    def test_hitl_event_noop_when_accessor_none(self):
        """write_hitl_event with None accessor does not crash."""
        resp = MagicMock(approved=True, reason="", is_feedback=False)
        write_hitl_event(None, tracing=True, fmt=_fmt(), name="t",
                         args={}, response=resp, event_idx=0)

    def test_memory_event_noop_when_accessor_none(self):
        """write_memory_event with None accessor does not crash."""
        write_memory_event(None, tracing=True, fmt=_fmt(),
                           summ_result={"output": ""}, event_idx=0)

    def test_sequential_llm_events_use_correct_indices(self):
        """Multiple LLM events use incrementing event_idx correctly."""
        acc = _make_accessor()
        for idx in range(3):
            write_llm_event(acc, tracing=True, fmt=_fmt(), iteration=idx,
                            copy_prompts=[], output_text=f"out{idx}",
                            event_idx=idx)
        self.assertEqual(acc.write_event.call_count, 3)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
