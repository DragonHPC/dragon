"""Tests for DictTracingProcessor — DDict-backed tracing backend."""

import dragon  # noqa: F401  # activates Dragon runtime
import multiprocessing as mp

from unittest import TestCase, main

from dragon.ai.agent.observability.ddict_tracer import DictTracingProcessor
from dragon.ai.agent.observability.tracer import Span
from dragon.ai.agent.observability.trace_protocol import SpanKind


class TestDictTracingProcessorInit(TestCase):
    """Verify DictTracingProcessor initialises the span list in DDict."""

    def test_initialises_empty_span_list(self):
        """Constructor writes an empty list to the trace key."""
        ddict = {}
        DictTracingProcessor(ddict, task_id="t1", agent_id="a1", dispatch_id="d1")
        key = "t1:a1:d1:trace"
        self.assertIn(key, ddict)
        self.assertEqual(ddict[key], [])

    def test_trace_key_format(self):
        """Trace key is formatted as {task_id}:{agent_id}:{dispatch_id}:trace."""
        ddict = {}
        DictTracingProcessor(ddict, task_id="T", agent_id="A", dispatch_id="D")
        self.assertIn("T:A:D:trace", ddict)


class TestDictTracingProcessorOnSpanStart(TestCase):
    """Verify on_span_start appends the span to the DDict list."""

    def test_appends_span_dict(self):
        """on_span_start appends span.to_dict() to the DDict span list."""
        ddict = {}
        proc = DictTracingProcessor(ddict, "t1", "a1", "d1")
        span = Span(name="test_op", kind=SpanKind.TASK)
        proc.on_span_start(span)

        spans = ddict["t1:a1:d1:trace"]
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0]["name"], "test_op")
        self.assertIsNone(spans[0]["end_time"])

    def test_multiple_spans_appended(self):
        """Multiple on_span_start calls append to the same list."""
        ddict = {}
        proc = DictTracingProcessor(ddict, "t1", "a1", "d1")
        proc.on_span_start(Span(name="op1", kind=SpanKind.TASK))
        proc.on_span_start(Span(name="op2", kind=SpanKind.LLM_CALL))

        spans = ddict["t1:a1:d1:trace"]
        self.assertEqual(len(spans), 2)
        self.assertEqual(spans[0]["name"], "op1")
        self.assertEqual(spans[1]["name"], "op2")


class TestDictTracingProcessorOnSpanEnd(TestCase):
    """Verify on_span_end updates the existing span entry in DDict."""

    def test_updates_existing_span_by_id(self):
        """on_span_end finds the span by span_id and replaces it."""
        ddict = {}
        proc = DictTracingProcessor(ddict, "t1", "a1", "d1")
        span = Span(name="op", kind=SpanKind.TOOL_CALL)
        proc.on_span_start(span)

        # Simulate span completion
        span.end_time = 999.0
        span.attributes["result"] = "ok"
        proc.on_span_end(span)

        spans = ddict["t1:a1:d1:trace"]
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0]["end_time"], 999.0)
        self.assertEqual(spans[0]["attributes"]["result"], "ok")

    def test_appends_if_start_was_missed(self):
        """If span_id not found (start missed), on_span_end appends the span."""
        ddict = {}
        proc = DictTracingProcessor(ddict, "t1", "a1", "d1")
        # Directly call on_span_end without on_span_start
        span = Span(name="late_op", kind=SpanKind.TASK)
        span.end_time = 100.0
        proc.on_span_end(span)

        spans = ddict["t1:a1:d1:trace"]
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0]["name"], "late_op")
        self.assertEqual(spans[0]["end_time"], 100.0)

    def test_updates_correct_span_among_multiple(self):
        """With multiple spans, only the matching span_id is updated."""
        ddict = {}
        proc = DictTracingProcessor(ddict, "t1", "a1", "d1")
        span1 = Span(name="op1", kind=SpanKind.TASK)
        span2 = Span(name="op2", kind=SpanKind.LLM_CALL)
        proc.on_span_start(span1)
        proc.on_span_start(span2)

        span2.end_time = 200.0
        proc.on_span_end(span2)

        spans = ddict["t1:a1:d1:trace"]
        self.assertEqual(len(spans), 2)
        # span1 should not have end_time set
        self.assertIsNone(spans[0]["end_time"])
        # span2 should have end_time
        self.assertEqual(spans[1]["end_time"], 200.0)

    def test_records_error_on_span(self):
        """Error information is persisted in the span dict."""
        ddict = {}
        proc = DictTracingProcessor(ddict, "t1", "a1", "d1")
        span = Span(name="fail", kind=SpanKind.TOOL_CALL)
        proc.on_span_start(span)

        span.error = "RuntimeError: boom"
        span.end_time = 50.0
        proc.on_span_end(span)

        spans = ddict["t1:a1:d1:trace"]
        self.assertEqual(spans[0]["error"], "RuntimeError: boom")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
