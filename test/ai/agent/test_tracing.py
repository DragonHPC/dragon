"""Tests for observability — Span, Trace, trace_span, NoOpProcessor."""

import dragon
import multiprocessing as mp


import time
import uuid

from unittest import TestCase, IsolatedAsyncioTestCase, main

from dragon.ai.agent.observability.tracer import (
    Span,
    Trace,
    TracingProcessor,
    NoOpProcessor,
    trace_span,
    get_current_span,
    get_current_trace_id,
    get_current_processor,
    set_current_processor,
    _current_span,
    _current_trace_id,
    _current_processor,
)
from dragon.ai.agent.observability.trace_protocol import SpanKind


# ========================================================================
# Span
# ========================================================================

class TestSpan(TestCase):
    """Verify Span creation, serialization, and deserialization."""

    def test_auto_generated_fields(self):
        """Span auto-generates span_id (UUID), start_time; end_time/error are None."""
        span = Span(name="test_op", kind=SpanKind.TASK)
        uuid.UUID(span.span_id)  # valid UUID
        self.assertIsInstance(span.start_time, float)
        self.assertIsNone(span.end_time)
        self.assertIsNone(span.error)
        self.assertIsNone(span.parent_id)
        self.assertEqual(span.attributes, {})

    def test_to_dict(self):
        """to_dict() includes name, kind value, and span_id as a string."""
        span = Span(name="op", kind=SpanKind.LLM_CALL)
        d = span.to_dict()
        self.assertEqual(d["name"], "op")
        self.assertEqual(d["kind"], SpanKind.LLM_CALL.value)
        self.assertIsInstance(d["span_id"], str)

    def test_from_dict_roundtrip(self):
        """from_dict(to_dict()) preserves all span fields."""
        span = Span(
            name="tool", kind=SpanKind.TOOL_CALL,
            trace_id="t1", attributes={"key": "val"},
        )
        d = span.to_dict()
        restored = Span.from_dict(d)
        self.assertEqual(restored.name, "tool")
        self.assertEqual(restored.kind, SpanKind.TOOL_CALL)
        self.assertEqual(restored.trace_id, "t1")
        self.assertEqual(restored.attributes, {"key": "val"})


# ========================================================================
# Trace
# ========================================================================

class TestTrace(TestCase):
    """Verify Trace creation and serialization roundtrip."""

    def test_auto_generated_fields(self):
        """Trace auto-generates trace_id (UUID) and start_time."""
        trace = Trace(name="pipeline_run")
        uuid.UUID(trace.trace_id)
        self.assertIsInstance(trace.start_time, float)
        self.assertIsNone(trace.end_time)

    def test_to_dict_roundtrip(self):
        """from_dict(to_dict()) preserves trace_id and name."""
        trace = Trace(name="run1")
        d = trace.to_dict()
        restored = Trace.from_dict(d)
        self.assertEqual(restored.name, "run1")
        self.assertEqual(restored.trace_id, trace.trace_id)


# ========================================================================
# NoOpProcessor
# ========================================================================

class TestNoOpProcessor(TestCase):
    """Verify NoOpProcessor methods are safe no-ops."""

    def test_methods_are_noops(self):
        """on_span_start and on_span_end do not raise."""
        proc = NoOpProcessor()
        span = Span(name="test")
        proc.on_span_start(span)  # no error
        proc.on_span_end(span)    # no error


# ========================================================================
# trace_span
# ========================================================================

class TestTraceSpan(IsolatedAsyncioTestCase):
    """Verify trace_span context manager lifecycle and nesting."""

    async def test_sets_end_time(self):
        """Exiting trace_span sets end_time >= start_time."""
        async with trace_span(None, "op", SpanKind.TASK) as span:
            self.assertIsNone(span.end_time)
        self.assertIsNotNone(span.end_time)
        self.assertGreaterEqual(span.end_time, span.start_time)

    async def test_records_error(self):
        """Exceptions inside trace_span are recorded in span.error."""
        with self.assertRaises(ValueError):
            async with trace_span(None, "fail_op", SpanKind.TASK) as span:
                raise ValueError("test error")
        self.assertIsNotNone(span.error)
        self.assertIn("ValueError", span.error)

    async def test_parent_child_linking(self):
        """Nested trace_span links child.parent_id to parent.span_id."""
        async with trace_span(None, "parent", SpanKind.TASK) as parent_span:
            async with trace_span(None, "child", SpanKind.LLM_CALL) as child_span:
                pass
        self.assertEqual(child_span.parent_id, parent_span.span_id)
        self.assertIsNone(parent_span.parent_id)

    async def test_restores_current_span(self):
        """After trace_span exits, get_current_span() returns None."""
        self.assertIsNone(get_current_span())
        async with trace_span(None, "outer", SpanKind.TASK):
            pass
        self.assertIsNone(get_current_span())

    async def test_notifies_processor(self):
        """Custom processor receives on_span_start and on_span_end callbacks."""
        starts = []
        ends = []

        class RecordingProcessor(TracingProcessor):
            def on_span_start(self, span):
                starts.append(span)

            def on_span_end(self, span):
                ends.append(span)

        proc = RecordingProcessor()
        async with trace_span(proc, "test", SpanKind.TASK) as span:
            pass

        self.assertEqual(len(starts), 1)
        self.assertEqual(len(ends), 1)
        self.assertIs(starts[0], span)
        self.assertIs(ends[0], span)

    async def test_attributes_mutable_during_span(self):
        """Span attributes dict can be modified while the span is active."""
        async with trace_span(None, "op", SpanKind.TOOL_CALL) as span:
            span.attributes["tool.result"] = "success"
        self.assertEqual(span.attributes["tool.result"], "success")

    async def test_inherits_trace_id_from_parent(self):
        """Child span inherits trace_id from its parent span."""
        async with trace_span(None, "root", SpanKind.TASK, trace_id="trace-abc") as root:
            async with trace_span(None, "child", SpanKind.LLM_CALL) as child:
                pass
        self.assertEqual(root.trace_id, "trace-abc")
        self.assertEqual(child.trace_id, "trace-abc")

    async def test_explicit_trace_id_override(self):
        """Explicit trace_id on a child span overrides parent's trace_id."""
        async with trace_span(None, "root", SpanKind.TASK, trace_id="root-trace") as root:
            async with trace_span(None, "child", SpanKind.LLM_CALL, trace_id="child-trace") as child:
                pass
        self.assertEqual(root.trace_id, "root-trace")
        self.assertEqual(child.trace_id, "child-trace")

    async def test_processor_receives_error_on_exception(self):
        """Processor on_span_end is called even when span raises."""
        ends = []

        class ErrProc(TracingProcessor):
            def on_span_start(self, span):
                pass
            def on_span_end(self, span):
                ends.append(span)

        with self.assertRaises(RuntimeError):
            async with trace_span(ErrProc(), "fail", SpanKind.TASK) as span:
                raise RuntimeError("boom")
        self.assertEqual(len(ends), 1)
        self.assertIn("RuntimeError", ends[0].error)

    async def test_resolves_processor_from_context_var(self):
        """When processor=None, trace_span resolves from _current_processor ContextVar."""
        calls = []

        class CtxProc(TracingProcessor):
            def on_span_start(self, span):
                calls.append(("start", span.name))
            def on_span_end(self, span):
                calls.append(("end", span.name))

        token = _current_processor.set(CtxProc())
        try:
            async with trace_span(None, "auto_resolved", SpanKind.TASK):
                pass
        finally:
            _current_processor.reset(token)

        self.assertEqual(calls, [("start", "auto_resolved"), ("end", "auto_resolved")])

    async def test_trace_id_from_context_var(self):
        """When no parent and no explicit trace_id, uses _current_trace_id ContextVar."""
        token = _current_trace_id.set("ctx-trace-123")
        try:
            async with trace_span(None, "op", SpanKind.TASK) as span:
                pass
        finally:
            _current_trace_id.reset(token)
        self.assertEqual(span.trace_id, "ctx-trace-123")

    async def test_three_level_nesting(self):
        """Three levels of nesting correctly chain parent_ids."""
        async with trace_span(None, "root", SpanKind.TASK) as root:
            async with trace_span(None, "mid", SpanKind.LLM_CALL) as mid:
                async with trace_span(None, "leaf", SpanKind.TOOL_CALL) as leaf:
                    pass
        self.assertIsNone(root.parent_id)
        self.assertEqual(mid.parent_id, root.span_id)
        self.assertEqual(leaf.parent_id, mid.span_id)


# ========================================================================
# Tracer utility functions
# ========================================================================

class TestTracerUtilityFunctions(IsolatedAsyncioTestCase):
    """Verify get_current_trace_id, get_current_processor, set_current_processor."""

    def test_get_current_trace_id_default_empty(self):
        """get_current_trace_id() returns empty string by default."""
        self.assertEqual(get_current_trace_id(), "")

    def test_get_current_processor_default_none(self):
        """get_current_processor() returns None by default."""
        self.assertIsNone(get_current_processor())

    def test_set_and_get_current_processor(self):
        """set_current_processor sets the value; get_current_processor retrieves it."""
        proc = NoOpProcessor()
        token = set_current_processor(proc)
        try:
            self.assertIs(get_current_processor(), proc)
        finally:
            _current_processor.reset(token)

    def test_set_current_processor_returns_reset_token(self):
        """set_current_processor returns a token that restores the previous value."""
        proc = NoOpProcessor()
        token = set_current_processor(proc)
        self.assertIs(get_current_processor(), proc)
        _current_processor.reset(token)
        self.assertIsNone(get_current_processor())

    async def test_get_current_span_inside_trace_span(self):
        """get_current_span() returns the active span inside trace_span."""
        async with trace_span(None, "active", SpanKind.TASK) as span:
            self.assertIs(get_current_span(), span)
        self.assertIsNone(get_current_span())


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
