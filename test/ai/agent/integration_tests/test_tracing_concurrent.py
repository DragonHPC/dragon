"""Integration tests — DictTracingProcessor with real DDict under concurrent load.

Verifies that span writes through DictTracingProcessor maintain correct
ordering and isolation when multiple concurrent tasks use their own processor
instances writing to the same DDict.

Run with:  dragon python -m unittest test.ai.agent.integration_tests.test_tracing_concurrent -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp


import asyncio
import uuid
from unittest import TestCase, main

from dragon.data.ddict import DDict

from dragon.ai.agent.config import TRACE_KEY
from dragon.ai.agent.ddict import DDictAccessor
from dragon.ai.agent.observability.ddict_tracer import DictTracingProcessor
from dragon.ai.agent.observability.tracer import (
    Span,
    trace_span,
    set_current_processor,
)
from dragon.ai.agent.observability.trace_protocol import SpanKind


class TestDictTracingProcessorWithRealDDict(TestCase):
    """DictTracingProcessor reads/writes against a real Dragon DDict."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_single_span_lifecycle(self):
        """on_span_start → on_span_end writes a completed span to DDict."""
        agent_id = "tracer-agent"
        dispatch_id = str(uuid.uuid4())

        proc = DictTracingProcessor(
            self.ddict, self.task_id, agent_id, dispatch_id,
        )
        trace_key = TRACE_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id,
        )

        span = Span(name="test_op", kind=SpanKind.TASK, trace_id="t1")
        proc.on_span_start(span)

        # Verify start written
        spans = self.ddict[trace_key]
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0]["name"], "test_op")
        self.assertIsNone(spans[0]["end_time"])

        # End span
        import time
        span.end_time = time.time()
        proc.on_span_end(span)

        spans = self.ddict[trace_key]
        self.assertEqual(len(spans), 1)
        self.assertIsNotNone(spans[0]["end_time"])

    def test_multiple_spans_ordered(self):
        """Multiple spans are appended in order."""
        agent_id = "multi-span"
        dispatch_id = str(uuid.uuid4())

        proc = DictTracingProcessor(
            self.ddict, self.task_id, agent_id, dispatch_id,
        )
        trace_key = TRACE_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id,
        )

        for i in range(5):
            span = Span(name=f"op_{i}", kind=SpanKind.TASK, trace_id="t1")
            proc.on_span_start(span)

        spans = self.ddict[trace_key]
        self.assertEqual(len(spans), 5)
        names = [s["name"] for s in spans]
        self.assertEqual(names, [f"op_{i}" for i in range(5)])

    def test_on_span_end_without_start_appends(self):
        """If on_span_start was missed, on_span_end appends the span (fallback)."""
        agent_id = "fallback"
        dispatch_id = str(uuid.uuid4())

        proc = DictTracingProcessor(
            self.ddict, self.task_id, agent_id, dispatch_id,
        )
        trace_key = TRACE_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id,
        )

        import time
        span = Span(name="missed_start", kind=SpanKind.TASK, trace_id="t1")
        span.end_time = time.time()
        proc.on_span_end(span)

        spans = self.ddict[trace_key]
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0]["name"], "missed_start")
        self.assertIsNotNone(spans[0]["end_time"])

    def test_error_span_recorded(self):
        """Span with error field is correctly stored in DDict."""
        agent_id = "err-agent"
        dispatch_id = str(uuid.uuid4())

        proc = DictTracingProcessor(
            self.ddict, self.task_id, agent_id, dispatch_id,
        )
        trace_key = TRACE_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id,
        )

        import time
        span = Span(name="failing_op", kind=SpanKind.TASK, trace_id="t1")
        proc.on_span_start(span)
        span.error = "RuntimeError: something broke"
        span.end_time = time.time()
        proc.on_span_end(span)

        spans = self.ddict[trace_key]
        self.assertEqual(spans[0]["error"], "RuntimeError: something broke")


class TestConcurrentTracingProcessors(TestCase):
    """Multiple DictTracingProcessor instances writing to the same DDict concurrently."""

    def setUp(self):
        self.ddict = DDict(managers_per_node=1, n_nodes=1, trace=False)
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        self.ddict.destroy()

    def test_two_processors_isolated_trace_keys(self):
        """Two processors (different dispatch_ids) write to separate trace keys."""
        agent_id = "shared-agent"
        dispatch_a = str(uuid.uuid4())
        dispatch_b = str(uuid.uuid4())

        proc_a = DictTracingProcessor(
            self.ddict, self.task_id, agent_id, dispatch_a,
        )
        proc_b = DictTracingProcessor(
            self.ddict, self.task_id, agent_id, dispatch_b,
        )

        # Write spans to each processor
        span_a = Span(name="op_a", kind=SpanKind.TASK, trace_id="trace-a")
        span_b1 = Span(name="op_b1", kind=SpanKind.TASK, trace_id="trace-b")
        span_b2 = Span(name="op_b2", kind=SpanKind.TASK, trace_id="trace-b")

        proc_a.on_span_start(span_a)
        proc_b.on_span_start(span_b1)
        proc_b.on_span_start(span_b2)

        key_a = TRACE_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_a,
        )
        key_b = TRACE_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_b,
        )

        spans_a = self.ddict[key_a]
        spans_b = self.ddict[key_b]

        self.assertEqual(len(spans_a), 1)
        self.assertEqual(spans_a[0]["name"], "op_a")
        self.assertEqual(len(spans_b), 2)
        self.assertEqual(spans_b[0]["name"], "op_b1")
        self.assertEqual(spans_b[1]["name"], "op_b2")

    def test_concurrent_async_trace_spans(self):
        """Concurrent async tasks using trace_span write isolated spans to DDict."""
        agent_id = "async-agent"
        dispatch_a = str(uuid.uuid4())
        dispatch_b = str(uuid.uuid4())

        proc_a = DictTracingProcessor(
            self.ddict, self.task_id, agent_id, dispatch_a,
        )
        proc_b = DictTracingProcessor(
            self.ddict, self.task_id, agent_id, dispatch_b,
        )

        async def work(processor, name):
            async with trace_span(processor, name, SpanKind.TASK) as span:
                await asyncio.sleep(0.01)  # simulate I/O

        async def run_both():
            await asyncio.gather(
                work(proc_a, "task_a"),
                work(proc_b, "task_b"),
            )

        asyncio.run(run_both())

        key_a = TRACE_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_a,
        )
        key_b = TRACE_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_b,
        )

        spans_a = self.ddict[key_a]
        spans_b = self.ddict[key_b]

        self.assertEqual(len(spans_a), 1)
        self.assertEqual(spans_a[0]["name"], "task_a")
        self.assertIsNotNone(spans_a[0]["end_time"])

        self.assertEqual(len(spans_b), 1)
        self.assertEqual(spans_b[0]["name"], "task_b")
        self.assertIsNotNone(spans_b[0]["end_time"])

    def test_nested_trace_spans_parent_ids(self):
        """Nested trace_span calls produce correct parent_id chain in DDict."""
        agent_id = "nesting-agent"
        dispatch_id = str(uuid.uuid4())

        proc = DictTracingProcessor(
            self.ddict, self.task_id, agent_id, dispatch_id,
        )

        async def nested_work():
            async with trace_span(proc, "outer", SpanKind.TASK) as outer:
                async with trace_span(proc, "inner", SpanKind.TASK) as inner:
                    pass

        asyncio.run(nested_work())

        trace_key = TRACE_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id,
        )
        spans = self.ddict[trace_key]

        # Find spans by name
        by_name = {s["name"]: s for s in spans}
        self.assertIn("outer", by_name)
        self.assertIn("inner", by_name)

        # Inner span's parent_id should be the outer span's span_id
        self.assertEqual(by_name["inner"]["parent_id"], by_name["outer"]["span_id"])
        self.assertIsNone(by_name["outer"]["parent_id"])

    def test_error_in_trace_span_recorded_to_ddict(self):
        """Exception inside trace_span records error field in DDict span."""
        agent_id = "err-trace"
        dispatch_id = str(uuid.uuid4())

        proc = DictTracingProcessor(
            self.ddict, self.task_id, agent_id, dispatch_id,
        )

        async def failing_work():
            async with trace_span(proc, "will_fail", SpanKind.TASK):
                raise ValueError("test error")

        with self.assertRaises(ValueError):
            asyncio.run(failing_work())

        trace_key = TRACE_KEY.format(
            task_id=self.task_id, agent_id=agent_id, dispatch_id=dispatch_id,
        )
        spans = self.ddict[trace_key]
        self.assertEqual(len(spans), 1)
        self.assertIn("ValueError", spans[0]["error"])
        self.assertIsNotNone(spans[0]["end_time"])


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
