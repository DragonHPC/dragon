"""
Unit tests for the batching module.

Tests the DynamicBatcher, BatchItem, and Batch classes.

These tests use Dragon multiprocessing primitives.
"""

import dragon
import multiprocessing as mp
import time
from unittest import TestCase, main

from dragon.ai.inference.batching import DynamicBatcher, BatchItem, Batch


class TestBatchItem(TestCase):
    """Test cases for BatchItem dataclass."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_batch_item_creation(self):
        """Test creating a BatchItem."""
        queue = mp.Queue()
        latency_metrics = (1.0, 0.1, 0.05)

        item = BatchItem(
            user_prompt="Hello",
            formatted_prompt="<user>Hello</user>",
            response_queue=queue,
            latency_metrics=latency_metrics,
        )

        self.assertEqual(item.user_prompt, "Hello")
        self.assertEqual(item.formatted_prompt, "<user>Hello</user>")
        self.assertEqual(item.response_queue, queue)
        self.assertEqual(item.latency_metrics, latency_metrics)


class TestBatch(TestCase):
    """Test cases for Batch dataclass."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_batch_creation(self):
        """Test creating a Batch."""
        queue = mp.Queue()
        items = [
            BatchItem("Hello", "<user>Hello</user>", queue, (1.0, 0.1, 0.05)),
            BatchItem("World", "<user>World</user>", queue, (1.1, 0.1, 0.05)),
        ]

        batch = Batch(items=items, batch_id=1, created_at=time.time())

        self.assertEqual(len(batch.items), 2)
        self.assertEqual(batch.batch_id, 1)
        self.assertEqual(batch.size, 2)

    def test_batch_user_prompts_property(self):
        """Test user_prompts property."""
        queue = mp.Queue()
        items = [
            BatchItem("Hello", "<user>Hello</user>", queue, (1.0, 0.1, 0.05)),
            BatchItem("World", "<user>World</user>", queue, (1.1, 0.1, 0.05)),
        ]

        batch = Batch(items=items, batch_id=1, created_at=time.time())

        self.assertEqual(batch.user_prompts, ["Hello", "World"])

    def test_batch_formatted_prompts_property(self):
        """Test formatted_prompts property."""
        queue = mp.Queue()
        items = [
            BatchItem("Hello", "<user>Hello</user>", queue, (1.0, 0.1, 0.05)),
            BatchItem("World", "<user>World</user>", queue, (1.1, 0.1, 0.05)),
        ]

        batch = Batch(items=items, batch_id=1, created_at=time.time())

        self.assertEqual(batch.formatted_prompts, ["<user>Hello</user>", "<user>World</user>"])

    def test_batch_response_queues_property(self):
        """Test response_queues property."""
        queue_1 = mp.Queue()
        queue_2 = mp.Queue()
        items = [
            BatchItem("Hello", "<user>Hello</user>", queue_1, (1.0, 0.1, 0.05)),
            BatchItem("World", "<user>World</user>", queue_2, (1.1, 0.1, 0.05)),
        ]

        batch = Batch(items=items, batch_id=1, created_at=time.time())

        self.assertEqual(batch.response_queues, [queue_1, queue_2])

    def test_batch_latency_metrics_property(self):
        """Test latency_metrics property."""
        queue = mp.Queue()
        items = [
            BatchItem("Hello", "<user>Hello</user>", queue, (1.0, 0.1, 0.05)),
            BatchItem("World", "<user>World</user>", queue, (1.1, 0.2, 0.06)),
        ]

        batch = Batch(items=items, batch_id=1, created_at=time.time())

        self.assertEqual(batch.latency_metrics, [(1.0, 0.1, 0.05), (1.1, 0.2, 0.06)])


class TestDynamicBatcher(TestCase):
    """Test cases for DynamicBatcher class."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_init_default_values(self):
        """Test initialization with default values."""
        batcher = DynamicBatcher(batch_wait_seconds=0.1, max_batch_size=10, enabled=True)

        self.assertEqual(batcher.batch_wait_seconds, 0.1)
        self.assertEqual(batcher.max_batch_size, 10)
        self.assertTrue(batcher.enabled)
        self.assertEqual(batcher.current_batch_size, 0)

    def test_add_item_batching_disabled(self):
        """Test that items are returned immediately when batching is disabled."""
        batcher = DynamicBatcher(batch_wait_seconds=0.1, max_batch_size=10, enabled=False)

        queue = mp.Queue()
        batch = batcher.add_item(
            user_prompt="Hello",
            formatted_prompt="<user>Hello</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )

        # Batch should be returned immediately when disabled
        self.assertIsNotNone(batch)
        self.assertEqual(batch.size, 1)
        self.assertEqual(batcher.current_batch_size, 0)

    def test_add_item_batching_enabled_not_ready(self):
        """Test that items are batched and not returned until ready."""
        batcher = DynamicBatcher(
            batch_wait_seconds=10.0,  # Long wait time
            max_batch_size=10,
            enabled=True,
        )

        queue = mp.Queue()
        batch = batcher.add_item(
            user_prompt="Hello",
            formatted_prompt="<user>Hello</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )

        # Batch should not be returned yet
        self.assertIsNone(batch)
        self.assertEqual(batcher.current_batch_size, 1)

    def test_add_item_max_batch_size_reached(self):
        """Test that batch is returned when max batch size is reached."""
        batcher = DynamicBatcher(batch_wait_seconds=10.0, max_batch_size=3, enabled=True)

        queue = mp.Queue()

        # Add items until max batch size
        for i in range(2):
            batch = batcher.add_item(
                user_prompt=f"Hello {i}",
                formatted_prompt=f"<user>Hello {i}</user>",
                response_queue=queue,
                latency_metrics=(1.0, 0.1, 0.05),
            )
            self.assertIsNone(batch)

        # Third item should trigger batch return
        batch = batcher.add_item(
            user_prompt="Hello 2",
            formatted_prompt="<user>Hello 2</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )

        self.assertIsNotNone(batch)
        self.assertEqual(batch.size, 3)
        self.assertEqual(batcher.current_batch_size, 0)

    def test_add_item_time_window_expired(self):
        """Test that batch is returned when time window expires."""
        batcher = DynamicBatcher(
            batch_wait_seconds=0.01,  # Very short wait time
            max_batch_size=100,
            enabled=True,
        )

        queue = mp.Queue()

        # Add first item
        batch = batcher.add_item(
            user_prompt="Hello",
            formatted_prompt="<user>Hello</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )
        self.assertIsNone(batch)

        # Wait for time window to expire
        time.sleep(0.02)

        # Add second item - should trigger batch return due to time
        batch = batcher.add_item(
            user_prompt="World",
            formatted_prompt="<user>World</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )

        self.assertIsNotNone(batch)
        self.assertEqual(batch.size, 2)

    def test_flush_batch_with_items(self):
        """Test flushing batch when items are present."""
        batcher = DynamicBatcher(batch_wait_seconds=10.0, max_batch_size=100, enabled=True)

        queue = mp.Queue()

        # Add some items
        batcher.add_item(
            user_prompt="Hello",
            formatted_prompt="<user>Hello</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )
        batcher.add_item(
            user_prompt="World",
            formatted_prompt="<user>World</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )

        # Flush the batch
        batch = batcher.flush_batch()

        self.assertIsNotNone(batch)
        self.assertEqual(batch.size, 2)
        self.assertEqual(batcher.current_batch_size, 0)

    def test_flush_batch_empty(self):
        """Test flushing batch when no items are present."""
        batcher = DynamicBatcher(batch_wait_seconds=10.0, max_batch_size=100, enabled=True)

        batch = batcher.flush_batch()

        self.assertIsNone(batch)

    def test_should_check_batch_no_items(self):
        """Test should_check_batch returns False when no items."""
        batcher = DynamicBatcher(batch_wait_seconds=0.01, max_batch_size=100, enabled=True)

        self.assertFalse(batcher.should_check_batch())

    def test_should_check_batch_time_not_expired(self):
        """Test should_check_batch returns False when time not expired."""
        batcher = DynamicBatcher(batch_wait_seconds=10.0, max_batch_size=100, enabled=True)

        queue = mp.Queue()
        batcher.add_item(
            user_prompt="Hello",
            formatted_prompt="<user>Hello</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )

        self.assertFalse(batcher.should_check_batch())

    def test_should_check_batch_time_expired(self):
        """Test should_check_batch returns True when time expired."""
        batcher = DynamicBatcher(batch_wait_seconds=0.01, max_batch_size=100, enabled=True)

        queue = mp.Queue()
        batcher.add_item(
            user_prompt="Hello",
            formatted_prompt="<user>Hello</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )

        # Wait for time to expire
        time.sleep(0.02)

        self.assertTrue(batcher.should_check_batch())

    def test_batch_counter_increments(self):
        """Test that batch counter increments correctly."""
        batcher = DynamicBatcher(batch_wait_seconds=10.0, max_batch_size=1, enabled=True)

        queue = mp.Queue()

        # First batch
        batch1 = batcher.add_item(
            user_prompt="Hello",
            formatted_prompt="<user>Hello</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )
        self.assertEqual(batch1.batch_id, 0)

        # Second batch
        batch2 = batcher.add_item(
            user_prompt="World",
            formatted_prompt="<user>World</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )
        self.assertEqual(batch2.batch_id, 1)

    def test_current_batch_age(self):
        """Test current_batch_age property."""
        batcher = DynamicBatcher(batch_wait_seconds=10.0, max_batch_size=100, enabled=True)

        queue = mp.Queue()

        # Add an item
        batcher.add_item(
            user_prompt="Hello",
            formatted_prompt="<user>Hello</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )

        # Wait a bit
        time.sleep(0.05)

        # Check age
        age = batcher.current_batch_age
        self.assertGreater(age, 0.04)
        self.assertLess(age, 0.2)  # Should be around 0.05 seconds

    def test_batch_start_time_starts_on_first_item(self):
        """Batch timer should start when first item is added."""
        batcher = DynamicBatcher(batch_wait_seconds=0.5, max_batch_size=10, enabled=True)

        # Immediately after init, there should be no active batch
        # and current_batch_age should be ~0 when first item arrives.
        queue = mp.Queue()
        time.sleep(0.05)

        # Add first item; this should start the timer and not flush yet.
        batch = batcher.add_item(
            user_prompt="Hello",
            formatted_prompt="<user>Hello</user>",
            response_queue=queue,
            latency_metrics=(1.0, 0.1, 0.05),
        )

        self.assertIsNone(batch)
        self.assertEqual(batcher.current_batch_size, 1)
        # Age should be small because timer was just started
        age = batcher.current_batch_age
        self.assertGreaterEqual(age, 0.0)
        self.assertLess(age, 0.2)


class TestDynamicBatcherIntegration(TestCase):
    """Integration tests for DynamicBatcher class."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_typical_batching_workflow(self):
        """Test a typical batching workflow with multiple items."""
        batcher = DynamicBatcher(batch_wait_seconds=0.1, max_batch_size=5, enabled=True)

        queue = mp.Queue()
        batches_received = []

        # Simulate adding items over time
        for i in range(12):
            batch = batcher.add_item(
                user_prompt=f"Prompt {i}",
                formatted_prompt=f"<user>Prompt {i}</user>",
                response_queue=queue,
                latency_metrics=(time.time(), 0.1, 0.05),
            )
            if batch:
                batches_received.append(batch)

        # We should have received 2 batches (5 + 5)
        # with 2 items still pending
        self.assertEqual(len(batches_received), 2)
        self.assertEqual(batches_received[0].size, 5)
        self.assertEqual(batches_received[1].size, 5)
        self.assertEqual(batcher.current_batch_size, 2)

        # Flush remaining items
        final_batch = batcher.flush_batch()
        self.assertIsNotNone(final_batch)
        self.assertEqual(final_batch.size, 2)

    def test_mixed_batching_and_flushing(self):
        """Test mixed batching with intermediate flushes."""
        batcher = DynamicBatcher(batch_wait_seconds=10.0, max_batch_size=100, enabled=True)

        queue = mp.Queue()

        # Add items
        for i in range(3):
            batcher.add_item(
                user_prompt=f"Prompt {i}",
                formatted_prompt=f"<user>Prompt {i}</user>",
                response_queue=queue,
                latency_metrics=(time.time(), 0.1, 0.05),
            )

        # Flush mid-way
        batch1 = batcher.flush_batch()
        self.assertEqual(batch1.size, 3)

        # Add more items
        for i in range(2):
            batcher.add_item(
                user_prompt=f"Prompt {i + 3}",
                formatted_prompt=f"<user>Prompt {i + 3}</user>",
                response_queue=queue,
                latency_metrics=(time.time(), 0.1, 0.05),
            )

        # Flush remaining
        batch2 = batcher.flush_batch()
        self.assertEqual(batch2.size, 2)


def process_batch_worker(input_q, output_q, worker_id):
    while True:
        try:
            batch = input_q.get(timeout=0.5)
            # Simulate processing
            result = {
                "worker_id": worker_id,
                "batch_id": batch.batch_id,
                "prompts_processed": len(batch.user_prompts),
            }
            output_q.put(result)
        except Exception:
            break


def batch_producer_worker(batch_queue: mp.Queue, num_batches: int, items_per_batch: int, worker_id: int):
    """Worker function that produces batches and puts them in a queue.

    :param batch_queue: Queue to put batches into
    :type batch_queue: mp.Queue
    :param num_batches: Number of batches to produce.
    :type num_batches: int
    :param items_per_batch: Number of items per batch.
    :type items_per_batch: int
    :param worker_id: Unique identifier for this worker
    :type worker_id: int
    """
    for batch_idx in range(num_batches):
        response_queue = mp.Queue()
        items = []

        for item_idx in range(items_per_batch):
            prompt = f"Worker {worker_id} - Batch {batch_idx} - Item {item_idx}"
            item = BatchItem(
                user_prompt=prompt,
                formatted_prompt=f"<user>{prompt}</user>",
                response_queue=response_queue,
                latency_metrics=(time.time(), 0.1, 0.05),
            )
            items.append(item)

        batch = Batch(
            items=items,
            batch_id=worker_id * 1000 + batch_idx,
            created_at=time.time(),
        )
        batch_queue.put(batch)


def batch_consumer_worker(batch_queue: mp.Queue, result_queue: mp.Queue, worker_id: int):
    """Worker function that consumes batches from a queue and processes them.

    Args:
        batch_queue: Queue to get batches from
        result_queue: Queue to put processing results into
        worker_id: Unique identifier for this worker
    """
    processed_batches = []

    while True:
        try:
            batch = batch_queue.get(timeout=2.0)

            if batch is None:
                # Sentinel value indicating no more batches
                break

            # Simulate processing the batch
            result = {
                "consumer_id": worker_id,
                "batch_id": batch.batch_id,
                "batch_size": batch.size,
                "first_prompt": batch.user_prompts[0] if batch.size > 0 else None,
                "processed_at": time.time(),
            }
            processed_batches.append(result)

        except Exception as e:
            # Timeout or other error
            break

    result_queue.put({"consumer_id": worker_id, "batches": processed_batches})


class TestMultiprocessBatchSharing(TestCase):

    def test_multiprocess_batch_creation_and_sharing(self):
        """Test creating batches in multiple producer processes and consuming in multiple consumer processes."""
        num_producers = 3
        num_consumers = 2
        batches_per_producer = 2
        items_per_batch = 4

        batch_queue = mp.Queue()
        result_queue = mp.Queue()

        # Start producer processes
        producer_processes = []
        for worker_id in range(num_producers):
            p = mp.Process(
                target=batch_producer_worker,
                args=(batch_queue, batches_per_producer, items_per_batch, worker_id),
            )
            p.start()
            producer_processes.append(p)

        # Start consumer processes
        consumer_processes = []
        for worker_id in range(num_consumers):
            p = mp.Process(
                target=batch_consumer_worker,
                args=(batch_queue, result_queue, worker_id),
            )
            p.start()
            consumer_processes.append(p)

        # Wait for all producers to finish
        for p in producer_processes:
            p.join(timeout=5.0)

        # Put sentinel values for consumers (one per consumer)
        for _ in range(num_consumers):
            batch_queue.put(None)

        # Wait for all consumers to finish
        for p in consumer_processes:
            p.join(timeout=5.0)

        # Collect results from consumers
        results = []
        while not result_queue.empty():
            try:
                result = result_queue.get(timeout=0.5)
                results.append(result)
            except Exception:
                break

        # Verify results
        self.assertEqual(len(results), num_consumers)

        # Count total batches processed
        total_batches_processed = sum(len(r["batches"]) for r in results)
        expected_total_batches = num_producers * batches_per_producer

        self.assertEqual(total_batches_processed, expected_total_batches)

        # Verify each batch has correct size
        for result in results:
            for batch_info in result["batches"]:
                self.assertEqual(batch_info["batch_size"], items_per_batch)
                self.assertIsNotNone(batch_info["first_prompt"])

    def test_batch_queue_communication(self):
        """Test that Batch objects can be passed through Dragon queues correctly."""
        queue = mp.Queue()

        # Create a batch
        response_queue = mp.Queue()
        items = [
            BatchItem(
                user_prompt=f"Prompt {i}",
                formatted_prompt=f"<user>Prompt {i}</user>",
                response_queue=response_queue,
                latency_metrics=(time.time(), 0.1, 0.05),
            )
            for i in range(5)
        ]

        original_batch = Batch(items=items, batch_id=42, created_at=time.time())

        # Put batch in queue
        queue.put(original_batch)

        # Get batch from queue
        retrieved_batch = queue.get(timeout=1.0)

        # Verify batch integrity
        self.assertEqual(retrieved_batch.batch_id, original_batch.batch_id)
        self.assertEqual(retrieved_batch.size, original_batch.size)
        self.assertEqual(retrieved_batch.user_prompts, original_batch.user_prompts)
        self.assertEqual(retrieved_batch.formatted_prompts, original_batch.formatted_prompts)

    def test_concurrent_batch_processing_pipeline(self):
        """Test a complete pipeline with batch creation, queuing, and parallel processing."""
        input_queue = mp.Queue()
        output_queue = mp.Queue()

        # Create batches with a batcher
        batcher = DynamicBatcher(batch_wait_seconds=0.01, max_batch_size=3, enabled=True)

        response_queue = mp.Queue()

        # Add items to create multiple batches
        created_batches = []
        for i in range(10):
            batch = batcher.add_item(
                user_prompt=f"Prompt {i}",
                formatted_prompt=f"<user>Prompt {i}</user>",
                response_queue=response_queue,
                latency_metrics=(time.time(), 0.1, 0.05),
            )
            if batch:
                created_batches.append(batch)
                input_queue.put(batch)

        # Flush remaining
        final_batch = batcher.flush_batch()
        if final_batch:
            created_batches.append(final_batch)
            input_queue.put(final_batch)

        # Start worker processes
        num_workers = 2
        workers = []
        for worker_id in range(num_workers):
            p = mp.Process(
                target=process_batch_worker,
                args=(input_queue, output_queue, worker_id),
            )
            p.start()
            workers.append(p)

        # Wait for workers to finish
        for p in workers:
            p.join(timeout=3.0)

        # Collect results
        results = []
        while not output_queue.empty():
            try:
                result = output_queue.get(timeout=0.5)
                results.append(result)
            except Exception:
                break

        # Verify all batches were processed
        self.assertEqual(len(results), len(created_batches))

        # Verify total prompts processed
        total_prompts = sum(r["prompts_processed"] for r in results)
        self.assertEqual(total_prompts, 10)


if __name__ == "__main__":
    main()
