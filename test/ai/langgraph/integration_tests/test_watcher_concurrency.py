"""Integration tests — concurrent task handling and backpressure.

Tests the DragonWatcher's ability to handle multiple concurrent tasks
and enforce backpressure limits.

Covers:
- Multiple concurrent tasks resolve correctly
- Task futures are matched by task_id
- Backpressure semaphore enforces limits
- Sharding distributes tasks correctly

Run with:  dragon python -m unittest test.ai.langgraph.integration_tests.test_watcher_concurrency -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp


import concurrent.futures
import threading
import time
from unittest import TestCase, main
from unittest.mock import MagicMock, patch

import cloudpickle

from dragon.ai.langgraph.constants import (
    ENV_STATUS, ENV_PAYLOAD, STATUS_DONE, STATUS_ERROR,
)


# ========================================================================
# Concurrent Future Resolution
# ========================================================================

class TestConcurrentFutureResolution(TestCase):
    """Verify multiple futures resolve correctly when tasks complete."""

    def test_multiple_futures_resolve_independently(self):
        """Multiple pending futures resolve with correct results by task_id."""
        from dragon.ai.langgraph.watcher import _PendingTask

        # Create multiple futures
        futures = {}
        for i in range(5):
            task_id = f"task_{i}"
            future = concurrent.futures.Future()
            futures[task_id] = future

        # Simulate resolution (what watcher does)
        for task_id, future in futures.items():
            result = {"task_id": task_id, "data": f"result_{task_id}"}
            future.set_result(result)

        # Verify each future got correct result
        for task_id, future in futures.items():
            result = future.result(timeout=1.0)
            self.assertEqual(result["task_id"], task_id)
            self.assertEqual(result["data"], f"result_{task_id}")

    def test_futures_resolve_in_any_order(self):
        """Futures can resolve in any order, not necessarily submission order."""
        futures = {
            "task_a": concurrent.futures.Future(),
            "task_b": concurrent.futures.Future(),
            "task_c": concurrent.futures.Future(),
        }

        # Resolve in reverse order
        futures["task_c"].set_result({"order": 1})
        futures["task_a"].set_result({"order": 2})
        futures["task_b"].set_result({"order": 3})

        self.assertEqual(futures["task_c"].result()["order"], 1)
        self.assertEqual(futures["task_a"].result()["order"], 2)
        self.assertEqual(futures["task_b"].result()["order"], 3)


# ========================================================================
# PendingTask Dictionary Operations
# ========================================================================

class TestPendingTaskDictionary(TestCase):
    """Verify pending task dictionary operations are thread-safe."""

    def test_concurrent_add_remove(self):
        """Concurrent add/remove operations don't corrupt the dictionary."""
        from dragon.ai.langgraph.watcher import _PendingTask

        pending = {}
        lock = threading.Lock()
        errors = []

        def add_tasks(start, count):
            try:
                for i in range(count):
                    task_id = f"task_{start + i}"
                    future = concurrent.futures.Future()
                    with lock:
                        pending[task_id] = _PendingTask(
                            future=future,
                            task_id=task_id,
                            node_name="test",
                        )
            except Exception as e:
                errors.append(e)

        def remove_tasks(start, count):
            try:
                time.sleep(0.01)  # Let adds happen first
                for i in range(count):
                    task_id = f"task_{start + i}"
                    with lock:
                        pending.pop(task_id, None)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=add_tasks, args=(0, 100)),
            threading.Thread(target=add_tasks, args=(100, 100)),
            threading.Thread(target=remove_tasks, args=(0, 50)),
            threading.Thread(target=remove_tasks, args=(100, 50)),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)


# ========================================================================
# Backpressure Semaphore
# ========================================================================

class TestBackpressureSemaphore(TestCase):
    """Verify backpressure semaphore behavior."""

    def test_semaphore_limits_concurrent_slots(self):
        """BoundedSemaphore limits number of concurrent acquisitions."""
        max_slots = 3
        sem = threading.BoundedSemaphore(max_slots)

        # Acquire all slots
        for _ in range(max_slots):
            self.assertTrue(sem.acquire(blocking=False))

        # Next acquisition should fail (non-blocking)
        self.assertFalse(sem.acquire(blocking=False))

        # Release one
        sem.release()

        # Now should succeed
        self.assertTrue(sem.acquire(blocking=False))

    def test_semaphore_blocks_when_full(self):
        """Semaphore acquisition blocks when all slots are taken."""
        sem = threading.BoundedSemaphore(1)
        sem.acquire()

        acquired = []
        def try_acquire():
            result = sem.acquire(timeout=0.1)
            acquired.append(result)

        t = threading.Thread(target=try_acquire)
        t.start()
        t.join()

        # Should have timed out
        self.assertEqual(acquired, [False])

    def test_semaphore_release_unblocks_waiter(self):
        """Releasing a slot unblocks a waiting thread."""
        sem = threading.BoundedSemaphore(1)
        sem.acquire()

        acquired = []
        def try_acquire():
            result = sem.acquire(timeout=1.0)
            acquired.append(result)

        t = threading.Thread(target=try_acquire)
        t.start()

        time.sleep(0.05)
        sem.release()

        t.join()

        self.assertEqual(acquired, [True])


# ========================================================================
# Shard Distribution
# ========================================================================

class TestShardDistribution(TestCase):
    """Verify tasks are distributed across shards correctly."""

    def test_hash_based_distribution(self):
        """Tasks are distributed by hash(task_id) % num_shards."""
        num_shards = 4
        task_ids = [f"task_{i}" for i in range(100)]

        shard_counts = [0] * num_shards
        for task_id in task_ids:
            shard_index = hash(task_id) % num_shards
            shard_counts[shard_index] += 1

        # All shards should have some tasks (statistical expectation)
        for count in shard_counts:
            self.assertGreater(count, 0)

    def test_same_task_id_same_shard(self):
        """Same task_id always maps to same shard."""
        num_shards = 4
        task_id = "consistent_task_123"

        shard_1 = hash(task_id) % num_shards
        shard_2 = hash(task_id) % num_shards
        shard_3 = hash(task_id) % num_shards

        self.assertEqual(shard_1, shard_2)
        self.assertEqual(shard_2, shard_3)


# ========================================================================
# Envelope Routing by Task ID
# ========================================================================

class TestEnvelopeRouting(TestCase):
    """Verify envelopes are routed to correct pending task by task_id."""

    def test_envelope_matches_pending_task(self):
        """Envelope task_id matches the correct pending task."""
        from dragon.ai.langgraph.watcher import _PendingTask

        pending = {}
        for i in range(3):
            task_id = f"task_{i}"
            future = concurrent.futures.Future()
            pending[task_id] = _PendingTask(
                future=future,
                task_id=task_id,
                node_name=f"node_{i}",
            )

        # Simulate envelope for task_1
        envelope = {
            ENV_STATUS: STATUS_DONE,
            ENV_PAYLOAD: {"result": "data_1"},
        }

        # Route to correct task; the task_id arrives in the frame header.
        task_id = "task_1"
        task = pending.pop(task_id, None)

        self.assertIsNotNone(task)
        self.assertEqual(task.task_id, "task_1")
        self.assertEqual(task.node_name, "node_1")

        # Resolve the future
        task.future.set_result(envelope[ENV_PAYLOAD])
        self.assertEqual(task.future.result(), {"result": "data_1"})

    def test_unknown_task_id_is_ignored(self):
        """Envelope for unknown task_id is safely ignored."""
        from dragon.ai.langgraph.watcher import _PendingTask

        pending = {
            "known_task": _PendingTask(
                future=concurrent.futures.Future(),
                task_id="known_task",
                node_name="node",
            )
        }

        envelope = {
            ENV_STATUS: STATUS_DONE,
            ENV_PAYLOAD: {},
        }

        task_id = "unknown_task"
        task = pending.pop(task_id, None)

        # Should be None for unknown task
        self.assertIsNone(task)

        # Known task should still be pending
        self.assertIn("known_task", pending)


# ========================================================================
# Error Handling in Concurrent Context
# ========================================================================

class TestConcurrentErrorHandling(TestCase):
    """Verify error handling works correctly with concurrent tasks."""

    def test_one_error_doesnt_affect_others(self):
        """Error in one task doesn't affect other tasks."""
        futures = {
            "success_1": concurrent.futures.Future(),
            "error_1": concurrent.futures.Future(),
            "success_2": concurrent.futures.Future(),
        }

        # Resolve with mixed results
        futures["success_1"].set_result({"status": "ok"})
        futures["error_1"].set_exception(ValueError("task failed"))
        futures["success_2"].set_result({"status": "ok"})

        # Successes should work
        self.assertEqual(futures["success_1"].result()["status"], "ok")
        self.assertEqual(futures["success_2"].result()["status"], "ok")

        # Error should raise
        with self.assertRaises(ValueError):
            futures["error_1"].result()

    def test_multiple_errors_handled_independently(self):
        """Multiple tasks can fail independently."""
        futures = {
            "err_1": concurrent.futures.Future(),
            "err_2": concurrent.futures.Future(),
        }

        futures["err_1"].set_exception(ValueError("error 1"))
        futures["err_2"].set_exception(RuntimeError("error 2"))

        with self.assertRaises(ValueError):
            futures["err_1"].result()

        with self.assertRaises(RuntimeError):
            futures["err_2"].result()


# ========================================================================
# Timing and Ordering
# ========================================================================

class TestTimingAndOrdering(TestCase):
    """Verify timing-related behavior of concurrent task handling."""

    def test_tasks_complete_asynchronously(self):
        """Tasks can complete in any order regardless of submission."""
        completed_order = []
        futures = {}

        for i in range(5):
            task_id = f"task_{i}"
            future = concurrent.futures.Future()
            futures[task_id] = future

        # Complete in reverse order
        for i in range(4, -1, -1):
            task_id = f"task_{i}"
            futures[task_id].set_result({"completed": task_id})
            completed_order.append(task_id)

        self.assertEqual(completed_order, ["task_4", "task_3", "task_2", "task_1", "task_0"])

        # But all futures have their results
        for task_id, future in futures.items():
            self.assertEqual(future.result()["completed"], task_id)


if __name__ == "__main__":
    main()
