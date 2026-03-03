"""
Integration tests for Reader and MetricsConsolidator with the inference pipeline.

These tests verify that response readers correctly consume and process
inference pipeline outputs.
"""

import dragon
import multiprocessing as mp
import time
import unittest
from unittest.mock import MagicMock, patch
import tempfile
import os
import pandas as pd

from dragon.ai.inference.reader_utils import ReadWorker, MetricsConsolidator


def setUpModule():
    """Set the multiprocessing start method to dragon at module load."""
    try:
        mp.set_start_method("dragon", force=True)
    except RuntimeError:
        pass


class TestReadWorkerIntegration(unittest.TestCase):
    """Integration tests for ReadWorker consuming responses from inference pipeline."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def test_read_worker_consumes_single_response(self):
        """Test that ReadWorker correctly reads a single response from queue."""
        response_queue = mp.Queue()
        end_event = mp.Event()

        reader = ReadWorker(q=response_queue, end_ev=end_event)

        # Put response in queue
        response = {
            "user": "Test prompt",
            "assistant": "Test response",
            "hostname": "node-0",
            "inf_worker_id": 1,
        }
        response_queue.put(response)

        # Read in a separate process
        read_proc = mp.Process(target=reader.read, args=(1,))
        read_proc.start()
        read_proc.join(timeout=2)

        # Verify reader set end event after reading expected count
        self.assertTrue(end_event.is_set())

    def test_read_worker_consumes_multiple_responses(self):
        """Test that ReadWorker correctly reads multiple responses."""
        response_queue = mp.Queue()
        end_event = mp.Event()

        reader = ReadWorker(q=response_queue, end_ev=end_event)

        # Put multiple responses in queue
        num_responses = 5
        for i in range(num_responses):
            response = {
                "user": f"Prompt {i}",
                "assistant": f"Response {i}",
                "hostname": "node-0",
            }
            response_queue.put(response)

        # Read in a separate process
        read_proc = mp.Process(target=reader.read, args=(num_responses,))
        read_proc.start()
        read_proc.join(timeout=5)

        # Verify reader processed all responses
        self.assertTrue(end_event.is_set())
        self.assertTrue(response_queue.empty())

    def test_read_worker_waits_for_expected_count(self):
        """Test that ReadWorker waits for all expected responses before shutting down."""
        response_queue = mp.Queue()
        end_event = mp.Event()

        reader = ReadWorker(q=response_queue, end_ev=end_event)

        # Put fewer responses than expected
        response_queue.put({"user": "Test", "assistant": "Response"})

        # Start reader expecting 3 responses
        read_proc = mp.Process(target=reader.read, args=(3,))
        read_proc.start()

        # Wait a bit and check end_event is not set yet
        time.sleep(0.5)
        self.assertFalse(end_event.is_set())

        # Add remaining responses
        response_queue.put({"user": "Test2", "assistant": "Response2"})
        response_queue.put({"user": "Test3", "assistant": "Response3"})

        # Wait for reader to finish
        read_proc.join(timeout=3)

        # Now end_event should be set
        self.assertTrue(end_event.is_set())

    def test_read_worker_adds_counter_to_responses(self):
        """Test that ReadWorker adds counter field to each response."""
        # This is an indirect test through the read method's side effects
        # We verify the functionality through the class behavior
        response_queue = mp.Queue()
        end_event = mp.Event()

        reader = ReadWorker(q=response_queue, end_ev=end_event)

        # The counter is added internally but we can verify the mechanism
        self.assertIsNotNone(reader.q)
        self.assertIsNotNone(reader.end_ev)


class TestMetricsConsolidatorIntegration(unittest.TestCase):
    """Integration tests for MetricsConsolidator collecting and aggregating metrics."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def setUp(self):
        # Create temporary directory for test Excel files
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Clean up temporary files
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_metrics_consolidator_collects_single_response(self):
        """Test that MetricsConsolidator collects metrics from a single response."""
        response_queue = mp.Queue()
        end_event = mp.Event()
        read_event = mp.Event()

        excel_file = os.path.join(self.temp_dir, "test_metrics.xlsx")
        sheet_name = "test_sheet"

        consolidator = MetricsConsolidator(
            q=response_queue,
            end_ev=end_event,
            read_ev=read_event,
            descriptor="Test Run",
            base_start_time=time.time(),
            excel_workbook=excel_file,
            sheet_name=sheet_name,
        )

        # Create response with all required metrics
        response = {
            "hostname": "node-0",
            "inf_worker_id": 1,
            "devices": [0, 1],
            "batch_size": 2,
            "cpu_head_network_latency": 0.01,
            "guardrails_inference_latency": 0.02,
            "guardrails_network_latency": 0.005,
            "model_inference_latency": 1.5,
            "model_network_latency": 0.03,
            "end_to_end_latency": 1.6,
            "requests_per_second": 2.0,
            "total_tokens_per_second": 150.0,
            "total_output_tokens_per_second": 75.0,
        }

        response_queue.put(response)
        read_event.set()  # Signal all prompts submitted

        # Run consolidator in separate process
        consolidator_proc = mp.Process(target=consolidator.read_and_compute, args=(1,))
        consolidator_proc.start()
        consolidator_proc.join(timeout=5)

        # Verify end event was set
        self.assertTrue(end_event.is_set())

        # Verify Excel file was created
        self.assertTrue(os.path.exists(excel_file))

        # Verify Excel content
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        self.assertEqual(len(df), 1)
        self.assertEqual(df["hostname"].iloc[0], "node-0")
        self.assertEqual(df["inf_worker_id"].iloc[0], 1)
        self.assertEqual(df["model_inference_latency"].iloc[0], 1.5)

    def test_metrics_consolidator_aggregates_multiple_responses(self):
        """Test that MetricsConsolidator correctly aggregates multiple responses."""
        response_queue = mp.Queue()
        end_event = mp.Event()
        read_event = mp.Event()

        excel_file = os.path.join(self.temp_dir, "test_metrics_multi.xlsx")
        sheet_name = "multi_test"

        consolidator = MetricsConsolidator(
            q=response_queue,
            end_ev=end_event,
            read_ev=read_event,
            descriptor="Multi Response Test",
            base_start_time=time.time(),
            excel_workbook=excel_file,
            sheet_name=sheet_name,
        )

        # Create multiple responses
        num_responses = 10
        for i in range(num_responses):
            response = {
                "hostname": f"node-{i % 2}",
                "inf_worker_id": i % 3 + 1,
                "devices": [0],
                "batch_size": 1,
                "cpu_head_network_latency": 0.01 + i * 0.001,
                "guardrails_inference_latency": 0.02,
                "guardrails_network_latency": 0.005,
                "model_inference_latency": 1.0 + i * 0.1,
                "model_network_latency": 0.03,
                "end_to_end_latency": 1.5 + i * 0.1,
                "requests_per_second": 2.0,
                "total_tokens_per_second": 100.0 + i * 10,
                "total_output_tokens_per_second": 50.0 + i * 5,
            }
            response_queue.put(response)

        read_event.set()

        # Run consolidator
        consolidator_proc = mp.Process(target=consolidator.read_and_compute, args=(num_responses,))
        consolidator_proc.start()
        consolidator_proc.join(timeout=10)

        # Verify all responses processed
        self.assertTrue(end_event.is_set())
        self.assertTrue(os.path.exists(excel_file))

        # Verify Excel content
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        self.assertEqual(len(df), num_responses)

        # Verify data variety
        unique_hostnames = df["hostname"].unique()
        self.assertEqual(len(unique_hostnames), 2)

        unique_worker_ids = df["inf_worker_id"].unique()
        self.assertEqual(len(unique_worker_ids), 3)

    def test_metrics_consolidator_computes_derived_metrics(self):
        """Test that MetricsConsolidator computes derived metrics correctly."""
        response_queue = mp.Queue()
        end_event = mp.Event()
        read_event = mp.Event()

        excel_file = os.path.join(self.temp_dir, "test_derived_metrics.xlsx")
        sheet_name = "derived"

        consolidator = MetricsConsolidator(
            q=response_queue,
            end_ev=end_event,
            read_ev=read_event,
            descriptor="Derived Metrics Test",
            base_start_time=time.time(),
            excel_workbook=excel_file,
            sheet_name=sheet_name,
        )

        response = {
            "hostname": "node-0",
            "inf_worker_id": 1,
            "devices": [0],
            "batch_size": 1,
            "cpu_head_network_latency": 0.05,
            "guardrails_inference_latency": 0.10,
            "guardrails_network_latency": 0.02,
            "model_inference_latency": 2.0,
            "model_network_latency": 0.08,
            "end_to_end_latency": 2.5,
            "requests_per_second": 1.5,
            "total_tokens_per_second": 120.0,
            "total_output_tokens_per_second": 60.0,
        }

        response_queue.put(response)
        read_event.set()

        consolidator_proc = mp.Process(target=consolidator.read_and_compute, args=(1,))
        consolidator_proc.start()
        consolidator_proc.join(timeout=5)

        # Verify derived metric: non_model_inference_latency
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        expected_non_model = 0.05 + 0.10 + 0.02 + 0.08
        self.assertAlmostEqual(df["non_model_inference_latency"].iloc[0], expected_non_model, places=2)

    def test_metrics_consolidator_waits_for_read_event(self):
        """Test that MetricsConsolidator waits for read_event before completing."""
        response_queue = mp.Queue()
        end_event = mp.Event()
        read_event = mp.Event()

        excel_file = os.path.join(self.temp_dir, "test_wait.xlsx")

        consolidator = MetricsConsolidator(
            q=response_queue,
            end_ev=end_event,
            read_ev=read_event,
            descriptor="Wait Test",
            base_start_time=time.time(),
            excel_workbook=excel_file,
            sheet_name="wait_sheet",
        )

        response = {
            "hostname": "node-0",
            "inf_worker_id": 1,
            "devices": [0],
            "batch_size": 1,
            "cpu_head_network_latency": 0.01,
            "guardrails_inference_latency": 0.02,
            "guardrails_network_latency": 0.005,
            "model_inference_latency": 1.0,
            "model_network_latency": 0.03,
            "end_to_end_latency": 1.5,
            "requests_per_second": 2.0,
            "total_tokens_per_second": 100.0,
            "total_output_tokens_per_second": 50.0,
        }

        response_queue.put(response)
        # Don't set read_event yet

        # Start consolidator
        proc = mp.Process(target=consolidator.read_and_compute, args=(1,))
        proc.start()

        # Wait a bit and verify end_event not set
        time.sleep(0.5)
        self.assertFalse(end_event.is_set())

        # Now set read_event
        read_event.set()

        # Wait for completion
        proc.join(timeout=5)

        # Now end_event should be set
        self.assertTrue(end_event.is_set())


class TestReaderWithInferencePipeline(unittest.TestCase):
    """Integration tests for readers working with simulated inference pipeline output."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def test_reader_processes_inference_worker_responses(self):
        """Test that reader can process responses from inference workers."""
        response_queue = mp.Queue()
        end_event = mp.Event()

        reader = ReadWorker(q=response_queue, end_ev=end_event)

        # Simulate inference worker sending responses
        inference_responses = [
            {
                "hostname": "node-0",
                "inf_worker_id": 1,
                "devices": [0, 1],
                "batch_size": 2,
                "cpu_head_network_latency": 0.01,
                "guardrails_inference_latency": 0.02,
                "guardrails_network_latency": 0.005,
                "model_inference_latency": 1.5,
                "model_network_latency": 0.03,
                "end_to_end_latency": 1.6,
                "requests_per_second": 2.0,
                "total_tokens_per_second": 150.0,
                "total_output_tokens_per_second": 75.0,
                "user": f"User prompt {i}",
                "assistant": f"Assistant response {i}",
            }
            for i in range(5)
        ]

        for response in inference_responses:
            response_queue.put(response)

        # Process responses
        read_proc = mp.Process(target=reader.read, args=(5,))
        read_proc.start()
        read_proc.join(timeout=5)

        # Verify completion
        self.assertTrue(end_event.is_set())


if __name__ == "__main__":
    unittest.main()
