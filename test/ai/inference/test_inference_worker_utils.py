"""
Unit tests for the inference worker utilities module.

Tests the InferenceWorker class.

These tests use Dragon multiprocessing primitives.
"""

import dragon
import multiprocessing as mp
import time
from unittest import TestCase, main
from unittest.mock import patch, MagicMock

from dragon.ai.inference.inference_worker_utils import InferenceWorker
from dragon.ai.inference.config import (
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)


class MockTelemetry:
    """Simple mock for dragon telemetry.

    Tracks add_data calls for test assertions.
    """

    def __init__(self):
        self.data = {}
        self.add_data_calls = []

    def add_data(self, key, value):
        self.data[key] = value
        self.add_data_calls.append((key, value))

    def assert_any_call(self, key, value):
        """Check if add_data was called with specific key-value pair."""
        if (key, value) not in self.add_data_calls:
            raise AssertionError(f"add_data was not called with ({key!r}, {value!r})")


class TestInferenceWorkerInit(TestCase):
    """Test cases for InferenceWorker initialization."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=2,
            dtype="bfloat16",
            max_tokens=100,
            top_k=50,
            top_p=0.95,
            padding_side="left",
            truncation_side="left",
        )
        self.batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=0.1,
            max_batch_size=60,
        )
        self.guardrails_config = GuardrailsConfig(
            enabled=True,
            prompt_guard_model="meta-llama/Prompt-Guard-86M",
            prompt_guard_sensitivity=0.5,
        )
        self.dynamic_worker_config = DynamicWorkerConfig(
            enabled=True,
            min_active_workers_per_cpu=1,
            spin_down_threshold_seconds=3600,
        )
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_init_basic(self):
        """Test basic initialization of InferenceWorker."""
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0, 1],
            head_cpu_pid=1234,
            inf_wrkr_id=1,
        )

        # Verify config objects are stored
        self.assertEqual(worker.model_config, self.model_config)
        self.assertEqual(worker.batching_config, self.batching_config)
        self.assertEqual(worker.guardrails_config, self.guardrails_config)
        self.assertEqual(worker.dynamic_worker_config, self.dynamic_worker_config)

        # Verify extracted values
        self.assertEqual(worker.model_name, "test-model")
        self.assertEqual(worker.dtype, "bfloat16")
        self.assertEqual(worker.hf_token, "test-token")
        self.assertEqual(worker.tp_size, 2)
        self.assertEqual(worker.max_new_tokens, 100)
        self.assertEqual(worker.top_k, 50)
        self.assertEqual(worker.top_p, 0.95)

        # Verify batching values
        self.assertTrue(worker.batch_toggle)
        self.assertEqual(worker.batch_wait_time, 0.1)
        self.assertEqual(worker.batch_limit_max, 60)
        self.assertEqual(worker.batch_type, "dynamic")

        # Verify guardrails values
        self.assertTrue(worker.prompt_guard_toggle)
        self.assertEqual(worker.prompt_guard_sensitivity, 0.5)
        self.assertEqual(worker.prompt_guard_model, "meta-llama/Prompt-Guard-86M")

        # Verify dynamic worker values
        self.assertTrue(worker.dynamic_inf_wrkr_toggle)
        self.assertEqual(worker.spin_down_threshold, 3600)

        # Verify runtime parameters
        self.assertEqual(worker.hostname, "test-host")
        self.assertEqual(worker.devices, [0, 1])
        self.assertEqual(worker.head_cpu_pid, 1234)
        self.assertEqual(worker.inf_wrkr_id, 1)

    def test_init_with_all_runtime_params(self):
        """Test initialization with all runtime parameters."""
        input_queue = mp.Queue()
        output_queue = mp.Queue()
        barrier = mp.Barrier(1)  # Only 1 party needed for init test
        end_ev = mp.Event()
        down_ev = mp.Event()
        manager_q = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0, 1, 2, 3],
            head_cpu_pid=5678,
            inf_wrkr_id=2,
            preprocessing_input_queue=input_queue,
            preprocessing_output_queue=output_queue,
            inf_wrkr_barrier=barrier,
            llm_proc_end_ev=end_ev,
            master_port="29500",
            inf_wrkr_down_ev=down_ev,
            inf_wrkr_manager_q=manager_q,
        )

        self.assertEqual(worker.preprocessing_input_queue, input_queue)
        self.assertEqual(worker.preprocessing_output_queue, output_queue)
        self.assertEqual(worker.inf_wrkr_barrier, barrier)
        self.assertEqual(worker.llm_proc_end_ev, end_ev)
        self.assertEqual(worker.master_port, "29500")
        self.assertEqual(worker.inf_wrkr_down_ev, down_ev)
        self.assertEqual(worker.inf_wrkr_manager_q, manager_q)

    def test_init_with_disabled_features(self):
        """Test initialization with disabled features."""
        disabled_batching = BatchingConfig(enabled=False)
        disabled_guardrails = GuardrailsConfig(enabled=False)
        disabled_dynamic = DynamicWorkerConfig(enabled=False)

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=disabled_batching,
            guardrails_config=disabled_guardrails,
            dynamic_worker_config=disabled_dynamic,
            dt=self.dt,
        )

        self.assertFalse(worker.batch_toggle)
        self.assertFalse(worker.prompt_guard_toggle)
        self.assertFalse(worker.dynamic_inf_wrkr_toggle)


class TestInferenceWorkerGuardrailsFiltering(TestCase):
    """Test cases for guardrails filtering in InferenceWorker."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=True, batch_type="dynamic")
        self.guardrails_config = GuardrailsConfig(enabled=True)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    def test_filter_with_guardrails_disabled(self, mock_guardrails_class):
        """Test filter_with_guardrails when guardrails is None."""
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
        )

        formatted_prompts = ["<user>Hello</user>", "<user>World</user>"]
        user_prompts = ["Hello", "World"]
        response_queues = [mp.Queue(), mp.Queue()]
        latency_metrics = [(1.0, 0.1, 0.05), (1.1, 0.1, 0.05)]

        (
            safe_formatted,
            safe_user,
            safe_queues,
            safe_metrics,
            preprocessing_time,
        ) = worker.filter_with_guardrails(
            formatted_prompts=formatted_prompts,
            user_prompts=user_prompts,
            response_queues=response_queues,
            latency_metrics=latency_metrics,
            guardrails=None,
        )

        # When guardrails is None, return everything as-is
        self.assertEqual(safe_formatted, formatted_prompts)
        self.assertEqual(safe_user, user_prompts)
        self.assertEqual(safe_queues, response_queues)
        self.assertEqual(safe_metrics, latency_metrics)
        self.assertEqual(preprocessing_time, 0)

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_filter_with_guardrails_malicious_prompts(self, mock_logging, mock_guardrails_class):
        """Guardrails path should send malicious responses and keep safe ones."""
        # Mock logger to avoid AttributeError
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
        )
        # Set the logger attribute that filter_with_guardrails expects
        worker.log = mock_logger

        # Prepare inputs: 2 prompts, second is malicious
        formatted_prompts = ["<user>Hello</user>", "<user>Bad</user>"]
        user_prompts = ["Hello", "Bad"]
        q_safe = mp.Queue()
        q_mal = mp.Queue()
        response_queues = [q_safe, q_mal]
        latency_metrics = [(1.0, 0.1, 0.05), (1.1, 0.2, 0.06)]

        # Configure mock guardrails
        mock_guardrails = mock_guardrails_class.return_value
        mock_guardrails.filter_batch.return_value = (
            ["Hello"],  # safe_user_prompts
            ["<user>Hello</user>"],  # safe_formatted_prompts
            [q_safe],  # safe_response_queues
            [latency_metrics[0]],  # safe_latency_metrics
            [1],  # malicious_indices -> index 1 is malicious
            0.01,  # preprocessing_time
        )
        error_msg = "Your input has been categorized as malicious."
        mock_guardrails.get_malicious_response.return_value = error_msg

        (
            safe_formatted,
            safe_user,
            safe_queues,
            safe_metrics,
            preprocessing_time,
        ) = worker.filter_with_guardrails(
            formatted_prompts=formatted_prompts,
            user_prompts=user_prompts,
            response_queues=response_queues,
            latency_metrics=latency_metrics,
            guardrails=mock_guardrails,
        )

        # Safe outputs are passed through from guardrails
        self.assertEqual(safe_user, ["Hello"])
        self.assertEqual(safe_formatted, ["<user>Hello</user>"])
        self.assertEqual(safe_queues, [q_safe])
        self.assertEqual(safe_metrics, [latency_metrics[0]])
        self.assertEqual(preprocessing_time, 0.01)

        # Malicious prompt should have received an error response on its queue
        result = q_mal.get(timeout=1)
        self.assertEqual(result["user"], "Bad")
        self.assertEqual(result["assistant"], error_msg)


class TestInferenceWorkerEntryPoints(TestCase):
    """Test cases for InferenceWorker entry points."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=False)
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch.object(InferenceWorker, "run_pre_processing_module")
    def test_preprocessing_entry_point(self, mock_run_preprocessing):
        """Test preprocessing_entry_point creates worker and calls run method."""
        inference_worker_args = {
            "end_event": self.end_event,
            "model_config": self.model_config,
            "batching_config": self.batching_config,
            "guardrails_config": self.guardrails_config,
            "dynamic_worker_config": self.dynamic_worker_config,
            "dt": self.dt,
            "hostname": "test-host",
            "devices": [0],
            "head_cpu_pid": 1234,
            "inf_wrkr_id": 1,
        }

        InferenceWorker.preprocessing_entry_point(inference_worker_args)

        # Verify run_pre_processing_module was called
        mock_run_preprocessing.assert_called_once()

    @patch.object(InferenceWorker, "run_llm_inference_module")
    def test_llm_inference_entry_point(self, mock_run_llm):
        """Test llm_inference_entry_point creates worker and calls run method."""
        inference_worker_args = {
            "end_event": self.end_event,
            "model_config": self.model_config,
            "batching_config": self.batching_config,
            "guardrails_config": self.guardrails_config,
            "dynamic_worker_config": self.dynamic_worker_config,
            "dt": self.dt,
            "hostname": "test-host",
            "devices": [0],
            "head_cpu_pid": 1234,
            "inf_wrkr_id": 1,
            "master_port": "29500",
        }

        InferenceWorker.llm_inference_entry_point(inference_worker_args)

        # Verify run_llm_inference_module was called
        mock_run_llm.assert_called_once()


class TestInferenceWorkerSendResponses(TestCase):
    """Test cases for _send_responses method."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=True, batch_type="dynamic")
        self.guardrails_config = GuardrailsConfig(enabled=True)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_send_responses_single(self, mock_logging):
        """Test _send_responses with single response."""
        # Mock logger to avoid AttributeError
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
        )

        worker.log = mock_logger

        queue = mp.Queue()
        user_prompts = ["Hello"]
        responses = ["Hi there!"]
        qs = [queue]
        preprocessing_time = 0.05
        tuple_latency_timestamps = [(100.0, 0.01, 0.02)]
        model_network_latency = 0.03
        metrics = {
            "inference_time": 0.5,
            "requests_per_second": 2.0,
            "total_tokens_per_second": 100.0,
            "output_tokens_per_second": 50.0,
        }

        worker._send_responses(
            user_prompts,
            responses,
            qs,
            preprocessing_time,
            tuple_latency_timestamps,
            model_network_latency,
            metrics,
        )

        # Verify queue received the response
        result = queue.get(timeout=1)
        self.assertEqual(result["hostname"], "test-host")
        self.assertEqual(result["inf_worker_id"], 1)
        self.assertEqual(result["devices"], [0])
        self.assertEqual(result["batch_size"], 1)
        self.assertEqual(result["user"], "Hello")
        self.assertEqual(result["assistant"], "Hi there!")
        self.assertEqual(result["guardrails_inference_latency"], preprocessing_time)
        self.assertEqual(result["model_inference_latency"], 0.5)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_send_responses_batch(self, mock_logging):
        """Test _send_responses with batch of responses."""
        # Mock logger to avoid AttributeError
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0, 1],
            inf_wrkr_id=2,
        )

        worker.log = mock_logger

        queue_1 = mp.Queue()
        queue_2 = mp.Queue()
        queue_3 = mp.Queue()

        user_prompts = ["Hello", "How are you?", "Goodbye"]
        responses = ["Hi!", "I'm fine!", "Bye!"]
        qs = [queue_1, queue_2, queue_3]
        preprocessing_time = 0.1
        tuple_latency_timestamps = [
            (100.0, 0.01, 0.02),
            (100.1, 0.01, 0.02),
            (100.2, 0.01, 0.02),
        ]
        model_network_latency = 0.05
        metrics = {
            "inference_time": 1.0,
            "requests_per_second": 3.0,
            "total_tokens_per_second": 150.0,
            "output_tokens_per_second": 75.0,
        }

        worker._send_responses(
            user_prompts,
            responses,
            qs,
            preprocessing_time,
            tuple_latency_timestamps,
            model_network_latency,
            metrics,
        )

        # Verify each queue received its response
        result_1 = queue_1.get(timeout=1)
        self.assertEqual(result_1["batch_size"], 3)
        self.assertEqual(result_1["user"], "Hello")
        self.assertEqual(result_1["assistant"], "Hi!")

        result_2 = queue_2.get(timeout=1)
        self.assertEqual(result_2["user"], "How are you?")
        self.assertEqual(result_2["assistant"], "I'm fine!")

        result_3 = queue_3.get(timeout=1)
        self.assertEqual(result_3["user"], "Goodbye")
        self.assertEqual(result_3["assistant"], "Bye!")

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_send_responses_records_telemetry(self, mock_logging):
        """Test that _send_responses records telemetry data."""
        # Mock logger to avoid AttributeError
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
        )

        worker.log = mock_logger

        queue = mp.Queue()
        user_prompts = ["Test"]
        responses = ["Response"]
        qs = [queue]
        preprocessing_time = 0.05
        tuple_latency_timestamps = [(100.0, 0.01, 0.02)]
        model_network_latency = 0.03
        metrics = {
            "inference_time": 0.5,
            "requests_per_second": 2.0,
            "total_tokens_per_second": 100.0,
            "output_tokens_per_second": 50.0,
        }

        worker._send_responses(
            user_prompts,
            responses,
            qs,
            preprocessing_time,
            tuple_latency_timestamps,
            model_network_latency,
            metrics,
        )

        # Verify telemetry data was recorded
        self.dt.assert_any_call("cpu_head_network_latency", 0.01)
        self.dt.assert_any_call("guardrails_inference_latency", 0.05)
        self.dt.assert_any_call("model_inference_latency", 0.5)
        self.dt.assert_any_call("model_network_latency", 0.03)
        self.dt.assert_any_call("requests_per_second", 2.0)
        self.dt.assert_any_call("total_tokens_per_second", 100.0)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_send_responses_with_fallback_metrics(self, mock_logging):
        """_send_responses should handle dicts missing some metric keys."""
        # Mock logger to avoid AttributeError
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
        )

        worker.log = mock_logger

        queue = mp.Queue()
        user_prompts = ["Hello"]
        responses = ["Error"]
        qs = [queue]
        preprocessing_time = 0.0
        tuple_latency_timestamps = [(100.0, 0.01, 0.02)]
        model_network_latency = 0.0
        # Simulate fallback metrics from error path
        fallback_metrics = {
            "inference_time": 0.0,
            "requests_per_second": 0.0,
            # intentionally omit total_tokens_per_second and output_tokens_per_second
        }

        worker._send_responses(
            user_prompts,
            responses,
            qs,
            preprocessing_time,
            tuple_latency_timestamps,
            model_network_latency,
            fallback_metrics,
        )

        result = queue.get(timeout=1)
        self.assertEqual(result["user"], "Hello")
        self.assertEqual(result["assistant"], "Error")
        self.assertEqual(result["model_inference_latency"], 0.0)


class TestInferenceWorkerMaliciousResponse(TestCase):
    """Test cases for _send_malicious_response method."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=True)
        self.guardrails_config = GuardrailsConfig(enabled=True)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_send_malicious_response(self):
        """Test _send_malicious_response sends correct error response."""
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
        )

        queue = mp.Queue()
        user_prompt = "Malicious prompt"
        latency_metric = (100.0, 0.01, 0.02)
        preprocessing_time = 0.05
        error_response = "Your input has been categorized as malicious."

        worker._send_malicious_response(
            user_prompt=user_prompt,
            response_queue=queue,
            latency_metric=latency_metric,
            preprocessing_time=preprocessing_time,
            error_response=error_response,
        )

        # Verify queue received the response
        result = queue.get(timeout=1)
        self.assertEqual(result["user"], user_prompt)
        self.assertEqual(result["assistant"], error_response)
        self.assertEqual(result["model_inference_latency"], 0)
        self.assertEqual(result["model_network_latency"], 0)
        self.assertEqual(result["batch_size"], 1)


class TestInferenceWorkerForwardToLLM(TestCase):
    """Test cases for _forward_to_llm method."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=True)
        self.guardrails_config = GuardrailsConfig(enabled=True)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_forward_to_llm_with_prompts(self):
        """Test _forward_to_llm forwards prompts to output queue."""
        output_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            preprocessing_output_queue=output_queue,
        )

        formatted_prompts = ["<user>Hello</user>"]
        user_prompts = ["Hello"]
        response_queues = [mp.Queue()]
        latency_metrics = [(1.0, 0.1, 0.05)]
        preprocessing_time = 0.05

        worker._forward_to_llm(
            formatted_prompts,
            user_prompts,
            response_queues,
            latency_metrics,
            preprocessing_time,
        )

        # Verify output queue received the data as a tuple
        # Format: (user_prompts, formatted_prompts, response_queues, latency_metrics, preprocessing_time, timestamp)
        result = output_queue.get(timeout=1)
        self.assertEqual(result[0], user_prompts)
        self.assertEqual(result[1], formatted_prompts)
        # Check response_queues: verify it's a list with 1 queue (Dragon queues may not preserve identity)
        self.assertIsInstance(result[2], list)
        self.assertEqual(len(result[2]), 1)
        self.assertIsInstance(result[2][0], mp.Queue().__class__)
        self.assertEqual(result[3], latency_metrics)
        self.assertEqual(result[4], preprocessing_time)
        # result[5] is the timestamp - just verify it exists
        self.assertIsInstance(result[5], float)

    def test_forward_to_llm_empty_prompts(self):
        """Test _forward_to_llm does nothing with empty prompts."""
        output_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            preprocessing_output_queue=output_queue,
        )

        worker._forward_to_llm(
            formatted_prompts=[],
            user_prompts=[],
            response_queues=[],
            latency_metrics=[],
            preprocessing_time=0,
        )

        # Verify output queue is empty (no data sent)
        self.assertTrue(output_queue.empty())


class TestInferenceWorkerLLMErrorPath(TestCase):
    """Tests for error-handling path in run_llm_inference_module."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=False)
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.inference_worker_utils.LLMInferenceEngine")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_send_responses_calls_error_handling(self, mock_logging, mock_engine_cls):
        """Test that _send_responses handles error metrics correctly."""
        # Mock the logger to avoid Dragon logging issues
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            head_cpu_pid=1234,
            inf_wrkr_id=1,
        )

        worker.log = mock_logger

        # Test the _send_responses method directly with error scenario
        response_queue = mp.Queue()
        user_prompts = ["Hello"]
        error_responses = ["LLM inference failed. Please try again later."]
        qs = [response_queue]
        preprocessing_time = 0.0
        latency_metric = (time.time(), 0.01, 0.02)
        model_network_latency = 0.0
        fallback_metrics = {
            "inference_time": 0.0,
            "requests_per_second": 0.0,
            "total_tokens_per_second": 0.0,
            "output_tokens_per_second": 0.0,
        }

        # Call _send_responses with error data
        worker._send_responses(
            user_prompts,
            error_responses,
            qs,
            preprocessing_time,
            [latency_metric],
            model_network_latency,
            fallback_metrics,
        )

        # Verify error response was enqueued
        result = response_queue.get(timeout=2)
        self.assertEqual(result["user"], "Hello")
        self.assertIn("failed", result["assistant"].lower())
        self.assertEqual(result["model_inference_latency"], 0.0)
        self.assertEqual(result["requests_per_second"], 0.0)


if __name__ == "__main__":
    main()
