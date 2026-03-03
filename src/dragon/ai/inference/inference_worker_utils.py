import dragon
import multiprocessing as mp

import time
from queue import Empty
from typing import Optional
from .config import (
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)
from .guardrails import GuardrailsProcessor
from .batching import DynamicBatcher
from .llm_engine import LLMInferenceEngine

# Logging imports
import logging
from ...dlogging.util import setup_BE_logging, DragonLoggingServices as dls

import socket
from ...infrastructure.parameters import this_process


def setup_logging(type: str) -> logging.Logger:
    """Auxiliary function to initialize and return a logger.

    :param type: Custom identifier for the log file.
    :type type: str
    :returns: Configured logger instance.
    :rtype: logging.Logger
    """
    fname = f"INFERENCE_INFWORKER_{socket.gethostname()}_client_{str(this_process.my_puid)}_{str(type)}.log"
    setup_BE_logging(service=dls.PG, fname=fname)
    return logging.getLogger(str(dls.PG))


class InferenceWorker:
    """The Inference Worker class orchestrates the pre-processing module &
    the main LLM inference module that is tensor-parallelized with batch-processing.

    Architecture:
    1. Batching Module (DynamicBatcher): Batching logic
    2. Guardrails Module (GuardrailsProcessor): Optional safety filtering
    3. LLM Inference Module: vLLM-based inference
    """

    @staticmethod
    def preprocessing_entry_point(inference_worker_args):
        """Entry point for the preprocessing worker process.

        :param inference_worker_args: Arguments for initializing the
            :class:`InferenceWorker`, including runtime parameters such as
            hostname, devices and queues.
        :type inference_worker_args: dict
        """
        inference_worker = InferenceWorker(**inference_worker_args)
        inference_worker.run_pre_processing_module()

    @staticmethod
    def llm_inference_entry_point(inference_worker_args):
        """Entry point for the LLM inference worker process.

        :param inference_worker_args: Arguments for initializing the
            :class:`InferenceWorker`, including runtime parameters such as
            hostname, devices and queues.
        :type inference_worker_args: dict
        """
        inference_worker = InferenceWorker(**inference_worker_args)
        inference_worker.run_llm_inference_module()

    def __init__(
        self,
        end_event,
        model_config: ModelConfig,
        batching_config: BatchingConfig,
        guardrails_config: GuardrailsConfig,
        dynamic_worker_config: DynamicWorkerConfig,
        dt,
        # Runtime parameters for preprocessing module
        hostname: str = None,
        devices: list = None,
        head_cpu_pid: int = None,
        inf_wrkr_id: int = None,
        preprocessing_input_queue=None,
        preprocessing_output_queue=None,
        inf_wrkr_barrier=None,
        llm_proc_end_ev=None,
        # Additional runtime parameters for LLM module
        master_port: str = None,
        inf_wrkr_down_ev=None,
        inf_wrkr_manager_q=None,
    ) -> None:
        """Initialize an inference worker instance.

        :param end_event: Primary event that terminates all processes.
        :type end_event: mp.Event
        :param model_config: Model configuration.
        :type model_config: ModelConfig
        :param batching_config: Batching configuration.
        :type batching_config: BatchingConfig
        :param guardrails_config: Guardrails/safety configuration.
        :type guardrails_config: GuardrailsConfig
        :param dynamic_worker_config: Dynamic worker configuration.
        :type dynamic_worker_config: DynamicWorkerConfig
        :param dt: Dragon telemetry object.
        :type dt: dragon.telemetry.telemetry.Telemetry
        :param hostname: Current process hostname.
        :type hostname: str
        :param devices: List of GPU ranks for the current inference worker.
        :type devices: list[int]
        :param head_cpu_pid: Head CPU worker PID for the current inference
            worker.
        :type head_cpu_pid: int
        :param inf_wrkr_id: Unique identifier for the current inference
            worker.
        :type inf_wrkr_id: int
        :param preprocessing_input_queue: Input queue for the preprocessing
            worker.
        :type preprocessing_input_queue: mp.Queue
        :param preprocessing_output_queue: Output queue for the preprocessing
            worker.
        :type preprocessing_output_queue: mp.Queue
        :param inf_wrkr_barrier: Barrier used to wait until all inference
            worker modules are ready.
        :type inf_wrkr_barrier: mp.Barrier
        :param llm_proc_end_ev: Event used to denote that the LLM module
            should spin down.
        :type llm_proc_end_ev: mp.Event
        :param master_port: Master port assigned to the inference worker.
        :type master_port: str
        :param inf_wrkr_down_ev: Event used to denote that the entire
            inference worker should tear down.
        :type inf_wrkr_down_ev: mp.Event
        :param inf_wrkr_manager_q: Queue of tuples of the form
            ``(hostname, devices, master_port, inf_wrkr_id)``.
        :type inf_wrkr_manager_q: mp.Queue
        """

        self.end_event = end_event
        self.dt = dt

        # Store config objects
        self.model_config = model_config
        self.batching_config = batching_config
        self.guardrails_config = guardrails_config
        self.dynamic_worker_config = dynamic_worker_config

        # Store runtime parameters
        self.hostname = hostname
        self.devices = devices
        self.head_cpu_pid = head_cpu_pid
        self.inf_wrkr_id = inf_wrkr_id
        self.preprocessing_input_queue = preprocessing_input_queue
        self.preprocessing_output_queue = preprocessing_output_queue
        self.inf_wrkr_barrier = inf_wrkr_barrier
        self.llm_proc_end_ev = llm_proc_end_ev
        self.master_port = master_port
        self.inf_wrkr_down_ev = inf_wrkr_down_ev
        self.inf_wrkr_manager_q = inf_wrkr_manager_q

        # Extract frequently used model config values
        self.model_name = model_config.model_name
        self.dtype = model_config.dtype
        self.hf_token = model_config.hf_token
        self.kv_cache_max_tokens = model_config.max_tokens
        self.max_new_tokens = model_config.max_tokens
        self.tp_size = model_config.tp_size
        self.padding_side = model_config.padding_side
        self.truncation_side = model_config.truncation_side
        self.top_k = model_config.top_k
        self.top_p = model_config.top_p

        # Extract batching config values
        self.batch_toggle = batching_config.enabled
        self.batch_wait_time = batching_config.batch_wait_seconds
        self.batch_limit_max = batching_config.max_batch_size
        self.batch_type = batching_config.batch_type

        # Extract guardrails config values
        self.prompt_guard_toggle = guardrails_config.enabled
        self.prompt_guard_sensitivity = guardrails_config.prompt_guard_sensitivity
        self.prompt_guard_model = guardrails_config.prompt_guard_model

        # Extract dynamic worker config values
        self.dynamic_inf_wrkr_toggle = dynamic_worker_config.enabled
        self.spin_down_threshold = dynamic_worker_config.spin_down_threshold_seconds
        self.min_active_inf_workers_per_cpu_head = dynamic_worker_config.min_active_workers_per_cpu

    def run_pre_processing_module(self):
        """The pre-processing module performs batching and optional guardrails filtering.

        Architecture:
        1. Batching: Collect prompts into batches (DynamicBatcher)
        2. Guardrails (optional): Filter malicious prompts (GuardrailsProcessor)
        3. Forward to LLM: Send safe batches to LLM inference module

        Uses instance attributes set in __init__:
            self.hostname, self.devices, self.preprocessing_input_queue,
            self.preprocessing_output_queue, self.head_cpu_pid, self.inf_wrkr_barrier,
            self.llm_proc_end_ev, self.inf_wrkr_id
        """
        # Re-initialize mp dragon within process.
        mp.set_start_method("dragon")

        # Initialize logger
        self.log = setup_logging(type=f"cpu-pid_{self.head_cpu_pid}_part2_inf_wrkr_{self.inf_wrkr_id}_pre-proc")
        self.log.info("Pre-Proc Module: Logger initialized")

        # Initialize DynamicBatcher - only if batching enabled
        batcher = None
        if self.batch_toggle and self.batch_type == "dynamic":
            batcher = DynamicBatcher(
                batch_wait_seconds=self.batch_wait_time,
                max_batch_size=self.batch_limit_max,
                enabled=True,
            )
            self.log.info("Pre-Proc Module: DynamicBatcher initialized")

        # Initialize GuardrailsProcessor
        guardrails = None
        if self.prompt_guard_toggle:
            guardrails = GuardrailsProcessor(self.guardrails_config, self.hf_token)
            self.log.info("Pre-Proc Module: GuardrailsProcessor initialized")
        else:
            self.log.info("Pre-Proc Module: GuardrailsProcessor disabled - skipping initialization")

        # Wait till all inf-worker modules are up and running
        self.inf_wrkr_barrier.wait()

        # Route to appropriate processing mode
        if not self.batch_toggle:
            # No batching - process single prompts with optional guardrails
            self.process_single_prompts(guardrails)
        elif self.batch_type == "dynamic":
            # Dynamic batching with optional guardrails
            self.process_with_batching(batcher, guardrails)
        elif self.batch_type == "pre-batch":
            # Pre-batched inputs with optional guardrails
            self.process_prebatched(guardrails)

    def _should_spin_down(self, baseline_start_time: float) -> bool:
        """Return True if this worker should spin down based on idle time.

        :param baseline_start_time: Timestamp when the worker last processed
            a prompt.
        :type baseline_start_time: float
        :returns: True if the worker should spin down, False otherwise.
        :rtype: bool
        """
        if not self.dynamic_inf_wrkr_toggle:
            return False

        if self.inf_wrkr_id <= self.min_active_inf_workers_per_cpu_head:
            return False

        return (time.time() - baseline_start_time) > self.spin_down_threshold

    def _update_guardrails_latency(self, tuple_latency_metric: tuple, guardrails_enabled: bool = True) -> tuple:
        """Update guardrails latency and return a new latency metrics tuple.

        Input tuple format: (input_entry_timestamp, cpu_head_network_latency,
        guardrails_start_timestamp).
        Output tuple format: (input_entry_timestamp, cpu_head_network_latency,
        guardrails_network_latency).

        :param tuple_latency_metric: Original latency metrics tuple.
        :type tuple_latency_metric: tuple
        :param guardrails_enabled: Whether guardrails processing is enabled.
        :type guardrails_enabled: bool
        :returns: Updated latency metrics tuple with guardrails network latency.
        :rtype: tuple
        """
        input_entry_timestamp = tuple_latency_metric[0]
        cpu_head_network_latency = tuple_latency_metric[1]
        # Only calculate guardrails network latency if guardrails is enabled
        if guardrails_enabled:
            guardrails_network_latency = round(time.time() - tuple_latency_metric[2], 2)
        else:
            guardrails_network_latency = 0
        return (
            input_entry_timestamp,
            cpu_head_network_latency,
            guardrails_network_latency,
        )

    def _flush_and_shutdown(
        self,
        batcher: Optional[DynamicBatcher],
        guardrails: Optional["GuardrailsProcessor"],
    ) -> None:
        """Flush any remaining batch (if any) and log shutdown.

        :param batcher: DynamicBatcher instance.
        :type batcher: DynamicBatcher or None
        :param guardrails: GuardrailsProcessor instance.
        :type guardrails: GuardrailsProcessor or None
        """
        if batcher is not None:
            final_batch = batcher.flush_batch()
            if final_batch:
                # Update guardrails network latency just before guardrails processing
                updated_latency_metrics = [
                    self._update_guardrails_latency(lm, guardrails_enabled=self.prompt_guard_toggle)
                    for lm in final_batch.latency_metrics
                ]
                self._guard_and_forward_batch(
                    formatted_prompts=list(final_batch.formatted_prompts),
                    user_prompts=list(final_batch.user_prompts),
                    response_queues=list(final_batch.response_queues),
                    latency_metrics=updated_latency_metrics,
                    guardrails=guardrails,
                )
        self.log.info("Pre-Proc Module: Shutting down pre-processing module")

    def _drain_and_process_single_prompts(self, guardrails) -> None:
        """Drain any remaining single prompts from the input queue.

        :param guardrails: Optional GuardrailsProcessor instance.
        :type guardrails: GuardrailsProcessor or None
        """
        while True:
            try:
                q_item = self.preprocessing_input_queue.get_nowait()
                user_prompt = q_item[0]
                formatted_prompt = q_item[1]
                response_queue = q_item[2]
                tuple_latency_metric = q_item[3]

                updated_latency_metric = self._update_guardrails_latency(
                    tuple_latency_metric, guardrails_enabled=self.prompt_guard_toggle
                )

                self._guard_and_forward_batch(
                    formatted_prompts=[formatted_prompt],
                    user_prompts=[user_prompt],
                    response_queues=[response_queue],
                    latency_metrics=[updated_latency_metric],
                    guardrails=guardrails,
                )
                self.log.info("Pre-Proc Module: Processed remaining prompt during shutdown")
            except Empty:
                break
            except Exception as e:
                self.log.error(f"Pre-Proc Module: Exception during queue drain {e=}")
                break

    def process_single_prompts(self, guardrails):
        """Process individual prompts without batching (batch_size=1).

        Architecture:
        1. Read single prompt from input queue
        2. Optionally filter through GuardrailsProcessor
        3. Forward to LLM module immediately (no batching)

        :param guardrails: Optional GuardrailsProcessor instance.
        :type guardrails: GuardrailsProcessor or None
        """
        self.log.info("Pre-Proc Module: starting single-prompt processing loop")
        baseline_start_time = time.time()

        while True:
            # Check for shutdown
            if self.end_event.is_set():
                # Drain any remaining items from the queue before shutting down
                self._drain_and_process_single_prompts(guardrails)
                self.log.info("Pre-Proc Module: Shutting down pre-processing module")
                break

            # Check for spin-down
            if self._should_spin_down(baseline_start_time):
                # Drain any remaining items from the queue before spinning down
                self._drain_and_process_single_prompts(guardrails)
                self.llm_proc_end_ev.set()
                self.log.info(f"Pre-Proc Module: Value of {self.llm_proc_end_ev.is_set()=}")
                self.log.info("Pre-Proc Module: Shutting down pre-processing module")
                break

            try:
                # Get single prompt from input queue
                q_item = self.preprocessing_input_queue.get(timeout=1)

                # Unpack: (user_prompt, formatted_prompt, response_queue, latency_metrics)
                user_prompt = q_item[0]
                formatted_prompt = q_item[1]
                response_queue = q_item[2]
                tuple_latency_metric = q_item[3]

                # Update guardrails network latency (only if guardrails enabled)
                updated_latency_metric = self._update_guardrails_latency(
                    tuple_latency_metric, guardrails_enabled=self.prompt_guard_toggle
                )

                # Apply guardrails (if enabled) and forward to LLM
                self._guard_and_forward_batch(
                    formatted_prompts=[formatted_prompt],
                    user_prompts=[user_prompt],
                    response_queues=[response_queue],
                    latency_metrics=[updated_latency_metric],
                    guardrails=guardrails,
                )

                # Reset idle time
                baseline_start_time = time.time()

            except Empty:
                pass
            except Exception as e:
                self.log.error(f"Pre-Proc Module: Exception caught {e=}")

    def _drain_and_process_prebatched(self, guardrails) -> None:
        """Drain any remaining pre-batched items from the input queue.

        :param guardrails: Optional GuardrailsProcessor instance.
        :type guardrails: GuardrailsProcessor or None
        """
        while True:
            try:
                q_item = self.preprocessing_input_queue.get_nowait()
                user_prompts = q_item[0]
                formatted_prompts = q_item[1]
                shared_queue = q_item[2]
                tuple_latency_metric = q_item[3]

                updated_latency_metric = self._update_guardrails_latency(
                    tuple_latency_metric, guardrails_enabled=self.prompt_guard_toggle
                )

                response_queues = [shared_queue for _ in user_prompts]
                latency_metrics = [updated_latency_metric for _ in user_prompts]

                self._guard_and_forward_batch(
                    formatted_prompts=formatted_prompts,
                    user_prompts=user_prompts,
                    response_queues=response_queues,
                    latency_metrics=latency_metrics,
                    guardrails=guardrails,
                )
                self.log.info("Pre-Proc Module: Processed remaining pre-batch during shutdown")
            except Empty:
                break
            except Exception as e:
                self.log.error(f"Pre-Proc Module: Exception during queue drain {e=}")
                break

    def process_prebatched(self, guardrails):
        """Process pre-batched inputs with optional guardrails filtering.

        Architecture:
        1. Receive already-batched inputs
        2. Optionally filter through GuardrailsProcessor
        3. Forward to LLM module

        :param guardrails: Optional GuardrailsProcessor instance.
        :type guardrails: GuardrailsProcessor or None
        """
        self.log.info("Pre-Proc Module: starting pre-batched processing loop")
        baseline_start_time = time.time()

        while True:
            # Check for shutdown
            if self.end_event.is_set():
                # Drain any remaining items from the queue before shutting down
                self._drain_and_process_prebatched(guardrails)
                self.log.info("Pre-Proc Module: Shutting down pre-processing module")
                break

            # If spin-down threshold is met, and if inf-wrkr is extra to the min defined inf-wrkrs, then spin-down extra inf-wrkr.
            if self._should_spin_down(baseline_start_time):
                # Drain any remaining items from the queue before spinning down
                self._drain_and_process_prebatched(guardrails)
                self.llm_proc_end_ev.set()
                self.log.info(f"Pre-Proc Module: Value of {self.llm_proc_end_ev.is_set()=}")
                self.log.info("Pre-Proc Module: Shutting down pre-processing module")
                break

            try:
                # Get pre-batched item from queue
                q_item = self.preprocessing_input_queue.get(timeout=1)

                # Extract pre-batch from queue item
                user_prompts = q_item[0]  # List of prompts
                formatted_prompts = q_item[1]  # List of formatted prompts
                shared_queue = q_item[2]  # Single shared queue for whole batch

                # Latency metrics - all prompts in pre-batch share same timestamp
                tuple_latency_metric = q_item[3]
                updated_latency_metric = self._update_guardrails_latency(
                    tuple_latency_metric, guardrails_enabled=self.prompt_guard_toggle
                )

                # In pre-batch mode, all prompts share the same response queue
                response_queues = [shared_queue for _ in user_prompts]
                latency_metrics = [updated_latency_metric for _ in user_prompts]

                # Apply guardrails (if enabled) and forward to LLM
                self._guard_and_forward_batch(
                    formatted_prompts=formatted_prompts,
                    user_prompts=user_prompts,
                    response_queues=response_queues,
                    latency_metrics=latency_metrics,
                    guardrails=guardrails,
                )

                # Reset idle time when processing input
                baseline_start_time = time.time()

            except Empty:
                pass
            except Exception as e:
                self.log.error(f"Pre-Proc Module: Exception caught {e=}")

    def process_with_batching(self, batcher, guardrails):
        """Process inputs with dynamic batching and optional guardrails filtering.

        Architecture:
        1. Read from input queue
        2. Add to DynamicBatcher produces Batch when ready
        3. Optionally filter Batch through GuardrailsProcessor
        4. Forward to LLM module

        :param batcher: DynamicBatcher instance.
        :type batcher: DynamicBatcher
        :param guardrails: Optional GuardrailsProcessor instance.
        :type guardrails: GuardrailsProcessor or None
        """
        self.log.info("Pre-Proc Module: starting dynamic batching processing loop")
        baseline_start_time = time.time()

        while True:
            # Check for shutdown
            if self.end_event.is_set():
                # Flush any remaining batch before shutting down
                self._flush_and_shutdown(batcher, guardrails)
                break

            # Check for spin-down
            if self._should_spin_down(baseline_start_time):
                # Flush any remaining batch before spinning down
                self._flush_and_shutdown(batcher, guardrails)
                self.llm_proc_end_ev.set()
                self.log.info(f"Pre-Proc Module: Value of {self.llm_proc_end_ev.is_set()=}")
                break

            try:
                # Get item from input queue
                q_item = self.preprocessing_input_queue.get(timeout=0.1)

                # Unpack item: (user_prompt, formatted_prompt, response_queue, latency_metrics)
                user_prompt = q_item[0]
                formatted_prompt = q_item[1]
                response_queue = q_item[2]
                latency_metrics = q_item[3]

                # Add item to batcher
                batch = batcher.add_item(
                    user_prompt=user_prompt,
                    formatted_prompt=formatted_prompt,
                    response_queue=response_queue,
                    latency_metrics=latency_metrics,
                )

                # If batch is ready, process it
                if batch:
                    # Update guardrails network latency just before guardrails processing
                    updated_latency_metrics = [
                        self._update_guardrails_latency(lm, guardrails_enabled=self.prompt_guard_toggle)
                        for lm in batch.latency_metrics
                    ]
                    self._guard_and_forward_batch(
                        formatted_prompts=list(batch.formatted_prompts),
                        user_prompts=list(batch.user_prompts),
                        response_queues=list(batch.response_queues),
                        latency_metrics=updated_latency_metrics,
                        guardrails=guardrails,
                    )

                # Reset idle time when processing input
                baseline_start_time = time.time()

            except Empty:
                # Check if we should flush batch based on time
                if batcher.should_check_batch():
                    batch = batcher.flush_batch()
                    if batch:
                        # Update guardrails network latency just before guardrails processing
                        updated_latency_metrics = [
                            self._update_guardrails_latency(lm, guardrails_enabled=self.prompt_guard_toggle)
                            for lm in batch.latency_metrics
                        ]
                        self._guard_and_forward_batch(
                            formatted_prompts=list(batch.formatted_prompts),
                            user_prompts=list(batch.user_prompts),
                            response_queues=list(batch.response_queues),
                            latency_metrics=updated_latency_metrics,
                            guardrails=guardrails,
                        )
            except Exception as e:
                self.log.error(f"Pre-Proc Module: Exception caught {e=}")

    def filter_with_guardrails(
        self,
        formatted_prompts: list,
        user_prompts: list,
        response_queues: list,
        latency_metrics: list,
        guardrails: Optional[GuardrailsProcessor],
    ):
        """Apply guardrails filtering and return safe prompts and metrics.

        :param formatted_prompts: List of formatted prompts for the LLM.
        :type formatted_prompts: list
        :param user_prompts: List of original user prompts.
        :type user_prompts: list
        :param response_queues: List of response queues for each prompt.
        :type response_queues: list
        :param latency_metrics: List of latency metrics for each prompt.
        :type latency_metrics: list
        :param guardrails: GuardrailsProcessor instance, or ``None`` if
            guardrails are disabled.
        :type guardrails: GuardrailsProcessor or None
        :returns: Tuple ``(safe_formatted_prompts, safe_user_prompts,
            safe_response_queues, safe_latency_metrics, preprocessing_time)``.
        :rtype: tuple
        """
        # If guardrails disabled, return everything as-is
        if guardrails is None:
            return (
                formatted_prompts,
                user_prompts,
                response_queues,
                latency_metrics,
                0,
            )

        # Apply guardrails filtering
        self.log.info("Pre-Proc Module: applying guardrails filtering")
        (
            safe_user_prompts,
            safe_formatted_prompts,
            safe_response_queues,
            safe_latency_metrics,
            malicious_indices,
            preprocessing_time,
        ) = guardrails.filter_batch(
            prompts=user_prompts,
            formatted_prompts=formatted_prompts,
            response_queues=response_queues,
            latency_metrics=latency_metrics,
        )

        # Handle malicious prompts - send error responses
        if malicious_indices:
            error_response = guardrails.get_malicious_response()
            for idx in malicious_indices:
                self._send_malicious_response(
                    user_prompt=user_prompts[idx],
                    response_queue=response_queues[idx],
                    latency_metric=latency_metrics[idx],
                    preprocessing_time=preprocessing_time,
                    error_response=error_response,
                )

        return (
            safe_formatted_prompts,
            safe_user_prompts,
            safe_response_queues,
            safe_latency_metrics,
            preprocessing_time,
        )

    def _send_malicious_response(
        self,
        user_prompt: str,
        response_queue: mp.Queue,
        latency_metric: tuple,
        preprocessing_time: float,
        error_response: str,
    ):
        """Send an error response for a malicious prompt.

        :param user_prompt: Original user prompt.
        :type user_prompt: str
        :param response_queue: Queue used to send the response.
        :type response_queue: mp.Queue
        :param latency_metric: Latency metrics for the prompt.
        :type latency_metric: tuple
        :param preprocessing_time: Time taken for preprocessing.
        :type preprocessing_time: float
        :param error_response: Error message to send for a malicious prompt.
        :type error_response: str
        """
        input_entry_timestamp = latency_metric[0]
        cpu_head_network_latency = latency_metric[1]
        guardrails_network_latency = latency_metric[2]
        end_to_end_latency = round(time.time() - input_entry_timestamp, 2)

        # Record metrics
        self.dt.add_data("cpu_head_network_latency", cpu_head_network_latency)
        self.dt.add_data("guardrails_inference_latency", preprocessing_time)
        self.dt.add_data("guardrails_network_latency", guardrails_network_latency)
        self.dt.add_data("model_inference_latency", 0)
        self.dt.add_data("model_network_latency", 0)
        self.dt.add_data("end_to_end_latency", end_to_end_latency)
        self.dt.add_data("requests_per_second", 0)
        self.dt.add_data("total_tokens_per_second", 0)
        self.dt.add_data("total_output_tokens_per_second", 0)

        # Send error response
        response_queue.put(
            {
                "hostname": self.hostname,
                "inf_worker_id": self.inf_wrkr_id,
                "devices": self.devices,
                "batch_size": 1,
                "cpu_head_network_latency": cpu_head_network_latency,
                "guardrails_inference_latency": preprocessing_time,
                "guardrails_network_latency": guardrails_network_latency,
                "model_inference_latency": 0,
                "model_network_latency": 0,
                "end_to_end_latency": end_to_end_latency,
                "requests_per_second": 0,
                "total_tokens_per_second": 0,
                "total_output_tokens_per_second": 0,
                "user": user_prompt,
                "assistant": error_response,
            }
        )

    def _forward_to_llm(
        self,
        formatted_prompts: list,
        user_prompts: list,
        response_queues: list,
        latency_metrics: list,
        preprocessing_time: float,
    ):
        """Forward prompts to the LLM module.

        :param formatted_prompts: List of formatted prompts for the LLM.
        :type formatted_prompts: list
        :param user_prompts: List of original user prompts.
        :type user_prompts: list
        :param response_queues: List of response queues for each prompt.
        :type response_queues: list
        :param latency_metrics: List of latency metrics for each prompt.
        :type latency_metrics: list
        :param preprocessing_time: Time taken for preprocessing.
        :type preprocessing_time: float
        """
        if len(formatted_prompts) > 0:
            self.preprocessing_output_queue.put(
                (
                    user_prompts,
                    formatted_prompts,
                    response_queues,
                    latency_metrics,
                    preprocessing_time,
                    time.time(),
                )
            )

    def _guard_and_forward_batch(
        self,
        formatted_prompts: list,
        user_prompts: list,
        response_queues: list,
        latency_metrics: list,
        guardrails: Optional[GuardrailsProcessor],
    ):
        """Apply guardrails (if enabled) and forward batch to the LLM.

        :param formatted_prompts: List of formatted prompts for the LLM.
        :type formatted_prompts: list
        :param user_prompts: List of original user prompts.
        :type user_prompts: list
        :param response_queues: List of response queues for each prompt.
        :type response_queues: list
        :param latency_metrics: List of latency metrics for each prompt.
        :type latency_metrics: list
        :param guardrails: Optional GuardrailsProcessor instance.
        :type guardrails: GuardrailsProcessor or None
        """
        if self.prompt_guard_toggle:
            (
                safe_formatted_prompts,
                safe_user_prompts,
                safe_response_queues,
                safe_latency_metrics,
                preprocessing_time,
            ) = self.filter_with_guardrails(
                formatted_prompts=formatted_prompts,
                user_prompts=user_prompts,
                response_queues=response_queues,
                latency_metrics=latency_metrics,
                guardrails=guardrails,
            )
        else:
            safe_formatted_prompts = formatted_prompts
            safe_user_prompts = user_prompts
            safe_response_queues = response_queues
            safe_latency_metrics = latency_metrics
            preprocessing_time = 0

        self._forward_to_llm(
            formatted_prompts=safe_formatted_prompts,
            user_prompts=safe_user_prompts,
            response_queues=safe_response_queues,
            latency_metrics=safe_latency_metrics,
            preprocessing_time=preprocessing_time,
        )

    def run_llm_inference_module(self):
        """The LLM inference module orchestrates GPUs in a tensor-parallel environment
        to perform batch inference using vLLM.

        Uses instance attributes set in __init__:
            self.hostname, self.head_cpu_pid, self.devices, self.master_port,
            self.preprocessing_input_queue (used as read_from_queue), self.inf_wrkr_barrier,
            self.llm_proc_end_ev, self.inf_wrkr_down_ev, self.inf_wrkr_manager_q, self.inf_wrkr_id
        """
        # Re-initialize mp dragon within process.
        mp.set_start_method("dragon")

        # Initialize logger
        self.log = setup_logging(type=f"cpu-pid_{self.head_cpu_pid}_part3_inf_wrkr_{self.inf_wrkr_id}_llm_module")

        self.log.info("LLM Module: Logger initialized")

        # Initialize LLM Inference Engine
        llm_engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname=self.hostname,
            devices=self.devices,
            master_port=self.master_port,
        )
        llm_engine.initialize()
        self.log.info("LLM Module: LLMInferenceEngine initialized")

        # Wait till all inf-worker modules are up and running
        self.inf_wrkr_barrier.wait()

        # Determine if preprocessing worker was created
        preprocessing_needed = self.prompt_guard_toggle or (self.batch_toggle and self.batch_type == "dynamic")
        read_from_queue = self.preprocessing_output_queue if preprocessing_needed else self.preprocessing_input_queue

        while True:
            try:
                # Get batch from preprocessing queue
                q_item = read_from_queue.get(timeout=1)

                if preprocessing_needed:
                    # Data comes from preprocessing module in batch format:
                    # (user_prompts, formatted_inputs, response_queues, latency_metrics, preprocessing_time, timestamp)
                    user_prompts = q_item[0]
                    formatted_inputs = q_item[1]
                    qs = q_item[2] if isinstance(q_item[2], list) else [q_item[2]] * len(user_prompts)
                    preprocessing_time = q_item[4] if self.prompt_guard_toggle else 0
                    tuple_latency_timestamps = (
                        q_item[3] if isinstance(q_item[3], list) else [q_item[3]] * len(formatted_inputs)
                    )
                    model_network_latency = round(time.time() - q_item[5], 2)
                else:
                    # Data comes directly from cpu_worker:
                    # (user_prompt(s), formatted_input(s), response_queue, tuple_latency_metric)
                    # Can be single-item (strings) or pre-batch (lists)
                    tuple_latency_metric = q_item[3]
                    preprocessing_time = 0

                    # Check if data is in pre-batch format (lists) or single-item format (strings)
                    if isinstance(q_item[0], list):
                        # Pre-batch format: first two elements are lists
                        user_prompts = q_item[0]
                        formatted_inputs = q_item[1]
                        qs = [q_item[2]] * len(user_prompts)
                    else:
                        # Single-item format: first two elements are strings
                        user_prompts = [q_item[0]]
                        formatted_inputs = [q_item[1]]
                        qs = [q_item[2]]

                    # Update latency tuple: set guardrails_network_latency to 0 (guardrails disabled)
                    updated_latency_metric = (
                        tuple_latency_metric[0],  # input_entry_timestamp
                        tuple_latency_metric[1],  # cpu_head_network_latency
                        0,  # guardrails_network_latency = 0
                    )
                    tuple_latency_timestamps = [updated_latency_metric] * len(user_prompts)
                    model_network_latency = round(time.time() - tuple_latency_metric[2], 2)

                # Generate responses using LLM Engine
                try:
                    responses, metrics = llm_engine.generate(formatted_inputs)
                    # Send responses back to the user with metrics
                    self._send_responses(
                        user_prompts,
                        responses,
                        qs,
                        preprocessing_time,
                        tuple_latency_timestamps,
                        model_network_latency,
                        metrics,
                    )
                except Exception as e:
                    self.log.error(f"LLM Module: Error during generate {e=}")
                    # On failure, send a structured error response for each prompt
                    fallback_metrics = {
                        "inference_time": 0.0,
                        "requests_per_second": 0.0,
                        "total_tokens_per_second": 0.0,
                        "output_tokens_per_second": 0.0,
                    }
                    error_message = "LLM inference failed. Please try again later."
                    self._send_responses(
                        user_prompts,
                        [error_message] * len(user_prompts),
                        qs,
                        preprocessing_time,
                        tuple_latency_timestamps,
                        model_network_latency,
                        fallback_metrics,
                    )

            except Empty:
                if self.end_event.is_set() or self.llm_proc_end_ev.is_set():
                    if self.llm_proc_end_ev.is_set():
                        self.log.info(
                            f"LLM Module: {self.llm_proc_end_ev.is_set()=} {self.hostname=} {self.devices=} {self.master_port=} {self.inf_wrkr_id=}"
                        )
                        self.inf_wrkr_manager_q.put(
                            (
                                self.hostname,
                                self.devices,
                                self.master_port,
                                self.inf_wrkr_id,
                            )
                        )
                        self.inf_wrkr_down_ev.set()

                    # Shutdown vLLM and clear GPU memory
                    llm_engine.shutdown()
                    break
                else:
                    continue
            except Exception as e:
                self.log.info(f"LLM Module: Exception caught {e=}")

    def _send_responses(
        self,
        user_prompts,
        responses,
        qs,
        preprocessing_time,
        tuple_latency_timestamps,
        model_network_latency,
        metrics,
    ):
        """Send LLM responses back to the user along with metrics.

        :param user_prompts: List of original user prompts.
        :type user_prompts: list
        :param responses: List of LLM-generated responses.
        :type responses: list
        :param qs: List of response queues for each prompt.
        :type qs: list[mp.Queue]
        :param preprocessing_time: Time taken by preprocessing.
        :type preprocessing_time: float
        :param tuple_latency_timestamps: List of latency metric tuples.
        :type tuple_latency_timestamps: list[tuple]
        :param model_network_latency: Network latency for the LLM module.
        :type model_network_latency: float
        :param metrics: Performance metrics from the LLM engine.
        :type metrics: dict
        """
        inference_elapsed_time = metrics.get("inference_time", 0.0)
        requests_per_second = metrics.get("requests_per_second", 0.0)
        total_tokens_per_second = metrics.get("total_tokens_per_second", 0.0)
        total_output_tokens_per_second = metrics.get("output_tokens_per_second", 0.0)

        for i in range(len(responses)):
            user_prompt = user_prompts[i]
            llm_response = responses[i]
            q = qs[i]
            tuple_latency_metric = tuple_latency_timestamps[i]

            # Calculate latency metrics
            input_entry_timestamp = tuple_latency_metric[0]
            cpu_head_network_latency = tuple_latency_metric[1]
            guardrails_network_latency = tuple_latency_metric[2]
            end_to_end_latency = round(time.time() - input_entry_timestamp, 2)

            # Latency
            self.dt.add_data("cpu_head_network_latency", cpu_head_network_latency)
            self.dt.add_data("guardrails_inference_latency", preprocessing_time)
            self.dt.add_data("guardrails_network_latency", guardrails_network_latency)
            self.dt.add_data("model_inference_latency", inference_elapsed_time)
            self.dt.add_data("model_network_latency", model_network_latency)
            self.dt.add_data("end_to_end_latency", end_to_end_latency)

            # Throughput
            self.dt.add_data("requests_per_second", requests_per_second)
            self.dt.add_data("total_tokens_per_second", total_tokens_per_second)
            self.dt.add_data("total_output_tokens_per_second", total_output_tokens_per_second)

            # Send response to the user
            q.put(
                {
                    # Identifiers
                    "hostname": self.hostname,
                    "inf_worker_id": self.inf_wrkr_id,
                    "devices": self.devices,
                    # Batch size
                    "batch_size": len(responses),
                    # Latency
                    "cpu_head_network_latency": cpu_head_network_latency,
                    "guardrails_inference_latency": preprocessing_time,
                    "guardrails_network_latency": guardrails_network_latency,
                    "model_inference_latency": inference_elapsed_time,
                    "model_network_latency": model_network_latency,
                    "end_to_end_latency": end_to_end_latency,
                    # Throughput
                    "requests_per_second": requests_per_second,
                    "total_tokens_per_second": total_tokens_per_second,
                    "total_output_tokens_per_second": total_output_tokens_per_second,
                    # Output
                    "user": user_prompt,
                    "assistant": llm_response,
                }
            )
