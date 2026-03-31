"""
LLM Inference Engine module for Dragon Inference Pipeline.

This module handles the actual LLM inference using vLLM,
separated from preprocessing, batching, and guardrails logic.
"""

import os
import time
import logging
import socket
import multiprocessing as mp
from typing import List, Tuple, Dict
from ...infrastructure.policy import Policy
from .config import ModelConfig, BatchingConfig

log = logging.getLogger(__name__)


def find_free_port(device_index: int = 0, base_port=20000, port_range_size=10000) -> str:
    """Return an available TCP port using the worker's device index as seed.

    The device index seeds a deterministic random starting point within the
    range, so concurrent workers on the same node begin scanning from widely
    separated ports, virtually eliminating collisions.  If a port is taken,
    the search wraps around until a free one is found.

    The default range (20000-29999) is intentionally disjoint from the
    vLLM ``get_open_port()`` patch range (30000-60000) so the two never
    interfere.

    :param device_index: GPU device index used to seed the random start.
    :type device_index: int
    :param base_port: Starting port of the search range.
    :type base_port: int
    :param port_range_size: Size of the port range to scan.
    :type port_range_size: int
    :raises IOError: If no free port is found in the range.
    :returns: A free port as a string.
    :rtype: str
    """
    import random

    rng = random.Random(device_index)
    start = rng.randint(0, port_range_size - 1)
    for i in range(port_range_size):
        port = base_port + (start + i) % port_range_size
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("", port))
            sock.close()
            return str(port)
        except OSError:
            sock.close()
    raise IOError("no free ports")


def chat_template_formatter(system_prompt, user_prompt, chat_history, model_name):
    if len(chat_history) == 0:
        formatted_prompt = f"<|system|>\n{system_prompt}\n<|user|>\n{user_prompt}\n<|assistant|>\n"
    else:
        chat_context = "You remember all relevant previous chat history below."
        chat_context = str(chat_context) + str("\n".join(chat_history))
        formatted_prompt = f"<|system|>\n{system_prompt}\n{chat_context}\n<|user|>{user_prompt}\n<|assistant|>\n"
    return formatted_prompt


class LLMInferenceEngine:
    """
    Handles LLM inference using vLLM in a tensor-parallel environment.

    This class is responsible ONLY for LLM inference, completely separated
    from batching, guardrails, and other preprocessing logic.
    """

    def __init__(
        self,
        model_config: ModelConfig,
        batching_config: BatchingConfig,
        hostname: str,
        devices: List[int],
    ):
        """Initialize the LLM inference engine.

        :param model_config: Model configuration.
        :type model_config: ModelConfig
        :param batching_config: Batching configuration (used for
            ``max_num_seqs``).
        :type batching_config: BatchingConfig
        :param hostname: Current process hostname.
        :type hostname: str
        :param devices: List of GPU device IDs.
        :type devices: list[int]
        """
        self.model_config = model_config
        self.batching_config = batching_config
        self.hostname = hostname
        self.devices = devices

        self.llm = None
        self.sampling_params = None

    def initialize(self) -> None:
        """
        Initialize the vLLM model and sampling parameters.

        This should be called within the worker process to avoid
        serialization issues with CUDA objects.
        """
        # Set environment variables
        os.environ["HF_TOKEN"] = self.model_config.hf_token
        os.environ["MASTER_ADDR"] = "127.0.0.1"
        # Discover a free port on this worker node for distributed communication; needed for vLLM's distributed executor backend
        os.environ["MASTER_PORT"] = find_free_port(device_index=self.devices[0])

        # Tell the Dragon get_open_port() patch which device offset to
        # use so that each co-located vLLM instance gets a unique
        # distributed-init port deterministically.
        os.environ["_DRAGON_DEVICE_OFFSET"] = str(self.devices[0])

        # Lazy import vLLM to ensure Dragon start method is maintained
        from vllm import LLM, SamplingParams
        from vllm.engine.arg_utils import EngineArgs
        import vllm
        import dataclasses
        from packaging import version

        # Configure sampling parameters
        self.sampling_params = SamplingParams(
            temperature=0.5,
            repetition_penalty=1.1,
            stop=["<|eot_id|>", "<END>"],
            top_p=self.model_config.top_p,
            top_k=self.model_config.top_k,
            max_tokens=self.model_config.max_tokens,
            ignore_eos=False,
            skip_special_tokens=False,
        )

        # Configure engine arguments
        # trust_remote_code was removed in vLLM 0.12.0
        engine_kwargs = dict(
            model=self.model_config.model_name,
            tensor_parallel_size=self.model_config.tp_size,
            enforce_eager=True,
            distributed_executor_backend="mp",
            disable_custom_all_reduce=True,
            dtype=self.model_config.dtype,
            gpu_memory_utilization=0.95,
            max_num_seqs=self.batching_config.max_batch_size,
            max_model_len=1024,
        )

        # Add trust_remote_code only for vLLM versions < 0.12.0
        vllm_version = version.parse(vllm.__version__)
        if vllm_version < version.parse("0.12.0"):
            engine_kwargs["trust_remote_code"] = True

        engine_args = EngineArgs(**engine_kwargs)

        # Initialize LLM
        self.llm = LLM(**dataclasses.asdict(engine_args))
        log.info(f"LLM Engine initialized on {self.hostname} " f"with devices {self.devices}")

    def generate(
        self,
        prompts: List[str],
    ) -> Tuple[List[str], Dict[str, float]]:
        """Generate responses for a batch of prompts.

        :param prompts: List of formatted prompts.
        :type prompts: list[str]
        :returns: Tuple ``(responses, metrics)`` where ``responses`` is a
            list of generated strings and ``metrics`` is a dictionary of
            performance metrics.
        :rtype: tuple[list[str], dict[str, float]]
        """
        if self.llm is None:
            raise RuntimeError("LLM not initialized. Call initialize() first.")

        import torch

        # Perform inference
        start_time = time.time()
        torch.cuda.synchronize()

        outputs = self.llm.generate(
            prompts,
            self.sampling_params,
            use_tqdm=False,
        )

        torch.cuda.synchronize()
        inference_time = time.time() - start_time

        # Extract responses
        responses = [output.outputs[0].text for output in outputs]

        # Calculate performance metrics
        metrics = self._calculate_metrics(outputs, inference_time)

        log.debug(
            f"Generated {len(responses)} responses in {inference_time:.2f}s "
            f"({metrics['requests_per_second']:.2f} req/s)"
        )

        return responses, metrics

    def _calculate_metrics(
        self,
        outputs: List,
        inference_time: float,
    ) -> Dict[str, float]:
        """Calculate performance metrics from inference results.

        :param outputs: vLLM output objects.
        :type outputs: list
        :param inference_time: Time taken for inference in seconds.
        :type inference_time: float
        :returns: Dictionary of performance metrics.
        :rtype: dict[str, float]
        """
        total_prompt_tokens = 0
        total_output_tokens = 0

        for output in outputs:
            total_prompt_tokens += len(output.prompt_token_ids) if output.prompt_token_ids else 0
            total_output_tokens += sum(len(o.token_ids) for o in output.outputs if o)

        total_tokens = total_prompt_tokens + total_output_tokens

        return {
            "inference_time": round(inference_time, 2),
            "requests_per_second": round(len(outputs) / inference_time, 2),
            "total_tokens_per_second": round(total_tokens / inference_time, 2),
            "output_tokens_per_second": round(total_output_tokens / inference_time, 2),
            "total_prompt_tokens": total_prompt_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_tokens,
        }

    def shutdown(self) -> None:
        """
        Shutdown the LLM engine and release resources.
        """
        if self.llm is None:
            return

        import gc
        import torch
        from contextlib import suppress

        llm = self.llm
        self.llm = None

        try:
            engine = getattr(llm, "llm_engine", None)
            if engine is not None:
                executor = getattr(engine, "model_executor", None)
                if executor is not None and hasattr(executor, "shutdown"):
                    executor.shutdown()
                if hasattr(engine, "shutdown"):
                    engine.shutdown()
        except Exception as e:
            log.warning(f"Error during vLLM shutdown: {e}")
        finally:
            del llm
            gc.collect()

            with suppress(Exception):
                if torch.distributed.is_available() and torch.distributed.is_initialized():
                    torch.distributed.destroy_process_group()

            if torch.cuda.is_available():
                with suppress(Exception):
                    torch.cuda.empty_cache()
                with suppress(Exception):
                    torch.cuda.ipc_collect()

        log.info("LLM Engine shutdown complete")
