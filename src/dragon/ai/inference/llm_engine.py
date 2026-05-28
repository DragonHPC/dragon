"""
LLM Inference Engine module for Dragon Inference Pipeline.

This module handles the actual LLM inference using vLLM,
separated from preprocessing, batching, and guardrails logic.
"""
import os
os.environ['DRAGON_PATCH_MP'] = ''
import time
import logging
from typing import List, Tuple, Dict
import socket
from .config import ModelConfig, BatchingConfig

log = logging.getLogger(__name__)


def find_free_port(
    device_index: int = 0, base_port=20000, port_range_size=10000
) -> str:
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
    """Format the prompt using the model's chat template via its tokenizer.

    Builds an OpenAI-format message list and applies the model's native
    chat template using ``AutoTokenizer.apply_chat_template()``.

    :param system_prompt: System prompt to be included in the chat.
    :type system_prompt: str or list[str]
    :param user_prompt: User prompt to be included in the chat.
    :type user_prompt: str
    :param chat_history: Previous chat history as a list of message dicts
        with ``role`` and ``content`` keys.
    :type chat_history: list[dict]
    :param model_name: Name/path of the model (used to load tokenizer).
    :type model_name: str
    :returns: Formatted prompt string ready for the LLM.
    :rtype: str
    """
    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(model_name)

    messages = []
    if isinstance(system_prompt, list):
        system_prompt = " ".join(system_prompt)
    messages.append({"role": "system", "content": system_prompt})
    if chat_history:
        messages.extend(chat_history)
    messages.append({"role": "user", "content": user_prompt})

    formatted_prompt = tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )
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
        os.environ["VLLM_LOGGING_LEVEL"] = self.model_config.vllm_log_level.upper()

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
            max_model_len=self.model_config.max_model_len,
        )

        # Add trust_remote_code only for vLLM versions < 0.12.0
        vllm_version = version.parse(vllm.__version__)
        if vllm_version < version.parse("0.12.0"):
            engine_kwargs["trust_remote_code"] = True

        engine_args = EngineArgs(**engine_kwargs)

        # Initialize LLM
        self.llm = LLM(**dataclasses.asdict(engine_args))
        log.info(
            f"LLM Engine initialized on {self.hostname} "
            f"with devices {self.devices}"
        )

        # ----- Derive stop token IDs from model config -----
        # Read eos_token_id directly from generation_config.json / config.json.
        # This is tokenizer-agnostic and works for all model families
        # (HF tokenizers, Mistral Tekken, etc.).
        import json as _json

        stop_token_ids = []
        model_dir = self.model_config.model_name
        for cfg_name in ("generation_config.json", "config.json"):
            cfg_path = os.path.join(model_dir, cfg_name)
            if os.path.isfile(cfg_path):
                with open(cfg_path) as f:
                    cfg = _json.load(f)
                eos = cfg.get("eos_token_id")
                if eos is not None:
                    stop_token_ids = [eos] if isinstance(eos, int) else eos
                    log.info(f"Stop token IDs from {cfg_name}: {stop_token_ids}")
                    break

        if not stop_token_ids:
            log.warning("No eos_token_id found in model config files. "
                        "Generation may not terminate properly.")

        # Configure sampling parameters after LLM init.
        self.sampling_params = SamplingParams(
            temperature=0.5,
            repetition_penalty=1.1,
            stop_token_ids=stop_token_ids,
            top_p=self.model_config.top_p,
            top_k=self.model_config.top_k,
            max_tokens=self.model_config.max_tokens,
            ignore_eos=False,
            skip_special_tokens=False,
        )

    def generate(
        self,
        prompts: List[str],
        json_schemas: List = None,
    ) -> Tuple[List[str], Dict[str, float]]:
        """Generate responses for a batch of prompts.

        :param prompts: List of formatted prompts.
        :type prompts: list[str]
        :param json_schemas: Per-prompt JSON schema for guided decoding.
            A list the same length as *prompts* where each element is
            either a ``dict`` (enable guided decoding for that prompt)
            or ``None`` (free-form generation).  Pass ``None`` to use
            free-form generation for every prompt.
        :type json_schemas: list[dict | None] | None
        :returns: Tuple ``(responses, metrics)`` where ``responses`` is a
            list of generated strings and ``metrics`` is a dictionary of
            performance metrics.
        :rtype: tuple[list[str], dict[str, float]]
        """
        if self.llm is None:
            raise RuntimeError("LLM not initialized. Call initialize() first.")

        # Build per-request SamplingParams from the engine's authoritative
        # defaults.  Structured-output configuration is attached only for
        # prompts that supply a json_schema.
        #
        # vLLM 0.12.0 replaced GuidedDecodingParams with
        # StructuredOutputsParams (set via SamplingParams.structured_outputs).
        if json_schemas is not None:
            import copy
            import vllm
            from packaging import version

            if version.parse(vllm.__version__) >= version.parse("0.12.0"):
                from vllm.sampling_params import StructuredOutputsParams
            else:
                from vllm.sampling_params import GuidedDecodingParams

            params = []
            for schema in json_schemas:
                if schema is not None:
                    sp = copy.deepcopy(self.sampling_params)
                    if version.parse(vllm.__version__) >= version.parse("0.12.0"):
                        sp.structured_outputs = StructuredOutputsParams(json=schema)
                    else:
                        sp.guided_decoding = GuidedDecodingParams(json=schema)
                    params.append(sp)
                else:
                    params.append(self.sampling_params)
        else:
            params = self.sampling_params

        # Perform inference
        start_time = time.time()

        outputs = self.llm.generate(
            prompts,
            params,
            use_tqdm=False,
        )

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

    def get_tokenizer(self):
        """Return the tokenizer from the underlying vLLM engine.

        Useful for callers that need to ``apply_chat_template()``
        before calling :meth:`generate`.
        """
        if self.llm is None:
            raise RuntimeError("LLM not initialized. Call initialize() first.")
        return self.llm.get_tokenizer()

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
            total_prompt_tokens += (
                len(output.prompt_token_ids) if output.prompt_token_ids else 0
            )
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
                if (
                    torch.distributed.is_available()
                    and torch.distributed.is_initialized()
                ):
                    torch.distributed.destroy_process_group()

            if torch.cuda.is_available():
                with suppress(Exception):
                    torch.cuda.empty_cache()
                with suppress(Exception):
                    torch.cuda.ipc_collect()

        log.info("LLM Engine shutdown complete")
