"""
LLM Inference Engine module for Dragon Inference Pipeline.

This module handles the actual LLM inference using vLLM,
separated from preprocessing, batching, and guardrails logic.
"""
import os
os.environ['DRAGON_PATCH_MP'] = ''
import time
import logging
from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional, Iterator
import socket
from .config import ModelConfig, BatchingConfig

log = logging.getLogger(__name__)


# ====================================================================== #
#  Streaming Support                                                       #
# ====================================================================== #

@dataclass
class StreamChunk:
    """A single chunk yielded during streaming generation.

    Each chunk represents incremental output from the LLM, typically one
    or more tokens. The final chunk for a request has ``is_finished=True``
    and includes complete metrics.

    :param request_index: Index of this request within the batch (0 for
        single-request streaming).
    :type request_index: int
    :param delta_text: New text generated since the last chunk. Empty
        string for the final sentinel chunk.
    :type delta_text: str
    :param accumulated_text: Full text generated so far for this request.
    :type accumulated_text: str
    :param is_finished: True when generation for this request is complete.
    :type is_finished: bool
    :param finish_reason: Reason generation stopped (e.g., "stop", "length").
        Only populated when ``is_finished=True``.
    :type finish_reason: str | None
    :param time_to_first_token: Time in seconds from request start to first
        token. Populated on every chunk after the first token arrives.
    :type time_to_first_token: float | None
    :param metrics: Performance metrics. Only populated on the final chunk.
    :type metrics: dict | None
    """
    request_index: int
    delta_text: str
    accumulated_text: str
    is_finished: bool
    finish_reason: Optional[str] = None
    time_to_first_token: Optional[float] = None
    metrics: Optional[Dict[str, float]] = None


class StreamDoneSentinel:
    """Sentinel object indicating the end of a streaming response.

    Put this on the response queue after the final ``StreamChunk`` to
    signal that no more chunks will be sent for this request.
    """
    __slots__ = ()

    def __repr__(self) -> str:
        return "STREAM_DONE_SENTINEL"


STREAM_DONE_SENTINEL = StreamDoneSentinel()


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
    """Handles LLM inference using vLLM in a tensor-parallel environment.

    This class is responsible ONLY for LLM inference, completely separated
    from batching, guardrails, and other preprocessing logic.

    **Engine Modes**

    The engine supports two operational modes controlled by the
    ``use_async_streaming`` config flag:

    - **Synchronous Mode** (``use_async_streaming: false``, default):
      Uses vLLM's synchronous ``LLM`` engine. Best for batch workloads.
      Call :meth:`generate` for single or batched requests.

    - **Async Streaming Mode** (``use_async_streaming: true``):
      Uses vLLM's V1 ``AsyncLLM`` engine for token-by-token streaming.
      Best for interactive workloads requiring low latency.
      Call :meth:`generate_stream` for streaming or :meth:`generate_single`
      for non-streaming responses.

    .. note::
        Async streaming and batching are mutually exclusive. Configuration
        validation will reject ``use_async_streaming: true`` with
        ``input_batching.toggle_on: true``.

    **Request Routing**

    +-----------------------+----------------+---------------------+
    | ``use_async_streaming``| ``stream``    | Method              |
    +=======================+================+=====================+
    | false                 | false          | :meth:`generate`    |
    +-----------------------+----------------+---------------------+
    | false                 | true           | Error               |
    +-----------------------+----------------+---------------------+
    | true                  | false          | :meth:`generate_single`|
    +-----------------------+----------------+---------------------+
    | true                  | true           | :meth:`generate_stream`|
    +-----------------------+----------------+---------------------+
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
        self.async_engine = None  # AsyncLLM for streaming (when enabled)
        self.sampling_params = None
        self._event_loop = None  # Persistent event loop for async operations

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
        from vllm import SamplingParams
        import vllm
        from packaging import version

        vllm_version = version.parse(vllm.__version__)

        # Common engine configuration
        engine_kwargs = dict(
            model=self.model_config.model_name,
            tensor_parallel_size=self.model_config.tp_size,
            enforce_eager=True,
            distributed_executor_backend="mp",
            disable_custom_all_reduce=True,
            dtype=self.model_config.dtype,
            gpu_memory_utilization=self.model_config.gpu_memory_utilization,
            max_num_seqs=self.batching_config.max_batch_size,
            max_model_len=self.model_config.max_model_len,
        )

        # Add trust_remote_code only for vLLM versions < 0.12.0
        if vllm_version < version.parse("0.12.0"):
            engine_kwargs["trust_remote_code"] = True

        # Choose engine based on use_async_streaming config:
        # - AsyncLLM (V1 engine): For streaming workloads
        # - Sync LLM: For batch-only workloads (no streaming)
        use_async = getattr(self.model_config, 'use_async_streaming', False)

        if use_async:
            # Initialize AsyncLLM for streaming-capable workloads
            try:
                from vllm.v1.engine.async_llm import AsyncLLM
                from vllm.engine.arg_utils import AsyncEngineArgs

                async_engine_args = AsyncEngineArgs(**engine_kwargs)
                self.async_engine = AsyncLLM.from_engine_args(async_engine_args)
                log.info(
                    f"AsyncLLM Engine initialized on {self.hostname} "
                    f"with devices {self.devices} (streaming enabled)"
                )
            except ImportError as e:
                raise RuntimeError(
                    f"use_async_streaming=True requires vLLM V1 engine (AsyncLLM). "
                    f"Import failed: {e}"
                )
        else:
            # Initialize sync LLM for batch-only workloads
            from vllm import LLM
            from vllm.engine.arg_utils import EngineArgs
            import dataclasses

            engine_args = EngineArgs(**engine_kwargs)
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

        # Configure sampling parameters after engine init.
        self.sampling_params = SamplingParams(
            temperature=self.model_config.temperature,
            repetition_penalty=self.model_config.repetition_penalty,
            stop_token_ids=stop_token_ids,
            top_p=self.model_config.top_p,
            top_k=self.model_config.top_k,
            max_tokens=self.model_config.max_tokens,
            ignore_eos=self.model_config.ignore_eos,
            skip_special_tokens=self.model_config.skip_special_tokens,
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
            raise RuntimeError(
                "Batch generation requires sync LLMEngine. "
                "Set 'use_async_streaming: false' in config.yaml for batch workloads."
            )

        return self._generate_sync_batch(prompts, json_schemas)

    def _generate_sync_batch(
        self,
        prompts: List[str],
        json_schemas: List = None,
    ) -> Tuple[List[str], Dict[str, float]]:
        """Batch generation using synchronous LLM engine."""
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

    def generate_single(
        self,
        prompt: str,
        json_schema: Optional[dict] = None,
    ) -> Tuple[str, Dict[str, float]]:
        """Generate a complete response for a single prompt (non-streaming).

        This method is used when the AsyncLLM engine is active but the
        HTTP request has ``stream=false``. It runs the async generator
        to completion and returns the final response.

        :param prompt: Formatted prompt string ready for the LLM.
        :type prompt: str
        :param json_schema: Optional JSON schema for guided decoding.
        :type json_schema: dict | None
        :returns: Tuple ``(response, metrics)`` where ``response`` is the
            complete generated string and ``metrics`` is a dictionary of
            performance metrics.
        :rtype: tuple[str, dict[str, float]]
        :raises RuntimeError: If AsyncLLM engine is not available.
        """
        if self.async_engine is None:
            raise RuntimeError(
                "generate_single requires AsyncLLM engine. "
                "Set 'use_async_streaming: true' in config.yaml."
            )

        # Collect all chunks and return final result
        final_text = ""
        final_metrics = {}

        for chunk in self._generate_stream_async(prompt, json_schema):
            final_text = chunk.accumulated_text
            if chunk.is_finished and chunk.metrics:
                final_metrics = chunk.metrics

        return final_text, final_metrics

    def generate_stream(
        self,
        prompt: str,
        json_schema: Optional[dict] = None,
    ) -> Iterator[StreamChunk]:
        """Generate a streaming response for a single prompt.

        Yields :class:`StreamChunk` objects as tokens are generated.
        The final chunk has ``is_finished=True`` and includes metrics.

        Requires ``use_async_streaming=True`` in config to enable the
        AsyncLLM (V1 engine) backend. Streaming is not available when
        using the synchronous LLMEngine.

        Streaming is single-request only (no batching) to ensure
        low latency token delivery.

        :param prompt: Formatted prompt string ready for the LLM.
        :type prompt: str
        :param json_schema: Optional JSON schema for guided decoding.
        :type json_schema: dict | None
        :yields: StreamChunk objects containing incremental text.
        :rtype: Iterator[StreamChunk]
        :raises RuntimeError: If :meth:`initialize` has not been called or
            if AsyncLLM is not available.
        """
        if self.async_engine is None:
            raise RuntimeError(
                "Streaming requires AsyncLLM engine. "
                "Set 'use_async_streaming: true' in config.yaml under 'llm' section."
            )

        yield from self._generate_stream_async(prompt, json_schema)

    def _generate_stream_async(
        self,
        prompt: str,
        json_schema: Optional[dict] = None,
    ) -> Iterator[StreamChunk]:
        """Stream generation using AsyncLLM (V1 engine).

        Uses vLLM's async generator with RequestOutputKind.DELTA for
        efficient delta-only streaming. Wrapped in sync for compatibility
        with the synchronous worker loop.
        """
        import asyncio
        import copy
        import uuid
        import vllm
        from packaging import version

        vllm_version = version.parse(vllm.__version__)

        # Build sampling params
        params = copy.deepcopy(self.sampling_params)

        # Use DELTA mode for efficient streaming (vLLM 0.12+)
        # DELTA mode returns only new tokens, not accumulated text
        use_delta_mode = False
        if vllm_version >= version.parse("0.12.0"):
            try:
                from vllm.sampling_params import RequestOutputKind
                params.output_kind = RequestOutputKind.DELTA
                use_delta_mode = True
            except (ImportError, AttributeError):
                log.warning("RequestOutputKind.DELTA not available, using full text mode")

        if json_schema is not None:
            if vllm_version >= version.parse("0.12.0"):
                from vllm.sampling_params import StructuredOutputsParams
                params.structured_outputs = StructuredOutputsParams(json=json_schema)
            else:
                from vllm.sampling_params import GuidedDecodingParams
                params.guided_decoding = GuidedDecodingParams(json=json_schema)

        request_id = str(uuid.uuid4())
        start_time = time.time()
        accumulated_text = ""
        total_tokens = 0
        prompt_tokens = 0
        first_token_time = None
        ttft = None

        async def async_generate():
            nonlocal accumulated_text, total_tokens, prompt_tokens, first_token_time, ttft
            try:
                async for output in self.async_engine.generate(
                    request_id=request_id,
                    prompt=prompt,
                    sampling_params=params,
                ):
                    for completion in output.outputs:
                        if use_delta_mode:
                            # DELTA mode: text is just the new tokens
                            delta = completion.text
                            accumulated_text += delta
                        else:
                            # Full text mode: need to compute delta
                            current_text = completion.text
                            delta = current_text[len(accumulated_text):]
                            accumulated_text = current_text

                        if hasattr(completion, 'token_ids') and completion.token_ids:
                            if use_delta_mode:
                                # DELTA mode: token_ids holds only the new
                                # tokens for this chunk, so accumulate them.
                                total_tokens += len(completion.token_ids)
                            else:
                                # Full-text mode: token_ids holds the full
                                # accumulated list, so assign directly.
                                total_tokens = len(completion.token_ids)
                        else:
                            total_tokens += len(delta.split()) if delta else 0

                        # Track time to first token
                        if first_token_time is None and delta:
                            first_token_time = time.time()
                            ttft = first_token_time - start_time

                        is_finished = output.finished
                        finish_reason = None
                        metrics = None

                        if is_finished:
                            finish_reason = str(completion.finish_reason) if completion.finish_reason else None
                            prompt_tokens = len(output.prompt_token_ids) if output.prompt_token_ids else 0
                            inference_time = time.time() - start_time
                            metrics = {
                                "inference_time": round(inference_time, 2),
                                "time_to_first_token": round(ttft, 4) if ttft else 0,
                                "requests_per_second": round(1 / inference_time, 2) if inference_time > 0 else 0,
                                "total_tokens_per_second": round((prompt_tokens + total_tokens) / inference_time, 2) if inference_time > 0 else 0,
                                "output_tokens_per_second": round(total_tokens / inference_time, 2) if inference_time > 0 else 0,
                                "total_prompt_tokens": prompt_tokens,
                                "total_output_tokens": total_tokens,
                                "total_tokens": prompt_tokens + total_tokens,
                            }

                        if delta or is_finished:
                            yield StreamChunk(
                                request_index=0,
                                delta_text=delta,
                                accumulated_text=accumulated_text,
                                is_finished=is_finished,
                                finish_reason=finish_reason,
                                time_to_first_token=round(ttft, 4) if ttft else None,
                                metrics=metrics,
                            )

                    if output.finished:
                        break
            except Exception as e:
                log.error(f"AsyncLLM streaming error: {e}")
                raise

        # Sync wrapper for async generator
        # Use persistent event loop to preserve AsyncLLM internal state across requests
        if self._event_loop is None or self._event_loop.is_closed():
            self._event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._event_loop)

        agen = async_generate()
        while True:
            try:
                chunk = self._event_loop.run_until_complete(agen.__anext__())
                yield chunk
            except StopAsyncIteration:
                break

    def get_tokenizer(self):
        """Return the tokenizer from the underlying vLLM engine.

        The inference worker uses this to apply a model-specific chat template
        for requests that arrive as OpenAI-style message lists.  The tokenizer
        is only available after :meth:`initialize` has constructed the vLLM
        engine instance.

        :returns: Tokenizer owned by the vLLM engine.
        :rtype: transformers.PreTrainedTokenizerBase
        :raises RuntimeError: If :meth:`initialize` has not been called.
        """
        if self.llm is not None:
            return self.llm.get_tokenizer()
        elif self.async_engine is not None:
            # AsyncLLM exposes tokenizer via get_tokenizer()
            return self.async_engine.get_tokenizer()
        else:
            raise RuntimeError("Engine not initialized. Call initialize() first.")

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
        if self.llm is None and self.async_engine is None:
            return

        import gc
        import torch
        from contextlib import suppress

        # Close persistent event loop
        if self._event_loop is not None and not self._event_loop.is_closed():
            try:
                self._event_loop.close()
            except Exception as e:
                log.warning(f"Error closing event loop: {e}")
            finally:
                self._event_loop = None

        # Shutdown AsyncLLM engine if present
        if self.async_engine is not None:
            try:
                self.async_engine.shutdown()
            except Exception as e:
                log.warning(f"Error during AsyncLLM shutdown: {e}")
            finally:
                self.async_engine = None

        # Shutdown sync LLM engine
        llm = self.llm
        self.llm = None

        if llm is not None:
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
