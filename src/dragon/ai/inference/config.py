"""
Configuration classes for Dragon Inference Pipeline.

This module defines clean, type-safe configuration dataclasses that replace
the dictionary-based configuration and reduce excessive parameter passing.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class HardwareConfig:
    """Hardware allocation and resource configuration.

    Controls which Dragon allocation nodes and GPUs the inference service uses,
    and how many GPU inference workers are grouped under each CPU-head worker.

    :param num_nodes: Number of nodes to use from the current Dragon allocation.
        ``-1`` means use all available nodes.
    :type num_nodes: int
    :param num_gpus: Number of GPUs to use per selected node. ``-1`` means use
        all visible GPUs on each selected node.
    :type num_gpus: int
    :param num_inf_workers_per_cpu: Maximum number of tensor-parallel inference
        workers assigned to one CPU-head worker. ``-1`` means auto-calculate the
        value as ``num_gpus // tp_size`` (with a minimum of one) once the GPU
        count is resolved.
    :type num_inf_workers_per_cpu: int
    :param node_offset: Starting node index within the allocation. Use this to
        give multiple inference services disjoint node slices.
    :type node_offset: int
    :param inf_wrkr_queue_maxsize: Maximum size of the per-CPU-head inference
        worker input queue. ``-1`` means auto-calculate the value as
        ``num_inf_workers_per_cpu * 2``.
    :type inf_wrkr_queue_maxsize: int
    """

    num_nodes: int = -1  # -1 means auto-detect
    num_gpus: int = -1  # -1 means use all available
    num_inf_workers_per_cpu: int = -1  # -1 means auto-calculate
    node_offset: int = 0  # For kubernetes multi-service deployments
    inf_wrkr_queue_maxsize: int = -1  # -1 means auto-calculate (num_inf_workers * 2)

    def validate(self, all_nodes: dict) -> None:
        """Validate hardware configuration against available resources.

        :param all_nodes: Dictionary of all available nodes in the cluster.
            Keys are hostnames, values are ``dragon.native.machine.Node``
            objects.
        :type all_nodes: dict
        :raises ValueError: If any configuration parameter is invalid or
            exceeds available resources.
        """

        available_nodes = len(all_nodes)
        if self.num_nodes != -1:
            if self.num_nodes <= 0:
                raise ValueError(f"num_nodes must be >= 1, got {self.num_nodes}")
            if self.num_nodes > available_nodes:
                raise ValueError(
                    f"Requested {self.num_nodes} nodes but only {available_nodes} available"
                )

        if self.num_gpus != -1:
            for hostname, node in all_nodes.items():
                if self.num_gpus > node.num_gpus:
                    raise ValueError(
                        f"{hostname} has {node.gpu_vendor} GPUs with visible devices: {node.num_gpus}\nYou have requested {self.num_gpus} GPUs per node. However, you only have available {node.num_gpus} GPUs in {hostname} node."
                    )
                if self.num_gpus <= 0:
                    raise ValueError(
                        f"{hostname} has {node.gpu_vendor} GPUs with visible devices: {node.num_gpus}\nYou have requested {self.num_gpus} GPUs per node. However, you need to at least specify 1 GPU."
                    )

        if self.num_inf_workers_per_cpu != -1 and self.num_inf_workers_per_cpu < 1:
            raise ValueError(
                f"num_inf_workers_per_cpu must be >= 1 or -1 (auto), got {self.num_inf_workers_per_cpu}"
            )

        if self.inf_wrkr_queue_maxsize != -1 and self.inf_wrkr_queue_maxsize < 1:
            raise ValueError(
                f"inf_wrkr_queue_maxsize must be >= 1 or -1 (auto), got {self.inf_wrkr_queue_maxsize}"
            )

        if self.node_offset < 0:
            raise ValueError(f"node_offset must be >= 0, got {self.node_offset}")

        if self.node_offset >= available_nodes:
            raise ValueError(
                f"node_offset ({self.node_offset}) must be less than "
                f"available nodes ({available_nodes})"
            )

    def calculate_inf_workers_per_cpu(self, num_gpus: int, tp_size: int) -> int:
        """Calculate recommended inference workers per CPU when set to auto (-1).

        Formula: num_gpus // tp_size

        Rationale:
        - Each inference worker requires tp_size GPUs
        - num_gpus // tp_size gives the maximum model instances per node
        - All model instances are assigned to a single CPU worker per node
        - Ensures at least 1 worker per CPU

        :param num_gpus: Number of GPUs per node.
        :type num_gpus: int
        :param tp_size: Tensor parallelism size (GPUs per model instance).
        :type tp_size: int
        :returns: Recommended number of inference workers per CPU worker.
        :rtype: int
        """
        if self.num_inf_workers_per_cpu != -1:
            return self.num_inf_workers_per_cpu
        return max(1, num_gpus // tp_size)


@dataclass
class ModelConfig:
    """LLM model and generation configuration.

    Describes the model to load, tensor parallelism, tokenizer behavior, and
    default sampling parameters used by the vLLM backend.

    :param model_name: Hugging Face model name or local model directory.
    :type model_name: str
    :param hf_token: Hugging Face token used when loading the model and
        tokenizer. A token string is required by the configuration even when the
        model is local.
    :type hf_token: str
    :param tp_size: Tensor-parallel size. Each inference worker consumes this
        many GPUs.
    :type tp_size: int
    :param dtype: Model precision passed to vLLM, such as ``"bfloat16"`` or
        ``"float16"``.
    :type dtype: str
    :param max_tokens: Maximum number of new tokens to generate per request.
    :type max_tokens: int
    :param max_model_len: Maximum model context length, including prompt and
        generated tokens.
    :type max_model_len: int
    :param padding_side: Tokenizer padding side for prompt formatting.
    :type padding_side: str
    :param truncation_side: Tokenizer truncation side for prompt formatting.
    :type truncation_side: str
    :param top_k: Number of highest-probability tokens kept for top-k sampling.
    :type top_k: int
    :param top_p: Nucleus sampling threshold in the range ``[0.0, 1.0]``.
    :type top_p: float
    :param temperature: Sampling temperature controlling randomness. ``0.0``
        yields greedy decoding; higher values increase randomness.
    :type temperature: float
    :param repetition_penalty: Penalty applied to previously generated tokens to
        discourage repetition. Values greater than ``1.0`` penalize repetition.
    :type repetition_penalty: float
    :param ignore_eos: If ``True``, generation continues after the EOS token is
        produced instead of stopping.
    :type ignore_eos: bool
    :param skip_special_tokens: If ``True``, special tokens are removed from the
        generated output text.
    :type skip_special_tokens: bool
    :param system_prompt: System instructions used by the direct
        :meth:`dragon.ai.inference.Inference.query` path.
    :type system_prompt: list[str]
    :param vllm_log_level: vLLM logging level, for example ``"error"`` or
        ``"info"``.
    :type vllm_log_level: str
    :param gpu_memory_utilization: Fraction of GPU memory vLLM uses for model
        weights and KV cache. Range is ``(0, 1]``.
    :type gpu_memory_utilization: float
    """

    model_name: str
    hf_token: str
    tp_size: int  # Tensor parallelism size
    dtype: str = "bfloat16"
    max_tokens: int = 100
    max_model_len: int = 8192  # Full context window for the model (prompt + output tokens)
    padding_side: str = "left"
    truncation_side: str = "left"
    top_k: int = 50
    top_p: float = 0.95
    temperature: float = 0.5
    repetition_penalty: float = 1.1
    ignore_eos: bool = False
    skip_special_tokens: bool = False
    system_prompt: List[str] = field(
        default_factory=lambda: ["You are a helpful chatbot"]
    )
    vllm_log_level: str = "error"
    gpu_memory_utilization: float = 0.95
    use_async_streaming: bool = False

    def validate(self, gpus_per_node: int) -> None:
        """Validate model configuration.

        :param gpus_per_node: Number of GPUs available per node.
        :type gpus_per_node: int
        :raises ValueError: If any configuration parameter is invalid.
        """
        if self.tp_size > gpus_per_node:
            raise ValueError(
                f"Tensor parallelism size ({self.tp_size}) cannot exceed "
                f"available GPUs per node ({gpus_per_node})"
            )

        if self.tp_size < 1:
            raise ValueError(f"tp_size must be >= 1, got {self.tp_size}")

        if self.max_tokens <= 0:
            raise ValueError(f"max_tokens must be > 0, got {self.max_tokens}")

        if not (0.0 <= self.top_p <= 1.0):
            raise ValueError(f"top_p must be in [0, 1], got {self.top_p}")

        if self.temperature < 0.0:
            raise ValueError(f"temperature must be >= 0, got {self.temperature}")

        if self.repetition_penalty <= 0.0:
            raise ValueError(
                f"repetition_penalty must be > 0, got {self.repetition_penalty}"
            )

        if not (0.0 < self.gpu_memory_utilization <= 1.0):
            raise ValueError(
                f"gpu_memory_utilization must be in (0, 1], got {self.gpu_memory_utilization}"
            )


@dataclass
class BatchingConfig:
    """Request batching configuration.

    Controls whether requests are sent to vLLM one at a time, collected by the
    service over a short time window, or supplied as caller-created batches.

    :param enabled: Enable batching. If ``False``, requests are processed with a
        batch size of one.
    :type enabled: bool
    :param batch_type: Batching mode. ``"dynamic"`` lets the service assemble
        batches; ``"pre-batch"`` expects callers to submit prompt lists.
    :type batch_type: str
    :param batch_wait_seconds: Maximum time to hold a dynamic batch open before
        flushing it to the LLM process.
    :type batch_wait_seconds: float
    :param max_batch_size: Maximum number of requests in one vLLM generation
        call. Also used as vLLM ``max_num_seqs``.
    :type max_batch_size: int
    """

    enabled: bool = True
    batch_type: str = "dynamic"  # 'dynamic' or 'pre-batch'
    batch_wait_seconds: float = 0.1
    max_batch_size: int = 60

    def validate(self) -> None:
        """Validate batching configuration."""

        if self.batch_type not in ["dynamic", "pre-batch"]:
            raise ValueError(
                f"batch_type must be 'dynamic' or 'pre-batch', got {self.batch_type}"
            )

        if self.batch_wait_seconds <= 0:
            raise ValueError(
                f"batch_wait_seconds must be > 0, got {self.batch_wait_seconds}"
            )

        if self.max_batch_size < 1:
            raise ValueError(f"max_batch_size must be >= 1, got {self.max_batch_size}")


@dataclass
class GuardrailsConfig:
    """Prompt guardrails configuration.

    Configures optional PromptGuard-based filtering before prompts reach the
    vLLM engine.

    :param enabled: Enable PromptGuard preprocessing. Disabled prompts bypass
        guardrail scoring and are considered safe.
    :type enabled: bool
    :param prompt_guard_model: Hugging Face model name or local path for the
        PromptGuard classifier.
    :type prompt_guard_model: str
    :param prompt_guard_sensitivity: Jailbreak score threshold in the range
        ``[0.0, 1.0]``. Prompts with scores greater than or equal to this value
        are rejected.
    :type prompt_guard_sensitivity: float
    """

    enabled: bool = True
    prompt_guard_model: str = "meta-llama/Prompt-Guard-86M"
    prompt_guard_sensitivity: float = 0.5

    def validate(self) -> None:
        """Validate guardrails configuration."""
        if not (0.0 <= self.prompt_guard_sensitivity <= 1.0):
            raise ValueError(
                f"prompt_guard_sensitivity must be in [0, 1], "
                f"got {self.prompt_guard_sensitivity}"
            )


@dataclass
class DynamicWorkerConfig:
    """Dynamic inference worker spin-up and spin-down configuration.

    Controls the optional energy-saving path where extra GPU inference workers
    can shut down after idle periods and restart when prompt pressure rises.

    :param enabled: Enable dynamic worker lifecycle management. If ``False``,
        the initially started workers remain active until service shutdown.
    :type enabled: bool
    :param min_active_workers_per_cpu: Minimum number of inference workers that
        remain active under each CPU-head worker.
    :type min_active_workers_per_cpu: int
    :param spin_down_threshold_seconds: Idle time before a worker above the
        minimum active count is allowed to shut down.
    :type spin_down_threshold_seconds: int
    :param spin_up_threshold_seconds: Rolling time window used to count incoming
        prompts for spin-up decisions.
    :type spin_up_threshold_seconds: int
    :param spin_up_prompt_threshold: Number of prompts within the spin-up window
        required to restart an available worker.
    :type spin_up_prompt_threshold: int
    """

    enabled: bool = True
    min_active_workers_per_cpu: int = 1
    spin_down_threshold_seconds: int = 3600
    spin_up_threshold_seconds: int = 3
    spin_up_prompt_threshold: int = 5

    def validate(self) -> None:
        """Validate dynamic worker configuration."""
        if self.min_active_workers_per_cpu < 1:
            raise ValueError(
                f"min_active_workers_per_cpu must be >= 1, "
                f"got {self.min_active_workers_per_cpu}"
            )

        if self.spin_down_threshold_seconds < 1:
            raise ValueError(
                f"spin_down_threshold_seconds must be >= 1, "
                f"got {self.spin_down_threshold_seconds}"
            )

        if self.spin_up_threshold_seconds < 1:
            raise ValueError(
                f"spin_up_threshold_seconds must be >= 1, "
                f"got {self.spin_up_threshold_seconds}"
            )

        if self.spin_up_prompt_threshold < 1:
            raise ValueError(
                f"spin_up_prompt_threshold must be >= 1, "
                f"got {self.spin_up_prompt_threshold}"
            )


@dataclass
class InferenceConfig:
    """Master configuration for the entire inference pipeline.

    Composes the model, hardware, batching, guardrails, and dynamic worker
    sections consumed by :class:`dragon.ai.inference.Inference`.
    Only ``model`` is required for direct construction. All other fields have
    defaults suitable for a single shared backend in agentic pipelines.

    .. note::

       Direct construction and :meth:`from_dict` have different defaults for
       guardrails and dynamic workers. Direct construction defaults both to
       disabled through this class's default factories. The YAML-compatible
       :meth:`from_dict` path defaults missing ``guardrails.toggle_on`` and
       ``dynamic_inf_wrkr.toggle_on`` values to ``True`` to match
       ``config.sample``.

    :param model: Required model and generation settings.
    :type model: ModelConfig
    :param hardware: Node and GPU allocation settings.
    :type hardware: HardwareConfig
    :param batching: Request batching settings.
    :type batching: BatchingConfig
    :param guardrails: Prompt guardrails settings.
    :type guardrails: GuardrailsConfig
    :param dynamic_worker: Dynamic inference worker lifecycle settings.
    :type dynamic_worker: DynamicWorkerConfig
    :param flask_secret_key: Secret key retained for compatibility with
        application configurations that include a Flask service.
    :type flask_secret_key: str
    :param run_type: Application run mode label used by drivers and examples.
    :type run_type: str
    :param token: Application token string used by drivers that require one.
    :type token: str
    """

    model: ModelConfig
    hardware: HardwareConfig = field(default_factory=HardwareConfig)
    batching: BatchingConfig = field(default_factory=BatchingConfig)
    guardrails: GuardrailsConfig = field(default_factory=lambda: GuardrailsConfig(enabled=False))
    dynamic_worker: DynamicWorkerConfig = field(default_factory=lambda: DynamicWorkerConfig(enabled=False))
    flask_secret_key: str = ""
    run_type: str = "backend_only"
    token: str = ""

    @classmethod
    def from_dict(cls, config_dict: dict) -> "InferenceConfig":
        """Create InferenceConfig from dictionary (loaded from YAML).

        :param config_dict: Configuration dictionary loaded from YAML.
        :type config_dict: dict
        :returns: InferenceConfig instance.
        :rtype: InferenceConfig
        """

        # Validate that only expected top-level keys are present
        expected_keys = {
            "run_type",
            "token",
            "required",
            "hardware",
            "llm",
            "input_batching",
            "guardrails",
            "dynamic_inf_wrkr",
        }
        actual_keys = set(config_dict.keys())
        unexpected_keys = actual_keys - expected_keys

        if unexpected_keys:
            raise ValueError(
                f"Unexpected keys in config.yaml: {unexpected_keys}. "
                f"Expected keys are: {expected_keys}"
            )

        print("\nValidating config.yaml schema...", flush=True)

        # Validate each section has only expected keys
        cls._validate_section_keys(
            config_dict.get("required", {}),
            {
                "model_name",
                "hf_token",
                "tp_size",
                "flask_secret_key",
            },
            "required",
        )
        cls._validate_section_keys(
            config_dict.get("hardware", {}),
            {"num_nodes", "num_gpus", "num_inf_wrkrs_per_cpu", "node_offset", "inf_wrkr_queue_maxsize"},
            "hardware",
        )
        cls._validate_section_keys(
            config_dict.get("llm", {}),
            {
                "dtype",
                "max_tokens",
                "max_model_len",
                "padding_side",
                "truncation_side",
                "top_k",
                "top_p",
                "temperature",
                "repetition_penalty",
                "ignore_eos",
                "skip_special_tokens",
                "system_prompt",
                "vllm_log_level",
                "gpu_memory_utilization",
                "use_async_streaming",
            },
            "llm",
        )
        cls._validate_section_keys(
            config_dict.get("input_batching", {}),
            {"toggle_on", "type", "input_batch_wait_seconds", "max_batch_limit"},
            "input_batching",
        )
        cls._validate_section_keys(
            config_dict.get("guardrails", {}),
            {"toggle_on", "prompt_guard_model", "prompt_guard_sensitivity"},
            "guardrails",
        )
        cls._validate_section_keys(
            config_dict.get("dynamic_inf_wrkr", {}),
            {
                "toggle_on",
                "min_active_inf_wrkrs_per_cpu",
                "spin_down_threshold_seconds",
                "spin_up_threshold_seconds",
                "spin_up_prompt_threshold",
            },
            "dynamic_inf_wrkr",
        )

        print("Config schema validated successfully\n", flush=True)

        # Parse hardware config
        hw = config_dict.get("hardware", {})
        hardware = HardwareConfig(
            num_nodes=hw.get("num_nodes", -1),
            num_gpus=hw.get("num_gpus", -1),
            num_inf_workers_per_cpu=hw.get("num_inf_wrkrs_per_cpu", -1),
            node_offset=hw.get("node_offset", 0),
            inf_wrkr_queue_maxsize=hw.get("inf_wrkr_queue_maxsize", -1),
        )

        # Parse model config
        req = config_dict.get("required", {})
        llm = config_dict.get("llm", {})
        model = ModelConfig(
            model_name=req["model_name"],
            hf_token=req["hf_token"],
            tp_size=req["tp_size"],
            dtype=llm.get("dtype", "bfloat16"),
            max_tokens=llm.get("max_tokens", 100),
            max_model_len=llm.get("max_model_len", 8192),
            padding_side=llm.get("padding_side", "left"),
            truncation_side=llm.get("truncation_side", "left"),
            top_k=llm.get("top_k", 50),
            top_p=llm.get("top_p", 0.95),
            temperature=llm.get("temperature", 0.5),
            repetition_penalty=llm.get("repetition_penalty", 1.1),
            ignore_eos=llm.get("ignore_eos", False),
            skip_special_tokens=llm.get("skip_special_tokens", False),
            system_prompt=llm.get("system_prompt", ["You are a helpful chatbot"]),
            vllm_log_level=llm.get("vllm_log_level", "error"),
            gpu_memory_utilization=llm.get("gpu_memory_utilization", 0.95),
            use_async_streaming=llm.get("use_async_streaming", False),
        )

        # Parse batching config
        batch = config_dict.get("input_batching", {})
        batching = BatchingConfig(
            enabled=batch.get("toggle_on", True),
            batch_type=batch.get("type", "dynamic"),
            batch_wait_seconds=batch.get("input_batch_wait_seconds", 0.1),
            max_batch_size=batch.get("max_batch_limit", 60),
        )

        # Parse guardrails config
        guard = config_dict.get("guardrails", {})
        guardrails = GuardrailsConfig(
            enabled=guard.get("toggle_on", True),
            prompt_guard_model=guard.get(
                "prompt_guard_model", "meta-llama/Prompt-Guard-86M"
            ),
            prompt_guard_sensitivity=guard.get("prompt_guard_sensitivity", 0.5),
        )

        # Parse dynamic worker config
        dyn = config_dict.get("dynamic_inf_wrkr", {})
        dynamic_worker = DynamicWorkerConfig(
            enabled=dyn.get("toggle_on", True),
            min_active_workers_per_cpu=dyn.get("min_active_inf_wrkrs_per_cpu", 1),
            spin_down_threshold_seconds=dyn.get("spin_down_threshold_seconds", 3600),
            spin_up_threshold_seconds=dyn.get("spin_up_threshold_seconds", 3),
            spin_up_prompt_threshold=dyn.get("spin_up_prompt_threshold", 5),
        )

        return cls(
            hardware=hardware,
            model=model,
            batching=batching,
            guardrails=guardrails,
            dynamic_worker=dynamic_worker,
            flask_secret_key=req["flask_secret_key"],
            run_type=config_dict["run_type"],
            token=config_dict["token"],
        )

    @staticmethod
    def _validate_section_keys(
        section_dict: dict, expected_keys: set, section_name: str
    ) -> None:
        """Validate that a config section contains only expected keys.

        :param section_dict: Configuration section dictionary.
        :type section_dict: dict
        :param expected_keys: Set of expected keys for the section.
        :type expected_keys: set
        :param section_name: Name of the configuration section (for error messages).
        :type section_name: str
        :raises ValueError: If unexpected keys are found in the section.
        """
        if not section_dict:
            return

        actual_keys = set(section_dict.keys())
        unexpected_keys = actual_keys - expected_keys

        if unexpected_keys:
            raise ValueError(
                f"Unexpected keys in '{section_name}' section: {unexpected_keys}. "
                f"Expected keys are: {expected_keys}"
            )

        print(f"'{section_name}' section validated", flush=True)

    def validate_all(self, all_nodes: dict) -> None:
        """Validate all configuration sections.

        :param all_nodes: Dictionary of all available nodes in the cluster.
            Keys are hostnames, values are ``dragon.native.machine.Node``
            objects.
        :type all_nodes: dict
        :raises ValueError: If any configuration parameter is invalid.
        """
        # Get GPUs from first node (assume homogeneous cluster)
        available_gpus = next(iter(all_nodes.values())).num_gpus if all_nodes else 0
        self.hardware.validate(all_nodes)
        self.model.validate(available_gpus)
        self.batching.validate()
        self.guardrails.validate()
        self.dynamic_worker.validate()

        # Streaming and batching are mutually exclusive:
        # - Streaming uses AsyncLLM (V1 engine) for token-by-token delivery
        # - Batching uses sync LLMEngine for efficient batch processing
        # Cannot use both simultaneously as they require different engines.
        if self.model.use_async_streaming and self.batching.enabled:
            raise ValueError(
                "Streaming and batching are mutually exclusive. "
                "Set either 'llm.use_async_streaming: true' (for streaming, no batching) "
                "or 'input_batching.toggle_on: true' (for batching, no streaming), not both."
            )
