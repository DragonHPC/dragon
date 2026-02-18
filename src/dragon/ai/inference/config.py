"""
Configuration classes for Dragon Inference Pipeline.

This module defines clean, type-safe configuration dataclasses that replace
the dictionary-based configuration and reduce excessive parameter passing.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class HardwareConfig:
    """Hardware allocation and resource configuration."""

    num_nodes: int = -1  # -1 means auto-detect
    num_gpus: int = -1  # -1 means use all available
    num_inf_workers_per_cpu: int = 4
    node_offset: int = 0  # For kubernetes multi-service deployments

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

        if self.num_inf_workers_per_cpu < 1:
            raise ValueError(
                f"num_inf_workers_per_cpu must be >= 1, got {self.num_inf_workers_per_cpu}"
            )


@dataclass
class ModelConfig:
    """LLM model configuration."""

    model_name: str
    hf_token: str
    tp_size: int  # Tensor parallelism size
    dtype: str = "bfloat16"
    max_tokens: int = 100
    padding_side: str = "left"
    truncation_side: str = "left"
    top_k: int = 50
    top_p: float = 0.95
    system_prompt: List[str] = field(
        default_factory=lambda: ["You are a helpful chatbot"]
    )

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


@dataclass
class BatchingConfig:
    """Dynamic batching configuration."""

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
    """Prompt guardrails/safety configuration."""

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
    """Dynamic inference worker spin-up/down configuration."""

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
    """Master configuration for the entire inference pipeline."""

    hardware: HardwareConfig
    model: ModelConfig
    batching: BatchingConfig
    guardrails: GuardrailsConfig
    dynamic_worker: DynamicWorkerConfig
    flask_secret_key: str

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
            {"num_nodes", "num_gpus", "num_inf_wrkrs_per_cpu"},
            "hardware",
        )
        cls._validate_section_keys(
            config_dict.get("llm", {}),
            {
                "dtype",
                "max_tokens",
                "padding_side",
                "truncation_side",
                "top_k",
                "top_p",
                "system_prompt",
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
            num_inf_workers_per_cpu=hw.get("num_inf_wrkrs_per_cpu", 4),
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
            padding_side=llm.get("padding_side", "left"),
            truncation_side=llm.get("truncation_side", "left"),
            top_k=llm.get("top_k", 50),
            top_p=llm.get("top_p", 0.95),
            system_prompt=llm.get("system_prompt", ["You are a helpful chatbot"]),
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
