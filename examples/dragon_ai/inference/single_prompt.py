"""Minimal standalone Dragon inference service example.

Run this script under the Dragon runtime:

    dragon single_prompt.py

Before running, edit the configuration constants below for your model and
allocation. These map directly to the configuration dataclasses in
``dragon.ai.inference.config`` (see ``src/dragon/ai/inference/config.py``).
A YAML-based alternative using ``src/dragon/ai/inference/config.sample`` and
``InferenceConfig.from_dict`` is described in the Dragon inference user guide.
"""

import logging

from dragon.ai.inference import (
    BatchingConfig,
    DynamicWorkerConfig,
    GuardrailsConfig,
    HardwareConfig,
    Inference,
    InferenceConfig,
    ModelConfig,
)
from dragon.native.queue import Queue


logging.basicConfig(level=logging.INFO, format="%(message)s")
LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
#
# Edit these values for your model and Dragon allocation. The example requires
# a Dragon allocation with at least one visible GPU per selected node.
# ---------------------------------------------------------------------------
MODEL_NAME = "/path/to/your/model"  # Hugging Face model name or local model path
HF_TOKEN = ""  # Hugging Face token for gated models, or an empty string
TP_SIZE = 1  # Number of GPUs per tensor-parallel model worker
NUM_NODES = 1  # Number of nodes to use from the allocation
NUM_GPUS = 1  # Number of GPUs per selected node


def build_config() -> InferenceConfig:
    """Build the inference configuration.

    Returns:
        Inference service configuration.
    """
    return InferenceConfig(
        model=ModelConfig(
            model_name=MODEL_NAME,
            hf_token=HF_TOKEN,
            tp_size=TP_SIZE,
            max_tokens=256,
            max_model_len=8192,
        ),
        hardware=HardwareConfig(
            num_nodes=NUM_NODES,
            num_gpus=NUM_GPUS,
            num_inf_workers_per_cpu=1,
        ),
        batching=BatchingConfig(enabled=True, batch_type="dynamic"),
        guardrails=GuardrailsConfig(enabled=False),
        dynamic_worker=DynamicWorkerConfig(enabled=False),
        run_type="chat",
        token="",
    )


def main() -> None:
    """Start the service, submit one prompt, and print the response."""
    inference_queue = Queue()
    response_queue = Queue(maxsize=1)
    service = Inference(build_config(), inference_queue)

    try:
        LOGGER.info("Initializing Dragon inference service...")
        service.initialize()
        service.query(("Give me a one-sentence definition of RDMA.", response_queue))

        result = response_queue.get()
        LOGGER.info("Assistant: %s", result["assistant"])
    finally:
        service.destroy()
        response_queue.close()


if __name__ == "__main__":
    main()