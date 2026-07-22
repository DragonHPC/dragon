"""Dragon AI inference service package.

This package provides the Dragon-native inference backend used by AI agents
and other applications that need a shared LLM service inside a Dragon runtime.
It combines Dragon queues, events, process groups, placement policies, and
telemetry with a vLLM model engine.

The main entry point is :class:`dragon.ai.inference.Inference`. Client code
normally submits chat requests through
:class:`dragon.ai.inference.DragonQueueLLMProxy`, while lower-level plain-text
and pre-batched workloads can use ``Inference.query()`` directly.

The package is experimental and its public API may change.
"""

from .config import (
    BatchingConfig,
    DynamicWorkerConfig,
    GuardrailsConfig,
    HardwareConfig,
    InferenceConfig,
    ModelConfig,
)
from .inference_utils import Inference
from .llm_engine import StreamChunk, StreamDoneSentinel, STREAM_DONE_SENTINEL
from .llm_proxy import DragonQueueLLMProxy, InferenceRequest, LLMProxy


__all__ = [
    "BatchingConfig",
    "DragonQueueLLMProxy",
    "DynamicWorkerConfig",
    "GuardrailsConfig",
    "HardwareConfig",
    "Inference",
    "InferenceConfig",
    "InferenceRequest",
    "LLMProxy",
    "ModelConfig",
    "STREAM_DONE_SENTINEL",
    "StreamChunk",
    "StreamDoneSentinel",
]
