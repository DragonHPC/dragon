"""Tests for the dragon.ai.inference public package API."""

from unittest import TestCase

import dragon.ai.inference as inference_api
from dragon.ai.inference.config import (
    BatchingConfig,
    DynamicWorkerConfig,
    GuardrailsConfig,
    HardwareConfig,
    InferenceConfig,
    ModelConfig,
)
from dragon.ai.inference.inference_utils import Inference
from dragon.ai.inference.llm_proxy import DragonQueueLLMProxy, InferenceRequest, LLMProxy


class TestInferencePublicAPI(TestCase):
    """Test package-level exports intended for user code."""

    def test_public_exports_resolve_to_implementation_classes(self):
        """Package-level imports should resolve to the implementation classes."""
        self.assertIs(inference_api.BatchingConfig, BatchingConfig)
        self.assertIs(inference_api.DynamicWorkerConfig, DynamicWorkerConfig)
        self.assertIs(inference_api.GuardrailsConfig, GuardrailsConfig)
        self.assertIs(inference_api.HardwareConfig, HardwareConfig)
        self.assertIs(inference_api.Inference, Inference)
        self.assertIs(inference_api.InferenceConfig, InferenceConfig)
        self.assertIs(inference_api.ModelConfig, ModelConfig)
        self.assertIs(inference_api.DragonQueueLLMProxy, DragonQueueLLMProxy)
        self.assertIs(inference_api.InferenceRequest, InferenceRequest)
        self.assertIs(inference_api.LLMProxy, LLMProxy)

    def test_all_lists_public_exports(self):
        """__all__ should include the supported package-level imports."""
        self.assertEqual(
            sorted(inference_api.__all__),
            [
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
            ],
        )