"""Dragon inference tests package.

When the optional ``vllm`` dependency is not installed (for example on the
CPU-only CI build platform, where ``vllm`` cannot be built without CUDA), the
inference tests still need ``vllm`` to be importable so that ``@patch("vllm.LLM")``
targets resolve.  The real vLLM code paths are never exercised by these tests --
they mock the model -- so a lightweight stub is sufficient.

The stub is installed only when the real package is absent, so machines with a
working ``vllm`` install are unaffected.
"""

import importlib.util
import sys
import types


if importlib.util.find_spec("vllm") is None:
    _vllm = types.ModuleType("vllm")
    _vllm.__version__ = "0.0.0+stub"

    class _StubLLM:
        """Placeholder for ``vllm.LLM`` (patched out by the tests)."""

    class _StubSamplingParams:
        def __init__(self, *args, **kwargs):
            pass

    _vllm.LLM = _StubLLM
    _vllm.SamplingParams = _StubSamplingParams

    # Submodules referenced by the lazy imports in
    # ``dragon.ai.inference.llm_engine`` so ``from vllm.<sub> import ...`` resolves.
    _sampling = types.ModuleType("vllm.sampling_params")

    class _StubStructuredOutputsParams:
        def __init__(self, *args, **kwargs):
            pass

    class _StubGuidedDecodingParams:
        def __init__(self, *args, **kwargs):
            pass

    _sampling.SamplingParams = _StubSamplingParams
    _sampling.StructuredOutputsParams = _StubStructuredOutputsParams
    _sampling.GuidedDecodingParams = _StubGuidedDecodingParams

    _engine = types.ModuleType("vllm.engine")
    _arg_utils = types.ModuleType("vllm.engine.arg_utils")

    class _StubEngineArgs:
        def __init__(self, *args, **kwargs):
            pass

    _arg_utils.EngineArgs = _StubEngineArgs
    _engine.arg_utils = _arg_utils

    sys.modules["vllm"] = _vllm
    sys.modules["vllm.sampling_params"] = _sampling
    sys.modules["vllm.engine"] = _engine
    sys.modules["vllm.engine.arg_utils"] = _arg_utils
