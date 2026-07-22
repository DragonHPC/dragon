.. _InferenceAPI:

Inference
+++++++++

The Dragon Inference module provides distributed, multi-GPU and multi-node LLM
inference capabilities for low-latency, high-throughput generative AI workloads
on HPC clusters. It features a pull-based distributed load balancing component
managed through RDMA-enabled shared Dragon Queues. The module also incorporates
dynamic batching of inference requests, optional prompt
guardrails, and a tensor-parallelized vLLM backend with Dragon's process and
communication primitives.

.. note::
    This module is experimental and not yet in its final state. See the
    :ref:`inference_tutorial` for installation and configuration instructions,
    :ref:`inference-cookbook` for examples, and
    :ref:`developer-guide-inference` for implementation details.

User code should import the main service, configuration dataclasses, and queue
proxy from ``dragon.ai.inference``. The submodule references below document the
implementation modules that define those public objects.

Python Reference
================

Core
----

Entry point for initializing and launching the full inference pipeline across
nodes and GPUs.

.. currentmodule:: dragon.ai.inference.inference_utils

.. autosummary::
    :toctree:
    :recursive:

    Inference


Configuration
-------------

Type-safe dataclasses covering hardware allocation, model parameters, batching,
guardrails, dynamic worker management, and the top-level composite config.

.. currentmodule:: dragon.ai.inference.config

.. autosummary::
    :toctree:
    :recursive:

    InferenceConfig
    HardwareConfig
    ModelConfig
    BatchingConfig
    GuardrailsConfig
    DynamicWorkerConfig


LLM Proxy
---------

Transport-agnostic interface for sending chat requests to the inference backend,
with a Dragon queue-backed implementation and a reusable response-queue
pool.

.. currentmodule:: dragon.ai.inference.llm_proxy

.. autosummary::
    :toctree:
    :recursive:

    LLMProxy
    DragonQueueLLMProxy
    InferenceRequest
    ResponseQueuePool


Batching
--------

Dynamic request batching: individual request items, assembled batches, and the
batcher that collects prompts over a configurable time window.

.. currentmodule:: dragon.ai.inference.batching

.. autosummary::
    :toctree:
    :recursive:

    DynamicBatcher
    Batch
    BatchItem


Guardrails
----------

Prompt safety checking using the PromptGuard model, separated from the main
inference logic.

.. currentmodule:: dragon.ai.inference.guardrails

.. autosummary::
    :toctree:
    :recursive:

    GuardrailsProcessor

.. currentmodule:: dragon.ai.inference.prompt_guard_utils

.. autosummary::
    :toctree:
    :recursive:

    PromptGuard


LLM Engine
----------

vLLM-based inference engine and supporting utilities for chat-template
formatting, port allocation, and streaming generation.

.. currentmodule:: dragon.ai.inference.llm_engine

.. autosummary::
    :toctree:
    :recursive:

    LLMInferenceEngine
    StreamChunk
    StreamDoneSentinel
    STREAM_DONE_SENTINEL
    chat_template_formatter
    find_free_port


Async Streaming Mode
--------------------

The inference engine supports two operational modes controlled by the
``use_async_streaming`` configuration flag:

**Synchronous Mode** (default, ``use_async_streaming: false``):
    Uses vLLM's synchronous ``LLM`` engine. Suitable for batch processing
    workloads where multiple requests are accumulated and processed together.
    Requests with ``stream: true`` will return an error in this mode.

**Async Streaming Mode** (``use_async_streaming: true``):
    Uses vLLM's V1 ``AsyncLLM`` engine for token-by-token streaming. Suitable
    for interactive workloads requiring low time-to-first-token latency.
    Supports both streaming (``stream: true``) and non-streaming
    (``stream: false``) requests.

.. note::
    Async streaming mode and batch mode are mutually exclusive. If
    ``use_async_streaming: true``, then ``input_batching.toggle_on`` must be
    ``false``. The configuration validator will raise an error if both are
    enabled.

Configuration example:

.. code-block:: yaml

    llm:
      model_name: "meta-llama/Llama-3.1-8B-Instruct"
      use_async_streaming: true  # Enable AsyncLLM for streaming
      # ... other model config

    input_batching:
      toggle_on: false  # Must be false when streaming is enabled

Request routing based on configuration and request flags:

+---------------------------+------------------+--------------------+----------------------------+
| ``use_async_streaming``   | Request ``stream``| Method Called     | Result                     |
+===========================+==================+====================+============================+
| ``false``                 | ``false``        | ``generate()``     | Sync batch/single response |
+---------------------------+------------------+--------------------+----------------------------+
| ``false``                 | ``true``         | Error              | Streaming not available    |
+---------------------------+------------------+--------------------+----------------------------+
| ``true``                  | ``false``        | ``generate_single()``| Complete response        |
+---------------------------+------------------+--------------------+----------------------------+
| ``true``                  | ``true``         | ``generate_stream()``| Chunked token stream     |
+---------------------------+------------------+--------------------+----------------------------+


Workers
-------

GPU inference workers and the CPU head worker that monitors concurrency and
dynamically spins inference workers up and down.

.. currentmodule:: dragon.ai.inference.inference_worker_utils

.. autosummary::
    :toctree:
    :recursive:

    InferenceWorker

.. currentmodule:: dragon.ai.inference.cpu_worker_utils

.. autosummary::
    :toctree:
    :recursive:

    CPUWorker


Reader and Metrics
------------------

Response collection from the output queue and latency/throughput metrics
consolidation.

.. currentmodule:: dragon.ai.inference.reader_utils

.. autosummary::
    :toctree:
    :recursive:

    ReadWorker
    MetricsConsolidator
