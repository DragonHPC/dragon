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
    ``src/dragon/ai/inference/README.md`` for installation and configuration
    instructions.

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
formatting and port allocation.

.. currentmodule:: dragon.ai.inference.llm_engine

.. autosummary::
    :toctree:
    :recursive:

    LLMInferenceEngine
    chat_template_formatter
    find_free_port


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
