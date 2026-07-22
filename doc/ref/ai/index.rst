.. _AIAPI:

AI
++

These interfaces enable integrations into key AI packages, such as `PyTorch <https://pytorch.org/>`__. A common use is
for enhanced data loading.


Python Reference
================

.. currentmodule:: dragon.ai

.. autosummary::
    :toctree:
    :recursive:

    torch.DragonDataset
    collective_group

Inference
=========

Distributed, multi-GPU and multi-node LLM inference with a pull-based load balancing component
managed through RDMA-enabled shared Dragon Queues, dynamic batching,
optional prompt guardrails, and a vLLM backend. See the :ref:`InferenceAPI`
for the full API reference.

.. toctree::
    :maxdepth: 2

    inference/index.rst

Agent Framework
===============

A multi-agent orchestration system for executing LLM-powered DAG workflows on
HPC clusters. See the :ref:`AgentAPI` for the full API reference.

.. toctree::
    :maxdepth: 2

    agent/index.rst