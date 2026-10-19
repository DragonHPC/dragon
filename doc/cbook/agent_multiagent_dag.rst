.. _cbook_agent_multiagent_dag:

Multi-Agent DAG Orchestration
+++++++++++++++++++++++++++++

Building on Example 01, this example demonstrates a multi-agent pipeline with
a directed acyclic graph (DAG) topology. Multiple agents are wired together so
upstream results automatically flow into downstream agents' context. It also
shows function nodes (pure Python execution without LLM) and all three tool
registration styles.

**Prerequisites:** Read ``01_single_agent.py`` first.

**What you'll learn:**

* How to build a multi-agent DAG with multiple upstream dependencies per node
* How to wire results from upstream agents into downstream agent context
* How to create function nodes (pure Python, no LLM) using ``PipelineNode(fn=...)``
* All three tool registration styles: direct callable, async decorator, sync decorator
* How downstream agents automatically receive upstream results

**Architecture:**

Four agents form a linear pipeline (planner → runner → analyzer → reporter),
plus a pure Python function node to save the final report.

Main Code
=========

Below is the complete example:

.. literalinclude:: ../../examples/dragon_ai/ai_agent/02_multi_agent_dag.py
    :language: python
    :linenos:
    :caption: **02_multi_agent_dag.py: Multi-agent DAG with function nodes**


Key Concepts
============

**DAG Topology:**

Each ``PipelineNode`` can list multiple ``upstream`` agent IDs. The framework
waits for all upstream nodes to complete, then passes their results to the
downstream agent's context automatically.

**Function Nodes:**

A ``PipelineNode(fn=...)`` runs pure Python (no LLM). Useful for:

* Aggregating/reporting results
* Data transformation
* Writing to disk or database
* Any non-agentic computation

**All Three Registration Styles:**

1. **Direct callable**: ``registry.register(existing_function)``
2. **Async decorator**: ``@registry.tool async def ...``
3. **Sync decorator**: ``@registry.tool def ...``

All generate JSON schemas automatically from parameter annotations.

Installation
============

See Example 01 (same dependencies).

System Description
===================

Tested on HPE Cray EX:

* 1–2 nodes sufficient
* Main vLLM backend: 1–2 GPUs
* Agents: any available CPU

How to Run
==========

.. code-block:: console

    dragon 02_multi_agent_dag.py

**Example output:**

.. code-block:: console

    $ dragon 02_multi_agent_dag.py
    Planner: Proposing strategy...
    Runner: Executing strategy...
    Analyzer: Reviewing execution...
    Reporter: Generating final report...
    Report saved to: report.txt

Next Steps
==========

* **03 — Human-in-the-Loop** (add approval gates before tool execution)
