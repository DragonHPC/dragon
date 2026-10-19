.. _cbook_langgraph_inference:

03 — On-Cluster Inference
+++++++++++++++++++++++++

Agents call an LLM served **on the cluster** by Dragon Inference (vLLM behind a
queue) through ``DragonQueueLLMProxy`` — no external API, no rate limits, and
air-gap friendly. Multiple agents share one inference queue and the same GPU
worker(s).

**What you'll learn:**

* How to stand up a Dragon Inference (vLLM) backend on the cluster
* How to call it from a LangGraph node via ``DragonQueueLLMProxy``
* How to request structured output with a JSON schema

**Needs:** GPU nodes.

Main Code
=========

.. literalinclude:: ../../examples/dragon_ai/langgraph/03_local_inference.py
    :language: python
    :linenos:
    :caption: **03_local_inference.py**

See Also
========

* :ref:`developer-guide-langgraph` — internals and code paths.
* :ref:`LangGraphAPI` — API reference.
