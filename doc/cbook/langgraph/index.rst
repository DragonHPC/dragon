.. _cbook_langgraph:

Dragon + LangGraph Examples
===========================

A guided path showing what **Dragon adds to a LangGraph agent graph** on an HPC
cluster — distributed agent execution, node placement, on-cluster LLM inference,
node-local data locality, and real-time supervision of live experiments. Every
example demonstrates a capability that plain LangGraph does **not** have. Read them
in order; each adds one new Dragon ability.

These examples run under the Dragon runtime, not plain ``python``::

    dragon examples/dragon_ai/langgraph/01_quickstart.py

For the internals, see :ref:`developer-guide-langgraph`.

.. toctree::
    :maxdepth: 1

    01 — Quickstart <../langgraph_01_quickstart>
    02 — Multinode Placement <../langgraph_02_multinode_placement>
    03 — On-Cluster Inference <../langgraph_03_local_inference>
    04 — Real-Time Supervision <../langgraph_04_realtime_supervision>
    05 — Data-Locality Crossover <../langgraph_05_data_locality_crossover>
    06 — Self-Driving Lab <../langgraph_06_self_driving_lab>
