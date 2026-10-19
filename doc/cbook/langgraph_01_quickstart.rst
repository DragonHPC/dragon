.. _cbook_langgraph_quickstart:

01 — Quickstart: LangGraph Agents as Dragon Processes
+++++++++++++++++++++++++++++++++++++++++++++++++++++

The first example in the Dragon + LangGraph series. It runs an ordinary LangGraph
graph on the Dragon runtime: each agent becomes a Dragon **AgentHost process** via
``executor.launch_host(...)`` and is wrapped with ``executor.node(name)``. The graph
definition is unchanged — only execution moves onto Dragon.

**What you'll learn:**

* How to create a ``DragonExecutor`` and launch an ``AgentHost``
* How to wrap a LangGraph node with ``executor.node(name)``
* How agent execution moves onto the Dragon runtime with no change to the graph

**Needs:** 1 node.

Main Code
=========

.. literalinclude:: ../../examples/dragon_ai/langgraph/01_quickstart.py
    :language: python
    :linenos:
    :caption: **01_quickstart.py**

See Also
========

* :ref:`developer-guide-langgraph` — internals and code paths.
* :ref:`LangGraphAPI` — API reference.
