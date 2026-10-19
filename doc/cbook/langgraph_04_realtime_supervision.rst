.. _cbook_langgraph_supervision:

04 — Real-Time Supervision
++++++++++++++++++++++++++

One of the two hero examples. Experiments run as first-class Dragon ``Process`` es
streaming progress over a ``Queue``; a supervisor **aborts diverging runs
mid-flight** via a ``DDict`` flag and reclaims their compute. This is a capability
plain LangGraph structurally lacks — its nodes are atomic, so a running experiment
cannot be observed or interrupted from within the graph.

**What you'll learn:**

* How to run an experiment as a supervised Dragon ``Process``
* How a supervisor streams live metrics and trips an abort flag
* How aborting frees the node for the next unit of work

**Needs:** 1 node.

Main Code
=========

.. literalinclude:: ../../examples/dragon_ai/langgraph/04_realtime_supervision.py
    :language: python
    :linenos:
    :caption: **04_realtime_supervision.py**

See Also
========

* :ref:`developer-guide-langgraph` — internals and code paths.
* :ref:`LangGraphAPI` — API reference.
