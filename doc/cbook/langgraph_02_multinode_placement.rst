.. _cbook_langgraph_multinode:

02 — Multinode Placement
++++++++++++++++++++++++

This example pins each ``AgentHost`` to a chosen cluster node. A single
``launch_host`` call runs its agents as threads in one process; to spread agents
across nodes you issue **one ``launch_host`` per node**, each with its own
``Policy``.

**What you'll learn:**

* How a ``Policy`` places an ``AgentHost`` on a specific node
* Why distributing agents means one ``launch_host`` per node
* How to confirm placement from the runtime (node name + host PID)
* How ``launch_hosts`` places several hosts in one call

**Needs:** 2+ nodes.

Main Code
=========

.. literalinclude:: ../../examples/dragon_ai/langgraph/02_multinode_placement.py
    :language: python
    :linenos:
    :caption: **02_multinode_placement.py**

Placing several hosts in one call
=================================

When you have several hosts to place, ``launch_hosts`` is a convenience wrapper
that mirrors the Dragon convention of passing a list of policies — one per host,
applied in order:

.. code-block:: python

    hosts = executor.launch_hosts(
        hosts=[
            {"researcher": researcher},
            {"analyzer": analyzer, "writer": writer},
        ],
        policies=[policy_node0, policy_node1],
    )

This is equivalent to calling ``launch_host`` once per entry. ``policies`` must
be the same length as ``hosts`` (or ``None`` to use default placement), and the
returned list of processes is in the same order as ``hosts``.

See Also
========

* :ref:`developer-guide-langgraph` — internals and code paths.
* :ref:`LangGraphAPI` — API reference.
