.. _cbook_langgraph_locality:

05 — Data-Locality Crossover
++++++++++++++++++++++++++++

The second hero example, and the one that proves *why* the data-heavy version holds
at GB scale. Bulk fields live in a distributed ``DDict`` (node-local); control-plane
and data-plane agents are pinned by ``Policy``; reducers reduce **in place**. The
example prints the funnel-vs-handles crossover table.

**What you'll learn:**

* How bulk data stays node-local in a distributed ``DDict``
* How to place data-plane agents where the data already lives
* How to read the funnel-vs-handles crossover and locality receipts

**Needs:** 1 node (more nodes show locality).

Main Code
=========

.. literalinclude:: ../../examples/dragon_ai/langgraph/05_data_locality_crossover.py
    :language: python
    :linenos:
    :caption: **05_data_locality_crossover.py**

See Also
========

* :ref:`developer-guide-langgraph` — internals and code paths.
* :ref:`LangGraphAPI` — API reference.
