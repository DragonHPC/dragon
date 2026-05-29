.. _DragonAPI:

API Reference
+++++++++++++

The best place to begin with Dragon is with :external+python:doc:`Python multiprocessing <library/multiprocessing>`.
Users move to the Dragon API directly as they find the need for more control, such as explicitly placing
processes or objects on CPU and GPU nodes, or they want to use other features in Dragon like the in-memory distributed
dictionary.


User API
========

.. toctree::
    :maxdepth: 2

    ref/mpbridge/multiprocessing.rst
    ref/data/index.rst
    ref/telemetry/index.rst
    ref/ai/index.rst
    ref/workflows/index.rst
    ref/native/index.rst
    ref/policy.rst


Low-level API
=============

.. toctree::
    :maxdepth: 2

    ref/inf/index.rst
    ref/client/index.rst
    ref/core/index.rst
