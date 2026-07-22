.. _DragonAPI:

API Reference
+++++++++++++

The best place to begin with Dragon is with :external+python:doc:`Python multiprocessing <library/multiprocessing>`.
Users move to the Dragon API directly as they find the need for more control, such as explicitly placing
processes or objects on CPU and GPU nodes, or they want to use other features in Dragon like the in-memory distributed
dictionary.

This reference is split into two layers. Most readers should begin with the
User API, which exposes Dragon's main capabilities through higher-level
interfaces for multiprocessing, data movement, AI, telemetry, workflows, and
native process orchestration. Move to the Low-level API when you need direct
control over the underlying runtime objects, infrastructure services, or client
abstractions that those user-facing APIs build on.

In practice, a simple rule works well: start with the User API unless you are
building new Dragon features, debugging runtime internals, or integrating with
lower-level services that are not surfaced in the higher-level interfaces.


User API
========

Choose the User API when you want to write or extend applications with Dragon
without managing runtime internals directly. These pages are usually the right
starting point for application developers.

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

Choose the Low-level API when you need to understand or control the runtime
implementation itself, such as infrastructure objects, client interfaces, or
core communication primitives.

.. toctree::
    :maxdepth: 2

    ref/inf/index.rst
    ref/client/index.rst
    ref/core/index.rst
