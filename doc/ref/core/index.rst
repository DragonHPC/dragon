.. _CoreAPI:

Core
++++

The Core API is for fundamental memory management and communication primitives on which all of Dragon is built.

Most application developers will not start here. The Core API is most useful
when you are working on Dragon internals, integrating with the lower-level
runtime primitives directly, or trying to understand the building blocks behind
the higher-level Native, multiprocessing, and data interfaces.

If you want the architectural explanation before diving into symbols, begin with
:ref:`runtime_design` in the developer guide.

.. _core_python_api:

Python Reference
================

.. currentmodule:: dragon

.. autosummary::
    :toctree:
    :recursive:

    dtypes
    managed_memory
    channels
    fli

Low-level Python Reference
==========================

.. currentmodule:: dragon

.. autosummary::
    :toctree:
    :recursive:

    pheap
    locks
    utils
    pmod

.. _core_c_api:

C Reference
===========

.. toctree::
    :maxdepth: 1

    c/managed_memory.rst
    c/channels.rst
    c/channelsets.rst
    c/fli.rst
