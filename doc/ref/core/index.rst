.. _CoreAPI:

Core
++++

The Core API is for fundamental memory management and communication primitives on which all of Dragon is built.

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