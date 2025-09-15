.. _DataAPI:

Data
++++

Dragon has APIs for managing data in a scalable fashion. A prominent member of
these APIs is the distributed dictionary. Descriptions and APIs are grouped
together for each of the supported Dragon Data Types.


Python Reference
================

In-Memory Distributed Dictionary
-----------------------------------

.. currentmodule:: dragon.data

.. autosummary::
    :toctree:
    :recursive:

    DDict

.. currentmodule:: dragon.data.ddict

.. autosummary::
    :toctree:
    :recursive:

    DDictError
    DDictSyncError
    DDictFullError
    DDictTimeoutError
    DDictPersistCheckpointError
    DDictKeyError
    DDictCheckpointSyncError
    DDictUnableToCreateError
    DDictFutureCheckpointError
    DDictManagerStats
    CheckpointPersister
    NULLCheckpointPersister
    PosixCheckpointPersister
    DAOSCheckpointPersister
    DDictKeysView
    DDictValuesView
    DDictItemsView
    FilterContextManager

Zarr Store
------------

.. currentmodule:: dragon.data.zarr

.. autosummary::
    :toctree:
    :recursive:

    Store


C Reference
===========

.. toctree::
    :maxdepth: 3

    C/ddict.rst

C++ Reference
===============

.. toctree::
    :maxdepth: 3

    CPP/dictionary.rst