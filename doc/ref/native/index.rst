Dragon Native
+++++++++++++

The following reference provides information about the Dragon Native API.  It is Dragon's top level API
intended for users that require all supported programming languages or extended functionality. The API has
bindings across Fortran, C, and C++ to the native Dragon run-time system. It is a cross-language API, so
objects created in one language are usable across processes and languages by sharing their names or unique
IDs. Dragon Native objects are available in :term:`mangaged <Managed Object>` and
:term:`unmanaged <Unmanaged Object>` version. As the naming suggests, unmanaged versions do not require
the Dragon runtime to run, but may have limited functionality. In particular, they are not garbage
collected and cannot be looked by :term:`name <Object Name>` or :term:`uid <Object UID>`.

Much of the API is inspired by Python Multiprocessing.

Reference by Language
=====================

.. toctree::
    :maxdepth: 2

    Python/index.rst

Specification by Language
=========================

The following specification documents the intended API in C, C++ and Fortran. It is subject to change.

.. toctree::
    :maxdepth: 2

    C/index.rst
    C++/index.rst
    Fortran/index.rst

Architecture
============

.. figure:: images/architecture.svg
    :scale: 75%
    :name: dragon-native-architecture

    **The Dragon native architecture**

Dragon native components use the Dragon Global Services Client API to implement
:term:`refcounted <Refcounted Object>`, :term:`managed <Managed Object>`, or
:term:`unmanaged <Unmanaged Object>` :term:`objects <Dragon Object>`.  Every
object on the Native API is made up of the four base components of the Dragon GS
Client API: processes, channels, (memory) pools and (hardware) nodes. The
life-cycle of these objects is in-turn managed by Dragons runtime services.