MPBridge 
++++++++

The Dragon MPbridge component maps the Python Multiprocessing API onto :ref:`ref/native/index:Dragon Native`
components. This is done by introducing a new Python Multiprocessing start method "dragon" and a new
`DragonContext` that contains the Python Multiprocessing API.

Python Reference
================

.. currentmodule:: dragon

.. autosummary::
    :toctree:
    :recursive:

    mpbridge

Architecture
============

Components
----------

.. figure:: images/mpbridge_architecture.svg
    :scale: 75%

    **Figure 1: MPBridge architecture**

Designing the MPBridge component, we had the following goals in mind:

    1. Standard packages using Python multiprocessing (e.g. `concurrent.futures`, `numpy`) work as expected using the "Dragon" start method.
    2. The "dragon" start method can be switched out and standard multiprocessing can still be used.
    3. The MPBridge component only adapts the native Dragon API to Python Multiprocessing, but does not add
       new features. I.e. functionally it is the thinnest possible layer.
    4. The MPBridge provides the functionality so that all Python Multiprocessing unit tests that can be reasonably expected to pass, indeed pass.
    5. The MPBridge component maintains the same public interface as standard Multiprocessing, i.e. no public methods or attributes are added to classes.
    6. The public Multiprocessing API remains unchanged, i.e. no additional public attributes or methods are introduced.

To achieve these goals, we are inheriting from classes in Dragon Native, e.g. ``dragon.native.queue.Queue``
into ``dragon.mpbridge.queue.PatchedDragonNativeQueue``. Methods and attributes are added or overloaded
whenever possible to pass the Multiprocessing unit tests from the CPython package. Finally the
``dragon.mpbridge.queue.PatchedDragonNativeQueue`` is inherited by `dragon.mpbridge.queue.DragonQueue`,
`dragon.mpbridge.queue.DragonSimpleQueue` and `dragon.mpbridge.queue.DragonJoinableQueue`, where the API is
modified so that the final objects conform to Multiprocessing's standard API. To retain the ability to add
public attributes and methods in Dragon Native that do not exist in Python Multiprocessing, we are explicitly
breaking inheritance here and remove public attributes and methods where necessary to keep the public
Multiprocessing API stable. The removal is done using a decorator.

Example: The Dragon Queue
^^^^^^^^^^^^^^^^^^^^^^^^^

For example, the size of a queue in Dragon native is `q.size()`, while in Multiprocessing it is `q.qsize()`.
We created a private method `q._size()` and have `q.size()` wrap it in Dragon Native. In MPBridge, we then
remove the `q.size()` that DragonQueue has inherited from Dragon Native's queue and add `q.qsize()` in
DragonQueue that wraps the same private method. 

Next we show a class diagram of Dragons queue implementation and how it is inserted into the Multiprocessing package.

.. figure:: images/mpbridge_class_diagram.svg

    **Figure 2: Class diagram of the mpbridge.queue implementation.**

Dragon's native queue implementation resides in ``dragon.native.queue.Queue``. Its public interface is the sum
of the public interface of the three Python Multiprocessing Queues: ``Queue``, ``JoinableQueue`` and
``SimpleQueue``. The MPBridge component inherits from ``dragon.native.queue.Queue`` into
``dragon.mpbridge.queues.DragonQueu``, ``dragon.mpbridge.queue.DragonSimpleQueue`` and
``dragon.mpbridge.queue.DragonJoinableQueue``. The public API is modified accordingly, so that it conforms with the
Multiprocessing API. 
The MPBridge component also contains 3 functions (``Queue``, ``SimpleQueue`` and ``JoinableQueue``) that return the corresponding
classes. The are called from the ``DragonContext``. 
Just as in Multiprocessing, the methods below the context are exported during startup into the module API. The context itself
is part of a list of contexts held at the top level, containing a context per start method. Setting the start method then means setting
the ``DefaultContext`` equal to one of these contexts. To add our start method to this mechanism, we add an ``AugmentedDefaultContext`` 
that adds our start method to the list of possible start methods and overloads the ``set_start_method`` method.

