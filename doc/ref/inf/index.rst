.. _InfrastructureAPI:

Infrastructure
++++++++++++++

:ref:`Services` in the Dragon runtime interact with each other using messages transported with a variety of
different means (mostly :ref:`Channels`). Although there is the Client API to construct and send these
messages, the messages themselves constitute the true internal interface. To that end, they are a convention.
Developers should use this API to add functionality to the Dragon Services through new messages.
It is not meant for users.

This reference is aimed at contributors extending the runtime itself, not at
application authors. If you are building normal Dragon applications, stay with
the User API and the Native API unless you specifically need to reason about
runtime descriptors, service messages, or infrastructure parameters.

For the broader system design and service relationships, see
:ref:`runtime_design` in the developer guide before using this reference.


Python Reference
================

.. currentmodule:: dragon.infrastructure

.. autosummary::
    :toctree:
    :recursive:

    channel_desc
    facts
    gpu_desc
    group_desc
    messages
    node_desc
    parameters
    pool_desc
    process_desc
    standalone_conn
    util

C Reference
===========

.. toctree::
    :maxdepth: 1

    logging
