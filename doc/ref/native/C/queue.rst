.. _DragonNativeQueueC:

Queue in C
++++++++++

This is the Dragon Native interface to a queue in C.

C Structures and Constants
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. doxygenstruct:: dragonQueuePutStream_t
    :members:

.. doxygenstruct:: dragonQueueGetStream_t
    :members:

.. doxygenstruct:: dragonQueueDescr_t
    :members:

.. doxygenstruct:: dragonQueueSerial_t
    :members:

.. doxygenstruct:: dragonQueueAttr_t
    :members:


Queue Functions for C
^^^^^^^^^^^^^^^^^^^^^^^

Attribute Control
...................

.. doxygenfunction:: dragon_policy_init

.. doxygenfunction:: dragon_queue_attr_init

Managed Lifecycle Functions
.............................

.. doxygenfunction:: dragon_managed_queue_create

.. doxygenfunction:: dragon_managed_queue_attach

.. doxygenfunction:: dragon_managed_queue_destroy

.. doxygenfunction:: dragon_managed_queue_detach

Unmanaged Lifecycle Functions
..............................

.. doxygenfunction:: dragon_queue_create

.. doxygenfunction:: dragon_queue_serialize

.. doxygenfunction:: dragon_queue_attach

.. doxygenfunction:: dragon_queue_destroy

.. doxygenfunction:: dragon_queue_detach


Operational Functions
............................

.. doxygenfunction:: dragon_queue_put

.. doxygenfunction:: dragon_queue_get

.. doxygenfunction:: dragon_queue_put_open

.. doxygenfunction:: dragon_queue_put_write

.. doxygenfunction:: dragon_queue_put_close

.. doxygenfunction:: dragon_queue_get_open

.. doxygenfunction:: dragon_queue_get_read

.. doxygenfunction:: dragon_queue_get_readinto

.. doxygenfunction:: dragon_queue_get_close