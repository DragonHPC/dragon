.. _DragonNativeQueueC++:

Queue
---------

The Queue class in C++ is a typed queue of serializables. Queues in C++ send one
type of :ref:`Serializable <DragonNativeSerializableC++>` through them. The
Serializable type you write could be a base class of many types of subclasses.
There is a lot of power in this allowing virtually any type of value to be passed
through the Queue with the proper serialization and deserialization.

.. code-block:: C++
    :linenos:
    :caption: Attaching to and Sending and Receiving on a Queue

    dragonError_t err;

    // attach to a queue provided in queue_ser. You would get
    // queue_ser as a command-line argument for intance.
    Queue<SerializableInt> q(queue_ser);
    SerializableInt x(25);
    q.put(x);
    q.put(x);
    SerializableInt y = q.get();
    assert(y.getVal() == x.getVal());
    y = q.get();
    assert(y.getVal() == x.getVal());

The Queue can be instantiated from multiple C++ processes and
sending and receiving can occur across multiple nodes and multiple
processes all at the same time. The Queue is truly a multiprocessing
Queue since there can be multiple senders and receivers at any point
in time.

The lifetime of the Queue is determined by the Python code that created
the Queue. The Python code would make sure that it didn't exit or otherwise
destroy the Queue before the C++ code was done using it.

.. doxygenclass:: dragon::Queue
   :members: