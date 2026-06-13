.. _DragonNativeSerializableC++:

Serializables
--------------

Serializable is a wrapper class used for serializing and deserializing C++
objects that will be used in conjunction with the :ref:`C++ DDict client
<DragonCoreCPPDictionary>` or the :ref:`C++ Queue < DragonNativeQueueC++>`.
Additionally, several more specific classes may be used for both Queues
and DDicts. The Serializable class definitions are provided here along
with a few *typedefs*/*using* definitions for common instances of
Serializables.

The DerivedSerializable documentation provides an example to be used when
writing your own subclasses of SerializableBase or any of the other classes.

.. doxygenclass:: dragon::DerivedSerializable
   :members:

.. doxygenclass:: dragon::Serializable

.. doxygenclass:: dragon::SerializableString
   :members:

.. doxygenclass:: dragon::SerializableScalar
   :members:

.. doxygenclass:: dragon::SerializableVector
   :members:

.. doxygenclass:: dragon::Serializable2DMatrix
   :members: