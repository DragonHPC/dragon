.. _DragonNativeSerializableC++:

Serializable
--------------

Serializable is an abstract base class used for serializing and deserializing C++
objects that will be used in conjunction with the :ref:`C++ DDict client
<DragonCoreCPPDictionary>` or the :ref:`C++ Queue < DragonNativeQueueC++>`.

The DerivedSerializable documentation provides an example to be used when
writing your own subclasses of Serializable.

.. doxygenclass:: dragon::Serializable

.. doxygenclass:: dragon::DerivedSerializable
   :members:

.. doxygenclass:: dragon::SerializableInt
   :members:

.. doxygenclass:: dragon::SerializableDouble
   :members:

.. doxygenclass:: dragon::SerializableString
   :members:

.. doxygenclass:: dragon::SerializableDouble2DVector
   :members: