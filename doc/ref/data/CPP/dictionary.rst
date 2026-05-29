.. _DragonCoreCPPDictionary:

The Distributed Dictionary C++ Client
=======================================

Dragon Distributed Dictionary supports both C and C++ user APIs, allowing both C and C++ clients to
interact with the dictionary like a map. The internal messaging of both clients is shared with the
Python client. In fact, a Python DDict manager handles the interaction from C and C++ clients.

A user must create the DDict in Python, serialize the DDict, and pass the serialized descriptor
to C++ code which can then attach to it. See :ref:`dragon.native.Popen <NativeProcess>` for the means to start a C++
program within Dragon. Please note that creating multiple client processes inside a single C/C++ program using
`fork()` is not supported in Dragon Distributed Dictionary. You must create client processes
from the Python API and attach to the dictionary in the C++ program.

When using the C++ DDict, keys and values must implement the :ref:`Serializable Interface<DragonNativeSerializableC++>`
which makes writing serialization/deserialization code for objects in C++ easily done
by using the FLI send and receive operations. See :ref:`The FLI API<DragonFileLikeInterface>`.
Some example Serializables are provided and can be found in the
`header file <https://github.com/DragonHPC/dragon/blob/main/src/include/dragon/serializable.hpp>`_
and the `implementation of Serializable <https://github.com/DragonHPC/dragon/blob/main/src/lib/serializable.cpp>`_.
The template provided here is not mean to be instantiated, but provides an outline of what you
would want to write. Documentation for Serializables can be found :ref:`here <DragonNativeSerializableC++>`.

.. literalinclude:: ./../../../../src/include/dragon/serializable.hpp
   :language: C++
   :linenos:
   :lines: 46-124
   :caption: A Serializable Example

:numref:`cpp_store_get` demonstrates how a DDict can be attached and values can
be stored and retrieved to/from it. Other example code can be found in
`test code for the C++ client <https://github.com/DragonHPC/dragon/blob/main/test/native/cpp_ddict.cpp>`_.

.. _cpp_store_get:

.. code-block:: C++
   :linenos:
   :caption: Storing a key/value pair in a DDict using C++

   void my_fun(const char * ddict_ser) {
      uint64_t manager_id;
      SerializableInt x(6); // key
      SerializableInt y(42); // value
      // This demonatrates attaching to a serialized DDict. The ddict_ser
      // would be provided to the program via a command-line argument or some
      // other means (like a Queue).
      DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
      manager_id = dd.which_manager(x); // Sample code, not needed here.
      dd[x] = y; // Stores the key x and maps it to y in the DDict dd.
      SerializableInt z = dd[x]; //Looks up key x to find its value.
      assert (z.getVal() == 42);
   }

C++ API Reference
====================

.. doxygenclass:: dragon::DDict
   :members: