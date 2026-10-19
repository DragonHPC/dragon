.. _serializables:

Shared Data and Objects in C++ and Python
+++++++++++++++++++++++++++++++++++++++++

Dragon not only supports running Python code in a distributed multi-node
environment, processes executing C++ compiled binaries are also supported.
The code in this tutorial is from on an example program that
is available to run in its entirety in our examples directory. The code is provided at
https://github.com/DragonHPC/dragon/tree/main/examples/dragon_native/serializable_heat
and in :ref:`heat_simulation` and :ref:`heat_worker` for your reference. The goal of
this program is to show how data and Dragon objects can be shared between Python and C++.

Data and Dragon objects may be serialized, the serialized description can then
passed to a new process (via some means) and then attached in the new process.
This is a common means of sharing within Dragon. Dragon objects are designed to
be shared between processes. In this example the main data sharing is done with
an n-dimensional array representation that can be passed between processes and
synced to a common location within a Dragon distributed dictionary (i.e. DDict).

The organization of most C++/Python hybrid programs run in the Dragon distributed
multi-processing framework is to have a Python orchestration program and C++
worker programs. Typically there is some setup first in the orchestration program
which creates objects to be shared and then starts the C++ worker programs. In
the Python orchestrator, setup proceeds as follows.

Orchestration Setup
--------------------

.. literalinclude:: ../../examples/dragon_native/serializable_heat/heat_simulation.py
  :language: python
  :caption: Orchestration Code
  :name: heat1
  :linenos:
  :start-after: begin-heat-orc-setup
  :end-before: end-heat-orc-setup

To demonstrate sharing a DDict between Python and C++ a DDict is created in the
first few steps in the :ref:`heat1` snippet that will be used to share config
data and will also be used as the central storage location for the 2D array used
in this simulation.

The DDict is serialized in anticipation of passing the serialized descriptor to
the C++ workers. The call to `pickler` on the `store` tells Python to create an
alias for the `store` DDict called `config` that uses the XPickler for
serializing keys and values stored within it. The XPickler is a Python pickler
that supports pickling and unpickling between Python and C++. Anything stored or
retrieved using the `config` alias is going to be readable/writable in both C++
and Python.

To demonstrate this cross language compatibility the 2D grid is stored in the
DDict along with a barrier, semaphore, and a queue. All of these objects are
supported in both Python and C++. Once stored, and with the C++ workers being
given the serialized DDict when they are started, all processes will be able to
access all the config data.

One thing to highlight is that the `grid` NDArray, when it is created, is given
the `ser_store` serialized representation of the DDict object. This is required
of NDArrays because they are stored within a DDict and when sent to another
process, only metadata about the `ndarray` is communicated. The actual data,
which may be large, is not copied between processes until absolutely necessary.
In this way, the `NDArray` provides lazy, last minute loading of data.

Not shown here a few more details are stored in the DDict providing each worker
with some additional process-specific code. Then each process is started using
the Dragon native `Popen` code. This is not the standard Posix Popen. The Dragon
native `Popen` will distributed the processes on all your nodes in a round-robin
fashion by default. This could be further refined if the programmer were to
provide policies to control process placement. Please refer to the
:ref:`policy-placement` documentation for more information. In addition, for more
control over groups of processes, there is a :ref:`ProcessGroup <NativeProcess>`
within the Dragon Native API that provides more control and faster launch of large
numbers of processes.

Worker Setup
--------------

.. literalinclude:: ../../examples/dragon_native/serializable_heat/heat_worker.cpp
  :language: C++
  :caption: Worker Code
  :name: heat2
  :linenos:
  :start-after: start-attaching-cpp
  :end-before: end-attaching-cpp

The workers connect to the shared config by creating a DDict using the
`ser_config` passed to the process when it starts as shown in the :ref:`heat2` snippet.
Once attached to the DDict, the workers can retrieve their config data from the
DDict in much the same way it was stored in it in the first place. This just
works because of a base class of Serializable objects and a collection of
Serializable subclasses. What's nice is that much of the conversion to
serializable objects is done automatically for the programmer. There are
serializables for strings, integers, doubles, Dragon objects, and the
n-dimensional array that is attachd when `grid` is initilialized. The `barrier`,
`ready` semaphore, and the `results` queue are all attachable right from there
DDict values: deceptively simple that makes it very simple to share objects and
data between C++ and Python. See the :ref:`DragonNativeC++` for more information
on all these classes of objects.

One thing to note is that the C++ workers and the Python orchestrator all fully
participate in the Barrier in the program. One barrier waiter is the Python
orchestrator, the others are the C++ workers. Dragon's barrier implementation,
which is also a part of multiprocessing, works in both C++ and Python.

The n-dimensional array support deserves a bit more description. In Python,
an `ndarray` is a `numpy` n-dimensional array and so all `ndarray` operations
work on it. The `numpy` object is wrapped in a Dragon native `NDArray` object
(which inherits from `ndarray` - so it is still an `ndarray` when wrapped). When
provided to C++, it becomes a Dragon C++ `SerializableNDArray` object as shown
above where `band` is initialized. When initialized like this a copy of the n-dimensional
array data is lazily cached as soon as it is indexed.

The NDArray, when it is serialized and passed to C++, or (not present in this
example) passed between C++ processes through a Queue or a DDict, contains all
the meta-information about the n-dimensional array. NDArray and
SerializableNDArray are designed to support n-dimensional arrays of any given
dimension and remember their dimensionality as they are passed between processes.

.. literalinclude:: ../../examples/dragon_native/serializable_heat/heat_worker.cpp
  :language: C++
  :caption: Slicing
  :name: heat3
  :linenos:
  :start-after: slice-start
  :end-before: slice-end

The :ref:`heat3` snippet provides code that is used to slice the grid and the band given as
work to each of the workers. A slice on an NDArray in C++ shares the same underlying
cached data, so later, when the `band` has `sync` called on it, it is then copied back
to the backing DDict that holds the shared copy of the data. Back in the orchestrator
process, `refresh` is called at the appropriate time to refresh the band and then
the various bands are copied back into the shared `grid` data by the orchestrator
program as shown in the :ref:`heat4` snippet.

.. literalinclude:: ../../examples/dragon_native/serializable_heat/heat_simulation.py
  :language: python
  :caption: Copying Back to Grid
  :name: heat4
  :linenos:
  :start-after: begin-grid-copyback
  :end-before: end-grid-copyback




Full Program
---------------

The full program comes in two parts, the Python orchestration code and the C++ worker code.

Python Orchestration Code
___________________________

.. literalinclude:: ../../examples/dragon_native/serializable_heat/heat_simulation.py
  :language: python
  :caption: Heat Simulation Python Orchestration Code
  :name: heat_simulation
  :linenos:

C++ Worker Code
___________________________

.. literalinclude:: ../../examples/dragon_native/serializable_heat/heat_worker.cpp
  :language: C++
  :caption: Heat Simulation C++ Worker Code
  :name: heat_worker
  :linenos:
