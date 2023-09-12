Multiprocessing with Dragon
+++++++++++++++++++++++++++

Many Python packages use the
`Python Multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_
package to provide low-level process parallelism to many other packages like
`concurrent.futures <https://docs.python.org/3/library/concurrent.futures.html>`_
and `joblib <https://joblib.readthedocs.io/en/latest/>`_ .

Python Multiprocessing with Dragon is designed to comply with the base
implementation as much as can reasonably be done given the differences in
implementations. Dragon is tested against the standard multiprocessing unit
tests to verify compliance. Most user applications can run with Dragon with very
few changes.

To use Python Multiprocessing with Dragon, you need to:


#. Import the Dragon Python Module **before** you import the Python Multiprocessing module.
#. Set the Dragon start method to "dragon".
#. Run your program with the `dragon` binary.

This blueprint

.. code-block:: python
  :linenos:

  import dragon
  import multiprocessing as mp

  # your imports here

  # your classes and functions here

  if __name__ == "__main__":
    mp.set_start_method("dragon")

    # your code here

illustrates the required changes. Note that the start method has to be set only once in the
beginning of the program.

Run your code with:

.. code-block:: console

  $> dragon dragon_script.py

See also :ref:`uguide/running_dragon:Running Dragon`.

Limitations
===========

Even though our goal is to make Multiprocessing "just work" out of the box, the
following limitations apply.

Multi-node Functionality
---------------------

The following components of the Multiprocessing API have limited multi-node support at the moment:

*  `multiprocessing.synchronize.Barrier`


Missing Parts of the Multiprocessing API
----------------------------------------

The following components of the Multiprocessing API are currently not supported by the Dragon start method and will
raise a `NotImplementedError` when used:

* `multiprocessing.manager`
* `multiprocessing.Value`
* `multiprocessing.Array`
* `multiprocessing.sharedctypes`
* `multiprocessing.Listener`
* `multiprocessing.Client`
* `multiprocessing.get_logger()`, `multiprocessing.log_to_stderr())`
* `multiprocessing.dummy`

Inheritance and Multiple Start Methods
--------------------------------------

Your code will break, if you inherit from Multiprocessing classes and switch the start method
from Dragon to another start method like `spawn` during program execution.
The following code will not work:

.. code-block:: python
  :linenos:

  import dragon
  import multiprocessing as mp

  class Foo(mp.queues.Queue):

    def bar(self):
      pass

  if __name__ == "__main__":

      mp.set_start_method("dragon")
      f1 = Foo() # works
      f1.put("test")
      test = f1.get()

      mp.set_start_method("spawn")
      f2 = Foo() # will likely break
      f2.put("test")

When the `import dragon` statement is executed, Dragon will replace all standard
Multiprocessing classes with Dragon equivalent classes before CPython resolves
the inheritance tree. This works well so long as you do not try to switch the
multiprocessing start method mid-application. That is, Dragon will not fix the
inheritance tree, replacing the original multiprocessing classes, after switching
the start method for class `Foo` in the example above. Indeed, class Foo will
still be inherited from `dragon.mpbridge.queues.DragonQueue`, not
`multiprocessing.queues.Queue`.

If you truly need to switch start methods mid-stream, then you can use
`multiprocessing.get_context()` to obtain objects from the current
Multiprocessing context:

.. code-block:: python
  :linenos:

  import dragon
  import multiprocessing as mp

  class Foo:

    def __init__(self):
      ctx = multiprocessing.get_context()
      self.q = ctx.Queue()

    def bar(self):
      pass

  if __name__ == "__main__":

      mp.set_start_method("dragon")
      f1 = Foo() # works
      f1.q.put("test")
      test = f1.q.get()

      mp.set_start_method("spawn")
      f2 = Foo() # works
      f2.q.put("test")

Sharing File Descriptors among Dragon Processes
-----------------------------------------------

In some circumstances, Python Multiprocessing allows child processes to use file
descriptors of the parent process. It does so by introducing a custom reducer in
`multiprocessing.reduction` that duplicates the parents file descriptor for the
child. This is used for example to pickle and share `multiprocessing.heap.Arena`
objects among a set of processes.

Dragon does not support sharing file descriptors among processes due to the fact
that Dragon processes are generally intended to run across distributed or
:term:`federated systems <Federated System>`.

The following methods in `multiprocessing.reduction` will raise a
`NotImplementedError`:

* `DupFd`, used to duplicate file descriptors during unpickling.
* `sendFds`, used to send file descriptors to other processes.
* `recvFds`, used to recv file descriptors from other proceses.

We have rewritten the parts of Multiprocessing that use this custom reducer to
use Dragon Managed Memory allocations.  If your program uses file
descriptors to share a common memory space among processes you will have to
rewrite that part of it, ideally using `dragon.managed_memory`.

Multiprocessing and Dragon without Patching
===========================================

If you want to use Python Multiprocessing with the Dragon Core libraries, but
avoid patching altogether, you can simply omit starting your code with the
`dragon` binary. The Dragon core library can still be imported via e.g. `from
dragon.managed_memory import MemoryPool` and used. In this case, the `"dragon"`
start method must not be set. The infrastructure will not be started.

Note that Dragon Core objects are :term:`unmanaged objects <Unmanaged Object>`
and not :term:`transparent <Transparency>` without the infrastructure services
running, i.e. you have to rely on pickling/serializing to share the
:term:`Serialized Descriptor` with other processes on the same node. Global name
or uid resolution and garbage collection cannot be provided anymore.

Note that all other parts of the Dragon stack, in particular the
:ref:`ref/native/index:Dragon Native` API require the running Dragon
infrastructure and are thus not supported without patching Multiprocessing.

.. code-block:: python
  :linenos:
  :caption: **A Python program using the Dragon core libraries without the infrastructure. It can be run with the standard Python executable `python3`**

  import multiprocessing as mp
  from dragon.managed_memory import MemoryPool
  from dragon.channels import Channel, Message

  if __name__ == "__main__":

    mp.set_start_method("spawn")
    q = mp.Queue()

    m_uid = 2
    c_uid = 3
    mpool = MemoryPool(1024**3, "MyDragonCoreMemPool", m_uid)
    ch = Channel(mpool, c_uid)