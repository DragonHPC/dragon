Dragon Batch
++++++++++++

The Batch service allows users to group a sequence of tasks (Python
functions, executables, or parallel jobs) into a single task that the
user can start and wait on. We will generally refer to this grouping of
tasks as *compiling* them into a single, compiled task. Users can
specify dependencies between tasks and the Batch service will
automatically parallelize them via an implicitly inferred directed
acyclic graph (DAG). The big picture idea is that the Batch service
allows users to think sequentially while reaping the benefits of a
parallelized work flow.

Below is a simple example using ``Batch`` to parallelize a list of
functions. In this example, dependencies actually force the functions to
execute serially, but it demonstrates the basics of the API.

::

   # Generate the powers of a matrix and write them to disk
   from dragon.workflows.batch import Batch
   from pathlib import Path
   from some_math_functions import gpu_matmul

   import numpy as np

   # A base directory, and files in it, will be used for communication of results
   batch = Batch()
   base_dir = Path("/some/path/to/base_dir")

   a = np.array([j for j in range(100)])
   m = np.vander(a)

   # Import a function to lazily run gpu_matful tasks in parallel. Notice that the
   # "ptd" in the file name refers to "parameterized task descriptor", since the yaml
   # file describes a collection of tasks that are parameterized by the arguments to
   # the returned function, gpu_matmul_func.
   get_file = lambda base_dir, i, j: Path(base_dir) / Path(f"file_{i + j}")
   gpu_matul_lazy = batch.import_func("gpu_matmul_ptd.yml", gpu_matmul, base_dir, get_file)

   results = []
   for i in range(1000):
       val = gpu_matmul_lazy(m, i)
       results.append(val)

   # If there was an exception while running the task, it will be raised when we call val.get()
   for val in results:
       try:
           print(f"{val.get()=}")
       except Exception as e:
           print(f"gpu_matmul failed with the following exception: {e}")

   batch.close()
   batch.join()

Notice that the call to ``import_func`` requires a file called
“gpu_matmul_ptd.yml”, which is a Parameterized Task Descriptor, or PTD,
file. This type of file describes a *collection* of tasks, rather than a
single task. The collection of tasks are parameterized by the arguments
to the function returned by ``import_func``: each set of arguments
generates a new task. Generated tasks are not run immediately, but
rather are queued up in the background and run lazily in parallelized
batches. Batches of tasks only run when you (1) call
``<task return value>.get()``, try to access either a distributed
dictionary or file that’s updated by a task, or call ``batch.fence()``.
The PTD file “gpu_matmul_ptd.yml” is shown directly below, and a PTD
template (with lots of explanations) is located in the examples
directory.

::

   ########################### import-time and call-time arguments specified here ###########################

   # these arguments are passed to import_func
   import_args:
     # function to run
     - gpu_matmul
     # base directory of files that will be accessed
     - base_dir
     # function to select file
     - get_file

   # import_kwargs:

   # these arguments are passed to gpu_matmul_lazy
   args:
     - matrix
     - i

   # kwargs:

   ######### definition of task executables, resources, dependencies, etc. (in terms of args above) #########

   # type of task (should be function, process, or job)
   type: function

   executables:
     # here we "plug in" the names of the function and its arguments (specified above)
     - target: gpu_matmul
       args:
         - matrix
         - i

   reads:
     - files:
       - get_file:
         - base_dir
         - i
         - 0

   writes:
     - files:
       - get_file:
         - base_dir
         - i
         - 1

   name: gpu_matmul_task

   timeout:
     day: 0
     hour: 0
     min: 0
     sec: 30

And here’s the same example as above, but done in a more manual way
without a PTD file or batching tasks in the background.

::

   # Generate the powers of a matrix and write them to disk
   from dragon.workflows.batch import Batch
   from pathlib import Path
   from some_math_functions import gpu_matmul

   import numpy as np

   # A base directory, and files in it, will be used for communication of results
   batch = Batch(background_batching=False)
   base_dir = Path("/some/path/to/base_dir")

   # Knowledge of reads and writes to files will also be used by the Batch service
   # to determine data dependencies and how to parallelize tasks
   get_read = lambda i: batch.read(base_dir, Path(f"file_{i}"))
   get_write = lambda i: batch.write(base_dir, Path(f"file_{i+1}"))

   a = np.array([j for j in range(100)])
   m = np.vander(a)

   # batch.function will create a task with specified arguments and reads/writes to the file system
   get_task = lambda i: batch.function(gpu_matmul, m, base_dir, i, reads=[get_read(i)], writes=[get_write(i)], timeout=30)

   # Package up the list of tasks into a single compiled task and create the DAG (done by batch.compile),
   # and then submit the compiled task to the Batch service (done by matrix_powers_task.start)
   serial_task_list = [get_task(i) for i in range(1000)]
   matrix_powers_task = batch.compile(serial_task_list)
   matrix_powers_task.start()

   # Wait for the compiled task to complete
   matrix_powers_task.wait()

   # If there was an exception while running the task, it will be raised when get() is called
   for task in serial_task_list:
       try:
           print(f"result={task.result.get()}")
           # print(f"stdout={task.stdout.get()}")
           # print(f"stderr={task.stderr.get()}")
       except Exception as e:
           print(f"gpu_matmul failed with the following exception: {e}")

   batch.close()
   batch.join()

The ``Batch.compile`` operation assumes that the order of functions in
the list represents a valid order in which a user would manually call
the functions in a sequential program. Given the list of functions,
``Batch.compile`` will produce a DAG that contains all the information
needed to efficiently parallelize the function calls. Calling
``matrix_powers_task.start`` will submit the *compiled* task to the
Batch service, and calling ``matrix_powers_task.wait`` will wait for the
completion of the task. The functions ``Batch.close`` and ``Batch.join``
are similar to the functions in ``multiprocessing.Pool`` with the same
names–``Batch.close`` lets the Batch service know that no more work will
be submitted, and ``Batch.join`` waits for all work submitted to the
Batch service to complete and for the service to shut down.

Individual (i.e., non-compiled) tasks can also be submitted to the Batch
service, but batching tasks together via ``Batch.compile`` will
generally give better performance in terms of task scheduling overhead.
There is no guaranteed ordering between separate tasks submitted to the
Batch service. So, for example, if a user submits several compiled and
non-compiled tasks to the Batch service, they will be executed in
parallel and in no particular order.

Any mix of Python functions, executables, and parallel jobs can be
submitted to the Batch service simulataneously, and dependencies can
exist between tasks of any type, e.g., an MPI job can depend on the
completion of a Python function if the MPI job reads from a file that
the function writes to. MPI jobs are specified using the ``Batch.job``
function, which will create a task that allows the user to run the
specified job. Likewise, the ``Batch.process`` function creates a task
for running a serial executable. Avoid using the ``Task`` class directly
to create tasks.

All tasks, regardless of the type of code that they run, have the same
interface: ``Task.start`` to start a task without waiting for its
completion; ``Task.wait`` to wait for the completion of a task;
``Task.run``, a blocking variant of ``Task.start``, to both start a task
and wait for its completion; and handles for getting the result, stdout,
or stderr of a task. Tasks have three handles for obtaining output:
``result``, ``stdout``, and ``stderr``, all of type ``AsyncDict``.
Calling the ``get`` method for any of these handles gets the associated
value, and waits for the completion of the task if necessary. If an
exception was thrown during the execution of a task, then calling
``AsyncDict.get()`` for the ``result`` handle of the task will raise the
same exception that was thrown by the task.

The initial creation of the ``Batch`` object sets up manager and worker
processes that implement an instance of the Batch service. ``Batch``
objects can be passed between processes to allow multiple clients to use
the Batch service. Unpickling a ``Batch`` object at a destination
process will register the new ``Batch`` client with the Batch service
and allow the user to submit tasks to it. All clients must call
``Batch.close`` to indicate that they are done with the Batch service.
Only the primary client (which created the initial ``Batch`` object)
needs to call ``Batch.join``. Note that ``Batch.join`` will block until
all clients have called ``Batch.close``.

Data Dependencies
=================

Dependencies between tasks are inferred based on the data being read and
written by each task. Data reads and writes are specified by
``Batch.read`` and ``Batch.write`` (or ``Task.read`` and ``Task.write``,
to directly associate reads and writes with a specific task). When a
task is created, a list of Read or Write objects, created by
``Batch.read`` and ``Batch.write``, can be specified:

::

   task = batch.job(target=mpi_exec, num_procs=256, reads=<list of Reads>, writes=<list of Writes>)

After a task has been create, further Reads and Writes can be specified
via ``task.read`` and ``task.write``:

::

   task.read(base_dir1, file_path1)
   task.write(base_dir2, file_path2)

The Batch service takes a *dataflow* approach to parallelizing tasks.
Like all dataflow systems, it assumes that tasks have no `side
effects <https://en.wikipedia.org/wiki/Side_effect_(computer_science)>`__
(think IO) beyond those specified by ``Batch.read`` and ``Batch.write``
calls. So, for instance, if a process task reads from a file, then a
corresponding Read object must be created using ``Batch.read`` and
appended to the list of Reads when creating the task (or added after
task creation using ``task.read``). If this Read isn’t associated with
the task, and the task is part of a compiled task, then the file read
could happen out-of-order relative to other operations on that file,
e.g., the file read could occur before the data intended to be read is
written to the file.

.. raw:: html

   <!---
   If your tasks do any IO beyond these three options, and you feel comfortable with dataflow concepts, then the Batch service provides
   custom *communication objects* that can help you with your use case. A communication object can be a Distributed Dictionary, a `Path`
   specifying a directory in a file system, a Queue, or a user-defined communication object. A *channel* is an abstaction for keys in a
   Distributed Dictionary, files in a directory, etc. There can be one or more channels associated with a communication object, i.e.,
   there are many keys for a Distributed Dictionary, but only a single default channel for a Queue. A communication object and zero or
   more communication channels are passed to `Batch.read` and `Batch.write`. A custom communication object can be created using the
   `CommunicationDomain` and `CommunicationObject` classes. A `CommunicationDomain` specifies possible [data hazards](https://en.wikipedia.org/wiki/Data_dependency),
   e.g., read-after-write, write-after-write, etc. For example, you can add socket communication dependencies to a task by associating
   an IP address with a communication domain, and ports with comunication channels within that domain:

       socket_domain = CommunicationDomain(rar=True, raw=True, war=True, waw=True)
       socket_object = CommunicationObject(socket_domain, ip_addr)
       socket_read = batch.read(socket_object, port_1)
       socket_write = batch.write(socket_object, port_2)
       task = batch.function(my_func, args, reads=[socket_read], writes=[socket_write])
   -->

Argument-passing Dependencies
=============================

Beyond the type of dependencies described above, there is a second type
of dependency: argument-passing dependencies. These dependencies are
inferred when a ``result``, ``stdout``, or ``stderr`` object is passed
as an argument to a ``Batch`` ``Function``, ``Process``, or ``Job``. For
example:

::

   def foo():
       return "Hi-diddly-ho!!!"

   def bar(hello: str):
       print(hello, flush=True)

   batch = Batch()

   task_foo = batch.function(foo)
   task_bar = batch.function(bar, task_foo.result)

   batch.compile([task_foo, task_bar]).run()
   # prints: "Hi-diddly-ho!!!"
   print(f"{task_bar.stdout.get()}", flush=True)

   batch.close()
   batch.join()

In the above example, the function ``bar`` will not run until ``foo``
completes, and ``bar`` will print the string returned by ``foo``.

Distributed Dictionary
======================

The Distributed Dictionary service provides a scalable, in-memory
distributed key-value store with semantics that are generally similar to
a standard Python dictionary. The Distrbuted Dictionary uses shared
memory and RDMA to handle communication of keys and values, and avoids
central coordination so there are no bottle-necks to scaling. Revisiting
an example above for the Batch service, we will replace the file system
as a means of inter-task communication with a Distributed Dictionary.
The only part that needs to be updated is the creation of subtasks for
the compiled task–everything from the ``Batch.compile`` call and down is
the same.

::

   # Generate the powers of a matrix and write them to a distributed dictionary
   from dragon.workflows.batch import Batch, DDict
   from some_math_functions import gpu_matmul

   import numpy as np

   # The Distributed Dictionary service will be used for communication of results
   batch = Batch()
   ddict = batch.ddict()

   # Knowledge of reads and writes to the Distributed Dictionary will also be used by
   # the Batch service to determine data dependencies and how to parallelize tasks
   get_read = lambda i: batch.read(ddict, i)
   get_write = lambda i: batch.write(ddict, i + 1)

   a = np.array([i for i in range(100)])
   m = np.vander(a)

   # batch.function will create a task with specified arguments and reads/writes to the
   # distributed dictionary
   get_task = lambda i: batch.function(gpu_matmul, (m, ddict, i), [get_read(i)], [get_write(i)])

The code for ``gpu_matmul`` might look something like this.

::

   def gpu_matmul(original_matrix: np.ndarray, ddict: DDict, i: int):
       # read the current matrix stored in the Distributed Dictionary at key=i
       current_matrix = ddict[i]

       # actual matrix multiplication happens here
       next_matrix = do_dgemm(original_matrix, current_matrix)

       # write the next power of the matrix to the Distributed Dictionary at key=i+1
       ddict[i + 1] = new_matrix

The Distributed Dictionary provides synchronization mechanisms to make
it possible for separate clients of the dictionary to effectively
cooperate when reading and writing data. For example, a
``wait_for_keys`` flag can be set when creating a Distributed
Dictionary, which allows readers of a specific key to block until
another client writes a value for that key. The Distributed Dictionary
also makes it easy to co-locate compute with data, reducing the amount
of network traffic required to access your data.

Queue
=====

The Queue service provides a distributed queue interface that’s similar
to Python’s multiprocessing.Queue, but can be accessed anywhere in your
deployment. Queues are easy to use with a simple put/get interface, and
blazing performance through shared-memory and a high bandwidth, low
latency RDMA network. Unlike the Distributed Dictionary, Queues are not
intended to be fault-tolerant, so the Distributed Dictionary will be the
right choice for applications requiring resiliency.

Multiple Clients
================


.. dbg does a Dragon runtime/deployment have it's own allocation of
   network/storage? how are these partitioned?

Instances of the Batch, Distrbuted Dictionary, and Queue services are
all represented by Python objects. All services allow for multiple
clients, i.e., a ``Batch`` object can be passed over a Queue,
Distributed Dictionary, or other communication mechanism to separate
Python processes, and used simultaneously by all processes that have a
copy of the object. Each client of an instance of the Batch service can
submit tasks independently to it, and tasks will be processed on a
“first come, first serve” basis. Each instance of the Batch service is
allocated its own share of the compute and network resources, and all
clients of the Batch instance share the same set of resources.

Limitations
===========

Fault-tolerance and Elasticity
------------------------------

In the future, the Batch service will be both fault-tolerant and
elastic, i.e., the available resources for compute will expand as
necessary based on the load and specified user requirements.

Further Information
===================

See the ``examples`` directory for examples of how to use the Batch,
Distributed Dictionary, and Queue services. A more extensive `user
guide <https://curly-bassoon-qzp7w5j.pages.github.io/guides/user/services/dragon_batch/>`__
for the Batch service is available. All cloud services are based on the
`Dragon distributed runtime <dragonhpc.org>`__. Check out the link for
examples and documentation for the Dragon runtime.
