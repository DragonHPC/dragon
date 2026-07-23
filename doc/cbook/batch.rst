.. _batch:

Graph-based Execution with Batch
++++++++++++++++++++++++++++++++

:py:class:`~dragon.workflows.batch.Batch` allows users to submit Python functions, executables, and
parallel jobs for distributed execution with automatic parallelization. Simply call
:py:meth:`~dragon.workflows.batch.Batch.function`, :py:meth:`~dragon.workflows.batch.Batch.process`,
or :py:meth:`~dragon.workflows.batch.Batch.job` to submit tasks, and
:py:class:`~dragon.workflows.batch.Batch` dispatches them to workers in the background automatically.
Users specify data dependencies between tasks via :py:meth:`~dragon.workflows.batch.Batch.read` and
:py:meth:`~dragon.workflows.batch.Batch.write` calls, and :py:class:`~dragon.workflows.batch.Batch`
infers an implicit directed acyclic graph (DAG) to maximize parallelism. Results are retrieved by
calling ``.get()`` on a task's handle, which blocks until
the task completes. Use :py:meth:`~dragon.workflows.batch.Batch.fence` to wait for all currently
submitted tasks to finish before proceeding, and :py:meth:`~dragon.workflows.batch.Batch.clear_results`
to free the memory used to hold task results (implicitly calls :py:meth:`~dragon.workflows.batch.Batch.fence`).
Task results remain in Batch's internal results DDict until ``.get()`` fetches them or
``clear_results()`` discards them; Batch teardown does not automatically copy unfetched
results back to the client.

Batch always creates and owns its internal results DDict. By default it allocates one gibibyte
per requested node for that store. For result-heavy workloads or tighter memory budgets, pass
``results_ddict_mem`` in bytes when constructing the Batch, for example
``Batch(results_ddict_mem=8 * 1024**3)``.

Below is a simple example using :py:class:`~dragon.workflows.batch.Batch` to parallelize a list of
functions. In this example, file-based dependencies force the functions to execute in order, but it
demonstrates the basics of the API.

.. code-block:: python
    :linenos:
    :caption: **Basic Batch example with YAML template**

    # Generate the powers of a matrix and write them to disk
    from dragon.workflows.batch import Batch
    from pathlib import Path
    from some_math_functions import gpu_matmul

    import numpy as np

    # A base directory, and files in it, will be used for communication of results.
    # Optionally size the internal results DDict explicitly in bytes.
    batch = Batch(results_ddict_mem=2 * 1024**3)
    base_dir = Path("/some/path/to/base_dir")

    a = np.array([j for j in range(100)])
    m = np.vander(a)

    # Import a function to lazily run gpu_matful tasks in parallel. Notice that the
    # "ptd" in the file name refers to "parameterized task descriptor", since the yaml
    # file describes a collection of tasks that are parameterized by the arguments to
    # the returned function, gpu_matmul_func.
    get_file = lambda base_dir, i, j: Path(base_dir) / Path(f"file_{i + j}")
    gpu_matmul_lazy = batch.import_func("gpu_matmul_ptd.yml", gpu_matmul, base_dir, get_file)

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

    batch.join()


Notice that the call to :py:meth:`~dragon.workflows.batch.Batch.import_func` requires a file called
“gpu_matmul_ptd.yml”, which is a Parameterized Task Descriptor, or PTD,
file. This type of file describes a *collection* of tasks, rather than a
single task. The collection of tasks are parameterized by the arguments
to the function returned by :py:meth:`~dragon.workflows.batch.Batch.import_func`: each set of arguments
generates a new task. Generated tasks are not run immediately, but
rather are queued up in the background and run lazily in parallelized
batches. Batches of tasks only run when you (1) call
``<task return value>.get()``, try to access either a distributed
dictionary or file that’s updated by a task, or call :py:meth:`~dragon.workflows.batch.Batch.fence`.
The PTD file “gpu_matmul_ptd.yml” is shown directly below, and a PTD
template (with lots of explanations) is located in the examples
directory.

.. code-block:: yaml
    :linenos:
    :caption: **Function YAML example**

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


And here is the same example as above, but done without a PTD file.

.. code-block:: python
    :linenos:
    :caption: **Basic Batch example without YAML template**

    # Generate the powers of a matrix and write them to disk
    from dragon.workflows.batch import Batch
    from pathlib import Path
    from some_math_functions import gpu_matmul

    import numpy as np

    # A base directory, and files in it, will be used for communication of results.
    # Optionally size the internal results DDict explicitly in bytes.
    batch = Batch(results_ddict_mem=2 * 1024**3)
    base_dir = Path("/some/path/to/base_dir")

    # Knowledge of reads and writes to files is used by Batch to infer data dependencies
    # and automatically parallelize tasks
    get_read = lambda i: batch.read(base_dir, Path(f"file_{i}"))
    get_write = lambda i: batch.write(base_dir, Path(f"file_{i+1}"))

    a = np.array([j for j in range(100)])
    m = np.vander(a)

    # Submit tasks directly — Batch dispatches them to workers in the background
    tasks = [batch.options(reads=[get_read(i)], writes=[get_write(i)], timeout=30)
                    .function(gpu_matmul, m, base_dir, i)
             for i in range(1000)]

    # Retrieve results — .get() waits for each task to complete if it hasn't yet
    for task in tasks:
        try:
            print(f"result={task.get()}")
        except Exception as e:
            print(f"gpu_matmul failed with the following exception: {e}")

    batch.join()


Tasks are submitted continuously (although batched in the background for better performance). Calls to
:py:meth:`~dragon.workflows.batch.Batch.function`, :py:meth:`~dragon.workflows.batch.Batch.process`, and
:py:meth:`~dragon.workflows.batch.Batch.job` return immediately and :py:class:`~dragon.workflows.batch.Batch`
batches and dispatches submitted tasks to workers in the background. The lifecycle methods are:
:py:meth:`~dragon.workflows.batch.Batch.close`, which is deprecated and retained as a no-op for compatibility;
:py:meth:`~dragon.workflows.batch.Batch.join`, which waits only for work started by the calling client and then detaches that client from the Batch instance shared by clients; and
:py:meth:`~dragon.workflows.batch.Batch.destroy`, which is only valid when the Batch was created with ``managed_lifecycle=True`` and shuts down that client-shared Batch instance after all clients have detached (or after ``force_timeout`` expires, if provided).

By default, ``Batch()`` uses unmanaged lifecycle mode. In that mode, the Batch instance shared by clients shuts down automatically when the last client detaches, so the usual cleanup pattern is just:

.. code-block:: python

    result = task.get()
    batch.join()

Any task results you still need must be fetched before that final ``join()``. If you need a joined
handle to keep reading results afterward, create the runtime with ``managed_lifecycle=True`` and
delay :py:meth:`~dragon.workflows.batch.Batch.destroy` until after retrieval.

Use ``Batch(managed_lifecycle=True)`` only when you intentionally want the runtime to outlive individual client handles, for example when serializing Batch handles through another service. In managed mode, any client may later call :py:meth:`~dragon.workflows.batch.Batch.destroy`, even after that client has already called :py:meth:`~dragon.workflows.batch.Batch.join`.

Any mix of Python functions, executables, and parallel jobs can be submitted to
:py:class:`~dragon.workflows.batch.Batch` simultaneously, and dependencies can exist between tasks
of any type, e.g., an MPI job can depend on the completion of a Python function if the MPI job
reads from a file that the function writes to. MPI jobs are specified using the
:py:meth:`~dragon.workflows.batch.Batch.job` function. Likewise, the
:py:meth:`~dragon.workflows.batch.Batch.process` function submits a task for running a serial
executable.

Batch metadata is now expected to be supplied through
:py:meth:`~dragon.workflows.batch.Batch.options`, for example
``batch.options(name="step1", stdout="step1.out").function(fn, *args, **kwargs)``.
Passing metadata such as ``reads``, ``writes``, ``name``, ``timeout``, ``stdout``,
or ``stderr`` directly to :py:meth:`~dragon.workflows.batch.Batch.function`,
:py:meth:`~dragon.workflows.batch.Batch.process`, or
:py:meth:`~dragon.workflows.batch.Batch.job` is still supported for compatibility,
but has been deprecated.

When a ``ProcessTemplate`` passed to :py:meth:`~dragon.workflows.batch.Batch.process` or
:py:meth:`~dragon.workflows.batch.Batch.job` includes an explicit host-name policy
(``Policy(placement=Policy.Placement.HOST_NAME, host_name=...)``), Batch preserves that request and,
for jobs, reserves those specific hosts before launch. Leave ``host_name`` unset if you want Batch
to choose task placement automatically across its managed topology.

The same precedence rule applies to task log files: ``stdout`` and ``stderr``
configured through :py:meth:`~dragon.workflows.batch.Batch.options` act as
defaults for process and job tasks, but any ``stdout`` or ``stderr`` already
present on an individual :py:class:`ProcessTemplate` wins for that template.
An explicit ``stdout``/``stderr`` path is always honored even when
``task_logs=False``; only the automatic per-task file capture is gated behind
``task_logs=True``.

Calling ``.get()`` on a task handle returns the task's return value, blocking until the task
completes if it has not finished yet. If an exception was thrown during the execution of a task,
calling ``.get()`` on the task's handle will re-raise that exception.

For :py:meth:`~dragon.workflows.batch.Batch.process` and
:py:meth:`~dragon.workflows.batch.Batch.job` tasks, the task's return value is the child
exit code. A non-zero exit code is returned like any other value rather than raised, so a
downstream task that consumes a process or job result (for example through an argument
dependency) receives that exit code as its input; check the code yourself to decide whether
the task succeeded. A single-process :py:meth:`~dragon.workflows.batch.Batch.process` returns
a scalar exit code, while a multi-rank :py:meth:`~dragon.workflows.batch.Batch.job` returns a
list of exit codes, one per launched process.

A :py:meth:`~dragon.workflows.batch.Batch.function` target must be importable by the Batch
workers, since the callable is reconstructed in the worker process. Functions defined in an
installed package or on the workers' ``PYTHONPATH`` work; a helper that only exists in the
submitting process (for example one made importable through a runtime ``sys.path`` insert)
cannot be re-imported on the worker, and ``.get()`` re-raises the resulting import error
(such as ``ModuleNotFoundError``) instead of blocking.

By default (``Batch()``), task ``stdout`` and ``stderr`` are not captured to files. Unless you pass
an explicit ``stdout``/``stderr`` path for a task, its output is forwarded to the client console like
any other Dragon process. Pass ``task_logs=True`` to :py:class:`~dragon.workflows.batch.Batch` to
instead redirect each task's ``stdout`` and ``stderr`` to files on the shared filesystem and enable
log discovery. With ``task_logs=True``, each Batch client owns a log directory rooted at
``runinfo/<batch-run-id>/client-<id>/task_logs`` with per-kind subdirectories such as
``function/``, ``process/``, and ``job/``. Use :py:meth:`~dragon.workflows.batch.Batch.log_dir`
to discover that directory, :py:meth:`~dragon.workflows.batch.Batch.log_manifest_path` to inspect
the per-client ``manifest.jsonl`` index of task log files, and :py:meth:`~dragon.workflows.batch.Task.log_paths`
on a specific task to get its resolved stdout/stderr file paths.

Batch also provides a small, experimental Python helper layer over the manifest
so users do not need to parse the JSONL file manually. These discovery helpers
may evolve as the logging interface matures:

* :py:meth:`~dragon.workflows.batch.Batch.iter_log_records` returns all current client's manifest records.
* :py:meth:`~dragon.workflows.batch.Batch.find_logs` returns one task's manifest record by ``tuid``.
* :py:meth:`~dragon.workflows.batch.Batch.read_logs` reads a task's stdout/stderr files from the shared filesystem and always returns a dictionary with ``stdout`` and ``stderr`` keys.

These experimental log-discovery helpers require ``task_logs=True``; they raise
``RuntimeError`` when task logging is disabled. For example:

.. code-block:: python
    :linenos:
    :caption: **Inspecting Batch task logs**

    batch = Batch(task_logs=True)
    task = batch.options(stdout="custom.out").function(my_func)
    task.get()

    print(task.log_paths())
    print(batch.log_dir())
    print(batch.log_manifest_path())
    print(batch.find_logs(task.uid))
    print(batch.read_logs(task.uid, log_type="stdout")["stdout"])

Even when ``log_type`` is specified, :py:meth:`~dragon.workflows.batch.Batch.read_logs`
still returns a two-key dictionary. The requested side contains the log text,
and the other side is ``None``.

Using :py:meth:`~dragon.workflows.batch.Batch.options` avoids conflicts between
Batch metadata and user function keyword arguments. The old direct style,
``batch.function(fn, stdout="task.out", ...)``, is deprecated specifically
because those keyword names are reserved by Batch in that compatibility path.

The initial creation of the :py:class:`~dragon.workflows.batch.Batch` object sets up manager and worker
processes. :py:class:`~dragon.workflows.batch.Batch`
objects can be passed between processes to allow multiple clients.
Unpickling a :py:class:`~dragon.workflows.batch.Batch` object at a destination
process registers a new :py:class:`~dragon.workflows.batch.Batch` client and
allows the user to submit tasks to the same client-shared Batch instance. All clients should
call :py:meth:`~dragon.workflows.batch.Batch.join` when they are done.
Calling :py:meth:`~dragon.workflows.batch.Batch.close` is optional and has no effect beyond a deprecation warning.
If the runtime was created with ``managed_lifecycle=True``, any client can later call
:py:meth:`~dragon.workflows.batch.Batch.destroy` to shut down that client-shared Batch instance.

Data Dependencies
=================

Dependencies between tasks are inferred based on the data being read and
written by each task. Data reads and writes are specified by
:py:meth:`~dragon.workflows.batch.Batch.read` and :py:meth:`~dragon.workflows.batch.Batch.write` (or task
``read`` and ``write``, to directly associate reads and writes with a specific task). When a
task is created, a list of Read or Write objects, created by
:py:meth:`~dragon.workflows.batch.Batch.read` and :py:meth:`~dragon.workflows.batch.Batch.write`, can be specified:

.. code-block:: python
    :linenos:

    task = batch.options(reads=<list of Reads>, writes=<list of Writes>).job(
        process_templates=[(256, ProcessTemplate(target=mpi_exec))]
    )

After a task has been create, further Reads and Writes can be specified
via ``task.read`` and ``task.write``:

.. code-block:: python
    :linenos:

    task.read(base_dir1, file_path1)
    task.write(base_dir2, file_path2)


:py:class:`~dragon.workflows.batch.Batch` takes a *dataflow* approach to parallelizing tasks.
Like all dataflow systems, it assumes that tasks have no `side
effects <https://en.wikipedia.org/wiki/Side_effect_(computer_science)>`__
(think IO) beyond those specified by :py:meth:`~dragon.workflows.batch.Batch.read` and
:py:meth:`~dragon.workflows.batch.Batch.write` calls. So, for instance, if a process task reads from a file, then a
corresponding Read object must be created using :py:meth:`~dragon.workflows.batch.Batch.read` and
appended to the list of Reads when creating the task (or added after
task creation using task ``read``). If this read isn’t associated with
the task, then the file read could happen out-of-order relative to other operations on that file,
e.g., the file read could occur before the data intended to be read is
written to the file.


Argument-passing Dependencies
=============================

Beyond the type of dependencies described above, there is a second type
of dependency: argument-passing dependencies. These dependencies are
inferred when a task handle is passed
as an argument to a ``Batch`` ``Function``, ``Process``, or ``Job``. Batch passes the
upstream task's return value into the downstream argument position once the upstream task
completes. For
example:

.. code-block:: python
    :linenos:
    :caption: **Example passing arguments between tasks**

    def foo():
        return "Hi-diddly-ho!!!"

    def bar(hello: str):
        print(hello, flush=True)

    batch = Batch()

    task_foo = batch.function(foo)
    task_bar = batch.function(bar, task_foo.result)
    task_bar.get()

    # prints: "Hi-diddly-ho!!!"

    batch.join()


In the above example, the function ``bar`` will not run until ``foo``
completes, and ``bar`` will print the string returned by ``foo``.

Synchronization
===============

By default, results can be retrieved in a fine-grained manner with ``.get()``, which blocks only for the specific
task being waited on. When you need a hard synchronization point, i.e., before
checkpointing state or starting a new phase of work that must not overlap with the previous one,
use :py:meth:`~dragon.workflows.batch.Batch.fence`. It blocks until every task submitted by this
client completes, then clears internal dependency state so the next batch of tasks starts from
a clean slate. To additionally free the memory used to hold task results, call
:py:meth:`~dragon.workflows.batch.Batch.clear_results`, which calls
:py:meth:`~dragon.workflows.batch.Batch.fence` and then clears the results dictionary.
After ``clear_results()``, any task result that has not already been retrieved by ``.get()`` is no
longer available from the client handle.

Distributed Dictionary
======================

The Distributed Dictionary provides a scalable, in-memory distributed key-value store with
semantics that are generally similar to a standard Python dictionary. The Distributed Dictionary
uses shared memory and RDMA to handle communication of keys and values, and avoids central
coordination so there are no bottlenecks to scaling. Revisiting an example above for the Batch
service, we will replace the file system as a means of inter-task communication with a Distributed
Dictionary. The only change needed is in the task definitions - the rest of the workflow
(submitting tasks, retrieving results, closing) is identical.

.. code-block:: python
    :linenos:
    :caption: **Example using Batch and DDict**

    # Generate the powers of a matrix and write them to a distributed dictionary
    from dragon.data import DDict
    from dragon.workflows.batch import Batch
    from some_math_functions import gpu_matmul

    import numpy as np

    def gpu_matmul(original_matrix: np.ndarray, ddict: DDict, i: int):
       # read the current matrix stored in the Distributed Dictionary at key=i
       current_matrix = ddict[i]

       # actual matrix multiplication happens here
       next_matrix = do_dgemm(original_matrix, current_matrix)

       # write the next power of the matrix to the Distributed Dictionary at key=i+1
       ddict[i + 1] = new_matrix

    # The Distributed Dictionary service will be used for communication of results
    batch = Batch()
    ddict = DDict()

    # Knowledge of reads and writes to the Distributed Dictionary will also be used by
    # the Batch service to determine data dependencies and how to parallelize tasks
    get_read = lambda i: batch.read(ddict, i)
    get_write = lambda i: batch.write(ddict, i + 1)

    a = np.array([i for i in range(100)])
    m = np.vander(a)

    # Submit tasks — Batch dispatches them in the background
    tasks = [batch.options(reads=[get_read(i)], writes=[get_write(i)])
                    .function(gpu_matmul, m, ddict, i)
             for i in range(1000)]

    # Retrieve results — .get() waits for each task to complete if needed
    for task in tasks:
        try:
            print(f"result={task.get()}")
        except Exception as e:
            print(f"gpu_matmul failed with the following exception: {e}")

    batch.join()


Inspecting the Topology
=======================

:py:meth:`~dragon.workflows.batch.Batch.topology` returns a
:py:class:`~dragon.workflows.batch.BatchTopology` object that describes how
:py:class:`~dragon.workflows.batch.Batch` has mapped managers and worker pools onto
the nodes of the allocation. The reported topology includes the dedicated
scheduler host separately from the pool-backed subnode managers. This is useful
for understanding how work will be distributed before submitting tasks.

The example below creates several :py:class:`~dragon.workflows.batch.Batch` instances
with different configurations, prints a summary of each resulting topology, and
shuts down cleanly without submitting any real work.

.. literalinclude:: ../../examples/workflows/batch/topology.py
    :language: python
    :linenos:
    :caption: **topology.py: Batch topology API walkthrough**
