.. _batch:

Batch Processing
++++++++++++++++

Dragon's :py:class:`~dragon.Batch` API lets you submit Python functions,
executables, and parallel jobs for distributed execution with automatic scheduling and placement
of tasks. You declare what each task reads and writes, and Dragon infers a directed acyclic graph (DAG)
to maximize parallel throughput — no manual dependency management needed.

When to Use Batch
=================

Use :py:class:`~dragon.Batch` when you have tasks that should be scheduled
automatically from declared read and write dependencies (on the parallel filesystem or Dragon
Distributed Dictionary), or from argument passing dependencies. It is especially useful when
you need to:

* Intermix Python functions, external processes, and multi-node MPI jobs in one workflow.
* Run large numbers of small function tasks with higher throughput.
* Let Dragon schedule around dependencies efficiently, prioritize the critical path, place tasks near their data, and make intelligent MPI placement decisions.

For simpler parallel-map patterns, `Python multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_ shows how ``Pool.map`` may be sufficient.

A Simple Batch Example
======================

The following example uses Batch to compute squares of numbers in parallel:

.. code-block:: python
    :linenos:
    :caption: **batch_squares.py — parallel function tasks with Batch**

    import dragon
    from multiprocessing import set_start_method
    from dragon import Batch


    def square(x):
        return x * x


    if __name__ == "__main__":
        set_start_method("dragon")

        with Batch() as batch:
            # Submit 20 tasks — Batch dispatches them in parallel automatically
            handles = [batch.function(square, i) for i in range(20)]

            # .get() blocks until the task completes and returns its result
            results = [h.get() for h in handles]
            print(results)

Run with:

.. code-block:: console

    dragon batch_squares.py

Context-manager Cleanup
=======================

Use ``Batch`` as a context manager when a block owns a Batch client for one
workflow phase:

.. code-block:: python

    with Batch() as batch:
        tasks = [batch.function(square, i) for i in range(20)]
        results = [task.get() for task in tasks]

On normal exit, the context manager calls :py:meth:`~dragon.Batch.join`.
This waits for work submitted by the current client and detaches that client
from the shared Batch runtime. In the default unmanaged mode, the runtime is
also shut down automatically when this is the last attached client. Fetch any
task results you still need before leaving the block, because shutting down an
unmanaged runtime destroys its internal results DDict.

If code inside the block raises an exception, the context manager calls
:py:meth:`~dragon.Batch.terminate` and then re-raises the
original exception. This is the appropriate cleanup path when work may still
be running and should not be allowed to continue.

For a runtime shared by multiple clients, use managed lifecycle mode explicitly:

.. code-block:: python

    batch = Batch(managed_lifecycle=True)
    with batch:
        task = batch.function(square, 7)
        print(task.get())

    # The context detached this client, but the shared runtime remains alive.
    batch.destroy()

``Batch.close()`` is deprecated and remains a no-op for compatibility. Use a
context manager for scoped work, or call ``join()`` when a client is done and
``destroy()`` when a managed runtime should be shut down.

Worker Resource Configuration
=============================

By default, each Batch worker pool uses the physical cores available on its
node. Pass the keyword-only ``workers_per_node`` option to limit the number of
task worker processes in each pool, for example ``Batch(workers_per_node=4)``.
This controls task workers and does not change the dedicated scheduler worker
count configured by ``scheduler_workers``.

Tasks with Data Dependencies
=============================

Batch tracks file dependencies between tasks and automatically orders execution so that
a task that reads a file waits until the task that writes it completes:

.. code-block:: python
    :linenos:
    :caption: **Chained tasks via file dependencies**

    import dragon
    from dragon import Batch
    from multiprocessing import set_start_method
    from pathlib import Path
    import numpy as np


    def generate(path):
        np.save(path, np.arange(1000, dtype=np.float64))


    def process(in_path, out_path):
        data = np.load(in_path)
        np.save(out_path, data ** 2)
        return float(data.sum())


    if __name__ == "__main__":
        set_start_method("dragon")

        batch = Batch()
        base = Path("/tmp/dragon_batch_demo")
        base.mkdir(exist_ok=True)

        raw = base / "raw.npy"
        processed = base / "processed.npy"

        # Task 1: generate data
        t1 = batch.options(
            writes=[batch.write(base, Path("raw.npy"))]
        ).function(generate, raw)

        # Task 2: process data — will not start until t1 finishes
        t2 = batch.options(
            reads=[batch.read(base, Path("raw.npy"))],
            writes=[batch.write(base, Path("processed.npy"))],
        ).function(process, raw, processed)

        print(f"Sum of raw data: {t2.get()}")

        batch.join()

Submitting Executables and MPI Jobs
=====================================

Batch is not limited to Python functions. Use
:py:meth:`~dragon.Batch.process` for executables and
:py:meth:`~dragon.Batch.job` for MPI/parallel jobs:

.. code-block:: python
    :linenos:
    :caption: **Mixed Python + executable tasks in one Batch**

    import dragon
    from dragon import Batch, ProcessTemplate
    from multiprocessing import set_start_method
    from dragon.native.process import Popen
    from pathlib import Path


    def prepare_input(path):
        path.write_text("42\n")


    if __name__ == "__main__":
        set_start_method("dragon")

        batch = Batch()
        base = Path("/tmp/dragon_batch_exec")
        base.mkdir(exist_ok=True)
        inp = base / "input.txt"

        # Python function writes the input file
        t1 = batch.options(
            writes=[batch.write(base, Path("input.txt"))]
        ).function(prepare_input, inp)

        # External executable reads the file produced by t1. process() takes a
        # ProcessTemplate; set stdout=Popen.PIPE to capture the process output.
        t2 = batch.options(
            reads=[batch.read(base, Path("input.txt"))],
        ).process(
            ProcessTemplate(target="cat", args=[str(inp)], stdout=Popen.PIPE),
        )

        t1.get()
        # .get() returns the process exit code and prints the captured stdout.
        exit_code = t2.get()
        print("cat exit code:", exit_code)

        batch.join()

For ``process`` and ``job`` tasks, ``.get()`` returns the child exit code. A non-zero exit
code is returned as a value rather than raised, so you must inspect it yourself to decide
whether the task succeeded, and a downstream task that depends on a process or job result
receives that exit code as its input. A single ``process`` returns a scalar exit code; a
multi-rank ``job`` returns a list of exit codes, one per launched process.

Lifecycle Methods
=================

* :py:meth:`~dragon.Batch.join` — wait for all tasks submitted by the
    current client to complete, then detach the client from the Batch instance
    shared by clients.
* :py:meth:`~dragon.Batch.destroy` — in ``managed_lifecycle=True`` mode,
        shut down the Batch instance shared by clients after they detach, or after ``force_timeout``
    expires if one was supplied.
* :py:meth:`~dragon.Batch.terminate` — force shutdown when work should not be
    allowed to continue; the context manager uses this path when the block raises an exception.
* :py:meth:`~dragon.Batch.fence` — wait for all currently submitted
  tasks to finish before returning (useful as a barrier between phases).
* :py:meth:`~dragon.Batch.clear_results` — free the memory used to hold
  task results (implicitly calls :py:meth:`~dragon.Batch.fence`).

Related Cookbook Examples
=========================

* :ref:`batch`
* :ref:`cbook_ai_in_the_loop`
