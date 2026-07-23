.. _batch:

Batch Processing
++++++++++++++++

Dragon's :py:class:`~dragon.workflows.batch.Batch` API lets you submit Python functions,
executables, and parallel jobs for distributed execution with automatic scheduling and placement
of tasks. You declare what each task reads and writes, and Dragon infers a directed acyclic graph (DAG)
to maximize parallel throughput — no manual dependency management needed.

When to Use Batch
=================

Use :py:class:`~dragon.workflows.batch.Batch` when you have tasks that should be scheduled
automatically from declared read and write dependencies (on the parallel filesystem or Dragon
Distributed Dictionary), or from argument passing dependencies. It is especially useful when
you need to:

* Intermix Python functions, external processes, and multi-node MPI jobs in one workflow.
* Run large numbers of small function tasks with higher throughput.
* Let Dragon schedule around dependencies efficiently, prioritize the critical path, place tasks near their data, and make intelligent MPI placement decisions.

For simpler parallel-map patterns, :ref:`tutorial1` shows how ``Pool.map`` may be sufficient.

A Simple Batch Example
======================

The following example uses Batch to compute squares of numbers in parallel:

.. code-block:: python
    :linenos:
    :caption: **batch_squares.py — parallel function tasks with Batch**

    import dragon
    from multiprocessing import set_start_method
    from dragon.workflows.batch import Batch


    def square(x):
        return x * x


    if __name__ == "__main__":
        set_start_method("dragon")

        batch = Batch()

        # Submit 20 tasks — Batch dispatches them in parallel automatically
        handles = [batch.function(square, i) for i in range(20)]

        # .get() blocks until the task completes and returns its result
        results = [h.get() for h in handles]
        print(results)

        batch.join()

Run with:

.. code-block:: console

    dragon batch_squares.py

Tasks with Data Dependencies
=============================

Batch tracks file dependencies between tasks and automatically orders execution so that
a task that reads a file waits until the task that writes it completes:

.. code-block:: python
    :linenos:
    :caption: **Chained tasks via file dependencies**

    import dragon
    from multiprocessing import set_start_method
    from dragon.workflows.batch import Batch
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
:py:meth:`~dragon.workflows.batch.Batch.process` for executables and
:py:meth:`~dragon.workflows.batch.Batch.job` for MPI/parallel jobs:

.. code-block:: python
    :linenos:
    :caption: **Mixed Python + executable tasks in one Batch**

    import dragon
    from multiprocessing import set_start_method
    from dragon.workflows.batch import Batch
    from dragon.native.process import ProcessTemplate, Popen
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

* :py:meth:`~dragon.workflows.batch.Batch.join` — wait for all tasks submitted by the
    current client to complete, then detach the client from the Batch instance
    shared by clients.
* :py:meth:`~dragon.workflows.batch.Batch.destroy` — in ``managed_lifecycle=True`` mode,
        shut down the Batch instance shared by clients after they detach, or after ``force_timeout``
    expires if one was supplied.
* :py:meth:`~dragon.workflows.batch.Batch.fence` — wait for all currently submitted
  tasks to finish before returning (useful as a barrier between phases).
* :py:meth:`~dragon.workflows.batch.Batch.clear_results` — free the memory used to hold
  task results (implicitly calls :py:meth:`~dragon.workflows.batch.Batch.fence`).

Related Cookbook Examples
=========================

* :ref:`batch`
* :ref:`cbook_ai_in_the_loop`
