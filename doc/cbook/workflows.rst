Workflows with Dragon
+++++++++++++++++++++

Dragon supports a range of workflow patterns, from tightly coupled AI/simulation loops to task-parallel parameter
sweeps and MPI application orchestration. The examples below cover some common use cases.

* **AI-in-the-loop**: Couple inference and simulation in a single managed workflow.
* **Dragon MPI Workflow**: Orchestrate MPI applications as part of a larger Python workflow.
* **Dragon Parsl MPI App**: Integrate with Parsl for flexible task-parallel workflow management.
* **Dragon joblib**: Use Dragon as a backend for joblib-based parallel computation.
* **Batch DAG Execution**: Build and execute directed acyclic graphs of tasks with the Batch API.

.. toctree::
    :maxdepth: 1

    ai-in-the-loop.rst
    dragon_mpi_workflow.rst
    dragon_parsl_mpi_app.rst
    dragon_joblib.rst
    batch.rst