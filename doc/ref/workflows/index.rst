.. _WorkflowsAPI:

Workflows
+++++++++

The following reference provides details about the Dragon Workflows API. Tools for graph-based execution of
tasks, data proessing, and integrations with other workflow systems are included.

Python Reference
================

Batch for Graph-based Execution
-------------------------------

Run functions, executables, and parallel applications through a high-level, graph-based
tasking API.

.. currentmodule:: dragon.workflows.batch

.. autosummary::
    :toctree:
    :recursive:

    Batch

.. currentmodule:: dragon.workflows.batch.batch

.. autosummary::
    :toctree:
    :recursive:

    BatchDDict
    BatchFile
    Function
    Job
    MakeTask
    SubmitAfterCloseError
    Task


Data Processing Tools
---------------------

Prototype API for creating dataflow processing pipelines.

.. currentmodule:: dragon.workflows

.. autosummary::
    :toctree:
    :recursive:

    data_mover


Other Workflow Tools
--------------------

Dragon can integrate into other workflow tools and enhance their scalability and performance. These APIs are
integrations into other tools. These are currently works-in-progress and are useful primarily for experimentation.

.. currentmodule:: dragon.workflows

.. autosummary::
    :toctree:
    :recursive:

    parsl_batch_executor
    parsl_executor
    parsl_mpi_app
