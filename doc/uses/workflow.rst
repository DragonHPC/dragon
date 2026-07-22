.. _workflows:

Workflows
+++++++++

A Dragon *workflow* combines multiple capabilities — parallel processes, shared data, AI
inference, and external executables — into a single, coordinated program. This page shows
how to assemble those pieces into an end-to-end workflow that scales from a laptop to a
supercomputer.

Building Blocks
===============

Dragon workflows typically involve some combination of:

* :py:class:`~dragon.workflows.batch.Batch` — automatic DAG-based task scheduling across
  Python functions, executables, and MPI jobs (see also :ref:`batch`).
* :py:class:`~dragon.data.ddict.DDict` — shared in-memory key-value store for passing data
  between stages without touching the filesystem.
* :py:class:`~dragon.native.process_group.ProcessGroup` — fine-grained process placement
  and lifecycle management.
* :ref:`Dragon Channels <NativeAPI>` — low-latency communication between workflow stages.

A Two-Stage Simulation + Analysis Workflow
==========================================

The following example shows a common HPC pattern: a simulation stage writes output into a
shared DDict, and an analysis stage reads from it and produces a result. Both stages run in
parallel wherever possible.

.. code-block:: python
    :linenos:
    :caption: **workflow.py — simulation followed by analysis using Batch + DDict**

    import dragon
    from multiprocessing import set_start_method
    from dragon.workflows.batch import Batch
    from dragon.data.ddict import DDict
    from dragon.native.machine import System
    import numpy as np


    def simulate(dd, run_id, n_steps):
        """Pretend simulation: write a result array into the shared DDict."""
        result = np.cumsum(np.random.randn(n_steps))
        dd[f"sim_{run_id}"] = result
        return run_id


    def analyze(dd, run_id):
        """Read simulation output and compute a summary statistic."""
        data = dd[f"sim_{run_id}"]
        return {"run_id": run_id, "mean": float(data.mean()), "std": float(data.std())}


    if __name__ == "__main__":
        set_start_method("dragon")

        alloc = System()
        nnodes = alloc.nnodes

        # Shared store for intermediate results — no filesystem round-trip needed
        dd = DDict(managers_per_node=1, n_nodes=nnodes,
                   total_mem=nnodes * 256 * 1024 * 1024)

        batch = Batch()

        n_runs = 32

        # Stage 1: submit all simulation tasks in parallel
        sim_handles = [
          batch.options(writes=[batch.write(dd, f"sim_{i}")])
             .function(simulate, dd, i, 10_000)
            for i in range(n_runs)
        ]

        # Stage 2: each analysis reads the key written by the corresponding simulation.
        # Batch uses the reads/writes declarations to enforce correct ordering.
        analysis_handles = [
          batch.options(reads=[batch.read(dd, f"sim_{i}")])
             .function(analyze, dd, i)
            for i in range(n_runs)
        ]

        # Collect results
        summaries = [h.get() for h in analysis_handles]
        for s in sorted(summaries, key=lambda x: x["run_id"]):
            print(f"Run {s['run_id']:02d}: mean={s['mean']:.4f}  std={s['std']:.4f}")

        batch.join()
        dd.destroy()

Run with:

.. code-block:: console

    dragon workflow.py

Going Further
=============

* :ref:`batch` — Full reference for the Batch API with PTD YAML templates.
* :ref:`cbook_ai_in_the_loop` — Workflow pattern that tightly couples HPC simulation with
  AI inference in the same Dragon job.
* :ref:`orchestrate_mpi` — Running parameter sweeps over MPI applications.
* :ref:`proxy` — Connecting workflow stages that run on separate systems.

Related Cookbook Examples
=========================

* :ref:`batch`
* :ref:`cbook_ai_in_the_loop`
