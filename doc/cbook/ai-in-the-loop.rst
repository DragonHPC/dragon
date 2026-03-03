.. _cbook_ai_in_the_loop:

AI-in-the-loop Workflow
+++++++++++++++++++++++++++++++++++++++++++++++++++

This is an example of how Dragon can be used to execute an AI-in-the-loop workflow.
Inspiration for this demo comes from the NERSC-10 Workflow Archetypes White Paper.
This workflow most closely resembles the workflow scenario given as part of archetype four.

In this example we use a small model implemented in PyTorch to compute an approximation to :math:`\sin(x)`.
In parallel to doing the inference with the model, we launch `sim-cheap` on four MPI ranks.
This MPI job computes the Taylor approximation to :math:`\sin(x)` and compares this with the output of the model.
If the difference is less than 0.05 we consider the model's approximation to be sufficiently accurate and print out the
result with the exact result.
If the difference is larger than 0.05 we consider this a failure and re-train the model on a new set of data.

To generate this data we launch `sim-expensive`.
This MPI job is launched on eight ranks-per-node and each rank generates 32 data points of the form :math:`(x, \sin(x))`
where :math:`x \in U(-\pi, \pi)`.
This data is aggregated into a PyTorch tensor and then used to train the model.
We then re-evaluate the re-trained model and decide if we need to re-train again or if the estimate is sufficiently
accurate. We continue this loop until we've had five successes.

:numref:`ai-in-the-loop`  presents the structure of this main loop. It shows when each MPI application is launched and
what portions are executed in parallel.

.. figure:: images/ai-in-the-loop-workflow.jpg
    :scale: 100%
    :name: ai-in-the-loop

    **Example AI-in-the-loop workflow**


This example consists of the following python files:

* `ai-in-the-loop.py` - This is the main file. It contains functions for launching both MPI executables and parsing the
  results as well as imports functions defined in `model.py` and coordinates the model inference and training with the MPI jobs.

* `model.py` - This file defines the model and provides some functions for model training and inference.

Below, we present the main Python code :example_workflows:`ai-in-the-loop.py <ai-in-the-loop/ai-in-the-loop.py>`, which
acts as the coordinator of the workflow. The code of the other files can be found in :example_workflows:`ai-in-the-loop`.

.. literalinclude:: ../../examples/workflows/ai-in-the-loop/ai-in-the-loop.py
    :language: python
    :linenos:
    :caption: **ai-in-the-loop.py: Main orchestrator for AI-in-the-loop demo**


Installation
============

After installing dragon, the only other dependency is on PyTorch. The PyTorch version and corresponding pip command can
be found here (https://pytorch.org/get-started/locally/).

.. code-block:: console

    pip install torch torchvision torchaudio

Description of the system used
==============================

For this example, HPE Cray Hotlum nodes were used. Each node has AMD EPYC 7763 64-core CPUs.

How to run
==========

Example Output when run on 16 nodes with 8 MPI ranks-per-node used to generate data and four MPI ranks to compute the cheap approximation
-----------------------------------------------------------------------------------------------------------------------------------------

.. code-block:: console
    :linenos:

    > make
    gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib   -c -o sim-cheap.o sim-cheap.c
    gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib  sim-cheap.o -o sim-cheap -lm -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib -lmpich
    gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib   -c -o sim-expensive.o
    gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib  sim-expensive.o -o sim-expensive -lm -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib -lmpich
    > salloc --nodes=16 --exclusive
    > dragon ai-in-the-loop.py
    training
    approx = 0.1283823400735855, exact = 0.15357911534767393
    training
    approx = -0.41591891646385193, exact = -0.4533079140996079
    approx = -0.9724616408348083, exact = -0.9808886564963794
    approx = -0.38959139585494995, exact = -0.4315753703483373
    approx = 0.8678910732269287, exact = 0.8812041533601648

