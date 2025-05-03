.. _gpu_affinity:

Controlling GPU Affinity
++++++++++++++++++++++++

This tutorial will draw from the :py:class:`~dragon.infrastructure.policy.Policy` documentation to walk through how
to use :py:class:`~dragon.native.ProcessGroup` to set what GPUs to use for a gien function or process as well
as how to do it with multiprocessing.