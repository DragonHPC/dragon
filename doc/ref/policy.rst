Resource Placement and Affinity
+++++++++++++++++++++++++++++++

The policy API controls where Dragon places work and resources. Use it when you
need to express CPU, GPU, host, or affinity preferences explicitly instead of
relying on the runtime's default placement decisions.

This is the reference surface behind many higher-level placement features in
the Native API and other orchestration interfaces. If you are deciding where a
process group, worker, or resource should run, this is the core policy object to
understand.

Python Reference
----------------

.. currentmodule:: dragon.infrastructure.policy
.. autosummary::
    :toctree:
    :recursive:

    Policy