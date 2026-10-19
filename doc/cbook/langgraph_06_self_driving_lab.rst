.. _cbook_langgraph_self_driving_lab:

06 — Self-Driving Lab
++++++++++++++++++++++++++++++++

The capstone that combines the earlier abilities into one workflow: an on-cluster
LLM planner + judge (03), a **Dragon Batch** experiment ensemble whose live
progress and bulk output ride a ``DDict`` (05), and real-time **mid-flight abort
via a DDict flag** (04) — with the agents placed across two nodes by ``Policy``.

**What you'll learn:**

* How a LangGraph loop drives an optimization campaign end-to-end
* How the supervisor runs an experiment ensemble via Dragon ``Batch``
* How a shared ``DDict`` acts as a two-way message board for live supervision
* How a diverging run is aborted mid-flight and its compute reclaimed

**Needs:** 1 node (GPU optional — the LLM judge is optional).

The Flow (orchestration, not the science)
=========================================

This is a **self-driving optimization campaign**: an AI loop that repeatedly
proposes experiments, runs them, watches them live, kills the bad ones early,
and refines toward the best result. The scientific kernel is a deliberately
simple stand-in — the point is the *orchestration*.

It is an ordinary LangGraph ``StateGraph`` with three nodes wired into a cycle:

.. code-block:: text

    START -> planner -> supervisor -> analyzer --(refine?)--> planner
                                          |
                                       (converged?) --> END

* **planner** — proposes N candidate experiments (each is one parameter).
* **supervisor** — runs those N experiments and babysits them **live**.
* **analyzer** — ranks the finished ones, updates the "best so far", and takes
  the conditional edge: refine (loop) or converged (``END``).

Three Dragon pieces sit underneath the graph:

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Piece
     - Role
   * - :py:class:`~dragon.ai.langgraph.DragonExecutor`
     - Launches the AgentHost processes and runs each agent as a **thread**
       inside one (the LangGraph integration). The planner + analyzer are two
       threads in one host process; the supervisor is a thread in a second host
       process on another node.
   * - :py:class:`~dragon.Batch`
     - A separate Dragon scheduler that runs the experiment tasks across the
       allocation.
   * - :py:class:`~dragon.DDict`
     - A shared distributed dictionary — the "blackboard" every party reads and
       writes.

**Placement.** The planner + analyzer share one host process (head node); the
supervisor runs in a second host process (compute node); Batch schedules the
experiments wherever there is room. On a single-node allocation everything
resolves to the one node.

One round in detail — what the supervisor actually does:

.. code-block:: text

    receive the candidate list from the planner
      |
      +- for each candidate: submit it to Batch as a task  --> Batch runs the
      |    (submission returns immediately; tasks run async)    experiments
      |
      +- enter a POLL LOOP (every POLL_INTERVAL seconds):
            for each still-running experiment:
               read  progress:{id}  from the DDict   <-- experiments write here
               |
               +- if it looks like it is diverging:
               |     write  control:{id} = "abort"   --> experiment reads this
               |                                          flag and stops itself
               |
               +- if the Batch task finished:
                     record its outcome; drop it from the pending set
      |
      +- when all experiments are done/aborted -> return outcomes to the analyzer

The key mechanism is the ``DDict`` used as a **two-way message board**:

* experiments **write** their live progress → ``progress:{id}``
* the supervisor **reads** that progress and, if a run goes bad, **writes** an
  abort flag → ``control:{id}``
* the experiment **reads** its own abort flag each step and, if set, **stops
  itself** and reports ``aborted``.

That is the whole "real-time supervision" idea: nobody waits for an experiment to
finish before deciding it is a dud — a diverging run is stopped mid-flight,
freeing its compute for the next round.

.. note::

   The LLM judge is optional. With ``USE_LLM_JUDGE=1`` (and a GPU) an on-cluster
   vLLM model makes the "is this diverging?" call; otherwise a simple threshold
   rule decides. Either way the flow above is identical.

Main Code
=========

.. literalinclude:: ../../examples/dragon_ai/langgraph/06_self_driving_lab.py
    :language: python
    :linenos:
    :caption: **06_self_driving_lab.py**

See Also
========

* :ref:`developer-guide-langgraph` — internals and code paths.
* :ref:`LangGraphAPI` — API reference.
