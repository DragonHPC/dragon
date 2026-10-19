.. _LangGraphAPI:

LangGraph Integration
+++++++++++++++++++++

The Dragon LangGraph backend lets existing `LangGraph <https://langchain-ai.github.io/langgraph/>`__
multi-agent graphs run on a HPC cluster with **zero changes** to the
graph definition or the node functions. Agent nodes are placed on Dragon
``AgentHost`` processes across cluster nodes, while graph topology, routing, and
checkpointing remain entirely LangGraph's responsibility.

A single :py:class:`~dragon.ai.langgraph.DragonExecutor` owns one or more shared
Dragon ``Channel`` completion lanes (``num_shards``, default 1) over which all
agent *results* are transported inline; cross-node sends use Dragon's transport
(HSTA/RDMA when remote, shared memory when local). Bulk scientific data (tensors,
arrays, datasets) stays the user's
responsibility: create your own Dragon :ref:`DDict <Dragon-Data-DDict>`, pass its handle
into your agent functions, store artifacts there, and return only a lightweight
handle/key through LangGraph state.

.. note::
    This module is experimental and not yet in its final state. It requires
    ``langgraph`` to be installed in the Dragon environment.

For the underlying architecture and components, see
:ref:`developer-guide-langgraph`. For examples, see :ref:`cbook_langgraph`.

Quick Start
===========

.. code-block:: python

    import dragon
    import multiprocessing as mp

    from langgraph.graph import StateGraph, START, END
    from dragon.ai.langgraph import DragonExecutor

    mp.set_start_method("dragon")

    def researcher(state): ...
    def writer(state): ...

    with DragonExecutor() as executor:
        # One launch_host() call = one Dragon process hosting these agents
        # as threads. Call it again (with a Policy) to place agents on
        # other cluster nodes.
        executor.launch_host(agents={
            "researcher": researcher,
            "writer": writer,
        })

        builder = StateGraph(State)
        builder.add_node("researcher", executor.node("researcher"))
        builder.add_node("writer",     executor.node("writer"))
        builder.add_edge(START, "researcher")
        builder.add_edge("researcher", "writer")
        builder.add_edge("writer", END)

        graph = builder.compile()
        result = graph.invoke({"messages": [...]})

Run it on a cluster with the Dragon launcher::

    dragon my_graph.py

Python Reference
================

Executor
--------

Lifecycle manager for Dragon ``AgentHost`` processes. Spawns the hosts that run
agent functions, and exposes a ``fn(state) -> dict`` callable for each agent
that plugs directly into ``StateGraph.add_node``.

.. currentmodule:: dragon.ai.langgraph

.. autosummary::
    :toctree:
    :recursive:

    DragonExecutor


Executor API Detail
-------------------

The ``DragonExecutor`` is the only public class in this module. It manages the
full lifecycle of Dragon processes that execute LangGraph agent functions.

**Constructor:**

.. code-block:: python

    DragonExecutor(
        *,
        max_concurrent_tasks: int = 64,
        num_shards: int = 1,
    )

.. list-table:: Constructor parameters
   :header-rows: 1
   :widths: 25 15 60

   * - Parameter
     - Default
     - Description
   * - ``max_concurrent_tasks``
     - ``64``
     - Maximum concurrent in-flight tasks across the whole executor. Acts as a
       backpressure limit — when hit, further dispatches block until a task
       completes. Size to your expected peak fan-out.
   * - ``num_shards``
     - ``1``
     - Number of independent completion lanes (each a Dragon ``Channel`` plus a
       watcher thread). Raise only if a single watcher thread becomes the
       bottleneck.

**Methods:**

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Method
     - Description
   * - ``launch_host(agents, *, policy=None, event_timeout=None, max_threads=None)``
     - Spawn one persistent Dragon ``Process`` hosting multiple agent functions
       as threads. Returns the ``Process`` object. Call multiple times with
       different ``Policy`` objects to place agents on different nodes.
   * - ``launch_hosts(hosts, *, policies=None, event_timeout=None, max_threads=None)``
     - Spawn multiple AgentHost processes in one call — one per entry in *hosts*.
       Each entry in *policies* is applied to the corresponding host. Returns a
       list of ``Process`` objects in the same order as *hosts*.
   * - ``node(node_name)``
     - Return a LangGraph ``RunnableCallable`` for the named agent. Carries both
       sync (``__call__``) and async (``acall``) paths for ``graph.invoke()`` and
       ``graph.ainvoke()`` respectively. Pass directly to ``StateGraph.add_node()``.
   * - ``shutdown(*, graceful=True)``
     - Stop all AgentHost processes and the watcher thread. Called automatically
       when used as a context manager (``with DragonExecutor() as executor:``).

**Properties:**

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Property
     - Type
     - Description
   * - ``max_concurrent_tasks``
     - ``int``
     - Maximum number of tasks allowed in flight at once.
   * - ``num_shards``
     - ``int``
     - Number of independent completion lanes in the watcher.

**Context manager:**

.. code-block:: python

    with DragonExecutor(max_concurrent_tasks=128) as executor:
        executor.launch_host(agents={"a": fn_a, "b": fn_b})
        # ... build and run graph ...
    # shutdown() is called automatically here


launch_host Parameters
----------------------

.. list-table::
   :header-rows: 1
   :widths: 22 15 63

   * - Parameter
     - Default
     - Description
   * - ``agents``
     - *(required)*
     - ``dict[str, Callable]`` mapping ``node_name`` to ``fn(state) -> dict``.
       All functions in this dict run as threads in a single Dragon process.
       Each name must be unique across all hosts.
   * - ``policy``
     - ``None``
     - Optional :py:class:`~dragon.Policy` to pin this
       host to a specific cluster node, NUMA zone, or GPU. ``None`` uses the
       default Dragon placement.
   * - ``event_timeout``
     - ``None``
     - Seconds before a task is considered timed out. ``None`` means wait
       indefinitely.
   * - ``max_threads``
     - ``None``
     - Maximum concurrent threads in the host's ``ThreadPoolExecutor``. Defaults
       to ``max(len(agents) * 4, 8)``.

**Returns:** :py:class:`~dragon.native.process.Process` — the Dragon Process
running this AgentHost. Useful for monitoring, joining, or killing the host
externally.

**Raises:** ``ValueError`` if any ``node_name`` in ``agents`` was already
registered in a previous ``launch_host()`` call.


launch_hosts Parameters
-----------------------

Convenience wrapper that spawns multiple hosts in one call, mirroring the Dragon
convention of passing a list of policies (one per item).

.. code-block:: python

    executor.launch_hosts(
        hosts=[agent_dict_0, agent_dict_1, ...],
        policies=[policy_0, policy_1, ...],
    )

.. list-table::
   :header-rows: 1
   :widths: 22 15 63

   * - Parameter
     - Default
     - Description
   * - ``hosts``
     - *(required)*
     - ``list[dict[str, Callable]]``. Each dict maps ``node_name`` to
       ``fn(state) -> dict`` and becomes one AgentHost process.
   * - ``policies``
     - ``None``
     - Optional ``list[Policy]``, one per host.  Must be the same length as
       *hosts* when provided.  ``None`` means all hosts use default placement.
   * - ``event_timeout``
     - ``None``
     - Seconds before a task is considered timed out (applied to every host).
   * - ``max_threads``
     - ``None``
     - Max threads per host. ``None`` uses the per-host default.

**Returns:** ``list[dragon.native.process.Process]`` — one Process per host, in
the same order as *hosts*.

**Raises:** ``ValueError`` if *policies* is provided but its length differs
from *hosts*.


Internal Components
-------------------

These are internal implementation classes not intended for direct use. They are
documented here for developers maintaining or extending the integration.

**DragonAgentNode** (``_node.py``)

The per-node callable that LangGraph invokes. Users get this through
``executor.node(name)`` — they never instantiate it directly. Provides:

* ``__call__(state) -> dict`` — sync path for ``graph.invoke()``.
* ``acall(state) -> dict`` — async path for ``graph.ainvoke()``.

**DragonWatcher** (``_watcher.py``)

The blocking-recv result router. Owns the shared completion Channel(s) and
resolves task Futures. One watcher per executor, one recv thread per shard.

**agent_host_entry** (``_host.py``)

The entry-point function that runs inside each Dragon AgentHost process.
Receives task messages via a Dragon ``Queue``, dispatches to agent functions
in a ``ThreadPoolExecutor``, and sends completion envelopes inline on the
shared completion ``Channel``.

**Completion envelope** (``_constants.py``)

The host and watcher agree on a small ``cloudpickle``'d dict per completion::

    {
      "task_id": str,                       # routes to the pending Future
      "status":  "done" | "error",
      "payload": <result_dict | exception>,
    }

Constants:

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - Constant
     - Value
     - Description
   * - ``STATUS_DONE``
     - ``"done"``
     - Agent function completed successfully.
   * - ``STATUS_ERROR``
     - ``"error"``
     - Agent function raised an exception.
   * - ``ENV_TASK_ID``
     - ``"task_id"``
     - Envelope field: routes the envelope to the correct Future.
   * - ``ENV_STATUS``
     - ``"status"``
     - Envelope field: success or failure.
   * - ``ENV_PAYLOAD``
     - ``"payload"``
     - Envelope field: the result dict or exception object.


Usage Patterns
==============

**Single host, all agents together (simplest):**

.. code-block:: python

    with DragonExecutor() as executor:
        executor.launch_host(agents={"a": fn_a, "b": fn_b, "c": fn_c})

        builder = StateGraph(State)
        builder.add_node("a", executor.node("a"))
        builder.add_node("b", executor.node("b"))
        builder.add_node("c", executor.node("c"))
        # ... add edges ...

**Multi-node, explicit placement:**

.. code-block:: python

    from dragon import System, Policy
    from dragon.native.machine import Node

    system = System()
    node0 = Node(system.nodes[0]).hostname
    node1 = Node(system.nodes[1]).hostname

    with DragonExecutor() as executor:
        executor.launch_hosts(
            hosts=[
                {"researcher": fn_research},
                {"writer": fn_write, "analyzer": fn_analyze},
            ],
            policies=[
                Policy(placement=Policy.Placement.HOST_NAME, host_name=node0),
                Policy(placement=Policy.Placement.HOST_NAME, host_name=node1),
            ],
        )

**With Dragon Inference (on-cluster LLM):**

.. code-block:: python

    from dragon.ai.inference.llm_proxy import DragonQueueLLMProxy
    from dragon.native.queue import Queue

    inference_queue = Queue()
    # ... start inference pipeline on GPU node ...
    llm = DragonQueueLLMProxy(inference_queue)

    with DragonExecutor() as executor:
        executor.launch_host(agents={
            "researcher": lambda state: researcher_fn(state, llm),
        })

**Agent with user-managed DDict (bulk data stays node-local):**

.. code-block:: python

    from dragon import DDict

    sim_data = DDict(managers_per_node=1, total_mem=10_000_000_000)

    def simulation_agent(state):
        result = run_simulation(state)
        sim_data["output_key"] = result  # bulk stays node-local
        return {"result_key": "output_key"}  # handle only

    with DragonExecutor() as executor:
        executor.launch_host(agents={"simulate": simulation_agent})


Error Handling
==============

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Scenario
     - Behavior
   * - Agent function raises
     - Exception packed into error envelope, sent inline on the completion
       Channel. Coordinator re-raises it. LangGraph sees a normal exception.
   * - Unpicklable exception
     - Falls back to ``RuntimeError(str(exc))`` so the envelope always
       serializes.
   * - Unknown ``node_name``
     - Host sends a ``ValueError`` error envelope. Caller never blocks forever.
   * - Task timeout
     - ``event_timeout`` triggers ``TimeoutError`` on the caller's Future.
   * - Host process crashes
     - No envelope arrives; ``event_timeout`` (if set) fires ``TimeoutError``.

Persistence
===========

A DDict-backed checkpointer and store are **not** currently provided. Use any
standard LangGraph checkpointer/store with ``DragonExecutor``:

.. code-block:: python

    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore

    graph = builder.compile(
        checkpointer=InMemorySaver(),
        store=InMemoryStore(),
    )

For production fault tolerance, use ``SqliteSaver`` or ``PostgresSaver`` so a
restarted coordinator can resume from the last checkpoint.


See Also
========

* :ref:`developer-guide-langgraph` — architecture walkthrough and component
  deep-dive.
* :ref:`cbook_langgraph` — guided examples (quickstart, multinode, inference,
  supervision, data locality).
