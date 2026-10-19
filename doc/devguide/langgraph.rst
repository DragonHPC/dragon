.. _developer-guide-langgraph:

LangGraph Integration
========================================

This document is a top-down walkthrough of the Dragon backend for LangGraph for
developers who will maintain and extend it. It starts with the big picture —
what the integration does and how the code is organized — then traces a single
node invocation through the system end-to-end, and finally drills into each
component in the order a request touches it.

For the Python API reference, see :ref:`LangGraphAPI`.

.. contents:: In this document
   :local:
   :depth: 2


.. _langgraph-big-picture:

1 — What This Integration Does
-------------------------------

The Dragon LangGraph backend lets an **existing** LangGraph ``StateGraph`` run
across the nodes of an HPC cluster **without changing the graph definition or the
node functions**. The user writes ordinary LangGraph code; the only difference is
that each node function executes inside a persistent Dragon **AgentHost process**
rather than in the local interpreter — a sync (``def``) node on the host's thread
pool, an async (``async def``) node on the host's event loop. Agents co-located on
the same host share memory, LLM clients, and other heavyweight resources.

The design splits cleanly into a **control plane**, an **execution plane**, and
a **data plane**:

* **Control plane (owned by this integration).** Node dispatch, result
  transport, completion signaling, backpressure, and result routing. Agent
  *results* travel inline as small ``cloudpickle`` envelopes over shared Dragon
  Channels. This is *how a task gets there and how its result comes back*.
* **Execution plane (shared — integration provides the host, user provides the
  compute).** Where the node function actually runs: a persistent Dragon
  **AgentHost process**, its thread pool, agent co-location and resource sharing,
  :py:class:`~dragon.Policy` placement, and any
  :py:class:`~dragon.native.process.Process` / ``ProcessGroup`` / ``Batch`` the
  agent itself launches — and *supervises and aborts mid-flight*. This is *where
  the science actually runs, and how it scales out and is steered*.
* **Data plane (owned by the user).** Bulk scientific data (tensors, arrays,
  datasets) belongs in a user-managed :py:class:`~dragon.DDict`. Agent
  functions store artifacts there and return only a lightweight handle/key
  through LangGraph state. This integration never creates or manages a DDict, and
  never moves bulk bytes through its transport. This is *where large data lives
  and moves*.

In one line: **control = signaling, execution = compute, data = storage.**

**LangGraph keeps full ownership of topology, routing, and checkpointing;
Dragon owns process placement, IPC, and completion signaling.** The two
layers meet at exactly one seam — a ``fn(state) -> dict``
callable per node.

Why Dragon on HPC
__________________

Agentic AI and HPC science are converging. The graph that *decides what to do
next* — which simulation to launch, which region to refine, which candidate to
keep — increasingly sits next to petascale compute and the multi-gigabyte
artifacts it produces. The hard part is no longer "call an LLM"; it is **keeping
the agent layer next to the data**, so a decision does not cost a network
round-trip of every result. Dragon addresses this with the **in-situ agent** —
the deciding graph runs *in place*, inside the allocation, next to the data and
compute it reasons about.

**Today this means integrating several separate systems.** Plain LangGraph runs
every node in the driver process (threads for ``invoke``, asyncio tasks for
``ainvoke``): it is bounded by one node's cores and one GIL, with no way to place
work near data. The usual workaround stitches together a workflow layer (such as
Parsl), a central store (such as Redis) so agents exchange results *by key*, and a
separate inference server for the LLMs — three or four systems to operate, and
results still pass **by value** through the store, so every GB-scale artifact is
serialized and copied over the network, often several times.

**In-situ agents run inside the allocation, next to the data.** The graph, the
agents, the simulations, and the data all live in the *same* Dragon runtime
spanning every node you were allocated. Backing each node with a Dragon
``AgentHost`` process, the same graph now:

* **Spans the allocation.** ``launch_host(..., policy=...)`` pins each agent host
  to a specific node, NUMA zone, or GPU set (through a
  :py:class:`~dragon.Policy`). Because you decide where each
  host lands, an agent can run on the same node where its data already lives.
* **Runs agents in real parallel, and lets them drive HPC jobs.** Each node
  function runs inside a host process out on the allocation, rather than on the
  driver's single interpreter. A step that fans out to many agents therefore
  executes concurrently across many nodes, instead of being serialized by one
  Python GIL. An agent can also launch its own Dragon
  ``ProcessGroup`` or ``Batch`` job of hundreds of ranks, supervise it, and abort
  it early if the partial results look wrong. In other words, a single decision
  node in the graph can steer a full-scale simulation.
* **Keeps bulk data node-local.** When an agent produces a large result, it writes
  that result to the node-local :py:class:`~dragon.DDict` manager, which
  lives in shared memory on the same node, so the write needs no serialization and
  no network copy. The agent then returns only a small **handle** (a key) through
  LangGraph state. The orchestrator passes handles between nodes and never moves
  the gigabytes themselves.
* **Moves data over the HPC fabric, not TCP.** If a later agent has to read data
  that lives on another node, Dragon transfers it over the cluster interconnect
  (RDMA/HSTA) rather than the slower kernel TCP stack.
* **Shares heavyweight resources.** Agents that sit in the same host share one
  address space, so expensive objects such as LLM clients, model weights, and
  database connections are loaded once and reused by every agent on that host.
* **Runs inference on the same runtime.** LLM serving can run *inside* the
  allocation through the Dragon inference service (on-cluster vLLM), reached over
  Dragon Queues and Channels. There is no separate inference server to stand up,
  secure, and route traffic to.
* **Keeps the coordinator thin.** Only small result envelopes travel back through
  the completion path, so the single coordinator node stays lightly loaded even
  when a step fans out to many agents at once.

The result is **one runtime instead of a stack of separate systems** — workflow
execution, node-local data, and LLM inference all run on Dragon. And your graph
does not change: the same ``StateGraph`` runs on top; only the substrate
underneath does.

.. note::
   **What the runtime provides vs. what this integration adds.** Running work as
   real processes across the allocation, launching and supervising a
   ``ProcessGroup``/``Batch``, node-local zero-copy data, data-aware placement, and
   RDMA transport are all properties of the Dragon *runtime*
   (``ProcessGroup`` + a node ``Policy`` + ``DDict``) — you get them with or
   without LangGraph. This integration adds a **zero-rewrite path** onto that
   substrate: keep your existing ``StateGraph`` — topology, conditional edges,
   checkpointing, state — and run it on Dragon, with the single-process
   orchestrator no longer executing every agent itself. The execution-plane
   benefit — true cross-node parallelism instead of one GIL, and agents that can
   drive full HPC jobs — applies to any multi-agent graph; the data-plane benefit
   is largest for **data-heavy** science, while for small JSON/text results a
   central store works just as well.

Architecture at a glance
________________________

One **coordinator** process (the driver running your graph) talks to one or more
persistent **AgentHost** processes spread across the allocation. A single host can
host **many agents** — each task message carries a ``node_name``, and the host
looks it up in its ``agents`` dict (``agents[node_name]``) to route the task to the
right agent function. The coordinator dispatches a task by putting a message on a
host's input ``Queue``; the host resolves the target agent by ``node_name`` and
runs *that* agent function (async on its event loop, sync on a thread pool), then
sends a small
result *envelope* back over a shared completion ``Channel``, where a single
watcher thread routes it to the waiting Future. The agent function runs on the
**execution plane** — and may itself launch supervised Dragon compute. Bulk data
never travels the control path — it lives in a user-managed ``DDict`` on the
compute nodes.

In the diagram below, the **LangGraph Pregel engine** is LangGraph's own,
built-in node scheduler — the thing ``graph.invoke()`` drives to decide which
nodes run and to merge their outputs back into state. It is unchanged by this
integration; ``DragonExecutor`` and ``DragonWatcher`` are the two pieces this
integration adds around it.

.. uml::
   :align: center

   @startuml
   skinparam componentStyle rectangle
   skinparam shadowing false

   node "Coordinator node" {
     frame "Driver process" {
       [LangGraph Pregel engine] as PREGEL
       [DragonExecutor] as EXEC
       [DragonWatcher\n(1 recv thread / shard)] as WATCH
     }
   }

   node "Compute node A" {
     frame "AgentHost process" as HOSTA {
       component "Dispatch\nagents[node_name]" as DISPA
       [ThreadPoolExecutor] as POOLA
       [researcher()\n(thread)] as FNA1
       [writer()\n(thread)] as FNA2
     }
     [ProcessGroup / Batch\n(agent-launched compute)] as CMPA
   }

   node "Compute node B" {
     frame "AgentHost process" as HOSTB {
       component "Dispatch\nagents[node_name]" as DISPB
       [ThreadPoolExecutor] as POOLB
       [analyzer()\n(thread)] as FNB1
     }
   }

   database "User-managed DDict\n(node-local shards)" as DDICT

   PREGEL -[#green]-> EXEC
   EXEC -[#green]-> WATCH
   WATCH -[#green]-> DISPA : input Queue\n(task message: node_name)
   WATCH -[#green]-> DISPB : input Queue\n(task message: node_name)
   DISPA -[#green]-> POOLA : route by node_name
   DISPB -[#green]-> POOLB : route by node_name
   POOLA -[#green]-> FNA1 : submit fn(state)
   POOLA -[#green]-> FNA2 : submit fn(state)
   POOLB -[#green]-> FNB1 : submit fn(state)
   FNA1 -[#green]-> WATCH : completion Channel\n(result envelope)
   FNA2 -[#green]-> WATCH : completion Channel\n(result envelope)
   FNB1 -[#green]-> WATCH : completion Channel\n(result envelope)
   FNA1 -[#orange]-> CMPA : launch + supervise\n(mid-flight abort)
   FNA1 ..[#blue].> DDICT : bulk artifacts
   FNB1 ..[#blue].> DDICT : bulk artifacts
   @enduml

Green arrows are the **control plane** (this integration); the orange arrow is the
**execution plane** (the agent launching and steering real compute); dotted blue
arrows are the **data plane** (the user's DDict). The rest of this guide follows
these arrows in order.


.. _langgraph-code-organization:

2 — Code Organization
----------------------

All source lives under ``src/dragon/ai/langgraph/``:

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Module
     - Responsibility
   * - ``__init__.py``
     - Public surface. Exports :py:class:`~dragon.ai.langgraph.DragonExecutor`
       and :py:func:`~dragon.ai.langgraph.get_llm`.
   * - ``executor.py``
     - ``DragonExecutor`` — lifecycle manager. Spawns hosts, registers one
       ``DragonAgentNode`` per agent, and returns LangGraph-compatible callables.
   * - ``_llm.py``
     - ``get_llm`` — per-host, per-pipeline accessor for a Dragon Inference
       ``DragonQueueLLMProxy``. Builds the (loop-bound) proxy lazily inside the
       host and caches it by inference queue.
   * - ``_host.py``
     - ``agent_host_entry`` — the function that runs inside each Dragon process.
       Listens for task messages and runs each agent function on a shared event
       loop (sync nodes offloaded to a thread pool).
   * - ``_node.py``
     - ``DragonAgentNode`` — the per-node callable (sync ``__call__`` and async
       ``acall``) that LangGraph invokes; delegates to the watcher.
   * - ``_watcher.py``
     - ``DragonWatcher`` — the blocking-recv result router. Owns the shared
       completion Channel(s) and resolves task futures.
   * - ``_constants.py``
     - Completion-envelope field names and status codes shared by host and
       watcher so the two sides never drift.


.. _langgraph-request-trace:

3 — End-to-End: One Node Invocation
------------------------------------

When LangGraph executes a node backed by this integration, a task flows from the
driver to a host and a result envelope flows back. The sequence below shows the
full round trip:

Before reading the trace, it helps to know what the **watcher** is. The watcher
is the coordinator-side component that collects *results coming back* from the
hosts — it does **not** dispatch jobs (jobs are sent over each host's input
``Queue``, steps 3–4 below). Its whole job is the return path: receive each
result envelope, deserialize it, and hand the result to the call that is waiting
for it.

To match a returning result to its caller, the watcher keeps two pieces of
bookkeeping per task, both populated **at dispatch time** (before the job is
sent):

* a **Future** in a **pending table** keyed by ``task_id`` — the ``Future`` is
  the standard Python placeholder the caller blocks on, and ``task_id`` is a
  unique id minted per task so the returning envelope can find it;
* a **slot** taken from a **semaphore** — the backpressure budget; each
  outstanding task holds one slot, so the slot count caps how many tasks are
  in flight at once.

For throughput the watcher can be split into ``num_shards`` independent
**shards** — parallel copies of {completion Channel + recv thread + pending
table + semaphore}, each handling a disjoint slice of tasks chosen by
``hash(task_id) % num_shards``. ``num_shards`` defaults to **1**, so on a first
read there is simply one lane.

Two diagrams cover the two halves of a task's life. First, **dispatch** — when a
request comes in, ``DragonAgentNode`` (the LangGraph node) calls
``submit_task`` on the watcher; the watcher mints a ``task_id``, hashes it to
pick a shard, reserves a slot from that shard (waiting if the shard is at its
concurrency cap — see :ref:`§4.4 <langgraph-components>` for how the sync and
async paths wait differently), records the Future in that shard's pending table,
and only then hands the job to the host — tagging the message with *that shard's*
completion Channel so the reply comes back to the right lane:

.. uml::
   :align: center
   :caption: Dispatch — a request comes in and is forwarded to a host.

   @startuml
   skinparam shadowing false
   skinparam componentStyle rectangle

   [DragonAgentNode\n(LangGraph node)] as N

   package "DragonWatcher" {
     [submit_task(state)\nmint task_id;\nshard = hash(task_id) % num_shards] as SUB
     package "chosen shard (of num_shards)" {
       [slots semaphore\n(backpressure)] as SEM
       [pending table\n{task_id -> Future}] as PT
     }
   }

   queue "host input Queue" as HQ

   N --> SUB : __call__ / acall
   SUB --> SEM : acquire a slot\n(block if none free)
   SUB --> PT : register the Future\nunder task_id
   SUB --> HQ : put task message\n{task_id, node_name, state,\ndone_q = this shard's Queue}
   @enduml

Then, **return** — when the host's result arrives, that same shard's recv thread
receives it, deserializes it, matches it to the Future by ``task_id``, resolves
it, and frees the slot:

.. uml::
   :align: center
   :caption: Return — a completed task's result is resolved.

   @startuml
   skinparam shadowing false
   skinparam componentStyle rectangle

   [AgentHost\n(sends result envelope)] as HOST

   package "DragonWatcher — one shard (of num_shards)" {
     queue "completion Channel\n(durable queue)" as CH
     [recv thread\nrecv_bytes + deserialize] as RT
     [pending table\n{task_id -> Future}] as PT
     [slots semaphore\n(backpressure)] as SEM
   }

   HOST --> CH : result envelope\n{task_id, status, payload}
   CH --> RT : receive + cloudpickle.loads
   RT --> PT : look up task_id,\nresolve its Future
   RT --> SEM : release the slot
   @enduml

So the pending table and semaphore are *populated* on dispatch and *drained* on
return, and the job itself never travels through the shard — only the small
result envelope does. With that in mind, here is the full round trip;
:ref:`§4.4 <langgraph-components>` covers sharding and backpressure in more depth.

.. uml::
   :align: center

   @startuml
   skinparam shadowing false
   autonumber

   actor "User code" as U
   participant "LangGraph\nPregel" as LG
   participant "DragonAgentNode" as N
   participant "DragonWatcher" as W
   queue "Host input Queue" as Q
   participant "AgentHost\nthread pool" as H
   queue "Completion Channel" as C

   U -> LG : run the graph (invoke / ainvoke)
   LG -> N : execute this node,\nhanding it the current state
   N -> W : hand the task to the watcher\n(submit_task)
   W -> W : pick a shard, take a backpressure slot,\nregister a Future under a new task_id
   W -> Q : send the job to the host\n{task_id, node_name, state, reply channel}
   Q -> H : host picks up the next task (Queue.get)
   H -> H : run fn(state)\n(async: on the loop; sync: on the thread pool)
   H -> C : send the result back inline\n{task_id, status, payload}
   C -> W : watcher receives + deserializes it (recv_bytes)
   W -> W : match task_id, resolve the Future,\nfree the backpressure slot
   W --> N : node unblocks with its result
   N --> LG : return the node's state update (dict)
   LG -> LG : merge update into graph state (apply_writes),\nthen schedule the next superstep
   @enduml

Step by step:

1. **Dispatch.** ``DragonAgentNode`` calls ``submit_task`` (sync) or
   ``submit_task_async`` (async) on the watcher.
2. **Register before dispatch.** The watcher picks a shard by
   ``hash(task_id) % num_shards``, acquires a backpressure slot from that shard's
   semaphore, and registers a ``_PendingTask`` (carrying the Future) in the
   shard's pending table — **before** the message leaves for the host. This is
   the core ordering invariant: a completion envelope can never arrive before the
   watcher knows which Future it resolves.
3. **To the host.** The task message ``{task_id, node_name, state,
   serialized_done_queue}`` is put on the host's input ``Queue``, which
   cloudpickles the whole message internally — so ``state`` rides as a raw
   object (no manual serialization). ``serialized_done_queue`` is the serialized
   descriptor of the shard's shared completion ``Queue``, which the host
   attaches to in order to reply.
4. **Execute.** The host runs the node as a concurrent task — ``await fn(state)``
   on its event loop for an async node, or offloaded to its thread pool for a sync
   node. Co-located agents reuse the host's shared LLM clients and weights.
5. **Reply inline.** The task puts a small envelope on the completion ``Queue``,
   whose ``EnvelopePickler`` frames and serializes it. On success the return
   dict rides inline; on failure the exception does.
6. **Route and resolve.** The shard's recv thread wakes from ``get``, decodes
   the envelope, looks up the Future by ``task_id``, resolves it (sync:
   ``set_result``; async: ``loop.call_soon_threadsafe``), and releases the slot.
7. **Return.** ``DragonAgentNode`` hands the result back and the LangGraph Pregel
   engine merges the returned dict into graph state (its ``apply_writes`` step)
   before starting the next **superstep** — Pregel's next scheduling round.


.. _langgraph-components:

4 — Component Deep-Dive
------------------------

4.1 — DragonExecutor (coordinator)
___________________________________

:py:class:`~dragon.ai.langgraph.DragonExecutor` is the user-facing lifecycle
manager and the owner of the shared result transport. It owns the single
:py:class:`DragonWatcher` that resolves all task futures. Its responsibilities:

* ``launch_host(agents, policy=...)`` — spawns one persistent
  ``dragon.native.process.Process`` running ``agent_host_entry``, performs a
  handshake to receive the host's input ``Queue`` handle, and registers one
  ``DragonAgentNode`` per agent. Call it multiple times (with different
  ``Policy`` objects) to place agents on different nodes.
* ``node(name)`` — returns a LangGraph ``RunnableCallable`` wrapping the agent's
  ``__call__`` (sync) and ``acall`` (async). This is what feeds
  ``StateGraph.add_node``.
* ``shutdown(graceful=...)`` — sends a ``None`` sentinel to each host, joins/kills
  the processes, and stops the watcher. Also runs via the context-manager
  ``__exit__``.

**Backpressure sizing.** ``max_concurrent_tasks`` caps how many tasks may be in
flight at once. It is a single dial with a fixed default (**64**): raise it for
more parallelism and pipelining, lower it to bound coordinator memory or to
protect a rate-limited downstream. Everything else — per-shard semaphore slots,
completion-channel capacity, and each host's input-queue size — is derived from
this one number, so you normally set only this. The completion Channels are sized
to the cap when the watcher is built (in the constructor) and are not resized
afterward.

The executor transports only agent *results*. It deliberately does **not** own
any DDict — bulk data is the user's responsibility.

4.2 — AgentHost (worker process)
_________________________________

``agent_host_entry`` (``_host.py``) is the function executed inside each Dragon
process. One host runs per cluster node (or hardware resource group) and
multiplexes many agent functions on a **single asyncio event loop** (mirroring
the Dragon agent framework's ``SubAgent.listen``). Because the agents share one
address space, they share LLM clients, model weights, and DB connections —
loaded once, reused by all.

Node functions may be **sync or async**, and each runs on the lane that suits it:

* A plain ``def`` node is offloaded to a bounded ``ThreadPoolExecutor``
  (``max_threads``), so its blocking work is isolated on its own thread.
* An ``async def`` node runs as a task **directly on the shared loop**, so many
  I/O-bound async agents interleave at ~zero threads each. An existing LangGraph
  graph with async nodes (or async LLM/tool clients) runs unchanged, and a
  loop-bound resource (e.g. an async client) created once is reused across calls.

The blocking Dragon IPC — the input ``Queue.get`` and the completion-Channel
sends — runs on a small separate pool so a burst of long sync nodes can never
starve task intake or result delivery.

The host is created by ``launch_host()`` through a short handshake: the host
allocates its own input ``Queue`` and ships the serialized handle back to the
coordinator, which blocks until it arrives before registering any node.

.. uml::
   :align: center

   @startuml
   skinparam shadowing false
   participant "DragonExecutor" as E
   participant "dragon.native.Process" as P
   participant "agent_host_entry" as H
   queue "reply_queue" as R

   E -> P : start(target=agent_host_entry,\nkwargs, policy)
   activate P
   P -> H : run in new process
   activate H
   H -> H : create input Queue
   H -> R : put(input_queue.serialize())
   R -> E : get()  (blocks until ready)
   E -> E : register one DragonAgentNode\nper agent name
   H -> H : enter listen loop on input Queue
   @enduml

Lifecycle:

1. Create a Dragon ``Queue`` and send its serialized handle back to the
   coordinator over the handshake ``reply_queue``.
2. Run one asyncio loop: receive each task message and dispatch it as a
   concurrent task — ``await fn(state)`` for an async node, or offload
   ``fn(state)`` to the thread pool for a sync node.
3. Each task serializes a completion envelope and puts it inline on the task's
   done-Queue (the shared completion ``Queue`` handed in per task).
4. Exit cleanly on the ``None`` sentinel, draining in-flight tasks first.

If a task names an unknown agent, the host still sends an error envelope so the
caller never blocks forever.

.. note::
   **The async-loop contract (and its blast radius).** Because async nodes share
   one loop, the standard asyncio rule applies: an ``async def`` node must not
   **block the loop** — a synchronous blocking call, or a long CPU-bound stretch,
   *without* ``await`` stalls every *other* agent co-located on that host until it
   returns. The blast radius is that one host process; other hosts are separate
   processes with their own loops. This is the normal responsibility of any async
   code — wrap blocking work in ``await asyncio.to_thread(...)``. You do **not**
   need to know about co-location to write a correct async node: following the
   standard rule is sufficient, and co-location only changes *who* is affected if
   the rule is broken. Plain ``def`` nodes are never subject to this — they run on
   their own thread.

4.3 — DragonAgentNode (the seam)
_________________________________

``DragonAgentNode`` (``_node.py``) is the ``fn(state) -> dict`` callable LangGraph
sees. It holds no per-task state; it just forwards to the watcher:

* ``__call__(state)`` — synchronous path used by ``graph.invoke``. LangGraph runs
  it inside a ``BackgroundExecutor`` thread that parks on ``future.result()``.
* ``acall(state)`` — asynchronous path used by ``graph.ainvoke``. It awaits an
  ``asyncio.Future``, suspending the coroutine on the event loop instead of
  parking a thread. N concurrent agents in a superstep therefore cost **zero**
  extra threads — only the event loop plus the watcher thread(s).

4.4 — DragonWatcher (result router)
____________________________________

``DragonWatcher`` (``_watcher.py``) is the heart of the control plane. Its
dispatch and return paths — and the four parts of a shard (completion Channel,
recv thread, pending table, slots semaphore) — are diagrammed in
:ref:`§3 <langgraph-request-trace>`; this section covers the design rationale.

**Why one shared Channel, not a per-task channel.** A Dragon Channel is a true
multi-producer / single-consumer durable queue: every message sent is reliably
enqueued and later received, whether or not the consumer happens to be parked in
``recv`` at that instant. Carrying the result as a *queued message* (rather than
an edge-triggered event, as a ChannelSet ``bcast_wait`` would) means a completion
can never be lost to a missed wakeup — so no task is ever left hanging. The
watcher is still fully event-driven: ``recv_bytes(timeout=None)`` parks the
thread in the kernel (zero CPU when idle) and wakes the instant a message
arrives. No polling.

**Sharding (``num_shards``).** The per-message route step
(``recv_bytes`` → ``cloudpickle.loads`` → resolve Future) is serial on one
thread. Setting ``num_shards > 1`` creates that many independent lanes
(``_Shard`` — its own Channel, recv thread, semaphore, and pending table); tasks
are partitioned by ``hash(task_id) % num_shards``, so each shard owns a disjoint
slice and no lock is ever contended across shards. Raise it only when a single
watcher thread becomes the bottleneck (very high completion rate of small
results).

**Backpressure.** ``max_concurrent_tasks`` caps simultaneous in-flight tasks via
a ``BoundedSemaphore`` (split evenly across shards). It has a fixed default
(**64**); pass a value to raise or lower it (see
:ref:`§4.1 <langgraph-components>`). When the
cap is hit, a further dispatch waits for a task to complete — the sync path
(``submit_task``) blocks its calling ``BackgroundExecutor`` thread, while the
async path (``submit_task_async``) *awaits* the slot without blocking the event
loop (the blocking wait is offloaded to a worker thread), so ``graph.ainvoke``
stays responsive under backpressure. Each shard's Channel is
sized to ``per_shard_ring + headroom``, so the channel can never overflow even if
every in-flight task completes at once. Total tasks over the executor's lifetime
are unlimited — only *peak* concurrency matters.

**Sizing cheat-sheet.** You tune exactly **one** number —
``max_concurrent_tasks`` — and everything else is derived from it, so no number is
ever a silent second bottleneck:

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Number
     - What it is / how it's set
   * - ``max_concurrent_tasks`` **(the knob)**
     - Max tasks *in flight* (dispatched but not yet completed) at once. Fixed
       default **64**; raise for more parallelism, lower to use less memory.
       Everything below follows from it.
   * - Per-shard slots
     - ``max_concurrent_tasks / num_shards`` — the in-flight cap split evenly
       across each shard's backpressure semaphore.
   * - Completion Channel capacity
     - ``per-shard slots + headroom`` — big enough to hold a full burst of
       completions; it can never overflow because at most that many tasks are in
       flight.
   * - Host input Queue size
     - ``max_concurrent_tasks + headroom`` (set by the executor, per host) — so
       the coordinator can always enqueue a dispatched task without ``put()``
       blocking on queue capacity (worst case: every in-flight task routes to one
       host). Backpressure stays solely on the semaphore.
   * - ``num_shards``
     - Number of parallel receive/decode lanes. Raise only if one watcher thread
       can't keep up with the completion rate (default 1).
   * - ``max_threads`` (per host)
     - Threads for **sync** (``def``) node bodies in each host's pool (default
       ``max(len(agents) * 4, 8)``). Async nodes run on the host's shared event
       loop and are **not** bounded by this.

In one line: **set ``max_concurrent_tasks``; the semaphore, the completion
channels, and the host input queues all size themselves from it.**

**Choosing ``max_concurrent_tasks`` and ``max_threads`` together.** Both have
sensible defaults and both are yours to override — but they bound *different*
things:

* ``max_concurrent_tasks`` (coordinator) bounds tasks *in flight across the whole
  system*. ``max_threads`` (per host) bounds how many **sync** (``def``) node
  bodies run at once *inside one host* (they run on a thread pool). **Async**
  (``async def``) nodes do not use that pool — they run on the host's shared event
  loop, bounded only by ``max_concurrent_tasks``.
* **Async (I/O-bound) agents:** the cap is the real limiter — raise it freely for
  more concurrency; ``max_threads`` does not gate them.
* **Sync agents:** admitting far more than a host's ``max_threads`` (``cap`` ≫
  per-host sync threads) just queues them — more coordinator/host memory and
  latency, no extra throughput (a host runs at most ``max_threads`` sync bodies at
  a time). Sizing the cap near the sync-thread capacity keeps workers busy without
  a backlog.

Neither knob changes the other: the cap never spawns host threads, and raising
``max_threads`` never widens the cap. Set them deliberately — you own the
behavior of the combination you pick.

**Return-message collection (fan-in).** This is where results from every host in
the allocation converge. Regardless of how many AgentHost processes are running
or how wide a superstep fans out, every completion is funneled back through the
same small set of shard channels and resolved by the shard's single recv thread —
multiple hosts (the *producers*) send envelopes into the sharded completion
channels, and each shard's recv thread (the *consumer*) drains its channel and
matches each envelope to a waiting Future by ``task_id``:

.. uml::
   :align: center

   @startuml
   skinparam shadowing false
   skinparam componentStyle rectangle

   ' ---- Producers: agent threads across many hosts/nodes ----
   package "AgentHost A (node 0)" {
     [planner()] as PA
     [runner_0()] as RA
   }
   package "AgentHost B (node 1)" {
     [runner_1()] as RB
     [runner_2()] as RC
   }
   package "AgentHost C (node 2)" {
     [analyzer()] as AC
   }

   ' ---- Sharded completion transport in the coordinator ----
   package "DragonWatcher (coordinator)" {
     queue "Completion Channel 0\n(Global Services)" as CH0
     queue "Completion Channel 1\n(Global Services)" as CH1
     [recv thread 0\nrecv_bytes(timeout=None)] as T0
     [recv thread 1\nrecv_bytes(timeout=None)] as T1
     [pending table 0\n{task_id -> Future}] as P0
     [pending table 1\n{task_id -> Future}] as P1
     [Futures\n(sync + asyncio)] as FUT
   }

   ' ---- Producers send envelopes to the shard chosen at dispatch ----
   PA -[#green]-> CH0 : envelope\n{task_id, status, payload}
   RA -[#green]-> CH1 : envelope
   RB -[#green]-> CH0 : envelope
   RC -[#green]-> CH1 : envelope
   AC -[#green]-> CH0 : envelope

   ' ---- Each shard drains its own channel independently ----
   CH0 --> T0 : durable MPSC queue\n(never a missed wakeup)
   CH1 --> T1
   T0 -> T0 : cloudpickle.loads
   T1 -> T1 : cloudpickle.loads
   T0 --> P0 : pop(task_id)
   T1 --> P1 : pop(task_id)
   P0 --> FUT : set_result / call_soon_threadsafe\n+ release backpressure slot
   P1 --> FUT
   @enduml

Reading the diagram: the number of consumer threads is fixed at ``num_shards`` —
not the number of hosts or the width of a superstep — so the coordinator's
footprint stays constant no matter how wide the allocation fans out. In effect the
shards are a **fixed-width fan-in**: the entire allocation's results are collected
through ``num_shards`` lanes and demultiplexed to the correct Futures purely by
table lookup.

4.5 — Completion envelope (``_constants.py``)
______________________________________________

The host and watcher agree on a tiny ``cloudpickle``'d dict per completion::

    {
      "task_id": str,                       # routes to the pending Future
      "status":  "done" | "error",
      "payload": <result_dict | exception>,
    }

The field-name constants live in ``_constants.py`` so the two sides can never
drift. On success the agent's return value rides inline; on failure the exception
is carried inline (falling back to a ``RuntimeError`` with the text if the
exception object cannot be pickled).


.. _langgraph-data-plane:

5 — Data-Plane Scaling: Central Store vs Node-Local DDict
------------------------------------------------------------

The three planes were defined in :ref:`§1 <langgraph-big-picture>`. The **data
plane** is the one that decides whether a *data-heavy* campaign scales: bulk
artifacts live in a user-managed ``DDict`` and only a lightweight handle/key
crosses LangGraph state, so the watcher stays a pure signal router and the
coordinator never materializes gigabytes of intermediate data. It is worth
seeing *why* that matters, by contrast with the usual central-store approach.

Before: the central-store funnel
________________________________

Without Dragon, a LangGraph orchestrator on one node glues itself to heavy
compute elsewhere through the filesystem or a central store (Redis, shared FS).
Even with the "handle-in-state" pattern (store the payload under a key, pass the
key), the bulk still lives in **one** server: every producer writes it over the
network and every consumer reads it back over the network — the consumer can
never be moved to the data. That server's RAM and NIC are the ceiling.

.. uml::
   :align: center

   @startuml
   skinparam shadowing false
   skinparam componentStyle rectangle

   node "Login / single node" {
     [LangGraph orchestrator] as ORCH
   }
   database "Central store\n(Redis / shared FS)" as REDIS
   node "Compute node 1" {
     [producer job] as J1
   }
   node "Compute node 2" {
     [consumer agent] as K1
   }

   ORCH --> J1 : submit (+ key in state)
   J1 -[#red]-> REDIS : write 2 GB over TCP
   REDIS -[#red]-> K1 : read 2 GB back over TCP\n(consumer can't move to data)
   K1 ..> ORCH : small result / key
   @enduml

After: node-local DDict + data-aware placement
______________________________________________

With Dragon the agent layer runs *inside* the allocation. A producer writes its
large result to the **node-local** DDict manager (zero-copy, no network) and
returns only a key through LangGraph state. The consumer is **placed on the node
where the data lives**, so its read is local; Dragon uses the HPC interconnect
(RDMA/HSTA) only when a read must cross nodes. The orchestrator only ever sees
handles.

.. uml::
   :align: center

   @startuml
   skinparam shadowing false
   skinparam componentStyle rectangle

   node "Head node" {
     [LangGraph graph\n+ DragonExecutor] as EX
   }
   node "Compute node A" {
     [Producer agent] as A1
     database "DDict manager\n(node-local shmem)" as D1
     A1 -[#green]-> D1 : write 2 GB LOCAL\n(zero-copy)
   }
   node "Compute node B" {
     [Consumer agent\nplaced where data lives] as A2
   }

   EX --> A1 : dispatch (handle)
   A1 ..> EX : return KEY only
   EX --> A2 : dispatch (handle)
   A2 ..> EX : small result
   D1 .[#blue].> A2 : cross-node read over RDMA\n(only if remote)
   @enduml

.. note::
   Locality and placement are properties of the **Dragon runtime**
   (``ProcessGroup`` + a node ``Policy`` + ``DDict``), available with or without
   LangGraph. This integration's job is to let an existing LangGraph graph reach
   that substrate without a rewrite — it does not, by itself, own locality. The
   data-plane win is real only for **data-heavy** results; for small JSON/text
   payloads a central store is perfectly adequate.


.. _langgraph-sync-async:

6 — Sync vs Async Concurrency
------------------------------

**What is the same.** ``invoke`` and ``ainvoke`` share the *entire* pipeline —
the same dispatch, the same watcher, the same backpressure semaphore, and, most
importantly, the **same host-side execution**. Whichever you call, a node runs on
the host the *same* way: a sync (``def``) node on the host's thread pool, an async
(``async def``) node on the host's event loop. The driver does **not** change
where or how your node runs, and both produce the identical result. Your node's
sync/async-ness is therefore *independent* of the ``invoke``/``ainvoke`` choice.

**What differs — only how the coordinator waits.** The single difference is on
the *caller* side: how the coordinator waits for each node's Future.

.. list-table::
   :header-rows: 1
   :widths: 18 41 41

   * - Path
     - ``graph.invoke`` (sync)
     - ``graph.ainvoke`` (async)
   * - Node entry
     - ``DragonAgentNode.__call__``
     - ``DragonAgentNode.acall``
   * - Future type
     - ``concurrent.futures.Future``
     - ``asyncio.Future`` bound to the running loop
   * - Caller waits by
     - parking a LangGraph ``BackgroundExecutor`` thread on ``result()``
     - suspending the coroutine on the event loop
   * - Coordinator threads for N concurrent nodes
     - up to N (one parked thread per in-flight node)
     - 0 extra (only the loop + watcher thread)

The watcher resolves async futures via ``loop.call_soon_threadsafe`` from its recv
thread, which is why ``ainvoke`` can fan out to many concurrent agents without
spawning a thread per node.

The same split applies to **backpressure**. When in-flight tasks reach
``max_concurrent_tasks``, the wait for a free slot follows the caller's
concurrency model: ``invoke`` blocks its ``BackgroundExecutor`` thread, while
``ainvoke`` *awaits* the slot — the blocking wait is offloaded to a worker thread
so the event loop keeps turning and resolving completions (which is exactly what
frees slots), instead of freezing. In both cases the single ``BoundedSemaphore``
per shard remains the one budget, so the completion-channel-can't-overflow
guarantee is unchanged.

**When to use which — this matters as you scale concurrent agents.** Because the
host runs your nodes identically either way, the choice is *purely* about the
coordinator's cost of waiting, which scales with **how many nodes are in flight at
once** (a superstep's fan-out width). Know what happens under the hood so you pick
responsibly:

* ``ainvoke`` is a functional **superset** — anything ``invoke`` does it can do
  (wrap it: ``asyncio.run(graph.ainvoke(...))``). Its only requirement is that you
  call it from an event loop. It adds **no capability** over ``invoke``; it only
  changes how the coordinator waits.
* **Chain or modest fan-out** (one, or a handful of, nodes in flight at a time):
  ``invoke`` parks that many ``BackgroundExecutor`` threads on ``future.result()``
  — idle threads (memory + a scheduler slot, no CPU), so the overhead is
  negligible. And ``invoke`` is simpler: a plain blocking call, no event loop,
  linear stack traces. **Prefer ``invoke``** here when your driver is synchronous.
* **Wide fan-out** (tens/hundreds/thousands of nodes concurrent in one superstep):
  ``invoke`` needs **one coordinator thread per in-flight node** and can saturate
  LangGraph's sync executor pool (serializing work beyond the pool size), while
  ``ainvoke`` awaits every node on **one** event loop at ~zero threads each.
  **Prefer ``ainvoke`` when you scale concurrency** — it is the difference between
  N parked threads (and a possible pool bottleneck) and none.

**Rule of thumb:** pick the driver by your **caller's shape and fan-out width**,
not by your node bodies. Synchronous driver + narrow graph → ``invoke``; async
application or wide fan-out → ``ainvoke``. Either way, ``max_concurrent_tasks``
still caps total in-flight work regardless of the driver (see
:ref:`§4.4 <langgraph-components>`).


.. _langgraph-dragon-primitives:

7 — Dragon Primitives Used
----------------------------

The integration is built on a small set of Dragon primitives. Understanding
which primitives are used and where is key to understanding the architecture.

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - Primitive
     - Module
     - Role in LangGraph Integration
   * - :py:class:`~dragon.native.queue.Queue`
     - ``dragon.native.queue``
     - Host input queues and the startup handshake reply queue. One Queue per
       AgentHost process, shared by all agents on that host. The primary
       dispatch mechanism (coordinator → host).
   * - :py:class:`~dragon.channels.Channel`
     - ``dragon.channels``
     - Shared completion channel (one per shard), created through Global
       Services and attached from its serialized descriptor — which is what
       makes it reachable from hosts on remote nodes. All hosts enqueue result
       envelopes; the watcher's recv thread drains it. Durable MPSC queue —
       messages are never lost.
   * - ``dragon.globalservices.channel``
     - ``dragon.globalservices``
     - ``create`` / ``release_refcnt`` / ``destroy`` for the completion
       channel(s). GS registration is what lets the transport agent route a
       remote host's send into the channel.
   * - :py:class:`~dragon.managed_memory.MemoryPool`
     - ``dragon.managed_memory``
     - Only ``attach_default()`` — the watcher creates no pool of its own; the
       completion channel(s) live in the runtime's default pool. The pool is
       shared infrastructure, so it is detached (never destroyed) at teardown.
   * - :py:class:`~dragon.channels.ChannelSendH` / :py:class:`~dragon.channels.ChannelRecvH`
     - ``dragon.channels``
     - Send/receive handles for the completion channel. One send handle per
       agent thread (opened per task), one recv handle per watcher shard
       (persistent).
   * - :py:class:`~dragon.native.process.Process`
     - ``dragon.native.process``
     - AgentHost process lifecycle. One Process per ``launch_host()`` call.
   * - :py:class:`~dragon.Policy`
     - ``dragon``
     - Optional placement control — pin a host to a specific cluster node,
       NUMA zone, or GPU set. Passed via ``launch_host(policy=...)``.

**Dragon import concentration.** Only 3 of 6 source files contain ``dragon.*``
imports:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - File
     - Dragon primitives
   * - ``executor.py``
     - ``Process`` (spawn), ``Queue`` (handshake reply)
   * - ``_host.py``
     - ``Queue`` (create input queue), ``ChannelSendH`` (send envelope)
   * - ``_watcher.py``
     - ``globalservices.channel`` (create/release/destroy the completion
       channel), ``MemoryPool.attach_default()`` (pool for that channel),
       ``Channel.attach`` (attach the descriptor), ``ChannelRecvH`` (recv loop),
       ``ChannelSendH`` (stop sentinel)

The remaining files (``__init__.py``, ``_node.py``, ``_constants.py``) are pure
Python with no Dragon imports.


.. _langgraph-error-handling:

8 — Error Handling
-------------------

The integration handles errors at every boundary:

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - Scenario
     - Handling
   * - Agent function raises
     - Exception is packed into an error envelope (``status="error"``,
       ``payload=exception``), sent inline on the completion Channel. The
       watcher resolves the future with ``set_exception()``. LangGraph sees
       a normal Python exception.
   * - Unpicklable exception
     - The Queue's ``EnvelopePickler`` substitutes a ``RuntimeError`` carrying
       the original text, so ``dump`` never raises and the envelope always
       reaches the wire under its own ``task_id``.
   * - Unknown ``node_name``
     - Host logs the error and sends a ``ValueError`` error envelope so the
       caller never blocks forever.
   * - Task timeout
     - Sync: ``future.result(timeout=...)`` raises ``TimeoutError``. Async:
       ``asyncio.wait_for`` raises ``TimeoutError``. The backpressure slot is
       released. A late-arriving completion for an unknown ``task_id`` is
       silently discarded by the watcher.
   * - Host process crashes
     - No envelope arrives. If ``event_timeout`` is set, the caller's
       ``future.result(timeout=...)`` or ``asyncio.wait_for`` raises
       ``TimeoutError``. The backpressure slot is released.
   * - Dispatch failure
     - If ``input_queue.put()`` raises, the watcher unwinds the registration:
       pops the pending task, releases the slot, and fails the Future
       immediately.
   * - Coordinator crashes
     - Use a persistent LangGraph checkpointer (``SqliteSaver``,
       ``PostgresSaver``) so a new coordinator can resume from the last
       checkpoint.

All error paths guarantee that exactly one of these happens for every
dispatched task:

1. The Future is resolved with a result, **or**
2. The Future is resolved with an exception, **or**
3. The caller's timeout fires.

No task is ever left hanging indefinitely (provided ``event_timeout`` is set
or the host process remains alive).


.. _langgraph-lifecycle:

9 — Lifecycle & Memory Management
-----------------------------------

The lifecycle of every resource managed by this integration:

::

    DragonExecutor()
      ├─ Attaches to the default MemoryPool (once, not per shard)
      ├─ Creates ONE shared completion Channel per shard
      │    via Global Services, in that default pool
      └─ Starts ONE watcher recv thread (per shard)

    executor.launch_host(agents={...}, policy=...)
      ├─ Creates Dragon Process (persistent)
      ├─ Process creates input Queue, sends handle back (handshake)
      └─ Executor creates DragonAgentNode per agent name

    [... graph runs, tasks dispatched ...]

    Per task (HOT PATH — nothing created/destroyed):
      ├─ Acquire backpressure slot (semaphore)
      ├─ Register Future in pending table
      ├─ Queue.put(msg) → host
      ├─ fn(state) → result (async: on the host loop; sync: on the thread pool)
      ├─ ChannelSendH.send_bytes(envelope) → watcher
      ├─ recv_bytes() → decode → resolve Future
      └─ Release backpressure slot

    DragonExecutor.shutdown() / __exit__()
      ├─ Send None sentinel to each host's input Queue
      ├─ Host stops accepting work, drains in-flight tasks, closes its pools
      ├─ proc.join(timeout=10), then proc.kill()
      └─ watcher.stop()
           ├─ Send stop sentinel envelope to each shard's channel
           ├─ Join each shard's recv thread
           ├─ Close recv handles
           ├─ Per shard: release_refcnt(cuid) → destroy(cuid) (Global Services)
           └─ Detach the default MemoryPool (never destroyed — shared
              runtime infrastructure)

**Memory guarantee:** This layer never leaks resources. The shared completion
Channels are created once at startup through Global Services and released and
destroyed the same way on executor exit; the default MemoryPool they live in is
only attached and detached, never destroyed, because it is shared runtime
infrastructure. No Dragon objects are created or destroyed on the hot path (per
task).

**User-managed DDict:** Any DDict you create is yours to manage. The executor
neither creates nor destroys it. Free it (``del`` keys / ``ddict.destroy()``)
when your experiment finishes.


.. _langgraph-multinode:

10 — Multi-Node Deployment
----------------------------

Each ``launch_host()`` call spawns one Dragon Process. Use a ``Policy`` to place
each host on a specific cluster node:

.. uml::
   :align: center
   :caption: Multi-node deployment — where each component runs.

   @startuml
   skinparam shadowing false
   skinparam componentStyle rectangle
   skinparam nodesep 14
   skinparam ranksep 26

   node "Node 0 (coordinator)" as N0 {
     [LangGraph graph\n+ DragonExecutor] as GRAPH
     [DragonWatcher\n(recv thread)] as WATCH
     GRAPH --> WATCH
   }

   node "Node 1 (agents)" as N1 {
     frame "AgentHost" {
       [researcher\n(thread)] as RES
       [writer\n(thread)] as WRI
     }
     [Shared LLM proxy] as LLM1
   }

   node "Node 2 (GPU inference)" as N2 {
     [vLLM\n(tp=4 GPUs)] as VLLM
   }

   node "Node 3 (compute)" as N3 {
     frame "AgentHost" {
       [simulator\n(thread)] as SIM
     }
     database "DDict\n(node-local)" as DD
     SIM --> DD : write bulk\n(zero-copy)
   }

   WATCH --> RES : input Queue
   WATCH --> SIM : input Queue
   RES --> WATCH : completion Channel
   SIM --> WATCH : completion Channel
   LLM1 .[#blue].> VLLM : inference Queue
   @enduml

All communication uses the Dragon primitives catalogued in
:ref:`§7 <langgraph-dragon-primitives>`: ``Queue`` for task dispatch and the
startup handshake, one shared ``Channel`` per shard for completions, ``Process``
per ``launch_host()`` call, ``Policy`` for placement, plus the user-owned
``DDict`` and any ``ProcessGroup`` an agent launches.


.. _langgraph-usage-patterns:

11 — Usage Patterns
--------------------

**Pattern A — Single host, all agents together (simplest)**

.. code-block:: python

    with DragonExecutor() as executor:
        executor.launch_host(agents={"a": fn_a, "b": fn_b, "c": fn_c})

All agents share one process, one thread pool, one set of resources.

**Pattern B — Multi-node, explicit placement (batch style)**

.. code-block:: python

    with DragonExecutor() as executor:
        executor.launch_hosts(
            hosts=[
                {"researcher": fn},
                {"analyzer": fn, "writer": fn},
            ],
            policies=[policy_node0, policy_node1],
        )

One host per node. Each entry in ``policies`` is applied to the corresponding
host. Agents on the same node share resources; agents on different nodes are
isolated.

**Pattern C — GPU isolation (one host per GPU)**

.. code-block:: python

    with DragonExecutor() as executor:
        executor.launch_hosts(
            hosts=[
                {"gpu_agent_0": fn},
                {"gpu_agent_1": fn},
            ],
            policies=[gpu0_policy, gpu1_policy],
        )

**Pattern D — With Dragon Inference (on-cluster LLM)**

Call :func:`~dragon.ai.langgraph.get_llm` **inside** an ``async`` node, passing
the inference pipeline's queue handle. The proxy is built lazily *in the host
process* and cached per pipeline, so it binds to the host's event loop and is
reused across calls. Do **not** build the proxy on the coordinator and pass it
in — it is loop-bound and per-process.

.. code-block:: python

    from dragon.ai.langgraph import DragonExecutor, get_llm

    input_queue = Queue()                       # the inference pipeline's ingress
    Process(target=start_inference, args=(input_queue,),
            policy=gpu_policy).start()

    async def researcher(state):                # async node → runs on the host loop
        resp = await get_llm(input_queue).chat([...])   # built once, in the host
        return {"research": resp}

    with DragonExecutor() as executor:
        executor.launch_host(agents={"researcher": researcher})
        graph.ainvoke(...)                      # async driver

Two models on one host is just two queues — each ``get_llm(q)`` returns that
pipeline's own cached proxy::

    async def a(state): return {"x": await get_llm(llama_q).chat([...])}
    async def b(state): return {"y": await get_llm(mistral_q).chat([...])}

(If your inference service exposes an OpenAI-compatible HTTP endpoint, you can
instead use a normal ``ChatOpenAI(base_url=...)`` created once at module scope —
lowest friction, at the cost of HTTP transport instead of Dragon Channels.)

**Pattern E — Agent dispatches HPC compute; bulk stays in user's DDict**

.. code-block:: python

    from dragon import DDict
    from dragon.ai.langgraph import get_llm

    sim_data = DDict(managers_per_node=1, total_mem=10_000_000_000)

    async def simulation_agent(state):
        plan = await get_llm(input_queue).chat([...])
        pg = ProcessGroup(nproc=256, policy=compute_policy)
        pg.start(target=run_sim, args=(plan, sim_data))
        pg.join()
        return {"result_key": "sim_output_42"}  # handle only

    with DragonExecutor() as executor:
        executor.launch_host(agents={"simulate": simulation_agent})
