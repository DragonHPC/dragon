# Dragon + LangGraph examples

## Prototype the graph on your laptop. Run the science on the HPC cluster — unchanged.

You already sketched your agentic workflow in LangGraph. Dragon turns each node
into a first-class HPC job — across **three planes**:

| Plane | What it means for you |
|-------|-----------------------|
| **Control plane** — *what you keep* | Your graph, untouched. Same `StateGraph`, same nodes, same routing. You wrap each agent with `executor.node(name)`; nothing about your graph logic changes. |
| **Execution plane** — *what you gain* | Each node becomes a **placed, resource-sharing agent** that can launch *and supervise* real Dragon compute — an MPI `ProcessGroup`, a `Batch` ensemble, on-cluster vLLM — and **abort a diverging run mid-flight** to reclaim the compute. |
| **Data plane** — *what stops being a bottleneck* | Bulk fields (tensors, meshes, trajectories) stay **node-local** in a `DDict`, zero-copy; only lightweight handles ride your graph state. Terabyte intermediates never funnel through the coordinator. |

> **control = signaling · execution = compute · data = storage.** You keep
> LangGraph's ergonomics; you gain the supercomputer.

A guided path showing what **Dragon adds to a LangGraph agent graph** on an HPC
cluster — distributed agent execution, node placement, on-cluster LLM
inference, node-local data locality, and real-time supervision of live
experiments.

Every example demonstrates a capability that plain LangGraph does **not** have.
Read them in order; each adds one new Dragon ability and builds toward a
self-driving scientific lab.

```bash
# Examples run under the Dragon runtime, not plain `python`:
dragon examples/dragon_ai/langgraph/01_quickstart.py
```

> **What the integration is.** `from dragon.ai.langgraph import DragonExecutor`
> runs your LangGraph agents on the Dragon runtime. You wrap each agent with
> `executor.node(name)` and register it with `executor.launch_host(...)`; the
> graph definition is unchanged, but execution moves onto Dragon and can scale
> across the cluster.

---

## The ladder

| # | File | New Dragon ability | Needs |
|---|------|--------------------|-------|
| 01 | [`01_quickstart.py`](01_quickstart.py) | Run LangGraph agents as a Dragon **AgentHost process** via `launch_host` + `executor.node()`. | 1 node |
| 02 | [`02_multinode_placement.py`](02_multinode_placement.py) | `Policy` pins each AgentHost to a chosen **cluster node** (one `launch_host` per node). | 2+ nodes |
| 03 | [`03_local_inference.py`](03_local_inference.py) | Async agents call an LLM served **on the cluster** by Dragon Inference (vLLM behind a queue) via `get_llm(queue)` — the per-host proxy accessor; no external API, no rate limits, air-gap friendly. | GPU nodes |
| 04 | [`04_realtime_supervision.py`](04_realtime_supervision.py) | Experiments run as first-class Dragon `Process`es streaming over a `Queue`; a supervisor **aborts diverging runs mid-flight** via a `DDict` flag and reclaims their compute. | 1 node |
| 05 | [`05_data_locality_crossover.py`](05_data_locality_crossover.py) | **Data locality + distributed agents, measured.** Bulk fields live in a distributed `DDict` (node-local); control-plane vs data-plane agents are pinned by `Policy`; reducers reduce **in place**. Prints the funnel-vs-handles crossover table. | 1 node (more shows locality) |
| 06 | [`06_self_driving_lab.py`](06_self_driving_lab.py) | The capstone: on-cluster LLM planner + judge (03) + a **Dragon Batch** experiment ensemble whose live progress and bulk output ride a `DDict` (05) + real-time **mid-flight abort via a `DDict` flag** (04), placed across two nodes with `Policy`. | 1 node (GPU optional) |

Examples **04** and **05** are the two hero stories — mid-flight steering of
live work (a capability plain LangGraph structurally lacks), and the measured
data-plane crossover that proves *why* the data-heavy version holds at GB
scale.

---

## Running the examples

All examples run under the Dragon runtime (`dragon <file>`), **not** plain
`python`. From the repository root:

```bash
# 01 — Quickstart (single node)
dragon examples/dragon_ai/langgraph/01_quickstart.py

# 02 — Multinode placement (needs a 2+ node allocation)
dragon examples/dragon_ai/langgraph/02_multinode_placement.py

# 03 — On-cluster inference (needs GPU nodes)
export HF_TOKEN=<your_hf_token>          # once per shell, for gated models
dragon examples/dragon_ai/langgraph/03_local_inference.py

# 04 — Real-time supervision (single node)
dragon examples/dragon_ai/langgraph/04_realtime_supervision.py

# 05 — Data-locality crossover (runs on 1 node; more nodes show real locality)
dragon examples/dragon_ai/langgraph/05_data_locality_crossover.py

# 06 — Self-driving lab
#   default: rule-based judge, no GPUs required
dragon examples/dragon_ai/langgraph/06_self_driving_lab.py
#   with a real on-cluster LLM judge (needs a GPU):
export HF_TOKEN=<your_hf_token>          # once per shell, for gated models
export USE_LLM_JUDGE=1
dragon examples/dragon_ai/langgraph/06_self_driving_lab.py
```

Exporting `HF_TOKEN` once keeps the secret out of your shell history and the
per-command process list. `unset USE_LLM_JUDGE` to return example 06 to the
rule-based judge.

To launch on a scheduler (Slurm/PBS), request the allocation first and run the
`dragon ...` command inside the job, e.g.:

```bash
# Slurm: 2 nodes for the multinode example
salloc --nodes=2 --exclusive
dragon examples/dragon_ai/langgraph/02_multinode_placement.py
```

> **Prerequisites.** `langgraph` must be installed in the Dragon environment.
> Examples 03 and 06 (with `USE_LLM_JUDGE=1`) additionally need `vllm` and a
> GPU allocation.

---

## Tuning: concurrency & memory knobs

Every example above runs on sensible **defaults** — you rarely need to touch
these. When you do, there are only two knobs you care about, and they control
*different* things:

```python
from dragon.ai.langgraph import DragonExecutor

with DragonExecutor(
    max_concurrent_tasks=64,   # default: agent tasks in flight across the WHOLE system
    num_shards=1,              # default: parallel completion lanes (raise only under a
                               #          very high completion rate of small results)
) as executor:
    executor.launch_host(
        agents={"researcher": researcher, "writer": writer},
        max_threads=8,         # default max(len(agents)*4, 8): concurrent SYNC (def)
                               # node bodies inside this host. Async nodes run on the
                               # host's event loop and are NOT bounded by this.
    )
```

- **`max_concurrent_tasks`** (coordinator, global) — the backpressure cap: at
  most this many agent tasks are in flight at once. Raise it for more
  parallelism/pipelining; lower it to bound memory or protect a rate-limited
  downstream. Everything internal (per-shard semaphore slots, completion-channel
  capacity, each host's input-queue size) is **derived from this one number**, so
  no other value is ever a hidden second bottleneck.
- **`max_threads`** (per host) — how many **sync** (`def`) node bodies run at once
  *inside one host* (they run on a thread pool). **Async** (`async def`) nodes run
  on the host's shared event loop and are bounded only by `max_concurrent_tasks`.

How they interact:

| Node type / setting | What happens under the hood |
|---------------------|------------------------------|
| **Async agents** (I/O-bound) | Run on the host loop; `max_concurrent_tasks` is the real limiter — raise it freely. `max_threads` does not gate them. |
| **Sync agents**, cap ≈ per-host `max_threads` | **Sweet spot.** Enough work is admitted to keep the sync workers busy, little backlog. |
| **Sync agents**, cap ≫ `max_threads` | Surplus admitted tasks pile up in the hosts' input queues: more memory/latency, but **no** extra throughput (a host runs at most `max_threads` sync bodies at a time). |

The cap never spawns host threads, and raising `max_threads` never widens the
cap — you own the behavior of the combination you pick. The full picture is in
the **Sizing cheat-sheet** of `doc/devguide/langgraph.rst`.

> **Writing async nodes.** `async def` nodes run on the host's shared event loop,
> so the standard asyncio rule applies: don't block the loop — wrap blocking work
> in `await asyncio.to_thread(...)`. A node that blocks the loop stalls the other
> agents sharing that host until it returns. (Plain `def` nodes are unaffected —
> they run on their own thread.)

### `graph.invoke` vs `graph.ainvoke` — what changes as you scale

**These pick how the *coordinator* waits, not how your nodes run.** A node runs on
the host the same way under either driver (sync → thread pool, async → host loop),
and both return the identical result — the choice is independent of your node
bodies. What differs is the coordinator's cost of waiting, which scales with how
many nodes are **in flight at once** (a superstep's fan-out width):

| Driver | Coordinator waits by | Cost for N concurrent nodes | Use when |
|--------|----------------------|-----------------------------|----------|
| `graph.invoke` (sync) | parking a thread on `future.result()` | up to **N parked threads** (can also saturate LangGraph's sync pool) | synchronous driver + chain / modest fan-out (overhead is a few idle threads) |
| `graph.ainvoke` (async) | awaiting on one event loop | **~0 extra threads** | async app, or **wide fan-out** where you scale many concurrent agents |

`ainvoke` is a functional superset (wrap with `asyncio.run(graph.ainvoke(...))`);
it adds no capability, only cheaper waiting under high concurrency. **Rule of
thumb: as you scale the number of concurrent agents, prefer `ainvoke`** — it turns
"one coordinator thread per in-flight node" into none. For a plain synchronous
driver with a narrow graph, `invoke` is simpler and its overhead is negligible.
`max_concurrent_tasks` caps total in-flight work either way. Full detail: §6 of
`doc/devguide/langgraph.rst`.

---

## Scaling the supervision pattern (beyond these examples)

Example 04 streams every experiment's progress through **one** `Queue` read by
**one** supervisor. That is the clearest illustration, but it does not scale to
hundreds of high-rate experiments: a single consumer draining
`O(N × steps)` events becomes the bottleneck and abort latency grows.

The production shape is **hierarchical supervision with local aggregation**:

- A **node-local supervisor** (one `launch_host` per node, pinned with a
  `Policy`) reads the detailed per-step stream of *its* experiments — high-rate
  traffic that never leaves the node.
- It emits a **low-rate summary** (best metric, count diverging, group health)
  up to a **global supervisor**, which makes only cross-group decisions.
- Local decisions (abort one diverging run) happen locally and instantly;
  only genuinely global decisions round-trip.

Same primitives — `Process`, `Queue`, `DDict`, node-pinned hosts — one more
tier. Add it when you actually have hundreds-to-thousands of concurrent runs;
below that, the single supervisor in example 04 is simpler and correct.
