# Architecture

How the pieces talk: processes, Batch tasks, DDict keys, and where each Dragon
call sits. Read [README.md](README.md) first for what the example is; this file
is for the reader who wants to know how it works or change it.

Contents:

1. [Which process actually runs what](#which-process-actually-runs-what)
2. [Everything Batch launches: four kinds](#everything-batch-launches-four-kinds)
3. [Where data lives: the two DDicts](#where-data-lives-the-two-ddicts)
4. [Where each Dragon call lives](#where-each-dragon-call-lives)
5. [What crosses the graph](#what-crosses-the-graph)
6. [What stays on the worker](#what-stays-on-the-worker)
7. [Waves overlap](#waves-overlap)
8. [The routing decision](#the-routing-decision)
9. [Stopping a run safely](#stopping-a-run-safely)
10. [The mesh protocol, and what is not claimed](#the-mesh-protocol-and-what-is-not-claimed)

---

## Which process actually runs what

The node table in the README is only the two AgentHosts. Most of the compute
happens in processes those hosts *launch* — and that boundary is the point of
the example. Below, `→` is a normal function call and `⇒` crosses a process
boundary.

```
┌─ LAUNCHER PROCESS ──────────────────────────────────────────────┐
│ campaign.main                                                   │
│   → execution.launch.Resources      (DDict, Batch, Inference)   │
│   → execution.clients.allocation_hostnames                      │
│   → executor.launch_hosts           (Policy pins each AgentHost)│
│   → build_graph(...).ainvoke        (LangGraph scheduler)       │
└───────┬─────────────────────────────────────────────┬───────────┘
        ⇓ node call                                   ⇓ node call
┌───────▼───────────────────────┐   ┌─────────────────▼─────────────┐
│ THINKER AgentHost (node A)    │   │ WORKER AgentHost (node B)     │
│                               │   │                               │
│ brief    → get_llm(queue).chat│   │ simulate → clients.           │
│          ⇒ vLLM               │   │            simulation_template│
│ propose  → clients.           │   │          → batch.job   ⇒⇒⇒    │
│            active_surrogate   │   │ watch    → steering.read_live │
│          → sampling.next_dials│   │          → worker.start_live_ │
│          → surrogate.predict  │   │            reader        ⇒⇒   │
│ analyze  → tools.analyze      │   │          → task.cancel        │
│          → TOOLS[action]      │   │ collect  → worker.finish_*    │
│          ⇒ vLLM               │   │          → batch.process ⇒⇒   │
│ publish  → surrogate.         │   │ train    → worker.start_      │
│            save_surrogate     │   │            training           │
│                               │   │          → batch.function ⇒⇒  │
└───────────────────────────────┘   └───────────────────────────────┘
                                            ⇓            ⇓      ⇓
          ┌─────────────────────────────────┘            │      │
          │                                              │      │
┌─────────▼────────────┐  ┌──────────────────────────────▼──┐ ┌─▼──────────────┐
│ MPI JOB (8 ranks,    │  │ PLACED READER (the run's own    │ │ FIT TASK       │
│ one machine)         │  │ machine)                        │ │ (any machine)  │
│                      │  │                                 │ │                │
│ miniAMR-live.x       │  │ data.reduce_worker.             │ │ worker.        │
│  → C hook            │  │   summarize_snapshot /          │ │  fit_member    │
│  → writes DDict      │  │   finalize_run / free_only      │ │  → surrogate.  │
│    while running ────┼──┼─► data.mesh.local_view          │ │    train_member│
└──────────────────────┘  │   (same machine, no network)    │ └────────────────┘
                          │  → data.mesh.summarize_snapshot │
                          │  → data.mesh.free_run           │
                          └─────────────────────────────────┘
```

Two things follow from this that the import graph hides:

- [execution/worker.py](execution/worker.py) imports
  [data/reduce_worker.py](data/reduce_worker.py) but never calls it. Those
  functions are only passed as `ProcessTemplate` targets, so they can be named
  across a process boundary; the code runs somewhere else.
- The MPI job and the placed reader touch the same mesh data on the same
  machine, but they are different processes with different lifetimes. That is
  why the reducer waits for the live reader, and why a cancellation has to be
  confirmed before anything is freed.

## Everything Batch launches: four kinds

Every Batch submission comes from the worker AgentHost through one
`runtime.batch` handle. There is one MPI job and three Python functions; the
Python ones are all in [data/reduce_worker.py](data/reduce_worker.py) and
[execution/worker.py](execution/worker.py).

```
                     submitted   Batch call        placed where       runs                        reads             writes                        when
                     by
┌─────────────────┐
│ 1  SOLVER       │  simulate    batch.job         Policy(host,       miniAMR-live.x, 8 ranks     --                mesh DDict:                   every wave, one
│    (MPI job)    │              campaign.py       round-robin)       (C binary + hook)                             live/*, mesh/*/{meta,field}   per chosen dial
└─────────────────┘
        │  mid-run: progress says a complete snapshot exists
        ▼
┌─────────────────┐
│ 2  LIVE READER  │  watch       batch.process     Policy(run.host)   reduce_worker.              mesh DDict        campaign DDict:               at most once per run,
│    summarize_   │              worker.py         = where 1 runs     summarize_snapshot          via local_view    summary/<c>/<run>/t<ts>       while 1 is still running
│    snapshot     │                                                                               deletes nothing   (~10 numbers)
└─────────────────┘
        │  solver terminal (done / cancelled / failed) and 2 has returned
        ▼
┌─────────────────┐
│ 3  FINAL        │  collect     batch.process     Policy(run.host)   reduce_worker.              mesh DDict        campaign DDict:               exactly once per run,
│    REDUCER      │              worker.py         = same machine     finalize_run                via local_view    desc/<c>/<run>                after 1 is terminal
│    finalize_run │                                                   last snapshot -> 1 number   then deletes every mesh/* and live/* key of the run
└─────────────────┘
        │  only if 3 did not confirm completion
        ▼
┌─────────────────┐
│ 3' CLEANUP      │  collect     batch.process     Policy(run.host)   reduce_worker.free_only     --                deletes mesh/* and live/*     rare; when 3 failed
│    free_only    │              worker.py                                                                          writes no summary
└─────────────────┘

                    ─────────── independent of any run; keyed on the label set ───────────

┌─────────────────┐
│ 4  FIT          │  train       batch.function    no Policy:         worker.fit_member ->        its argument      return value (weights)        when >= 4 new labels
│    fit_member   │              worker.py         Batch picks        surrogate.train_member      (the frozen       goes through Batch's own      exist; three at once,
│    x3           │                                a free machine                                 label list)       result store to the worker    one per member
└─────────────────┘
```

How to read it:

- **1 is the only `job`** (MPI, C, many ranks). **2, 3, 3' are `process`**
  (Python, pinned to a machine). **4 is `function`** (Python, not pinned).
- 2, 3, 3' are all pinned to `run.host`, the machine where 1 ran, because they
  read what 1 wrote to that machine's own DDict manager. That is the whole of
  the data-locality story.
- 2 and 3 run the **same reduction** (`mesh.summarize_snapshot`). The
  difference is when and what for: 2 is mid-run, read-only, and feeds the
  steering rules; 3 is after the run, frees the mesh when done, and its one
  number becomes a training label.
- 3' is only a repair path for 3. A normal campaign may never launch one.
- 4 never touches the mesh DDict. It reads a frozen copy of `training_set`
  passed in as an argument.

A normal run is 1 → 2 → 3, three Batch tasks. One training round is three 4s.

## Where data lives: the two DDicts

Two DDicts, and one rule: bytes that are large stay on the machine that made
them. This diagram follows the graph top to bottom and shows every read, write,
and delete each node or launched process makes.

```
LEGEND   ──▶ write    ◀── read    ✕ delete    ◀── pop ── read then delete    ══▶ Batch launch
         [thinker] / [worker]  = which AgentHost runs the node
         M  = the machine simulate picked for this run; solver, live reader, and final reducer all run there
         ┃  = machine M's own DDict manager. Keys on it never leave M.
         k  = one MPI rank, 0..7 (miniAMR calls it "pe"); r<k> in a key is that rank's own data

                                          CAMPAIGN DDict (small Python objects,   MESH DDict (raw bytes)
                                          routed: lands anywhere, nobody cares)   -------------------------------------------
                                          -------------------------------------
 ┌────────────┐
 │  brief     │ [thinker]   LLM only. Touches no DDict.
 └─────┬──────┘
       ▼
 ┌────────────┐
 │  propose   │ [thinker]   ◀── surrogate_version   (every wave)
 │            │             ◀── surrogate           (only if version changed)
 └─────┬──────┘
       ▼
 ┌────────────┐             ══▶ ┌──────────────────────────────────────────────────────────────────────────────┐
 │  simulate  │ [worker]        │ SOLVER  miniAMR-live.x, 8 ranks on M                                         │
 └─────┬──────┘                 │                                                                              │
       │                        │  SHARED numbers -- identical on all 8 ranks after MPI_Allreduce,             │
       │                        │  so rank 0 writes them ONCE. Routed keys; readable from any machine.         │
       │                        │    rank 0  ──▶ live/<run>/t<ts>       telemetry, every 5 steps               │
       │                        │    rank 0  ──▶ live/<run>/progress    written after MPI_Barrier. Holds the   │
       │                        │                                        newest t<ts> whose snapshot is         │
       │                        │                                        complete: meta AND field present for   │
       │                        │                                        ALL 8 ranks. Readers trust this,       │
       │                        │                                        never the mesh/ keys alone.            │
       │                        │                                                                              │
       │                        │  PER-RANK data -- different on every rank, so each rank writes its own,      │
       │                        │  to M's manager only. Never leaves M.                                        │
       │                        │                                                                              │
       │                        │  A SNAPSHOT is the mesh/ keys of one t<ts>: two keys per rank, meta then     │
       │                        │  field, 16 keys for 8 ranks. Written every 25 steps and once at the end.     │
       │                        │                                                                              │
       │                        │    rank k  ──▶ mesh/<run>/t<ts>/r<k>/meta                              ┃      │
       │                        │                  block list: the table of contents of this rank's part of    │
       │                        │                  the snapshot. One entry per block the rank holds:           │
       │                        │                  refinement level + block centre (x,y,z), 4 int32 = 16 B.    │
       │                        │                  Its length / 16 is the block count, and that count is how   │
       │                        │                  long field must be. Small: tens of KB. Written first.       │
       │                        │    rank k  ──▶ mesh/<run>/t<ts>/r<k>/field                             ┃      │
       │                        │                  the cells of every block in the list, float64, in the       │
       │                        │                  list's order. Written block by block into one value         │
       │                        │                  (live_put_begin / chunk / end). ~100s of MB per rank.       │
       │                        │    rank k   ✕  its own two keys of the oldest snapshot   keeps 4       ┃      │
       ▼                        └──────────────────────────────────────────────────────────────────────────────┘
 ┌────────────┐
 │  watch     │ [worker]                                              ◀── live/<run>/progress   routed read
 │            │                                                       ◀── live/<run>/t<ts>      routed read
 │            │             ══▶ ┌──────────────────────────────┐
 │            │                 │ LIVE READER   on M           │      ◀── mesh/<run>/t<ts>/r0..7/{meta,field}   ┃
 │            │                 │ summarize_snapshot           │
 │            │                 └──────────────┬───────────────┘
 │            │                                └──▶ summary/<run>/t<ts>
 │            │             ◀── pop ── summary/<run>/t<ts>
 │            │             ══▶ Task.cancel(SOLVER)   if a rule fires
 └─────┬──────┘
       ▼
 ┌────────────┐             ══▶ ┌──────────────────────────────┐
 │  collect   │ [worker]        │ FINAL REDUCER   on M         │      ◀── live/<run>/progress   which ts is last
 │            │                 │ finalize_run                 │      ◀── mesh/<run>/t<last>/r0..7/{meta,field} ┃
 │            │                 │                              │       ✕  mesh/<run>/*   live/<run>/*            ┃
 │            │                 └──────────────┬───────────────┘
 │            │                                └──▶ desc/<run>
 │            │             ◀── desc/<run>   then  ──▶ desc/<run>  (+dials, failure)
 │            │             ──▶ training_set  (append, if valid)
 └─────┬──────┘
       ▼
 ┌────────────┐
 │  analyze   │ [thinker]   ◀── desc/*   ◀── training_set    (three LLM tools, read only)
 └─────┬──────┘
       ▼
 ┌────────────┐
 │  train     │ [worker]    ◀── training_set   ──▶ train/<version>   (frozen copy)
 │            │             ══▶ ┌──────────────────────────────┐ ×3
 │            │                 │ FIT   any free machine       │  gets the copy as an argument;
 │            │                 │ fit_member                   │  returns weights through Batch
 │            │                 └──────────────────────────────┘
 │            │             ◀── train/<version>   ──▶ surrogate   ──▶ surrogate_version   ✕ train/<version>
 └─────┬──────┘
       │ next_step
       ├── launch still allowed ────────────────▶ back to  propose
       ├── nothing to launch, work outstanding ─▶ back to  watch
       ▼ nothing left
 ┌────────────┐
 │  publish   │ [thinker]   ◀── surrogate      writes model + card to disk
 └────────────┘

 Every arrow touching  ┃  is on machine M, inside SOLVER, LIVE READER, or FINAL REDUCER.
 [worker] reads only the two routed live/ keys.  [thinker] never opens the mesh DDict.
```

The keys in detail. `<c>` is the campaign id (dropped from the diagram above
for width), `<run>` a run id, `<ts>` a timestep, `<k>` an MPI rank. "Local"
means the writer used the manager on its own machine; "routed" means the key
hashed to some manager and the writer did not care which.

**Mesh DDict** — raw bytes, written only by C, 16 GB per node. A *snapshot* is
the `meta` + `field` pair of every rank for one timestep; `progress` names the
newest timestep whose snapshot is complete.

| key | value | who writes | on which machine | when | who reads | from where | who deletes |
|---|---|---|---|---|---|---|---|
| `live/<c>/<run>/t<ts>` | 104 B of telemetry: block counts, max level, checksum, wall seconds | solver rank 0 (C) | routed | every 5 steps | worker AgentHost, `read_live` | worker's machine, routed read | final reducer |
| `live/<c>/<run>/progress` | 8 B: newest telemetry `ts`, newest complete snapshot `ts` | solver rank 0 (C) | routed | after each snapshot + barrier | worker AgentHost, `read_live`; final reducer | routed read | final reducer |
| `mesh/<c>/<run>/t<ts>/r<k>/meta` | block list: per block, refinement level + centre (x,y,z), 16 B each; length / 16 = block count | solver rank `k` (C) | **local** to the solver's machine | every 25 steps + at end | live reader; final reducer | **same machine**, `local_view` | C hook (rolling, keeps 4); final reducer / cleanup (all) |
| `mesh/<c>/<run>/t<ts>/r<k>/field` | the cells of those blocks, float64, in the list's order; ~100s of MB | solver rank `k` (C) | **local** | same as meta | same as meta | same | same |

**Campaign DDict** — pickled Python objects, 1 GB per node. Every access is
routed; the objects are small, so where they land does not matter.

| key | who writes | when | who reads | when |
|---|---|---|---|---|
| `training_set` | launcher (`[]`); worker AgentHost appends | at start; each time a final reducer returns valid | worker AgentHost; thinker analyze tool | before each fit; during analyze |
| `train/<version>` | worker AgentHost | fit start (deep copy of `training_set`) | worker AgentHost | fit end, then deleted |
| `surrogate` | launcher (`None`); worker AgentHost | on each promotion, after three fits return | thinker AgentHost, `active_surrogate`; thinker publish | each propose, only if version changed |
| `surrogate_version` | launcher (`""`); worker AgentHost | with `surrogate` | thinker AgentHost | every propose (one small read) |
| `summary/<c>/<run>/t<ts>` | live reader (placed on `run.host`) | when it finishes reading | worker AgentHost, `poll_summary` | next watch visit; popped, so read once |
| `desc/<c>/<run>` | final reducer (placed on `run.host`); then worker AgentHost rewrites | when reducer finishes; then in collect | worker AgentHost `read_final_summary`; thinker analyze tools | collect; analyze |

Three facts the diagram and tables make visible:

- **Cross-machine traffic is only ever small.** Every `mesh/` read is on the
  machine that wrote it. What crosses is a `summary`/`desc` dict of a few
  hundred bytes, or the ensemble weights (a few KB) once per promotion.
- **The worker AgentHost never opens a `mesh/` key.** It reads two small
  `live/` keys and launches placed processes for the rest.
- **The thinker AgentHost never opens the mesh DDict at all.** It reads
  `surrogate`, `surrogate_version`, `training_set`, and `desc/*`, all from
  the campaign DDict.

The byte layout of each mesh key is in [data/mesh.py](data/mesh.py)'s module
docstring; the campaign keys are listed in [settings.py](settings.py).

## Where each Dragon call lives

Every call that carries a pitch is either in [campaign.py](campaign.py) or in a
function that `campaign.py` calls directly. One click from the node, no further.
Each one is marked in the source with a comment starting `# Dragon:` that says
what it buys you, so `grep -rn "# Dragon:" --include="*.py" .` prints the whole
map.

| what Dragon does for you, and why it matters | the call | where |
|---|---|---|
| Put an agent on a chosen machine. The thinker and the worker stop competing for one node's memory and cores. | `Policy(HOST_NAME)` + `launch_hosts` | [campaign.py#L369-L370](campaign.py#L369-L370) |
| Verify it landed there. Placement is a request; the check turns it into a fact you can print. | `Node(process.h_uid).hostname` | [campaign.py#L374](campaign.py#L374) |
| One DDict both hosts read and write. Two machines share campaign state with no database, no files, no serialization code of yours. | `DDict(...)` + `serialize()` | [campaign.py#L342-L346](campaign.py#L342-L346) |
| Read a model another machine wrote, once per version. The worker promotes, the thinker picks it up on its next propose; unchanged versions cost one small read. | DDict read + `K_VERSION` check | [clients.py#L53-L58](execution/clients.py#L53-L58) from [campaign.py#L120](campaign.py#L120) |
| Launch 8 MPI ranks and return at once. The node does not block, so the next wave launches while this one computes. | `batch.job` | [campaign.py#L146](campaign.py#L146) |
| Hand the solver its DDict through the environment. The C binary needs no config file and no Python; one serialized descriptor is enough. | `ProcessTemplate.env["MINIAMR_DDICT"]` → `dragon_ddict_attach` | [clients.py#L107](execution/clients.py#L107) → [miniamr_live.c#L428](miniamr/miniamr_live.c#L428), [#L447](miniamr/miniamr_live.c#L447) |
| The solver pins a handle to its own node's manager. Every rank's field goes to the manager on the machine it computed on; that is what makes the later placed read local. | `dragon_ddict_local_manager` + `dragon_ddict_manager` | [miniamr_live.c#L449-L451](miniamr/miniamr_live.c#L449-L451) |
| The solver writes its field through that pinned handle, mid-run, in chunks. Hundreds of MB per snapshot land in memory on the same node, with no output file and no MPI-IO. | `live_put(&g_local, ...)` + `live_put_begin/chunk/end` | [miniamr_live.c#L340](miniamr/miniamr_live.c#L340) (meta), [#L352-L374](miniamr/miniamr_live.c#L352-L374) (field) |
| Rank 0 writes small telemetry and progress through the plain handle. These ~100 bytes may land anywhere, so any machine can read them to steer. | `live_put(&g_dd, ...)` | [miniamr_live.c#L405](miniamr/miniamr_live.c#L405), [#L415](miniamr/miniamr_live.c#L415) |
| Read what a running solver has written. Steering decisions use numbers from the current run, not from its log after it ends. | DDict reads of `live/...` | [steering.py#L66-L82](execution/steering.py#L66-L82) |
| Send a reader to the data instead of moving the data. Hundreds of MB stay put; about ten numbers come back. | `batch.process` + `Policy(HOST_NAME, run.host)` | [worker.py#L128-L131](execution/worker.py#L128-L131), [worker.py#L146-L149](execution/worker.py#L146-L149) |
| Ask "is it done?" without waiting. One node can check every run in one pass and hand control back to the graph. | `Task.get(block=False)` | [clients.py#L75](execution/clients.py#L75) via [campaign.py#L168](campaign.py#L168) |
| Stop a job while it is still computing. A run headed for miniAMR's block-cap abort is cut early, and its node-hours go to the next wave. | `Task.cancel` | [clients.py#L134](execution/clients.py#L134) |
| Fit a model on whatever machine is free. Training runs while the next wave launches, not after. | `batch.function` | [worker.py#L234](execution/worker.py#L234) |
| Talk to an LLM inside the allocation. No API key, no rate limit, no run data leaving the cluster. | `get_llm(queue).chat` | [campaign.py#L94](campaign.py#L94), [tools.py#L111](llm/tools.py#L111) |
| Read only the local manager's copy. This is the data-locality line: the read never touches the network. | `ddict.manager(ddict.local_manager)` | [mesh.py#L82](data/mesh.py#L82) (runs inside the placed process) |
| Free memory without a filesystem. Deleting a key returns the manager's memory; no cleanup job, no scratch quota. | `del ddict[key]` | [mesh.py#L188](data/mesh.py#L188) |

The four `miniamr_live.c` rows are the solver's side of the same DDict: C code
inside the MPI job, writing while it computes. The last two rows run inside the
process that `batch.process` placed. Neither can be in `campaign.py` by
definition; [data/reduce_worker.py](data/reduce_worker.py) is kept short so that
hop is a cheap one. Line numbers are correct for this revision;
`grep -rn "# Dragon:"` is the way to find them after edits.

## What crosses the graph

Ten fields. Every one is small, and every one is read by a node that did not
write it. No task handle, model weight, or mesh byte is among them.

| field | holds | written by |
|---|---|---|
| `brief` | bounds, run budget, error target | `brief` |
| `wave` | which launch round we are on | `propose` |
| `sims_run` | how many runs have been submitted | `simulate` |
| `selected_inputs` | the settings to launch next, then cleared | `propose`, `simulate` |
| `recent_runs` | run ids that just became results | `collect` |
| `steer_region` | a smaller box to sample from, or empty | `analyze` |
| `model_info` | version, held-out error, and the counts behind it | `train` |
| `pending` | is any job, reader, fit, or cleanup still outstanding | worker nodes |
| `report` | the worker's final counts, handed over once | `train` |
| `published` | where the model and card were written | `publish` |

## What stays on the worker

Everything needed to run and track Dragon work is owned by one `WorkerRuntime`
in [execution/worker.py](execution/worker.py), built inside the worker process on
first use: the run records, the current fit, the counters, and one Batch client.

Each run holds its own task, stage, machine, live readings, and cancellation
note — one record per run instead of one list per stage. Because the graph runs
one node at a time, that owner is only ever touched by one node at a time.

The worker never hands those records to the graph. When the campaign ends it
joins its Batch client, detaches its DDict clients, and passes back a small
`report` — which is how the final counts reach `publish` and the summary line.

## Waves overlap

A wave is one trip around `propose → simulate → watch → collect → analyze →
train`. Nothing waits for a whole wave to finish:

```
time ─────────────────────────────────────────────────────────────────────▶

wave 1   [simulate 4 runs]────run w1-p2 keeps going────────────────────────┐
wave 2            [simulate 4 runs]──────────────────────┐                 │
                  [3 fits submitted]──fits run──┐        │                 │
wave 3                     [simulate 4 runs]     │        │                 │
                                    fits back ◀──┘        │                 │
wave 4                              [simulate 4 runs]     │                 │
                                             w2-p* collected ◀┘             │
                                             ...   w1-p2 collected ◀────────┘
                                                   labelled launch_wave=1, collect_wave=4
```

`watch` returns as soon as two runs have something worth looking at, or after
eight passes, whichever comes first. `collect` never waits at all: one pass,
then back round the loop. A slow run is picked up in a later wave. Model fits
are submitted and left to run. So a printed line may name an earlier wave than
the banner above it — that is the overlap, not a mistake. The wave banner marks
a **launch**, not a barrier.

## The routing decision

One conditional edge, after `train`:

- **`propose`** while the run budget and the wave limit both allow another
  launch and the error target has not been met.
- **`watch`** when no more launches are allowed but runs, readers, fits, or
  cleanup are still outstanding.
- **`publish`** when nothing is left.

It reads the small graph state only — never the worker's records — so the same
state gives the same decision. "Target met" is judged from `model_info`, the
counts the promoted model was actually fit on.

## Stopping a run safely

`Task.cancel()` returning true means the request was **accepted**, not that the
job has stopped. So the handle stays tracked until the task reports a terminal
result, and only then is the run counted as cancelled and its data freed. If
termination cannot be confirmed within the timeout the campaign stops rather
than deleting data that something may still be writing.

The same care applies to reading a result. A fit is a Python function, so an
exception it raises is its answer. A solver or reader is a process, so a raised
exception may instead mean Dragon could not start or stop it — which is not a
confirmed exit, and stops the campaign.

## The mesh protocol, and what is not claimed

`meta` is written before `field`, and `progress` only after a barrier, so
"complete" means every rank finished. The reader in [data/mesh.py](data/mesh.py)
still checks for itself: every rank present and exact byte lengths. `meta` is
only counted, never interpreted — its length says how many blocks the rank
holds. Ghost cells are skipped, because miniAMR does not fill them.

The reader answers one question: **how many blocks does this snapshot hold, and
are its real cells finite?** That is the single number a run contributes, and it
is what `watch` and `collect` both ask for.

Readers run on the machine holding the data and send back a few numbers. That
shows the shape — send the work to the data, keep it isolated in its own process
— but it does not prove a process is faster for maths this small, and the Python
reader does copy buffers. **No end-to-end zero-copy or speed claim is made.**
