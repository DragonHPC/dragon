# The eight nodes

What each graph node does, what it prints, and what the numbers mean. For how
the nodes talk to Dragon and to each other, see [ARCHITECTURE.md](ARCHITECTURE.md).

Every node is a plain function in [campaign.py](campaign.py). Blue nodes run on
the thinker AgentHost, orange on the worker.

```
brief → propose → simulate → watch → collect → analyze → train ─┬→ propose
                                                                ├→ watch
                                                                └→ publish
```

---

## `brief` — read the request

Takes the plain-English `--brief` text and turns it into numbers the rest of the
campaign uses: the speed and size box to search, how many runs are allowed, and
the error to aim for.

With an LLM it asks once, with a fixed JSON shape, then checks the answer stays
inside the real domain. Any failure falls back to the defaults in
[settings.py](settings.py). The settings go into the graph's `brief` field, which
every later node reads.

```
[brief] settings={'bounds': [[0.004, 0.01], [0.4, 1.0]], 'sim_budget': 32, ...} source=llm inside the allocation, 2.1s
```

## `propose` — pick the next settings

No model yet: pick a few settings at random inside the box.

Once a model exists: draw a large pool of cheap candidate points, ask the
three-member ensemble to predict every one, and keep the few they **disagree**
about most. Disagreement means the model has not measured there, so a real run
teaches it the most. One point per wave ignores that and is drawn at random, so
the search never parks in one corner. If `analyze` named a region, the pool is
drawn from it.

The model is read from this host's memory when its version has not changed.

```
[propose] wave=3 selected=4 model=v12-a1b2c3d4e5 region=steered by analyze
```

## `simulate` — launch the runs

For each chosen setting, picks a machine round-robin and submits one placed MPI
job of 8 ranks. Environment variables tell the C hook where to write, which run
and campaign it belongs to, and how often to publish.

The call returns straight away, so all four runs go at once and the node never
blocks. Because each job is pinned to a machine, the hundreds of megabytes it
writes stay on that machine — which is what makes the later reads cheap.

```
==================== launch wave 3 (not a barrier) ====================
[simulate] still running from earlier waves: solvers=5 live_readers=1 reducers=2 cleanups=0 fits=3
[simulate] w3-p0: submitted 8 ranks on node4
```

## `watch` — read the running jobs, and stop the bad ones

The part that makes this an online campaign rather than a batch one. The loop
itself is in `campaign.watch_node`, so every Dragon call it makes is on the page.
What it calls stays separate: reading is [steering.read_live](execution/steering.py),
the rules are `steering.stop_reason` over `steering.assess`, and only the run's
own record can ask Dragon to cancel it. `assess` takes numbers and returns a
reason, so it is tested with no Dragon task at all.

One pass, per running run:

1. Pick up a snapshot summary asked for earlier, if it has come back.
2. Check whether the job itself has finished.
3. Read the small progress and telemetry records the solver publishes.
4. Ask for **one** snapshot summary, placed on the machine holding the data.
5. Apply the rules below, and request a cancel if one fires.

The rules, each recording the numbers that fired it:

| rule | what it sees |
|---|---|
| nonfinite interior | the snapshot summary found NaN/Inf in real cells |
| nonfinite checksum | telemetry checksum is not a finite number |
| block cap | the newest per-rank block count has reached 90% of the cap, where miniAMR would abort |
| submission timeout | the run has been outstanding too long, queue time included |
| target met | the campaign already reached its error target |

A cancel stops one whole 8-rank run, never a single rank. The node then sleeps a
second and looks again, up to eight passes, and returns early once two runs have
something worth looking at. It is the only node that waits, and it waits for a
short window — not for the wave, the fits, or the readers.

No LLM is involved. Missing or malformed data is logged as unknown, not as
healthy. The final timestep is never read here; `collect` owns that one.

```
[watch] w3-p1 t60 live: blocks/rank=3810 global=4120 wall=11.8s; read from the mesh DDict while 8 ranks compute
[watch] w3-p1 t75: snapshot summary submitted on node3
[watch] w3-p1 t75 snapshot: valid=True ranks=8/8 read 629145600 bytes on node3 in 0.41s; 318 bytes crossed back reason=
[watch] cancel requested: w3-p2: per-rank blocks 5412 >= 5400 at t60; accepted=True error=
```

## `collect` — turn finished runs into data

For every run that has actually finished, this submits a final reader **on the
machine that ran it**. That reader opens the last snapshot in place, reduces it
to a few numbers, frees what the run left behind, and writes the small result
back. The large field never crosses the network.

A result becomes a training point only if all of this holds: every rank was
read, the timestep is the planned final one, the interior is finite, and the run
was not cancelled or failed. Anything else is counted as invalid, with the
reason printed. Cancelled and truncated runs never become labels.

The visit is a single pass: every run advances at most one stage — solver, then
final reader, then cleanup if that reader failed — and the node returns. A run
that is not ready yet is picked up on the next trip round the loop, so a slow
run never holds up the wave. The ids that became results this pass are handed to
`analyze` in `recent_runs`.

```
[collect] w3-p0: solver terminal=done; launch_wave=3 collect_wave=3 cancel_reason=
[collect] w3-p0: valid; ranks=8/8 read 629145600 bytes on node4 in 0.44s same_node=True; 331 bytes crossed to the campaign DDict
[collect] w3-p2: invalid: solver cancelled: ...
```

## `analyze` — read the results with tools

The only place the LLM can take an action rather than just answer. It runs a
short loop in [llm/tools.py](llm/tools.py): the model returns one JSON action, the code
looks that action up in the `TOOLS` table and runs it, and the result is fed
back as the next message.

Three read-only tools:

- **`list_runs()`** — the finished run ids, whether each was valid, and its
  block count.
- **`get_run_summary(run_id)`** — one run in detail, including the dials that
  produced it.
- **`get_training_summary()`** — how many points exist, and the dials at the
  smallest and largest block counts measured so far.

The order is not fixed by the code. Seeing an invalid run is a reason to open
it; seeing a wide spread of block counts is a reason to look at the training set
instead. That choice is the only thing the model is really being asked for.

Hard limits, enforced in Python and not in the prompt: **4 turns, 3 tool
actions**. When the budget runs out the reply is decoded against the `finish`
schema alone, so the loop cannot run on. Finishing requires at least one
successful observation, a valid region inside the bounds, and a short piece of
evidence. Unknown actions, repeated identical requests, unknown run ids,
made-up paths or hosts, launching solvers, and cancelling anything are all
rejected — and a rejection is fed back as the next observation, so the model can
correct itself. If the budget runs out or the reply is malformed, the campaign
falls back to plain ensemble sampling and keeps going. Only `steer_region`
reaches the graph; the evidence is printed. With no `--llm`, on a pass that
produced no results, or once no more waves may launch (the region would never
be sampled), this node does nothing at all.

The prompt tells the model what the region is *for*: coverage, not
optimisation. It is asked to steer toward dials where data is thin or block
counts change fastest, and told that a high or low block count is not good or
bad by itself. Without that sentence the model invents a goal each time.

```
[analyze] turn 1/4  thought: I have no observations yet, so I should first see which runs finished and which were valid.
[analyze] turn 1/4  act:     list_runs({})
[analyze] turn 1/4  observe: {'runs': [{'run': 'w3-p0', 'valid': True, 'blocks': 4120}, ...]}
[analyze] turn 2/4  thought: w3-p2 is the only invalid run; its dials will tell me where refinement got out of hand.
[analyze] turn 2/4  act:     get_run_summary({'run_id': 'w3-p2'})
[analyze] turn 2/4  observe: {'run': 'w3-p2', 'valid': False, ...}
[analyze] turn 3/4  thought: The invalid run sat at high speed and large size, so the next wave should stay away from that corner.
[analyze] decision: steer_region=[[0.007, 0.01], [0.7, 1.0]]; evidence: runs at high speed refine hardest near the upper size bound
```

This is a ReAct loop, printed in full. `thought` is a required field on every
action the model returns, so each turn shows what it read from the last
observation and why it chose the next tool; `act` is the tool call Python ran;
`observe` is what came back and was fed to the model. The `finish` turn prints
its thought, then the decision.

## `train` — fit the model

Picks up any member fits that finished, then decides whether to start more.

When all three members are back, it checks they were fit on the same frozen data
(same version, same wave, same count), combines them, and promotes the result.
The reported error is the **ensemble's own** held-out error, as a fraction of
the block cap — not an average of the members' separate errors.

To start a round it copies the training set under a version key and submits
three fits, which then run on free machines while later waves launch.

```
[train] model=v12-a1b2c3d4e5 member=0 launch_wave=3 ... submitted
[train] model=v12-... member=0 launch_wave=3 host=node1 ... completed

========================================================================
[train] SURROGATE PROMOTED  v12-a1b2c3d4e5  (fit submitted in wave 3)
  labelled runs     9 train + 3 held-out = 12 total
  held-out error    0.0830 of the per-rank block cap (6000)  = off by ~498 blocks on average; labels are 8-rank totals, so this can exceed 1
  target            error <= 0.1000 and >= 12 labelled runs  -> MET
  error history     [0.31, 0.19, 0.083]
========================================================================
```

The boxed block is printed once per promoted model. `MET` on the target line is
the same test `next_step` uses to stop launching; it needs both the error and
the minimum number of labelled runs.

## `publish` — save the model

Runs only after the worker has confirmed that every job, reader, fit, and
cleanup has ended and handed over its `report`. Writes the trained model and a
short card of measured facts, then answers a large batch of settings from it to
show what the model replaced. The card holds numbers only; no text is generated
for it.

```
[publish] saved model=v12-... n_train=15 n_validate=5 n_total=20 held-out block error=0.0830 of the per-rank block cap
```

---

## What the numbers mean

The science here is deliberately thin, because it is not what the example is
showing. In full:

> Each run writes big field snapshots on the machine that produced them. A
> placed reader turns one run into **one number** — how many blocks the mesh
> ended up with. A small ensemble learns that number from the two dials. Where
> its members disagree, nothing has been measured, so that is where the next
> runs go.

**Training input and label.** One training pair is
`{"run": "w3-p1", "dials": [speed, size], "quantity": 4120.0}`. The input is
the two dials, scaled onto 0..1 by the fixed ranges in
[science/solver.py](science/solver.py). The label is `quantity`: the total
block count over all 8 ranks in the run's **final** snapshot, as computed by the
placed reducer. Only runs that finished cleanly at the planned last timestep
with every rank present and a finite interior become labels; cancelled,
timed-out, and mid-run summaries never do.

**Train / validation split.** Not random. `training_set` is in arrival order;
the first three quarters train, the last quarter (at least one point) is held
out. All three ensemble members see the same frozen copy and the same split;
they differ only in their random seed. `validate_error` is the ensemble mean's
mean absolute error on the held-out points, divided by the **per-rank** block
cap (6000). So `0.083` means the model is off by about 500 blocks on average.
It is not an accuracy. Because the label is an 8-rank total (up to ~14000 in
practice) while the cap is per rank, an early model that has only seen small
runs can print an error above 1; that is a real miss, not a bug. The members'
standard deviation on a candidate point is the "disagreement" `propose`
samples on.

Stopping uses the counts the promoted model was actually fit on, not points that
arrived afterwards.

### Will the model converge? Usually not, and that is fine

The default budget is 32 runs, eight waves of four. That is enough for every
Dragon behaviour in this example to appear at least twice — overlapping waves,
mid-run cancels, placed readers, a model trained while the next wave launches,
the LLM steering a region — and for the surrogate to pick points itself: the
first fit is usually promoted around wave 6, so the last two waves print
`[propose] ... model=v17-...` instead of `model=random`. It is not enough data
for a two-input network to reach a 10% target reliably, and the campaign is not
tuned to make it.

Expect the last line to say `stopped: budget spent (32/32)`. `stopped: target
met (...)` can happen and is a bonus; the line then prints the error and count
that met it, and adds `budget also spent` if both happened at once. What to
look at is the `held-out error history` trending down as labels accumulate,
not whether it crosses the line. Raising `DEFAULT_BUDGET` in
[settings.py](settings.py) buys more waves, not more kinds of evidence.

A note on honesty: miniAMR's field is synthetic and its refinement follows the
object's position rather than the field, so the block count is a real function
of the run but stands in for something a production solver would compute. It is
not solution error and not a measure of mesh quality.
