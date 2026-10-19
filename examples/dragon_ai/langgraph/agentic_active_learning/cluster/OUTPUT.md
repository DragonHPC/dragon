# Running it, and reading the output

## Running it

With a verified binary (see [BUILD.md](BUILD.md)), run the campaign inside an
allocation of several machines (do **not** pass `-s`). The campaign needs at
least two distinct machines, one per AgentHost, and stops at startup if it does
not get them:

```bash
dragon campaign.py --bin <miniAMR-live.x>            # no LLM: brief and analyze are skipped
dragon campaign.py --bin <miniAMR-live.x> --llm <model-path> --gpus 1
```

Without `--llm` the campaign uses the default settings and picks points by
ensemble disagreement alone. Other options:
`--brief`, `--seed`, `--campaign-id`, `--max-tokens`, `--temperature`, `--out`.

## Reading the terminal output

Every line starts with the node that printed it. The lines below are the ones
that show what Dragon is doing for the workflow; everything else is bookkeeping.
Read them in this order.

Two things about the stream itself. Everything before the first `[startup]`
line is vLLM loading the model; it is not part of the campaign. And the thinker
and the worker are two processes on two machines writing to one console, so
their lines interleave: a `[simulate]` line can appear before the `[propose]`
that caused it. Read by run id and wave number, not by line order.

### 1. Placement: who is where

```
[startup] allocation: ['node1', 'node2', 'node3', 'node4']; MPI jobs round-robin over all of them
[startup] LLM meta-llama/Llama-3.1-8B-Instruct running as Dragon processes on ['node1']; no endpoint outside this allocation
[startup] thinker: node1 puid=4294967298
[startup] worker: node2 puid=4294967299
```

Two different hostnames on the `thinker` and `worker` lines. That is the
`Policy` doing its job, and the campaign refuses to start if they match. The
LLM line names the machines vLLM was started on: it is inside your allocation,
not a service you called out to. Keep the allocation list in view; every later
`on <host>` refers back to it.

### 2. Data locality: the field stays, a summary travels

```
[simulate] w3-p0: submitted 8 ranks on node4
[watch]    w3-p0 t75: snapshot summary submitted on node4
[watch]    w3-p0 t75 snapshot: valid=True ranks=8/8 read 629145600 bytes on node4 in 0.41s; 318 bytes crossed back reason=
[collect]  w3-p0: valid; ranks=8/8 read 629145600 bytes on node4 in 0.44s same_node=True; 331 bytes crossed to the campaign DDict
```

Follow one run id. The job was placed on `node4`; the reader for it was
placed on `node4`; the read happened on `node4`. `same_node=True` is
the check, not an assumption. Then compare the two byte counts on the same
line: about 600 MB read where it was written, about 300 bytes sent to the
campaign DDict. The ratio is the pitch. `read ... in 0.41s` is a local memory
read; there is no file open and no transfer in that number.

At the end:

```
[summary] mesh bytes read where they were written=25165824000 (same_node=40 other_node=0); bytes crossed to the campaign DDict=13240
```

`other_node=0` should hold for the whole campaign. If it is not zero, a reader
landed on the wrong machine and the placement is not doing what the code asks.

### 3. Waves overlap: nothing waits for a wave

```
==================== launch wave 4 (not a barrier) ====================
[simulate] still running from earlier waves: solvers=6 live_readers=1 reducers=1 cleanups=0 fits=3
[simulate] w4-p0: submitted 8 ranks on node3
...
[train] model=v12-a1b2c3d4e5 member=1 launch_wave=3 host=node1 n_train=9 n_validate=3 n_total=12 completed
[collect] w2-p3: solver terminal=done; launch_wave=2 collect_wave=4 cancel_reason=
```

The `still running` line is printed at the moment a new wave launches. Non-zero
counts mean earlier waves are still computing while this one starts; `fits=3`
means a model is training at the same time. Two more signs: a `[train] ...
completed` line whose `launch_wave` is behind the current banner, and a
`[collect]` line where `launch_wave` and `collect_wave` differ. A run launched
in wave 2 and collected in wave 4 is the overlap made visible. If every
`still running` line says all zeros and every `launch_wave` equals its
`collect_wave`, the runs are finishing faster than the loop turns, and the
example is not exercising this.

### 4. Mid-run steering: reading, then acting, before the job ends

```
[watch] w3-p2 t40 live: blocks/rank=4102 global=29310 wall=8.1s; read from the mesh DDict while 8 ranks compute
[watch] w3-p2 t60 live: blocks/rank=5412 global=38870 wall=12.3s; read from the mesh DDict while 8 ranks compute
[watch] cancel requested: w3-p2: per-rank blocks 5412 >= 5400 at t60; accepted=True error=
[collect] w3-p2: solver terminal=cancelled; launch_wave=3 collect_wave=3 cancel_reason=w3-p2: per-rank blocks 5412 >= 5400 at t60
[collect] w3-p2: invalid: cancellation accepted; result excluded; ...
```

The `live:` lines are the solver's own numbers, read from the mesh DDict while
its ranks are still stepping. The timestep goes up between lines because the
job is running. The `cancel requested` line names the rule and the number that
fired it. `accepted=True` means Dragon took the request, not that the job is
gone; `solver terminal=cancelled` two lines later is the confirmation, and only
then is the run counted. A cancelled run never becomes a training point.

There is a second cancel reason, `campaign target met`: once the model is good
enough, every run still computing is stopped. That one is a campaign rule, not
a data-driven one. The block-cap line above is the one that shows steering from
the solver's own numbers. With this workload the block cap is the only
data-driven rule that can fire, since miniAMR's synthetic field never goes
non-finite; a campaign whose runs stay well under the cap will report
`confirmed cancellations=0`.

### 5. The LLM acts inside the cluster, and its decision lands

```
[analyze] turn 1/4  thought: No observations yet; list the finished runs first.
[analyze] turn 1/4  act:     list_runs({})
[analyze] turn 1/4  observe: {'runs': [{'run': 'w3-p0', 'valid': True, 'blocks': 4120}, {'run': 'w3-p2', 'valid': False, ...}]}
[analyze] turn 2/4  thought: w3-p2 failed; I want the dials that produced it before choosing a region.
[analyze] turn 2/4  act:     get_run_summary({'run_id': 'w3-p2'})
[analyze] turn 2/4  observe: {'run': 'w3-p2', 'valid': False, 'reason': 'cancellation accepted...', ...}
[analyze] turn 3/4  thought: The failure sat at high speed and large size; steer the next wave below both.
[analyze] decision: steer_region=[[0.004, 0.007], [0.4, 0.8]]; evidence: the cancelled run sat at high speed and large size
[propose] wave=4 selected=4 model=v12-a1b2c3d4e5 region=steered by analyze
```

Each `turn` is one ReAct step: `thought` is the model's reason, written by the
model and required on every action; `act` is the tool it chose, run by Python
against the campaign DDict; `observe` is the result that went back to it. The
order of tools is the model's, so it will differ between runs. `decision:` is
what it concluded. The `[propose]` line on the next wave says `region=steered by
analyze` when that decision was used, and `region=full bounds` when it was
not. Without `--llm`, no `[analyze]` lines appear and every propose says
`full bounds`.

### 6. The drain, then a clean shutdown

```
==================== no more launches: budget spent (32/32) ====================
[drain] still running: solvers=5 live_readers=2 reducers=3 cleanups=0 fits=3
[drain] the graph now cycles watch -> collect -> analyze -> train until all of it has ended, then publishes
...
[analyze] skipped: no more launches, so a steered region would not be used
...
[summary] stopped: budget spent (32/32); runs=32 invalid=3 confirmed cancellations=2 training points=27 held-out error history=[0.31, 0.19, 0.12]
```

The `no more launches` banner marks the end of the campaign proper: after it
nothing new is submitted, and the loop only waits for what is already running.
It prints once, with the reason and what was still outstanding at that moment.
`analyze` stops calling the LLM from here on, since its region would never be
sampled.

`stopped:` says why the loop ended; on the default budget expect `budget spent`
(see [Will the model converge?](NODES.md#will-the-model-converge-usually-not-and-that-is-fine)).
If the model did reach the target it reads `target met (held-out error 0.0307
<= 0.1000 on 16 labelled runs)`, and `, budget also spent (32/32)` is appended
when the last launch and the promotion landed in the same wave.
The error history is the promoted ensemble's held-out error after each fit. If
you see `[summary] ABORT` instead, something could not be confirmed stopped and
the campaign chose not to free memory it might still be writing to.

Lines are tagged with the node that printed them. `[startup]` and `[summary]`
are labels, not nodes. The byte counter in the summary covers the final readers
only; live and inspection reads print separately. Counted bytes are the rank
fields actually accepted, not all DDict traffic.
