"""Where the campaign can be tuned. Every knob lives here; no logic does.

Three groups: how much to run, how long to wait, and where things are stored.
brief validation is here too, because it is the one place the LLM may change a
setting and every change is checked against these defaults.
"""

import math

from science.solver import SIZE_RANGE, SPEED_RANGE

# How much to run. Sized for a demo: every Dragon behaviour shows up in the
# first few waves, so a long campaign adds wall time, not evidence.
MAX_WAVES = 8                       # hard stop on launch rounds, whatever the budget says
DEFAULT_BUDGET = 32                 # total simulations a campaign may submit (8 waves of 4); the first fit lands around wave 6, so 24 never let the surrogate pick a point
RUNS_PER_WAVE = 4                   # simulations launched per wave
WATCH_UNTIL_READY = 2               # runs with something to look at; then watch hands the wave back
CANDIDATE_POOL = 400                # cheap points scored by the ensemble before picking
MIN_NEW_POINTS = 4                  # new labels needed before another fit is worth it
DRAIN_WAVES = 1000                  # no-progress passes (~1 s each); must exceed RUN_TIMEOUT + CANCEL_TIMEOUT
TARGET_FRACTION = 0.10              # held-out error (fraction of the block cap) that counts as done
MIN_POINTS_TO_STOP = 12             # never stop on a model fit to fewer points than this
DEMAND_QUERIES = 2000               # points answered at publish, to show what the model replaced
JOB_NODES = 1                       # one machine per MPI job; the 8 ranks share it

# How long to wait.
RUN_TIMEOUT = 900.0                 # a run outstanding this long is cancelled, queue time included
TASK_TIMEOUT = 120.0                # a reader or reducer this slow is cancelled
CANCEL_TIMEOUT = 30.0               # after this, an unconfirmed cancellation aborts the campaign

# Where things are stored.
DDICT_MEM_PER_NODE = 1024 * 1024**2   # campaign DDict: small objects
MESH_MEM_PER_NODE = 16 * 1024**3      # mesh DDict: hundreds of MB per snapshot

# Keys in the campaign DDict. <c> = campaign id, <run> = run id (w3-p1),
# <ts> = timestep. Thinker and worker are the two AgentHosts; reader and
# reducer are the placed processes in data/reduce_worker.py.
#
#   training_set
#     value    list of {run, dials, quantity}         (the labels)
#     writers  launcher sets [];  worker appends one entry per valid run
#     readers  worker (to count, and to copy for a fit);  thinker analyze tool
#
#   surrogate
#     value    dict: member_weights x3, n_train, n_validate, n_total, validate_error, version, launch_wave
#     writers  launcher sets None;  worker replaces it on every promotion
#     readers  thinker propose (only when surrogate_version changed);  thinker publish
#
#   surrogate_version
#     value    str "v<n>-<hex>", or "" before the first fit
#     writers  launcher sets "";  worker on every promotion
#     readers  thinker propose, every wave (one small read decides whether to fetch surrogate)
#
#   train/<version>
#     value    frozen copy of training_set for one fit
#     lifetime worker writes it at fit start, reads it at fit end, then deletes it
#
#   summary/<c>/<run>/t<ts>
#     value    mid-run snapshot summary (see data/mesh.py summarize_snapshot)
#     lifetime placed reader writes it;  worker pops it on the next watch visit
#
#   desc/<c>/<run>
#     value    final snapshot summary + dials  (permanent record of the run)
#     writers  placed reducer writes it;  worker rewrites it once with dials and failure
#     readers  worker collect;  thinker analyze tools
#
# Mesh DDict keys: data/mesh.py.  The C writer: miniamr/miniamr_live.h.
K_TRAIN = "training_set"
K_MODEL = "surrogate"
K_VERSION = "surrogate_version"


def default_brief():
    return {"bounds": [list(SPEED_RANGE), list(SIZE_RANGE)],
            "sim_budget": DEFAULT_BUDGET, "target_fraction": TARGET_FRACTION}


def valid_region(region, bounds):
    try:
        return (isinstance(region, list) and len(region) == 2 and len(bounds) == 2 and
                all(isinstance(axis, list) and len(axis) == 2 and
                    all(type(v) in (int, float) and math.isfinite(v) for v in axis) and
                    box[0] <= axis[0] < axis[1] <= box[1]
                    for axis, box in zip(region, bounds)))
    except (TypeError, ValueError, OverflowError):
        return False


def validate_brief(reply):
    """The LLM may narrow the defaults, never widen them or leave the domain."""
    default = default_brief()
    try:
        budget, target, bounds = reply["sim_budget"], reply["target_fraction"], reply["bounds"]
        if type(budget) is not int or not 1 <= budget <= DEFAULT_BUDGET:
            return default
        if type(target) not in (int, float) or not math.isfinite(target) or not 0 < target <= 1:
            return default
        if not valid_region(bounds, default["bounds"]):
            return default
        return {"bounds": bounds, "sim_budget": budget, "target_fraction": float(target)}
    except (KeyError, TypeError, ValueError, OverflowError):
        return default
