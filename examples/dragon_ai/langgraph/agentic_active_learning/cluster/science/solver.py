"""
The expensive part: miniAMR, a real MPI simulation used as a stand-in for a
production solver.

Unlike a Python function, this is a compiled MPI program, so campaign.py starts
it as a Dragon ``batch.job`` tied to a chosen machine. Built with the hook in
miniamr/miniamr_live.h, it writes telemetry and field snapshots straight into the
mesh DDict from C, on the machine it ran on, so there are no output files to
parse.

This file only builds the command line and records the memory layout that the C
writer and the Python reader (data/mesh.py) must agree on. It imports nothing from
Dragon; starting the job and choosing its machine happen in campaign.py.

The two names used throughout:
  * dials    -- the settings of one run, [object_speed, object_size]. A sphere is
                pushed through the grid; how fast and how big it is decides how
                much extra detail the grid has to add to follow it.
  * quantity -- the single number we keep from a run: how many blocks the mesh
                ended up with, counted in data/mesh.py.
"""

from __future__ import annotations


# Binary produced by `make` in miniAMR/ref, built WITH miniamr_live.c so it
# writes mid-run telemetry and snapshots into the DDict. Point --bin at it.
MINIAMR_BIN = "miniAMR-live.x"

# miniAMR needs npx*npy*npz == number of MPI ranks, and each job stays on one
# node (settings.JOB_NODES = 1).
NPX = NPY = NPZ = 2
RANKS = NPX * NPY * NPZ          # 8 ranks per run, r0..r7 in the keys

# Workload settings. Chosen for a short demo run; not measured on this system.
NX = NY = NZ = 8            # cells per block, per direction
NUM_VARS = 8                # variables per cell
NUM_REFINE = 5              # refinement levels allowed; 6 overflows MAX_BLOCKS
MAX_BLOCKS = 6000           # per-rank block cap; miniAMR aborts if it is hit
NUM_TSTEPS = 200
STAGES_PER_TS = 20          # the main runtime lever; does not change mesh size
CHECKSUM_FREQ = 4

# Mid-run write cadence, passed to the hook as env vars. SNAP_FREQ must divide
# NUM_TSTEPS, or the end-of-run snapshot lands next to the last scheduled one.
LIVE_FREQ = 5               # timesteps between 104-byte telemetry records
SNAP_FREQ = 25              # timesteps between full field snapshots
SNAP_KEEP = 4               # snapshots kept alive per rank at once; the live reader queues ~10s behind Batch, so 2 was too few

# One block is [NUM_VARS, x, y, z] float64 with a one-cell ghost each side. This
# MUST match the packing in miniamr/miniamr_live.c.
MESH_DIMS = (NUM_VARS, NX + 2, NY + 2, NZ + 2)

# A sphere starts in a corner and moves diagonally. Speed and size are the two
# dials; everything else is fixed, so the sweep is two-dimensional.
OBJECT_TYPE = 2             # surface of a spheroid
OBJECT_BOUNCE = 0           # let it leave the mesh
OBJECT_START = -1.71        # centre at the start, each axis
OBJECT_INC = 0.0            # no size change over time

# Bounds may be narrowed by brief.
SPEED_RANGE = (0.004, 0.010)
SIZE_RANGE = (0.40, 1.00)


def build_argv(dials: list[float], npx: int | None = None,
               npy: int | None = None, npz: int | None = None) -> list[str]:
    """The full miniAMR command line for one run (the arguments after the program).

    The rank grid defaults to the module NPX/NPY/NPZ, so campaign.py calls this
    with just the dials; probe_live.py passes an explicit grid.

    --report_perf 0 silences miniAMR's end-of-run performance report. The result
    is read from the mesh DDict, not from anything printed or written to disk.
    """
    npx = NPX if npx is None else npx
    npy = NPY if npy is None else npy
    npz = NPZ if npz is None else npz
    speed, size = float(dials[0]), float(dials[1])
    return [
        "--num_refine", str(NUM_REFINE),
        "--max_blocks", str(MAX_BLOCKS),
        "--npx", str(npx), "--npy", str(npy), "--npz", str(npz),
        "--nx", str(NX), "--ny", str(NY), "--nz", str(NZ),
        "--num_tsteps", str(NUM_TSTEPS),
        "--stages_per_ts", str(STAGES_PER_TS),
        "--num_vars", str(NUM_VARS),
        "--checksum_freq", str(CHECKSUM_FREQ),
        "--report_perf", "0",
        "--num_objects", "1",
        # The 14 numbers after --object: type, bounce, centre, speed, size, growth.
        "--object", str(OBJECT_TYPE), str(OBJECT_BOUNCE),
        str(OBJECT_START), str(OBJECT_START), str(OBJECT_START),
        str(speed), str(speed), str(speed),
        str(size), str(size), str(size),
        str(OBJECT_INC), str(OBJECT_INC), str(OBJECT_INC),
    ]
