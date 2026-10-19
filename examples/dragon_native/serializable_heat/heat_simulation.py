"""
A 2D heat diffusion simulation split between Python and C++.

Python owns the simulation: it builds the grid, decomposes it into row bands,
starts a C++ worker per band and stitches the results back together after every
step. The C++ workers do the stencil arithmetic.

Nothing is passed to the workers on the command line except a serialized
Distributed Dictionary. The grid, the per worker bands, the Barrier used to
synchronize each step, the Semaphore used for the startup handshake and the
Queue used to report results are all stored in that dictionary and travel to
C++ as Serializables.

Run it with, for example:

    make
    dragon heat_simulation.py 4

The number of workers and the number of steps may both be given on the command
line, so `dragon heat_simulation.py 4 200` runs 200 steps across 4 workers.
"""

import multiprocessing as mp
import os
import pathlib
import sys
import time

import numpy as np

import dragon
from dragon import DDict
from dragon.infrastructure.facts import DRAGON_LIB_DIR
from dragon.native.barrier import Barrier
from dragon.native.process import Popen
from dragon.native.queue import Queue
from dragon.native.semaphore import Semaphore
from dragon.utils import XNDArray, XPickler

HERE = pathlib.Path(__file__).resolve().parent
WORKER = HERE / "heat_worker"

ROWS = 34
COLS = 34
STEPS = 500
HOT = 100.0

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("LD_LIBRARY_PATH", ""))
ENV["DYLD_FALLBACK_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("DYLD_FALLBACK_LIBRARY_PATH", ""))


def row_bands(rows: int, nworkers: int) -> list:
    """Split the interior rows as evenly as possible across the workers."""

    interior = rows - 2
    base, extra = divmod(interior, nworkers)

    bands = []
    start = 1

    for worker in range(nworkers):
        count = base + (1 if worker < extra else 0)
        bands.append((start, start + count))
        start += count

    return bands


def initial_grid(rows: int, cols: int) -> np.ndarray:
    """A cold plate with a hot top edge."""

    grid = np.zeros((rows, cols))
    grid[0, :] = HOT

    return grid


def render(grid: np.ndarray, size: int = 16) -> str:
    """Draw a coarse picture of the plate."""

    ramp = " .:-=+*#%@"
    rows = np.linspace(0, grid.shape[0] - 1, size).astype(int)
    cols = np.linspace(0, grid.shape[1] - 1, size).astype(int)

    lines = []
    for i in rows:
        line = ""
        for j in cols:
            shade = int(grid[i, j] / HOT * (len(ramp) - 1))
            line += ramp[min(shade, len(ramp) - 1)] * 2
        lines.append("    " + line)

    return "\n".join(lines)


def main():
    nworkers = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    steps = int(sys.argv[2]) if len(sys.argv) > 2 else STEPS

    if not WORKER.exists():
        raise SystemExit(f"{WORKER} does not exist. Build it by running make in {HERE}.")

    if ROWS - 2 < nworkers:
        raise SystemExit(f"{nworkers} workers is too many for a grid with {ROWS - 2} interior rows.")

    print(f"Diffusing heat across a {ROWS}x{COLS} plate for {steps} steps using {nworkers} C++ workers.\n")

    # [begin-heat-orc-setup]
    # One dictionary holds the array data and everything the workers need to find.
    store = DDict(1, 1, 64 * 1024 * 1024)
    ser_store = store.serialize()

    # The X picklers are what make the values readable as Serializables in C++.
    config = store.pickler(key_pickler=XPickler(), value_pickler=XPickler())

    # An XNDArray keeps its data in the dictionary, so passing it to another
    # process only passes its meta data.
    grid = XNDArray(initial_grid(ROWS, COLS), ser_store)

    # The extra party is this process.
    barrier = Barrier(parties=nworkers + 1)
    ready = Semaphore(value=0)
    results = Queue(maxsize=nworkers, pickler=XPickler())

    config["cols"] = COLS
    config["steps"] = steps
    config["grid"] = grid
    config["barrier"] = barrier
    config["ready"] = ready
    config["results"] = results
    # [end-heat-orc-setup]

    bands = []

    for worker, (start, end) in enumerate(row_bands(ROWS, nworkers)):
        band = XNDArray(np.zeros((end - start, COLS)), ser_store)

        config[f"band_{worker}"] = band
        config[f"start_{worker}"] = start
        config[f"end_{worker}"] = end

        bands.append((start, end, band))

    procs = [
        Popen(executable=str(WORKER), args=[ser_store, str(worker)], env=ENV)
        for worker in range(nworkers)
    ]

    # Wait for every worker to attach before timing the run.
    for _ in range(nworkers):
        ready.acquire()

    began = time.monotonic()

    # [begin-grd-copyback]
    for _ in range(steps):
        # Every worker has published its band.
        barrier.wait()

        for start, end, band in bands:
            band.refresh()
            grid[start:end, :] = band

        grid.sync()

        # The new grid is published, so the workers may start the next step.
        barrier.wait()
    # [end-grid-copyback]

    elapsed = time.monotonic() - began

    deltas = [results.get() for _ in range(nworkers)]

    for proc in procs:
        proc.wait()

    failed = [worker for worker, proc in enumerate(procs) if proc.returncode != 0]
    if failed:
        raise SystemExit(f"Workers {failed} exited with an error.")

    grid.refresh()

    print(render(grid))
    print(f"\n    center temperature  {grid[ROWS // 2, COLS // 2]:.4f}")
    print(f"    largest last change {max(deltas):.3e}")
    print(f"    {steps} steps in {elapsed:.2f} seconds\n")

    for _, _, band in bands:
        band.destroy()

    grid.destroy()
    results.destroy()
    store.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    main()
