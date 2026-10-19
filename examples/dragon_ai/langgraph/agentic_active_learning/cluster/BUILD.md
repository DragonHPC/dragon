# Build miniAMR with the hook

miniAMR is LGPL and is not shipped here. You clone it, drop in our two files,
make six small edits, and build. Then you prove the build with
[probe_live.py](probe_live.py) before touching the campaign.

The short form of the same edits is in the WIRING section of
[miniamr/miniamr_live.h](miniamr/miniamr_live.h).

## The sequence

```bash
# 1. A clone of your own, separate from any other miniAMR build.
git clone https://github.com/Mantevo/miniAMR.git miniAMR-live
cd miniAMR-live/ref

# 2. Our two files go next to miniAMR's sources.
cp /path/to/this/example/miniamr/miniamr_live.c .
cp /path/to/this/example/miniamr/miniamr_live.h .

# 3. Six edits: four in main.c, two in driver.c, plus the Makefile.
#    Each one is shown below as the stock lines with the new line marked +.

# 4. Dragon must be on the path before make. DRAGON_BASE_DIR is what the
#    Makefile uses to find dragon/ddict.h and libdragon.
module load dragon          # or however your site activates Dragon
echo $DRAGON_BASE_DIR       # must print a path

# 5. Build and check the link.
make
ldd miniAMR-live.x | grep dragon
```

## The six edits

Line numbers are from the current Mantevo `master`; search for the quoted text
if they drift. Lines marked `+` are the ones you add; everything else is
already there.

**main.c — edit 1, the include (around line 36):**

```c
  #include "block.h"
  #include "comm.h"
  #include "timer.h"
  #include "proto.h"
+ #include "miniamr_live.h"

  int main(int argc, char** argv)
```

**main.c — edit 2, attach before the run (around line 348):**

```c
     timer_main = timer() - t1;
+    miniamr_live_open();
     driver();
```

**main.c — edit 3, the end-of-run snapshot (around line 351):**

```c
     profile();
+    miniamr_live_final();

     deallocate();
```

`final()` must come before `deallocate()`, which frees the blocks it reads.

**main.c — edit 4, detach (around line 359):**

```c
     MPI_Barrier(MPI_COMM_WORLD);
+    miniamr_live_close();
     MPI_Finalize();
```

**driver.c — edit 5, the include (around line 34):**

```c
  #include "block.h"
  #include "comm.h"
  #include "timer.h"
  #include "proto.h"
+ #include "miniamr_live.h"

  // Main driver for program.
  void driver(void)
```

**driver.c — edit 6a, publish at the end of every timestep (around line 165):**

The timestep loop ends with a timing block. The new call goes after that
block, as the last statement inside the `for` loop:

```c
        if (!(ts%refine_freq) || done) {
           ...
           for (i = 0; i < k; i++) {
              ...
              MPI_Allreduce(ctime[i], tmp_time, 5, MPI_DOUBLE, MPI_MIN,
                            MPI_COMM_WORLD);
              for (j = 0; j < 5; j++)
                 tmin[j] += tmp_time[j];
           }
           last = ts;
        }
+       if (!done)
+          miniamr_live_step(ts);
     }

     end_time = sim_time;
     num_tsteps = ts - 1;
```

It has to be after `refine(ts)` earlier in the loop, because that is what
changes the block counts. The last timestep is skipped: `final()` covers it.

**driver.c — edit 6b, flush when miniAMR gives up (around line 100):**

Inside the checksum test there is a `return` that ends the run early. Add the
flush immediately before it:

```c
                    if (fabs(sum - grid_sum[var])/grid_sum[var] > tol) {
                       if (!my_pe)
                          printf("Time step %d sum %lf (old %lf) variable %d difference too large\n", ts, sum, grid_sum[var], var);
+                      miniamr_live_flush(ts);
                          return;
                    }
```

The indentation in the stock file is misleading: that `return` is not under
`if (!my_pe)`, so every rank runs it. The flush is placed so it is collective
too. Without it, a run miniAMR abandoned looks the same as a short healthy one.

**Makefile — edit 7:**

```make
  CC   = mpicc
  LD   = mpicc
- CPPFLAGS = -I.
+ CPPFLAGS = -I. -I$(DRAGON_BASE_DIR)/include
  CFLAGS = -O3
- LDFLAGS =
+ LDFLAGS = -L$(DRAGON_BASE_DIR)/lib
- LDLIBS = -lm
+ LDLIBS = -lm -ldragon

- EXEC = miniAMR.x
+ EXEC = miniAMR-live.x

  OBJS = block.o check_sum.o comm_block.o comm.o comm_parent.o comm_refine.o \
         comm_util.o driver.o init.o main.o move.o pack.o plot.o profile.o \
-        rcb.o refine.o sfc.o stencil.o util.o
+        rcb.o refine.o sfc.o stencil.o util.o miniamr_live.o
```

and two more lines further down:

```make
  clean:
-	rm *.o miniAMR.x
+	rm *.o miniAMR-live.x

  # need dependencies
+ miniamr_live.o: miniamr_live.h block.h

  block.o: block.h proto.h comm.h timer.h
```

The binary is renamed so a stock build and a hooked build cannot be mixed up;
`science/solver.py` expects `miniAMR-live.x`.

## What each hook does

So the edits are not blind:

| call | where it goes | what it does |
|---|---|---|
| `miniamr_live_open()` | main.c, before `driver()` | reads `MINIAMR_*` from the environment, attaches the mesh DDict, pins a handle to this node's manager |
| `miniamr_live_step(ts)` | driver.c, end of the timestep loop | on schedule: rank 0 writes telemetry; every rank writes its field snapshot to the local manager |
| `miniamr_live_flush(ts)` | driver.c, the give-up branch | one last telemetry record, so a run miniAMR abandoned does not look healthy |
| `miniamr_live_final()` | main.c, after `profile()` | the end-of-run snapshot the final reducer reads |
| `miniamr_live_close()` | main.c, before `MPI_Finalize` | detaches, reports any failed writes |

The solver only ever writes. It never reads a control flag; the campaign stops
it from Python with `Task.cancel()`. A non-zero return from `open()` means live
writing is off and the run proceeds as stock miniAMR; it never aborts the
simulation.

## Prove the build

Before touching the campaign:

```bash
cd /path/to/this/example
dragon probe_live.py --bin /path/to/miniAMR-live/ref/miniAMR-live.x --snap-freq 0   # telemetry only, fast
dragon probe_live.py --bin /path/to/miniAMR-live/ref/miniAMR-live.x                 # with field snapshots
dragon probe_live.py --bin /path/to/miniAMR-live/ref/miniAMR-live.x --cancel-at 40  # a partial write
```

Each run ends with a `[pass]`/`[FAIL]` checklist. All three must pass. If the
first reports no telemetry at all, the binary is a stock build or `open()`
returned non-zero; check `DRAGON_BASE_DIR` and that the edits landed. While it
runs, note the largest value in the `maxblk` column: the campaign's steering
rule fires when a rank reaches 90% of `MAX_BLOCKS` (5400 by default), so if
the probe never gets near that at the top of the dial ranges, the campaign
will not cancel any run on its own numbers.

Two things that have gone wrong before:

- **`multiple definition of 'main'` at link time.** driver.c was overwritten
  with main.c's contents during copy-paste. Restore the stock driver.c and
  redo edits 5, 6a, 6b.
- **`undefined reference to miniamr_live_open`.** `miniamr_live.o` is missing
  from `OBJS`. Check the last line of edit 7.
