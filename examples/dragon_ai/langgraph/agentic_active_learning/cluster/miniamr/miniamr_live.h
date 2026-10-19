/*
 * Mid-run writes from miniAMR into the Dragon DDict.
 *
 * Stock miniAMR writes nothing a reader can use until it exits. This hook
 * publishes while the run is going, so the agent can act on a run before it
 * ends. The solver only ever WRITES; it never reads a control flag. The agent
 * stops a run from Python with Task.cancel().
 *
 * Two kinds of key, on purpose. Four in total -- the layout is kept small so a
 * reader can hold all of it in their head.
 *
 *   small, routed normally, readable from any machine
 *
 *     live/<run>/t<ts>      one 104-byte record, written by rank 0:
 *                             int32  timestep
 *                             int32  largest block count on any one rank
 *                             int32  global block count
 *                             int32  highest refinement level in use
 *                             int32  how many of the level slots are valid
 *                             int32  padding
 *                             double checksum
 *                             double wall seconds since the run started
 *                             int64  global blocks at level 0..7
 *                           Python reads it as "<6i2d8q".
 *
 *     live/<run>/progress   two int32: the newest timestep published, and the
 *                           newest COMPLETE field snapshot.
 *
 *   large, pinned to the node that produced it
 *
 *     mesh/<run>/t<ts>/r<pe>/meta   int32 x 4 per block: level, cen0..cen2.
 *                                   Its length gives the block count, so there
 *                                   is no separate count key.
 *     mesh/<run>/t<ts>/r<pe>/field  every one of this rank's blocks, streamed
 *                                   into a SINGLE value in meta's order. About
 *                                   200 MB. No per-block keys.
 *
 * Only rank 0 writes telemetry. Every other field in the record is already a
 * globally reduced number, and the one that is not -- the per-rank block count,
 * which matters because the cap is per rank -- is reduced with one extra
 * MPI_Allreduce before the write. So eight identical records become one.
 *
 * Three ordering rules the reader depends on:
 *   - meta goes BEFORE field, so a rank stopped part-way still leaves enough
 *     behind to say what it was writing.
 *   - the field is one value, so it either landed or it did not. There are no
 *     half-written block sets to detect.
 *   - progress is written by rank 0 only after an MPI_Barrier, so its snapshot
 *     number means "every rank finished that snapshot". That is why there is no
 *     per-rank "ok" key.
 *
 * Retention while the run is going: before writing a snapshot, a rank deletes
 * its oldest one once it already holds MINIAMR_SNAP_KEEP of them. So a run
 * holds a bounded amount per node however far behind the agent falls.
 *
 * Retention at the end: the solver frees NOTHING. The end-of-run write is just
 * another snapshot, at the final timestep, in the same key shape. Whatever is
 * left when the run exits belongs to the agent, which frees it after reducing
 * it -- or keeps it deliberately, to ask a different question of it later.
 * That is why there is no "hold" flag for the solver to poll.
 *
 * Nothing here is fatal. If the store misbehaves the simulation carries on and
 * the failure count is printed at shutdown.
 *
 * ---------------------------------------------------------------------------
 * WIRING
 *
 * miniamr_live.c and miniamr_live.h are the only files that come from us.
 * miniAMR is LGPL and we do not ship a modified copy of it, so you apply these
 * edits to your own clone.
 *
 *   cp miniamr_live.c miniamr_live.h  /path/to/miniAMR-live/ref/
 *
 * Six edits: four in main.c, two in driver.c, five lines in the Makefile.
 * BUILD.md ("The six edits") shows each one as the stock lines with the new
 * line marked; the list below is the same edits in short form.
 *
 * --- miniAMR/ref/main.c -----------------------------------------------------
 *
 * 1. With the other includes, near "#include "proto.h"":
 *
 *       #include "miniamr_live.h"
 *
 * 2. Between "timer_main = timer() - t1;" and "driver();":
 *
 *       miniamr_live_open();
 *
 *    A non-zero return only means live writing is off; the run is unaffected.
 *
 * 3. Between "profile();" and "deallocate();":
 *
 *       miniamr_live_final();
 *
 *    This is the end-of-run mesh write. It must stay before deallocate(),
 *    which frees blocks[].
 *
 * 4. Just before "MPI_Finalize();":
 *
 *       miniamr_live_close();
 *
 * --- miniAMR/ref/driver.c ---------------------------------------------------
 *
 * 5. With the other includes, after "#include "proto.h"":
 *
 *       #include "miniamr_live.h"
 *
 * 6. Two calls inside driver().
 *
 *    (a) At the END of the timestep loop body -- after the MPI_Allreduce block
 *        guarded by "if (!(ts%refine_freq) || done)", as the last statement
 *        before the loop's closing brace:
 *
 *           if (!done)
 *              miniamr_live_step(ts);
 *
 *        It has to go after refine(), which is what moves the block counts.
 *        The last timestep is skipped because the end-of-run keys cover it and
 *        a snapshot written there would be freed again straight away. `done` is
 *        the same on every rank, so this stays collective.
 *
 *    (b) In the checksum branch that gives up mid-run -- the one that prints
 *        "difference too large" and then returns -- add this immediately before
 *        that "return;":
 *
 *           miniamr_live_flush(ts);
 *
 *        Note the stock code has no braces there, so the return runs on every
 *        rank, not just rank 0. That makes the flush collective too, which is
 *        what it needs. Without it, a run that miniAMR abandoned looks exactly
 *        like a short healthy one, because it still writes its end-of-run mesh.
 *
 * --- miniAMR/ref/Makefile ---------------------------------------------------
 *
 * 7. Five lines in the Makefile. The first renames the binary so it cannot be
 *    confused with a stock build; solver.MINIAMR_BIN expects this name.
 *
 *       EXEC     = miniAMR-live.x
 *       CPPFLAGS = -I. -I$(DRAGON_BASE_DIR)/include
 *       LDFLAGS  = -L$(DRAGON_BASE_DIR)/lib
 *       LDLIBS   = -lm -ldragon
 *       OBJS    += miniamr_live.o
 *
 *    a dependency line with the others at the bottom:
 *
 *       miniamr_live.o: miniamr_live.h block.h
 *
 *    and in the clean target, miniAMR-live.x in place of miniAMR.x.
 *
 * Then "make" in miniAMR-live/ref with DRAGON_BASE_DIR set, and check the
 * result with probe_live.py before wiring it into a campaign.
 *
 * ---------------------------------------------------------------------------
 * ENVIRONMENT (campaign.py sets these on the job)
 *
 *   MINIAMR_DDICT      required. Serialized mesh DDict, from DDict.serialize().
 *   MINIAMR_RUN        required. Run id, e.g. "w3-p1".
 *   MINIAMR_CAMPAIGN   optional. Campaign id. When set, every key gains a
 *                      "<campaign>/" segment after "mesh/" or "live/", so two
 *                      campaigns can share one allocation without colliding.
 *   MINIAMR_LIVE_FREQ  optional, default 5.  Timesteps between telemetry
 *                      writes. Best kept a multiple of --refine_freq, since
 *                      the block counts only move when refine() runs.
 *   MINIAMR_SNAP_FREQ  optional, default 25. Timesteps between field
 *                      snapshots. 0 turns snapshots off (telemetry only) --
 *                      use that for calibration runs and for cheap testing.
 *   MINIAMR_SNAP_KEEP  optional, default 2.  Snapshots kept per rank.
 *
 * With none of the optional vars set and MINIAMR_SNAP_FREQ=0, this writes the
 * end-of-run mesh plus a stream of small telemetry records, and nothing else.
 */

#ifndef MINIAMR_LIVE_H
#define MINIAMR_LIVE_H

/* Attach and read the environment. Call once, after the parameter broadcast
 * and allocate(), before driver(). Returns 0 on success; any non-zero means
 * live writing is off and the run proceeds normally. */
int miniamr_live_open(void);

/* Publish for this timestep, if it is due. Call at the end of every timestep,
 * after refine() has updated the block counts. Does nothing when not open. */
void miniamr_live_step(int ts);

/* Publish telemetry now regardless of cadence. Used on the paths where miniAMR
 * gives up mid-run, so the last record shows why. */
void miniamr_live_flush(int ts);

/* The end-of-run mesh write. Call after profile() and before deallocate(). */
void miniamr_live_final(void);

/* Detach and report any failures. Call before MPI_Finalize. */
void miniamr_live_close(void);

#endif /* MINIAMR_LIVE_H */
