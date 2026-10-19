/*
 * Mid-run DDict writes for miniAMR. See miniamr_live.h for the key layout, the
 * ordering rules the reader depends on, and how to wire this into a clone.
 *
 * All Dragon C-API use is isolated in the small put/stream/pop helpers at the
 * top. Grounded in src/include/dragon/ddict.h:
 *   dragon_ddict_attach, dragon_ddict_local_manager, dragon_ddict_manager,
 *   dragon_ddict_create_request, dragon_ddict_write_bytes, dragon_ddict_put,
 *   dragon_ddict_pop, dragon_ddict_read_bytes,
 *   dragon_ddict_finalize_request, dragon_ddict_detach.
 *
 * The put sequence, per that header's contract: write the key bytes, call put
 * (which ends the key and begins the value), then write the value bytes -- as
 * many times as you like, which is what lets one ~200 MB field stream in block
 * by block with no staging buffer -- then finalize.
 */

#include "miniamr_live.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <mpi.h>

#include <dragon/ddict.h>
#include <dragon/return_codes.h>

#include "block.h"

#define LIVE_KEY_MAX     192
#define LIVE_KEEP_MAX    8
#define LIVE_MAX_LEVELS  8       /* blocks_by_level[] slots in the record */

/* One telemetry record, written by rank 0 only. 104 bytes, no padding holes on
 * any ABI we build for. The Python reader unpacks it as "<6i2d8q"; the assert
 * below is the other half of that agreement.
 *
 * Every field except max_local_blocks is already a globally reduced number in
 * miniAMR, so it is identical on every rank. max_local_blocks is the largest
 * per-rank block count -- the number the kill rule needs, because the block cap
 * is per rank -- reduced with one MPI_Allreduce(MAX) before the write. */
typedef struct {
   int32_t ts;
   int32_t max_local_blocks;
   int32_t global_blocks;
   int32_t max_level;
   int32_t num_levels;              /* how many blocks_by_level slots are valid */
   int32_t pad;
   double  checksum;
   double  wall_seconds;
   int64_t blocks_by_level[LIVE_MAX_LEVELS];
} live_rec;

typedef char live_rec_is_104_bytes[(sizeof(live_rec) == 104) ? 1 : -1];

/* Handles held for the life of the rank: the attached dictionary for the small
 * routed keys, and a copy pinned to this node's manager for the field, so the
 * large data never leaves the node that computed it. */
static dragonDDictDescr_t g_dd;
static dragonDDictDescr_t g_local;
static int g_open = 0;

static const char *g_run = NULL;
static char g_ns[64] = "";     /* "" or "<campaign>/" */

static int g_live_freq = 5;
static int g_snap_freq = 25;
static int g_keep = 2;

static double g_t0 = 0.0;
static long g_fail_put = 0;
static long g_fail_pop = 0;
static int  g_stop_ts = 0;      /* set by flush() on the give-up path */

/* Progress, rank 0 only: newest telemetry timestep, newest complete snapshot. */
static int32_t g_progress[2] = {0, 0};

/* Packing buffers, allocated on first use and kept for the run. */
static double  *g_packed = NULL;
static size_t   g_packed_cells = 0;
static int32_t *g_meta = NULL;
static int      g_meta_cap = 0;

/* What this rank currently holds, oldest first, so retention needs no reads. */
static struct { int ts; int nb; } g_hist[LIVE_KEEP_MAX];
static int g_hist_n = 0;


/* --------------------------------------------------------------------------
 * Dragon calls. Every one of these returns 0 on success and never aborts: a
 * store problem must not take the simulation down with it.
 * -------------------------------------------------------------------------- */

/* Write one key with one value in a single request. Used for every small
 * write: telemetry, progress, meta. `handle` picks routed (g_dd) or local (g_local). */
static int live_put(dragonDDictDescr_t *handle, const char *key,
                    const void *buf, size_t nbytes)
{
   dragonDDictRequestDescr_t req;

   if (dragon_ddict_create_request(handle, &req) != DRAGON_SUCCESS) {
      g_fail_put++;
      return 1;
   }
   if (dragon_ddict_write_bytes(&req, strlen(key), (uint8_t *) key)
         != DRAGON_SUCCESS)
      goto fail;
   if (dragon_ddict_put(&req) != DRAGON_SUCCESS)
      goto fail;
   if (nbytes > 0 && dragon_ddict_write_bytes(&req, nbytes, (uint8_t *) buf)
         != DRAGON_SUCCESS)
      goto fail;
   if (dragon_ddict_finalize_request(&req) != DRAGON_SUCCESS) {
      g_fail_put++;
      return 1;
   }
   return 0;

fail:
   dragon_ddict_finalize_request(&req);
   g_fail_put++;
   return 1;
}


/* A large value streamed in pieces: begin, then chunk repeatedly, then end.
 * The key is written up front so every chunk lands in the same value. */
static int live_put_begin(dragonDDictDescr_t *handle, const char *key,
                          dragonDDictRequestDescr_t *req)
{
   if (dragon_ddict_create_request(handle, req) != DRAGON_SUCCESS)
      return 1;
   if (dragon_ddict_write_bytes(req, strlen(key), (uint8_t *) key)
         != DRAGON_SUCCESS) {
      dragon_ddict_finalize_request(req);
      return 1;
   }
   if (dragon_ddict_put(req) != DRAGON_SUCCESS) {
      dragon_ddict_finalize_request(req);
      return 1;
   }
   return 0;
}

/* Append one more piece to the value opened by live_put_begin. */
static int live_put_chunk(dragonDDictRequestDescr_t *req,
                          const void *buf, size_t nbytes)
{
   if (nbytes == 0)
      return 0;
   return dragon_ddict_write_bytes(req, nbytes, (uint8_t *) buf)
          == DRAGON_SUCCESS ? 0 : 1;
}

/* Close the streamed value; only after this does the key exist for readers. */
static int live_put_end(dragonDDictRequestDescr_t *req)
{
   return dragon_ddict_finalize_request(req) == DRAGON_SUCCESS ? 0 : 1;
}


/* Delete one key. The value has to be drained before finalizing, per the pop
 * contract in ddict.h. A missing key is not an error -- a rank that was stopped
 * part-way leaves gaps, and freeing what is there is the whole point. */
static int live_pop(dragonDDictDescr_t *handle, const char *key)
{
   dragonDDictRequestDescr_t req;
   dragonError_t err;

   if (dragon_ddict_create_request(handle, &req) != DRAGON_SUCCESS) {
      g_fail_pop++;
      return 1;
   }
   if (dragon_ddict_write_bytes(&req, strlen(key), (uint8_t *) key)
         != DRAGON_SUCCESS) {
      dragon_ddict_finalize_request(&req);
      g_fail_pop++;
      return 1;
   }

   err = dragon_ddict_pop(&req);
   if (err != DRAGON_SUCCESS) {
      dragon_ddict_finalize_request(&req);
      if (err != DRAGON_KEY_NOT_FOUND)
         g_fail_pop++;
      return 1;
   }

   for (;;) {
      size_t got = 0;
      uint8_t *bytes = NULL;
      err = dragon_ddict_read_bytes(&req, 0, &got, &bytes);
      if (bytes != NULL)
         free(bytes);
      if (err != DRAGON_SUCCESS)
         break;
   }

   dragon_ddict_finalize_request(&req);
   return 0;
}


/* --------------------------------------------------------------------------
 * Snapshot write and retention.
 *
 * What one snapshot is. At timestep ts, EVERY rank writes two keys through
 * g_local, so they land on this machine's DDict manager and nowhere else:
 *
 *   mesh/<campaign>/<run>/t<ts>/r<my_pe>/meta    4 int32 per active block:
 *                                                level, cen[0], cen[1], cen[2]
 *                                                (cen = block centre, integer
 *                                                coordinates on the finest grid)
 *   mesh/<campaign>/<run>/t<ts>/r<my_pe>/field   every active block's values,
 *                                                [num_vars][x+2][y+2][z+2] double,
 *                                                blocks in the same order as meta
 *
 * The reader never decodes meta; len(meta)/16 tells it how many blocks the
 * rank had, which is the one number the campaign keeps. field is the bulk.
 *
 * Order and why. meta is written first and is tiny, so a rank that is killed
 * mid-write still leaves a key that names what it was doing; the cleanup uses
 * that to free the leftovers. field is streamed block by block into ONE value
 * (begin / chunk / end), so a killed rank leaves either a complete value or
 * none -- never a truncated one the reader would misparse.
 *
 * Retention. Each rank keeps at most g_keep (MINIAMR_SNAP_KEEP, default 2)
 * snapshots alive. g_hist[] is that rank's own list, oldest first, so dropping
 * needs no DDict reads. Before a scheduled write, live_drop_oldest() pops until
 * g_keep-1 remain, then the new one is written. The end-of-run snapshot from
 * miniamr_live_final() passes drop_first=0: it adds one more without dropping,
 * and the solver frees nothing after that. Freeing is the agent's job, done by
 * the placed reducer once the run is confirmed terminal.
 *
 * Who says a snapshot is complete. Not this file. miniamr_live_step() calls
 * MPI_Barrier after every rank's write, then rank 0 sets progress[1] = ts. A
 * reader that sees progress[1] == ts knows all ranks' t<ts> keys are there.
 *
 * TODO(ddict-checkpoint): DDict checkpointing could replace three things here
 * -- the t<ts> in the key, the g_hist[] retention, and the barrier + progress
 * marker. Create the mesh DDict with working_set_size=SNAP_KEEP and
 * wait_for_keys=True; each rank calls dragon_ddict_checkpoint() before every
 * snapshot and writes mesh/<c>/<run>/r<pe>/{meta,field} with no timestep in
 * the key. The manager then keeps the last SNAP_KEEP checkpoints and retires
 * older ones itself (no live_drop_oldest), and a reader at checkpoint N that
 * asks for r<pe>/field blocks until rank <pe> has written it (no barrier, no
 * progress[1]). The reader would find the newest checkpoint with
 * dragon_ddict_sync_to_newest_checkpoint(). Cost: a reader on a retired
 * checkpoint gets DDictCheckpointSyncError instead of a missing key, and
 * every key in the store would need to be checkpoint-aware or persistent.
 * The barrier saved is one per SNAP_FREQ steps; the real gain is deleting
 * the retention and progress code. Not done: this version keeps the protocol
 * readable from Python without checkpoint semantics.
 * -------------------------------------------------------------------------- */

/* Allocate (or resize) the two scratch buffers: one packed block's cells, and
 * one meta array for up to max_num_blocks blocks. Called before every snapshot;
 * a no-op once sized. Returns 0 if malloc failed, and the snapshot is skipped. */
static int live_ensure_buffers(void)
{
   int xs = x_block_size + 2, ys = y_block_size + 2, zs = z_block_size + 2;
   size_t cells = (size_t) num_vars * xs * ys * zs;

   if (g_packed == NULL || g_packed_cells != cells) {
      free(g_packed);
      g_packed = (double *) malloc(cells * sizeof(double));
      g_packed_cells = (g_packed != NULL) ? cells : 0;
   }
   if (g_meta == NULL || g_meta_cap < max_num_blocks) {
      free(g_meta);
      g_meta = (int32_t *) malloc(4 * (size_t) max_num_blocks * sizeof(int32_t));
      g_meta_cap = (g_meta != NULL) ? max_num_blocks : 0;
   }
   return g_packed != NULL && g_meta != NULL;
}


/* Drop the oldest snapshot this rank holds. No hold flag any more -- the agent
 * owns whatever is left when the run ends. */
static void live_drop_oldest(void)
{
   char key[LIVE_KEY_MAX];
   int ts, i;

   if (g_hist_n <= 0)
      return;
   ts = g_hist[0].ts;

   snprintf(key, sizeof(key), "mesh/%s%s/t%d/r%d/field", g_ns, g_run, ts, my_pe);
   live_pop(&g_local, key);
   snprintf(key, sizeof(key), "mesh/%s%s/t%d/r%d/meta", g_ns, g_run, ts, my_pe);
   live_pop(&g_local, key);

   for (i = 1; i < g_hist_n; i++)
      g_hist[i - 1] = g_hist[i];
   g_hist_n--;
}


/* Pack this rank's active blocks and write them, node-local: meta first (which
 * carries the block count in its length), then every block streamed into one
 * field value in meta's order. drop_first rolls the retention window; the
 * end-of-run snapshot passes 0, so it leaves the mid-run ones for the agent. */
static void live_write_snapshot(int ts, int drop_first)
{
   char key[LIVE_KEY_MAX];
   dragonDDictRequestDescr_t req;
   int xs, ys, zs, n, nb, v, ii, jj, kk;
   size_t cells, p;

   if (!live_ensure_buffers())
      return;

   if (drop_first)
      while (g_hist_n >= g_keep)
         live_drop_oldest();

   xs = x_block_size + 2;
   ys = y_block_size + 2;
   zs = z_block_size + 2;
   cells = (size_t) num_vars * xs * ys * zs;

   /* Pass 1: record what each block is. Cheap -- no field touched -- so meta
    * lands before any bulk write and a killed rank still leaves it behind. */
   nb = 0;
   for (n = 0; n < max_active_block; n++) {
      if (blocks[n].number < 0)
         continue;
      if (nb >= g_meta_cap)
         break;
      g_meta[4 * nb + 0] = (int32_t) blocks[n].level;
      g_meta[4 * nb + 1] = (int32_t) blocks[n].cen[0];
      g_meta[4 * nb + 2] = (int32_t) blocks[n].cen[1];
      g_meta[4 * nb + 3] = (int32_t) blocks[n].cen[2];
      nb++;
   }

   snprintf(key, sizeof(key), "mesh/%s%s/t%d/r%d/meta", g_ns, g_run, ts, my_pe);
   live_put(&g_local, key, g_meta, 4 * (size_t) nb * sizeof(int32_t));

   /* Record the snapshot before the bulk write, so a rank stopped mid-stream
    * still gets its leftovers freed on the next drop. */
   if (g_hist_n < LIVE_KEEP_MAX) {
      g_hist[g_hist_n].ts = ts;
      g_hist[g_hist_n].nb = nb;
      g_hist_n++;
   }

   /* Pass 2: the field, one value, blocks in meta's order. */
   snprintf(key, sizeof(key), "mesh/%s%s/t%d/r%d/field", g_ns, g_run, ts, my_pe);
   if (live_put_begin(&g_local, key, &req)) {
      g_fail_put++;
      return;
   }
   nb = 0;
   for (n = 0; n < max_active_block; n++) {
      if (blocks[n].number < 0)
         continue;
      if (nb >= g_meta_cap)
         break;
      p = 0;
      for (v = 0; v < num_vars; v++)
         for (ii = 0; ii < xs; ii++)
            for (jj = 0; jj < ys; jj++)
               for (kk = 0; kk < zs; kk++)
                  g_packed[p++] = blocks[n].array[v][ii][jj][kk];
      if (live_put_chunk(&req, g_packed, cells * sizeof(double))) {
         g_fail_put++;
         break;
      }
      nb++;
   }
   if (live_put_end(&req))
      g_fail_put++;
}


/* --------------------------------------------------------------------------
 * Telemetry (rank 0 only).
 * -------------------------------------------------------------------------- */

/* Fill one live_rec from miniAMR's globals and write it as live/<c>/<run>/t<ts>
 * through the routed handle. max_local comes from the caller's Allreduce. */
static void live_write_telemetry(int ts, int max_local)
{
   char key[LIVE_KEY_MAX];
   live_rec rec;
   int i, n = num_refine + 1;

   memset(&rec, 0, sizeof(rec));
   if (n > LIVE_MAX_LEVELS)
      n = LIVE_MAX_LEVELS;
   rec.ts               = (int32_t) ts;
   rec.max_local_blocks = (int32_t) max_local;
   rec.global_blocks    = (int32_t) global_active;
   rec.max_level        = (int32_t) cur_max_level;
   rec.num_levels       = (int32_t) n;
   rec.checksum         = (grid_sum != NULL) ? grid_sum[0] : 0.0;
   rec.wall_seconds     = MPI_Wtime() - g_t0;
   for (i = 0; i < n; i++)
      rec.blocks_by_level[i] = (int64_t) num_blocks[i];

   snprintf(key, sizeof(key), "live/%s%s/t%d", g_ns, g_run, ts);
   live_put(&g_dd, key, &rec, sizeof(rec));
}


/* Overwrite live/<c>/<run>/progress with g_progress[2]: {newest telemetry ts,
 * newest complete snapshot ts}. Rank 0 only, after the barrier in the caller. */
static void live_write_progress(void)
{
   char key[LIVE_KEY_MAX];
   snprintf(key, sizeof(key), "live/%s%s/progress", g_ns, g_run);
   live_put(&g_dd, key, g_progress, sizeof(g_progress));
}


/* --------------------------------------------------------------------------
 * Public API.
 * -------------------------------------------------------------------------- */

/* Read the MINIAMR_* environment, attach the mesh DDict, and pin a second
 * handle to this node's manager. Called once per rank before driver(). Any
 * non-zero return turns live writing off; the simulation runs unchanged. */
int miniamr_live_open(void)
{
   const char *serial = getenv("MINIAMR_DDICT");
   const char *campaign = getenv("MINIAMR_CAMPAIGN");
   const char *s;
   uint64_t local_id;

   g_run = getenv("MINIAMR_RUN");
   if (serial == NULL || g_run == NULL)
      return 1;

   if (campaign != NULL && campaign[0] != '\0')
      snprintf(g_ns, sizeof(g_ns), "%s/", campaign);

   if ((s = getenv("MINIAMR_LIVE_FREQ")) != NULL) g_live_freq = atoi(s);
   if ((s = getenv("MINIAMR_SNAP_FREQ")) != NULL) g_snap_freq = atoi(s);
   if ((s = getenv("MINIAMR_SNAP_KEEP")) != NULL) g_keep = atoi(s);
   if (g_keep < 1) g_keep = 1;
   if (g_keep > LIVE_KEEP_MAX) g_keep = LIVE_KEEP_MAX;

   /* NULL timeout: block rather than fail, matching the Python default. */
   if (dragon_ddict_attach(serial, &g_dd, NULL) != DRAGON_SUCCESS)
      return 2;
   if (dragon_ddict_local_manager(&g_dd, &local_id) != DRAGON_SUCCESS)
      return 3;
   if (dragon_ddict_manager(&g_dd, &g_local, local_id) != DRAGON_SUCCESS)
      return 4;

   g_t0 = MPI_Wtime();
   g_open = 1;
   return 0;
}


/* Called by every rank at the end of every timestep. Does nothing unless ts is
 * on the telemetry or snapshot schedule. Otherwise: Allreduce the per-rank
 * block max, write this rank's snapshot if due, barrier, then rank 0 writes
 * telemetry and progress. Collective: all ranks must call it with the same ts. */
void miniamr_live_step(int ts)
{
   int do_live, do_snap, max_local = 0;

   if (!g_open)
      return;

   do_live = g_live_freq > 0 && (ts % g_live_freq) == 0;
   do_snap = g_snap_freq > 0 && (ts % g_snap_freq) == 0;
   if (!do_live && !do_snap)
      return;

   /* The largest per-rank block count. Collective, so every rank calls it
    * whenever any write is due this timestep. */
   MPI_Allreduce(&num_active, &max_local, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

   if (do_snap)
      live_write_snapshot(ts, 1);

   /* The per-rank writes above must all be in before rank 0 announces them, so
    * progress means "every rank finished", not "one rank got there". */
   MPI_Barrier(MPI_COMM_WORLD);

   if (my_pe == 0) {
      if (do_live) {
         live_write_telemetry(ts, max_local);
         g_progress[0] = (int32_t) ts;
      }
      if (do_snap)
         g_progress[1] = (int32_t) ts;
      live_write_progress();
   }
}


/* Called on miniAMR's give-up path (checksum diverged) just before it returns
 * from driver(). Writes one last telemetry record and progress so the agent
 * sees the run stopped at ts, and remembers ts so final() labels its snapshot
 * with the step actually reached. Collective, like step(). */
void miniamr_live_flush(int ts)
{
   int max_local = 0;

   if (!g_open)
      return;
   g_stop_ts = ts;    /* final() labels its snapshot with the reached timestep */

   MPI_Allreduce(&num_active, &max_local, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
   MPI_Barrier(MPI_COMM_WORLD);
   if (my_pe == 0) {
      live_write_telemetry(ts, max_local);
      g_progress[0] = (int32_t) ts;
      live_write_progress();
   }
}


/* Called once after driver() returns, before deallocate(). Writes the
 * end-of-run snapshot every rank, then rank 0 marks it complete in progress.
 * This is the snapshot the final reducer reads to produce the training label. */
void miniamr_live_final(void)
{
   int stop_ts;

   if (!g_open || !live_ensure_buffers())
      return;

   stop_ts = g_stop_ts ? g_stop_ts : num_tsteps;

   /* One more snapshot, at the timestep the run actually reached, in the same
    * key shape as the mid-run ones. drop_first is 0, so the mid-run snapshots
    * this rank still holds are left for the agent -- the solver frees nothing
    * at the end. */
   live_write_snapshot(stop_ts, 0);

   MPI_Barrier(MPI_COMM_WORLD);
   if (my_pe == 0) {
      g_progress[1] = (int32_t) stop_ts;
      live_write_progress();
   }
}


/* Called once before MPI_Finalize. Reports failed writes/deletes, frees the
 * scratch buffers, detaches both handles. Deletes no keys: the agent owns them. */
void miniamr_live_close(void)
{
   if (!g_open)
      return;

   if (g_fail_put || g_fail_pop)
      printf("miniamr_live: rank %d had %ld failed writes, %ld failed deletes\n",
             my_pe, g_fail_put, g_fail_pop);

   free(g_packed);
   free(g_meta);
   g_packed = NULL;
   g_meta = NULL;
   g_packed_cells = 0;
   g_meta_cap = 0;

   dragon_ddict_detach(&g_local);
   dragon_ddict_detach(&g_dd);
   g_open = 0;
}
