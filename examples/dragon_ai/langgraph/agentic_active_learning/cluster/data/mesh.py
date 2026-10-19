"""Read the four-key C protocol locally; the raw pickler copies bytes.

Keys in the mesh DDict. All are written by the C hook inside the MPI job
(miniamr/miniamr_live.c). <c> = campaign id (may be ""), <run> = run id,
<ts> = timestep, <pe> = MPI rank 0..7.

  live/<c>/<run>/t<ts>
    value    104 bytes (LIVE_REC): ts, max blocks on one rank, global blocks,
             max level, level count, pad, checksum, wall seconds, blocks per level x8.
             checksum is miniAMR's own sum of field variable 0; only its finiteness is used.
    writer   rank 0, every LIVE_FREQ steps, routed to whichever manager the hash picks
    reader   worker AgentHost (read_telemetry)
    deleter  placed reducer, when the run is finished

  live/<c>/<run>/progress
    value    8 bytes (PROGRESS): newest telemetry ts, newest COMPLETE snapshot ts
    writer   rank 0, after an MPI barrier, so "complete" means every rank wrote
    reader   worker AgentHost (read_progress);  placed reducer
    deleter  placed reducer

  mesh/<c>/<run>/t<ts>/r<pe>/meta
    value    4 int32 per active block: refinement level, then the block centre
             (cen x, y, z) as integer coordinates on the finest grid.
             Never decoded here: len/16 is the block count, and that is all we use.
    writer   rank <pe>, every SNAP_FREQ steps and once at the end, to ITS OWN
             node's manager (local handle), so the bytes stay on that machine
    reader   placed reader and reducer only, through local_view
    deleter  the C hook drops the oldest beyond SNAP_KEEP; the placed reducer
             or cleanup deletes whatever is left when the run is finished

  mesh/<c>/<run>/t<ts>/r<pe>/field
    value    every active block's cells, float64, [nb][NUM_VARS][NX+2][NY+2][NZ+2],
             blocks in meta's order
    writer / reader / deleter   same as meta

The worker AgentHost never reads a mesh/ key. Anything that needs the field
is sent to the machine that holds it as a placed process (data/reduce_worker.py).
"""

import math
import struct

import numpy as np

from science import solver

LIVE_REC = struct.Struct("<6i2d8q")
PROGRESS = struct.Struct("<2i")


class _RawKeyPickler:
    def dumps(self, key):
        return key.encode("utf-8") if isinstance(key, str) else bytes(key)

    def loads(self, data):
        return bytes(data).decode("utf-8")


class _RawValuePickler:
    def dump(self, value, file):
        file.write(bytes(value))

    def load(self, file):
        buf = bytearray()
        try:
            while True:
                buf.extend(file.read(0))
        except EOFError:
            return bytes(buf)


def raw_bytes_view(ddict):
    """A view of a Dragon DDict that hands back bytes, not Python objects."""
    # Dragon: a custom pickler lets C and Python share one DDict without agreeing on a Python format.
    return ddict.pickler(_RawKeyPickler(), _RawValuePickler())


def local_view(ddict):
    """A view of a Dragon DDict pinned to this machine's manager, where its own run wrote the field."""
    bytes_ddict = raw_bytes_view(ddict)
    # Dragon: this is the data-locality line. Reads hit the manager on this node; nothing crosses the network.
    return bytes_ddict.manager(bytes_ddict.local_manager)


def ns(campaign):
    return f"{campaign}/" if campaign else ""


def read_progress(routed_ddict, campaign, run):
    """Newest timestep and newest complete snapshot, published after a barrier."""
    try:
        raw = routed_ddict[f"live/{ns(campaign)}{run}/progress"]
    except KeyError:
        return 0, 0
    if len(raw) != PROGRESS.size:
        raise ValueError("progress length mismatch")
    latest, snap = PROGRESS.unpack(raw)
    if not (0 <= latest <= solver.NUM_TSTEPS and 0 <= snap <= solver.NUM_TSTEPS):
        raise ValueError("progress outside planned timesteps")
    return latest, snap


def read_telemetry(routed_ddict, campaign, run, ts):
    """One 104-byte record rank 0 wrote mid-run; small enough to live anywhere."""
    try:
        raw = routed_ddict[f"live/{ns(campaign)}{run}/t{ts}"]
    except KeyError:
        return None
    if len(raw) != LIVE_REC.size:
        return None
    r = LIVE_REC.unpack(raw)
    if r[0] != ts or min(r[1:3]) < 0 or not math.isfinite(r[7]) or r[7] < 0:
        return None
    return {"timestep": r[0], "max_local_blocks": r[1], "global_blocks": r[2],
            "checksum_finite": math.isfinite(r[6]), "wall_seconds": r[7]}


def read_snapshot_rank(local_ddict, campaign, run, ts, pe):
    """One rank's blocks: how many, and their values. Meta is only counted."""
    base = f"mesh/{ns(campaign)}{run}/t{ts}/r{pe}"
    try:
        meta_raw, field_raw = local_ddict[base + "/meta"], local_ddict[base + "/field"]
    except KeyError:
        return None
    if len(meta_raw) % 16:
        return None
    nb = len(meta_raw) // 16
    if nb > solver.MAX_BLOCKS or len(field_raw) != nb * math.prod(solver.MESH_DIMS) * 8:
        return None
    field = np.frombuffer(field_raw, dtype="<f8").reshape((nb,) + solver.MESH_DIMS)
    return nb, field


def summarize_snapshot(local_ddict, campaign, run, ts, ranks):
    """Require every rank. Keep one number: how many blocks the mesh holds.

    Reads mesh/<c>/<run>/t<ts>/r<pe>/{meta,field} for every rank through the
    local manager; writes nothing. The dict it returns is what both the live
    reader (summary/...) and the final reducer (desc/...) store:
      run, timestep       which snapshot this is
      ranks_read          how many of `ranks` had both keys with sane lengths
      complete            ranks_read == ranks
      blocks              sum over ranks of len(meta)/16
      interior_finite     all non-ghost cells finite; None if incomplete
      valid               complete and interior_finite and blocks > 0
      reason              why not valid, else ""
      quantity            float(blocks) if valid else None -- the training label
      bytes_read          field bytes actually read on this machine
    The placed process adds host, read_seconds, summary_bytes before storing.
    """
    if ranks < 1:
        raise ValueError("invalid rank count")
    blocks = read_bytes = ranks_read = 0
    finite = True
    missing = []
    for pe in range(ranks):
        got = read_snapshot_rank(local_ddict, campaign, run, ts, pe)
        if got is None:
            missing.append(pe)
            continue
        ranks_read += 1
        nb, field = got
        blocks += nb
        read_bytes += field.nbytes
        if nb:
            # Ghost cells are skipped: miniAMR does not fill them.
            finite = bool(np.isfinite(field[:, :, 1:-1, 1:-1, 1:-1]).all()) and finite
    complete = ranks_read == ranks
    valid = complete and finite and blocks > 0
    return {
        "run": run, "timestep": ts, "complete": complete,
        "valid": valid, "reason": "" if valid else (
            f"missing/invalid rank fields: {missing}" if not complete else
            "nonfinite interior" if not finite else "empty snapshot"),
        "quantity": float(blocks) if valid else None,
        "blocks": blocks, "bytes_read": read_bytes, "ranks_read": ranks_read,
        "interior_finite": finite if complete else (False if not finite else None),
    }


def free_snapshot(local_ddict, campaign, run, ts, ranks):
    """Delete one timestep's meta and field for every rank; returns keys freed."""
    freed = 0
    for pe in range(ranks):
        for suffix in ("field", "meta"):
            try:
                # Dragon: deleting a key frees the manager's memory; no filesystem, no cleanup job.
                del local_ddict[f"mesh/{ns(campaign)}{run}/t{ts}/r{pe}/{suffix}"]
                freed += 1
            except KeyError:
                pass
    return freed


def free_run(local_ddict, campaign, run, ranks):
    """Delete every mesh/ key this run could have written, on this machine.

    Walks all 1..NUM_TSTEPS rather than only the published ones: a cancelled
    rank may have written a snapshot whose progress marker never followed.
    """
    return sum(free_snapshot(local_ddict, campaign, run, ts, ranks)
               for ts in range(1, solver.NUM_TSTEPS + 1))
