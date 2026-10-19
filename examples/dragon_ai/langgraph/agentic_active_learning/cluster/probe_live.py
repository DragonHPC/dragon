"""
Phase 1 acceptance test: run one miniAMR and watch it publish while it runs.

No agent, no LLM, no graph. Start one MPI run, print the telemetry as it
arrives, check each field snapshot, print pass/fail. If this does not pass,
nothing built on top of it will work.

    dragon probe_live.py --bin /path/to/miniAMR-live.x
    dragon probe_live.py --bin ... --snap-freq 0      # telemetry only, fast
    dragon probe_live.py --bin ... --cancel-at 40     # stop it part-way

The binary must be the online-steering build: a miniAMR clone with
miniamr/miniamr_live.c added and the edits from miniamr_live.h applied. A
stock miniAMR.x writes nothing mid-run, so this will report no telemetry.

Key names here have no campaign segment, which is what the hook writes when
MINIAMR_CAMPAIGN is unset.
"""

from __future__ import annotations

import argparse
import math
import sys
import time

import dragon           # noqa: F401  -- registers the Dragon start method
from dragon.data.ddict import DDict
from dragon.infrastructure.policy import Policy
from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import Batch

import settings
from data import mesh
from execution import clients
from science import solver

RUN = "probe"
DIALS = [sum(solver.SPEED_RANGE) / 2, sum(solver.SIZE_RANGE) / 2]
GRID = {1: (1, 1, 1), 8: (2, 2, 2)}   # ranks -> miniAMR's npx/npy/npz
BLOCK_BYTES = 8 * math.prod(solver.MESH_DIMS)
POLL_SECONDS = 1.0


def get(ddict, key: str):
    """A key's bytes, or None if it is not there yet."""
    try:
        return ddict[key]
    except KeyError:
        return None


def node_view(ddict: DDict, host: str):
    """A view pinned to the manager on `host`. The field is written node-local,
    so it can only be read through that node's manager."""
    bytes_ddict = mesh.raw_bytes_view(ddict)
    # Dragon: manager_nodes maps each DDict manager to its machine, so we can read exactly where the run wrote.
    for mid, node in ddict.manager_nodes.items():
        if str(node.hostname) == host:
            return bytes_ddict.manager(mid)
    raise ValueError(f"no mesh manager on {host}")


def telemetry_row(routed_ddict, ts: int) -> str | None:
    """One timestep's record (rank 0 wrote it), as a line to print."""
    row = mesh.read_telemetry(routed_ddict, "", RUN, ts)
    if row is None:
        return None
    return (f"{ts:>5} {row['max_local_blocks']:>7} {row['global_blocks']:>8} "
            f"{row['wall_seconds']:>7.1f}")


def check_snapshot(local_ddict, ts: int, ranks: int) -> dict:
    """Did every rank finish this snapshot, and does the field match meta?"""
    blocks = with_meta = whole_ranks = 0
    consistent = True

    for pe in range(ranks):
        meta = get(local_ddict, f"mesh/{RUN}/t{ts}/r{pe}/meta")
        if meta is None:
            continue
        with_meta += 1
        if len(meta) % 16:
            consistent = False
            continue
        nb = len(meta) // 16                          # 4 int32 per block
        blocks += nb

        field = get(local_ddict, f"mesh/{RUN}/t{ts}/r{pe}/field")
        if field is not None:
            if len(field) == nb * BLOCK_BYTES:
                whole_ranks += 1
            else:
                consistent = False

    return {"ts": ts, "blocks": blocks, "with_meta": with_meta,
            "whole_ranks": whole_ranks, "consistent": consistent,
            "whole": whole_ranks == ranks}


def still_held(local_ddict, timesteps: list[int]) -> int:
    """How many snapshots rank 0 is still holding. The hook drops its oldest
    before writing a new one, so this must not exceed --keep."""
    return sum(1 for ts in timesteps
               if get(local_ddict, f"mesh/{RUN}/t{ts}/r0/meta") is not None)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bin", default=solver.MINIAMR_BIN)
    p.add_argument("--ranks", type=int, default=8, choices=sorted(GRID))
    p.add_argument("--tsteps", type=int, default=solver.NUM_TSTEPS)
    p.add_argument("--live-freq", type=int, default=5)
    p.add_argument("--snap-freq", type=int, default=25, help="0 = no snapshots")
    p.add_argument("--keep", type=int, default=2)
    p.add_argument("--cancel-at", type=int, default=0,
                   help="cancel the run at this timestep, to test a partial write")
    args = p.parse_args()
    args.live_freq = max(1, args.live_freq)
    return args


def launch(batch: Batch, args: argparse.Namespace, mesh_ser, host: str | None):
    npx, npy, npz = GRID[args.ranks]
    argv = solver.build_argv(DIALS, npx, npy, npz)
    argv[argv.index("--num_tsteps") + 1] = str(args.tsteps)

    template = ProcessTemplate(
        target=args.bin, args=tuple(argv),
        env={
            "MINIAMR_DDICT": mesh_ser.decode() if isinstance(mesh_ser, bytes)
                             else str(mesh_ser),
            "MINIAMR_RUN": RUN,
            "MINIAMR_LIVE_FREQ": str(args.live_freq),
            "MINIAMR_SNAP_FREQ": str(args.snap_freq),
            "MINIAMR_SNAP_KEEP": str(args.keep),
        },
        stdout=ProcessTemplate.DEVNULL,
        policy=Policy(placement=Policy.Placement.HOST_NAME,
                      host_name=host) if host else None,
    )
    # Dragon: 8 ranks as one job; Batch handles the MPI launch and returns a handle at once.
    return batch.job([(args.ranks, template)])


def main() -> int:
    args = parse_args()
    hosts = clients.allocation_hostnames()
    host = hosts[0] if hosts else None
    n_nodes = len(hosts) or 1

    mesh_ddict = DDict(1, n_nodes, settings.MESH_MEM_PER_NODE * n_nodes)
    batch = Batch(managed_lifecycle=True)
    terminal = False

    try:
        task = launch(batch, args, mesh_ddict.serialize(), host)
        submitted_at = time.monotonic()
        cancel_at = None
        routed_ddict = mesh.raw_bytes_view(mesh_ddict)      # small keys, routed normally
        local_ddict = node_view(mesh_ddict, host or "")     # the field, on the run's machine

        print(f"{RUN}: {args.ranks} ranks on {host or 'anywhere'}, "
              f"{args.tsteps} timesteps, telemetry every {args.live_freq}, "
              f"snapshots every {args.snap_freq or 'never'}\n")
        print(f"{'ts':>5} {'maxblk':>7} {'global':>8} {'wall s':>7}")
        print("-" * 40)

        seen: set[int] = set()
        snapshots: list[dict] = []
        snap_seen: list[int] = []
        held = latest = 0
        went_backwards = False
        outcome = "ended early"

        while True:
            raw = get(routed_ddict, f"live/{RUN}/progress")
            now, snap = mesh.PROGRESS.unpack(raw) if raw is not None else (0, 0)
            went_backwards |= now < latest
            latest = max(latest, now)

            for ts in range(args.live_freq, latest + 1, args.live_freq):
                if ts in seen:
                    continue
                row = telemetry_row(routed_ddict, ts)
                if row:
                    seen.add(ts)
                    print(row, flush=True)

            if snap and snap not in snap_seen:
                snap_seen.append(snap)
                info = check_snapshot(local_ddict, snap, args.ranks)
                snapshots.append(info)
                held = max(held, still_held(local_ddict, snap_seen))
                print(f"      snapshot t{snap}: {info['blocks']} blocks over "
                      f"{info['with_meta']}/{args.ranks} ranks, "
                      f"field {info['whole_ranks']}/{args.ranks}, "
                      f"{'consistent' if info['consistent'] else 'MISMATCH'}",
                      flush=True)

            status, result = clients.poll_task(task, process=True)
            if status != "pending":
                terminal = True
                outcome = ("finished" if status == "done" and clients.exit_ok(result, args.ranks) else status)
                break
            if cancel_at is None and ((args.cancel_at and latest >= args.cancel_at) or
                                     time.monotonic() - submitted_at >= settings.RUN_TIMEOUT):
                cancel_at = time.monotonic()
                try:
                    accepted = task.cancel(timeout=settings.CANCEL_TIMEOUT)
                    print(f"\ncancel requested at t{latest}; accepted={accepted}, awaiting terminal result", flush=True)
                except Exception as exc:
                    print(f"cancel request failed: {exc}", flush=True)
            if cancel_at is not None and time.monotonic() - cancel_at > settings.CANCEL_TIMEOUT:
                raise clients.UnsafeTermination("probe task termination unknown")
            time.sleep(POLL_SECONDS)

        whole = [s for s in snapshots if s["whole"]]
        checks = [
            ("telemetry arrived", bool(seen)),
            ("progress never went backwards", not went_backwards),
            ("field length matches meta", all(s["consistent"] for s in snapshots)),
            ("observed retention within --keep plus final", held <= args.keep + 1),
            ("solver terminal outcome expected", outcome == ("cancelled" if args.cancel_at else "finished")),
        ]
        if outcome == "finished":
            final_snapshot = check_snapshot(local_ddict, args.tsteps, args.ranks)
            checks.append(("final snapshot complete", final_snapshot["whole"] and final_snapshot["consistent"]))
        if outcome == "cancelled" and snap_seen:
            # A rank stopped part-way wrote meta before streaming the field, and
            # the field value only appears once the stream finalizes -- so a
            # cancelled rank leaves meta (nameable, freeable) and no half field.
            partial = check_snapshot(local_ddict, snap_seen[-1], args.ranks)
            checks.append(("partial write leaves meta, no half field",
                           partial["with_meta"] > 0 and partial["consistent"]))

        print("\n" + "=" * 70)
        print(f"run {outcome} at t{latest}: {len(seen)} telemetry timesteps, "
              f"{len(whole)}/{len(snapshots)} snapshots complete, "
              f"at most {held} held at once")
        if whole:
            mb = whole[-1]["blocks"] * BLOCK_BYTES / 1024 ** 2
            print(f"a snapshot is about {mb:.0f} MB, so --keep {args.keep} costs "
                  f"up to {args.keep * mb:.0f} MB per run per machine")
        print()
        for name, passed in checks:
            print(f"  [{'pass' if passed else 'FAIL'}] {name}")
        print("=" * 70)

        return 0 if all(passed for _, passed in checks) else 1

    finally:
        if terminal:
            try:
                batch.join(timeout=settings.CANCEL_TIMEOUT)
                batch.destroy(timeout=settings.CANCEL_TIMEOUT)
                mesh_ddict.destroy()
            except Exception as exc:
                print(f"probe cleanup unconfirmed: {exc}; operator cleanup required", flush=True)
        else:
            print("probe task termination unknown; resources not explicitly destroyed", flush=True)


if __name__ == "__main__":
    sys.exit(main())
