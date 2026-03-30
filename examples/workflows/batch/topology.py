"""
batch_topology.py — Batch topology API walkthrough
===================================================

This example demonstrates how ``Batch.topology()`` exposes the node layout
chosen by Dragon when you create a ``Batch`` instance.  It walks through
several configurations to build intuition about how managers, worker pools,
and physical cores are assigned across an allocation.

Concepts
--------
Worker pools
    All nodes are divided into equal-sized pools, one pool per manager.
    Every node in a pool contributes ``num_cpus // 2`` workers (one per
    physical core, assuming hyperthreading doubles the logical count).

Manager co-location
    Each manager process runs on the **first node of its own pool**.  No
    core is reserved for the manager — the full physical-core count of every
    pool node is available to workers.

Remainder distribution
    When nodes cannot be divided evenly by ``pool_nodes``, the extra nodes
    are distributed one-at-a-time to the first N pools so that no node is
    left idle.

Usage
-----
Run on an allocation of at least a few nodes::

    dragon topology.py

The script does not submit any real work — it just creates ``Batch``
objects, prints their ``topology()``, and tears them down cleanly.
"""

from dragon.workflows.batch import Batch, BatchTopology
from dragon.native.machine import System


def section(title: str) -> None:
    width = 72
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def show(label: str, batch: Batch) -> None:
    """Print a labelled summary of a Batch topology."""
    t: BatchTopology = batch.topology()
    print(f"\n[{label}]")
    print(t)
    print(f"  repr  : {repr(t)}")
    print(f"  total workers : {sum(t.workers_per_pool)}")


def main() -> None:
    alloc = System()
    avail = len(alloc._node_objs)
    cpus_per_node = max(1, alloc._node_objs[0].num_cpus // 2)

    print(f"Allocation: {avail} node(s), {cpus_per_node} physical core(s) per node")

    # ── 1. Default: all nodes, one node per pool ──────────────────────────────
    section("1  Default — all nodes, one node per pool (pool_nodes=1)")

    print(
        "\n  With pool_nodes=1 (the default) each manager owns exactly one worker\n"
        "  node. The manager process is co-located on that node and does not\n"
        "  reserve a core — all physical cores are available as workers.\n"
        "  Setting pool_nodes=1 gives the maximum number of independent pools and\n"
        "  is optimized for high throughput of many short-lived tasks.\n"
    )

    # Keep b_default open; we reuse it for the field-by-field walkthrough
    # (section 5) to avoid an extra Batch lifecycle.
    b_default = Batch()
    show("Batch()", b_default)

    # ── 2. Explicit node count smaller than the allocation ────────────────────
    if avail >= 4:
        section("2  Explicit node count — use half of the allocation")

        half = max(2, avail // 2)
        print(
            f"\n  num_nodes={half} restricts the Batch to {half} node(s) even though\n"
            f"  {avail} are available.  This is useful when you want to reserve\n"
            f"  nodes for other work running in parallel.\n"
        )
        b = Batch(num_nodes=half)
        show(f"Batch(num_nodes={half})", b)
        b.close()
        b.join()

    # ── 3. Larger pools — multiple nodes per pool ─────────────────────────────
    if avail >= 4:
        section("3  Larger pools — pool_nodes=2")

        print(
            "\n  pool_nodes=2 groups two nodes into each pool.  Each pool still has\n"
            "  a single manager process co-located on the first node.  The manager\n"
            f"  launches 2 x {cpus_per_node} = {2 * cpus_per_node} workers spread across its two nodes.\n"
            "  Use this when tasks are large enough to benefit from more workers\n"
            "  per pool (e.g. MPI jobs that span multiple nodes).\n"
        )
        b = Batch(pool_nodes=2)
        show("Batch(pool_nodes=2)", b)
        b.close()
        b.join()

    # ── 4. Remainder distribution ─────────────────────────────────────────────
    #
    # With pool_nodes=2, a remainder arises when the node count is odd:
    #   num_managers = num_nodes // 2
    #   remainder    = num_nodes - num_managers * 2
    # e.g. 5 nodes => 2 managers, remainder=1 => pools of 3 and 2 nodes.
    if avail >= 4:
        # Use an odd node count to guarantee remainder=1 with pool_nodes=2.
        candidate = avail if avail % 2 != 0 else avail - 1
        if candidate >= 3:
            n_managers = candidate // 2
            section("4  Remainder nodes — uneven node / pool_nodes ratio")
            print(
                f"\n  num_nodes={candidate}, pool_nodes=2 => {n_managers} manager(s),\n"
                f"  remainder = {candidate - n_managers * 2} node(s).\n"
                f"  The remainder node is given to the first pool so every\n"
                f"  node is used (no node sits idle).\n"
            )
            b = Batch(num_nodes=candidate, pool_nodes=2)
            show(f"Batch(num_nodes={candidate}, pool_nodes=2)", b)
            b.close()
            b.join()

    # ── 5. BatchTopology field-by-field walkthrough ───────────────────────────
    #
    # Reuses b_default from section 1 — no extra Batch creation needed.
    section("5  BatchTopology field-by-field walkthrough")

    t = b_default.topology()

    print(f"\n  t.total_nodes        = {t.total_nodes}   (total nodes used by this Batch)")
    print(
        f"  t.manager_hostnames  = {t.manager_hostnames}"
        "\n                           (first-node hostname for each pool's manager)"
    )
    print(f"  t.pool_hostnames     = (list of {len(t.pool_hostnames)} pool(s))")
    for i, (hosts, wpp) in enumerate(zip(t.pool_hostnames, t.workers_per_pool)):
        print(f"    pool {i}: {wpp} worker(s) on {hosts}")
    print(
        f"  t.workers_per_pool   = {t.workers_per_pool}"
        "\n                           (physical_cores x nodes for each pool)"
    )
    print(f"\n  Total workers = {sum(t.workers_per_pool)}")

    b_default.close()
    b_default.join()


if __name__ == "__main__":
    main()
