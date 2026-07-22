"""
batch_topology.py — Batch topology API walkthrough
===================================================

This example demonstrates how ``Batch.topology()`` exposes the node layout
chosen by Dragon when you create a ``Batch`` instance. It walks through the
current manager model: one dedicated scheduler colocated with the client plus
one subnode manager and one worker pool per requested node.

Concepts
--------
Worker pools
    Each requested node contributes one worker pool. Every pool contributes
    ``num_cpus // 2`` workers (one per physical core, assuming hyperthreading
    doubles the logical count).

Manager co-location
    The scheduler runs on the client host. Each subnode manager runs
    on the nodes backing its worker pool. No core is reserved for the
    manager — the full physical-core count of every pool node is available to
    workers.

Useful relationships
    ``total_managers`` is always ``total_nodes + 1`` in the current runtime:
    one dedicated scheduler plus one subnode manager per requested node.
    ``manager_hostnames`` and ``pool_hostnames`` therefore have the same
    length, and each subnode manager host lines up with exactly one worker
    pool.

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


def explain_fields(t: BatchTopology) -> None:
    """Walk through the key relationships exposed by BatchTopology."""
    print(f"\n  t.total_nodes        = {t.total_nodes}   (requested worker-pool nodes)")
    print(f"  t.scheduler_hostname = {t.scheduler_hostname}   (dedicated scheduler host)")
    print(f"  t.total_managers     = {t.total_managers}   (1 scheduler + one subnode manager per pool)")
    print(
        f"  t.manager_hostnames  = {t.manager_hostnames}"
        "\n                           (subnode manager hostnames, one per worker pool)"
    )
    print(f"  t.pool_hostnames     = (list of {len(t.pool_hostnames)} pool(s))")
    for i, (mgr_host, hosts, wpp) in enumerate(zip(t.manager_hostnames, t.pool_hostnames, t.workers_per_pool)):
        print(f"    pool {i}: {wpp} worker(s) on {hosts}  [mgr: {mgr_host}]")
    print(
        f"  t.workers_per_pool   = {t.workers_per_pool}"
        "\n                           (physical cores x nodes for each pool)"
    )


def explain_invariants(t: BatchTopology) -> None:
    """Print the current topology invariants that users are most likely to care about."""
    print("\n  Current invariants")
    print(f"    total_managers = total_nodes + 1  ->  {t.total_managers} = {t.total_nodes} + 1")
    print(
        "    len(manager_hostnames) == len(pool_hostnames)"
        f"  ->  {len(t.manager_hostnames)} == {len(t.pool_hostnames)}"
    )
    print(
        "    one subnode manager backs each worker pool"
        f"  ->  {all(mgr == hosts[0] for mgr, hosts in zip(t.manager_hostnames, t.pool_hostnames))}"
    )
    print(f"    total workers = sum(workers_per_pool)  ->  {sum(t.workers_per_pool)}")


def main() -> None:
    alloc = System()
    avail = len(alloc._node_objs)
    cpus_per_node = max(1, alloc._node_objs[0].num_cpus // 2)

    print(f"Allocation: {avail} node(s), {cpus_per_node} physical core(s) per node")

    # 1. Default: all nodes, one node per pool
    section("1  Default — scheduler plus one pool per node")

    print(
        "\n  The default topology creates one dedicated scheduler manager on the\n"
        "  client host plus one subnode manager and one worker pool for every\n"
        "  requested node. That matches the intuitive model of requesting N\n"
        "  nodes and getting N node-local worker pools.\n"
    )

    # Keep b_default open; we reuse it for the field-by-field walkthrough
    # (section 5) to avoid an extra Batch lifecycle.
    b_default = Batch()
    show("Batch()", b_default)

    # 2. Explicit node count smaller than the allocation
    if avail >= 2:
        section("2  Explicit node count — use part of the allocation")

        subset = max(1, avail // 2)
        if subset >= avail:
            subset = avail - 1

        print(
            f"\n  num_nodes={subset} restricts the Batch to {subset} node(s) even though\n"
            f"  {avail} are available.  This is useful when you want to reserve\n"
            f"  nodes for other work running in parallel.\n"
        )
        b = Batch(num_nodes=subset)
        show(f"Batch(num_nodes={subset})", b)
        b.join()

    # 3. Manager count tracks requested nodes
    if avail >= 2:
        candidate = min(avail, max(2, avail // 2))
        section("3  Manager count follows requested nodes")
        print(
            f"\n  num_nodes={candidate} creates {candidate} worker pool(s), {candidate}\n"
            f"  subnode manager(s), and one extra scheduler manager. In other words,\n"
            f"  the topology has {candidate + 1} total managers for {candidate}\n"
            f"  requested nodes.\n"
        )
        b = Batch(num_nodes=candidate)
        show(f"Batch(num_nodes={candidate})", b)
        b.join()

    # 4. BatchTopology field-by-field walkthrough
    #
    # Reuses b_default from section 1 — no extra Batch creation needed.
    section("4  BatchTopology field-by-field walkthrough")

    t = b_default.topology()
    explain_fields(t)
    explain_invariants(t)

    b_default.join()


if __name__ == "__main__":
    main()
