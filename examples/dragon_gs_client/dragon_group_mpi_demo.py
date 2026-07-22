import os
import math

import dragon
from dragon.globalservices import node as gs_node
from dragon.globalservices import group
from dragon.globalservices import process

from dragon.infrastructure import process_desc
from dragon.infrastructure.facts import PMIBackend
from dragon.infrastructure.policy import Policy
from dragon.native.machine import System, Node


def main() -> None:
    run_dir = os.getcwd()

    # Create a policy that mimics typical job launcher block distribution with some ppn with extra ranks tacked onto the last
    num_processes = gs_node.query_total_cpus() // 2
    print(f"Starting {num_processes} processes", flush=True)

    my_alloc = System()
    node_list = my_alloc.nodes
    nnodes = my_alloc.nnodes
    num_procs_per_node = math.floor(num_processes / nnodes)

    # Distribute the processes across the nodes in the allocation,
    # with a policy that mimics typical job launcher block distribution with some ppn with extra ranks
    # tacked onto the last node if necessary
    create_items = []
    for node_id in node_list:
        node = Node(node_id)
        process_create_msg = process.get_create_message(
            exe=os.path.join(run_dir, "mpi_hello"),
            run_dir=run_dir,
            args=[],
            pmi=PMIBackend.CRAY,
            env=None,
            policy=Policy(Policy.Placement.HOST_NAME, host_name=node.hostname),
        )

        # Establish the list and number of process ranks that should be started
        if node_id == node_list[-1]:  # if this is the last node, add any remaining ranks to it
            num_ranks = num_procs_per_node + (num_processes - num_procs_per_node * nnodes)
        else:
            num_ranks = num_procs_per_node

        create_items.append((num_ranks, process_create_msg.serialize()))

    # Ask Dragon to create the process group. Note: launch with the PMIx backend *requires* the ProcessGroup API.
    # Directly using the Global Services API for a PMIx launch will fail.
    grp = group.create(items=create_items, policy=None, soft=False)

    group_puids = []
    for resources in grp.sets:
        group_puids.extend(
            [
                resource.desc.p_uid
                for resource in resources
                if resource.desc.state == process_desc.ProcessDescriptor.State.ACTIVE
            ]
        )
    if len(group_puids) > 0:
        process.multi_join(group_puids, join_all=True)


if __name__ == "__main__":
    main()
