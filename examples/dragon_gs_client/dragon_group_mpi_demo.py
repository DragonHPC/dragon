import os

import dragon

from dragon.globalservices import node
from dragon.globalservices import group
from dragon.globalservices import process
from dragon.globalservices import policy_eval
from dragon.infrastructure import process_desc
from dragon.infrastructure.facts import PMIBackend


def main() -> None:
    run_dir = os.getcwd()

    process_create_msg = process.get_create_message(
        exe=os.path.join(run_dir, "mpi_hello"), run_dir=run_dir, args=[], pmi=PMIBackend.CRAY, env=None
    )

    # num_processes = 4
    num_processes = node.query_total_cpus() // 2
    print(f"Starting {num_processes} processes", flush=True)

    # Establish the list and number of process ranks that should be started
    create_items = [(num_processes, process_create_msg.serialize())]

    # Ask Dragon to create the process group
    grp = group.create(items=create_items, policy=policy_eval.Policy(), soft=False)

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
