from dragon.native.process import Process, ProcessTemplate, Popen
from dragon.native.process_group import ProcessGroup
from dragon.infrastructure.connection import Connection
from dragon.infrastructure.policy import Policy
from dragon.infrastructure.facts import PMIBackend
from dragon.native.machine import System, Node

import os


def parse_results(stdout_conn: Connection) -> str:
    """Read stdout from the Dragon connection.

    :param stdout_conn: Dragon connection to stdout
    :type stdout_conn: Connection
    :return: string of output received on stdout
    :rtype: str
    """
    output = ""
    try:
        # this is brute force
        while True:
            output += stdout_conn.recv()
    except EOFError:
        pass
    finally:
        stdout_conn.close()

    return output.strip("/n")


def main_policy_example():
    # an abstraction of my allocated nodes
    my_alloc = System()
    num_procs_per_node = 8
    node_list = my_alloc.nodes
    nnodes = my_alloc.nnodes
    num_nodes_to_use = int(nnodes / 2)

    print(f"Using {num_nodes_to_use} of {nnodes}", flush=True)

    nodes = {}
    for node_id in node_list:
        node = Node(node_id)
        nodes[node.hostname] = node

    for hostname, node in nodes.items():
        print(f"{hostname} has {node.gpu_vendor} GPUs with visible devices: {node.gpus}", flush=True)

    # define mpi application and my args
    exe = os.path.join(os.getcwd(), "mpi_hello")
    args = []
    run_dir = os.getcwd()

    # restrict cpu affinity for every member of the group
    cpu_affinity = [0, 16, 32, 48, 64, 80, 96, 112]
    group_policy = Policy(cpu_affinity=cpu_affinity)

    # Define group and give it the group policy
    grp = ProcessGroup(restart=False, pmi=PMIBackend.CRAY, policy=group_policy)

    # Add processes to the group with local policies specifying what node to be placed on
    for node_num in range(num_nodes_to_use):
        node_name = list(nodes.keys())[node_num]
        local_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node_name)
        grp.add_process(
            nproc=num_procs_per_node,
            template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=Popen.PIPE, policy=local_policy),
        )

    grp.init()
    grp.start()

    # wait for workers to finish
    grp.join()

    # Since the workers are done, we'll query "inactive puids" to get a list of
    # them. The "puids" property would be empty since all the workers are done
    group_procs = [Process(None, ident=puid) for puid, _ in grp.inactive_puids]
    for proc in group_procs:
        # get info printed to stdout from each rank
        if proc.stdout_conn:
            stdout = parse_results(proc.stdout_conn)
            print(f"{proc.puid} returned output: {stdout}", flush=True)

    # Shutdown the process group
    grp.close()


if __name__ == "__main__":
    main_policy_example()
