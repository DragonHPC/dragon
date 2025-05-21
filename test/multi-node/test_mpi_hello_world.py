"""This file contains Dragon multi-node acceptance tests for the
PMOD MPI job launcher. The test runs 4 MPI jobs simulataneously and
waits for their completion.

The test is run with `dragon test_mpi_hello_world.py -f -v`
"""

import unittest
import dragon.channels as dch
import dragon.managed_memory as dmm
import dragon.pmod as djm
from dragon.utils import B64
from dragon.dtypes import DragonError
import multiprocessing as mp
import socket
import subprocess
import os

SUCCESS = DragonError.SUCCESS
FAILURE = DragonError.FAILURE

# tracks base PIDs currently in use by active MPI jobs
pid_dict = {}


class MPIJobParams:
    def __init__(self, ppn, nid, nnodes, job_id):
        self.ppn = ppn
        self.nid = nid
        self.nnodes = nnodes
        self.id = job_id


def allocate_mpi_pid_base(pid_bases, nranks):

    # reserve 0 for the single HSTA agent. this will not be needed
    # when we begin launching HSTA with PMOD
    min_pid = 1

    # handle the case where pid_bases is empty

    if not pid_bases:
        pid_bases.append((min_pid, nranks))
        return min_pid

    # loop over (base, size) pairs looking for a free segment of PIDS
    # with size at least nranks

    # (1) for a given (base, size) pair, check the segment starting at
    # the PID just past the previous segment

    prev_end = min_pid

    for base, size in pid_bases:
        if base - prev_end >= nranks:
            pid_bases.append((prev_end, nranks))
            pid_bases.sort()
            return prev_end
        else:
            prev_end = base + size

    # (2) check the final segment from the end of the last (base, size)
    # pair extending to the maximum PID

    max_pid = 510

    if (max_pid + 1) - prev_end >= nranks:
        pid_bases.append((prev_end, nranks))
        pid_bases.sort()
        return prev_end

    return -1


def free_mpi_pid_base(pid_bases, base, size):

    pid_bases.remove((base, size))


def node_shepherd(job_params, q_to_spawner):

    # get list of hostnames for all nodes in job

    hostname = socket.gethostname()
    q_to_spawner.put(hostname)
    hostname_list = q_to_spawner.get()
    base_pid = q_to_spawner.get()

    if job_params.nid == 0:
        print(f"Running MPI job with id {job_params.id} on the following hosts: {hostname_list}", flush=True)

    # create memory pool

    pool_size = 2**30
    # each job can have up to 2**20 nodes
    pool_uid = (2**20 * job_params.id) + job_params.nid
    pool_name = "test_pmod_mpi_hello_world-" + str(job_params.id)
    mpool = dmm.MemoryPool(pool_size, pool_name, pool_uid)

    # we are currently assuming an equal number of ranks per node
    # (but this isn't necessary, just convenient for this test)

    nranks = job_params.ppn * job_params.nnodes

    nidlist = []

    for nid in range(job_params.nnodes):
        for lrank in range(job_params.ppn):
            nidlist.append(nid)

    pmod = djm.PMOD(job_params.ppn, job_params.nid, job_params.nnodes, nranks, nidlist, hostname_list, job_params.id)

    child_handles = []

    os.environ["LD_PRELOAD"] = "libdragon.so"
    os.environ["MPICH_OFI_CXI_PID_BASE"] = str(base_pid)
    os.environ["DL_PLUGIN_RESILIENCY"] = "1"
    os.environ["_DRAGON_PALS_ENABLED"] = "1"

    for lrank in range(job_params.ppn):
        # create and serialize a channel that we can use to talk to this child process

        channel_uid = 2**20 * job_params.id + lrank + 42
        child_ch = dch.Channel(mpool, channel_uid)
        os.environ["DRAGON_PMOD_CHILD_CHANNEL"] = str(B64(child_ch.serialize()))

        # no way to dynamically search for a free port, since the first free port
        # may differ from node to node, depending on how MPI jobs are laid out. so
        # we simply use a unique offset from a arbitrary starting port

        port = 7777 + job_params.id
        os.environ["PMI_CONTROL_PORT"] = str(port)

        handle = subprocess.Popen(
            "./mpi_hello",
        )

        # now send the basic PMI data to the child process

        pmod.send_mpi_data(lrank, child_ch)

        child_handles.append(handle)

    passed = True

    for handle in child_handles:
        handle.wait()
        if handle.returncode != SUCCESS:
            passed = False

    q_to_spawner.put(passed)

    if job_params.nid == 0:
        print(f"child processes for job {job_params.id} are done!")

    return passed


def start_mpi_job(job_id, ppn, nnodes):

    process_handles = []
    mpi_pid_bases = []
    queue_list = []
    hostname_list = []

    # start a shepherd on each node in the job

    for nid in range(nnodes):
        job_params = MPIJobParams(ppn, nid, nnodes, job_id)

        q = mp.Queue()
        queue_list.append(q)

        p = mp.Process(
            target=node_shepherd,
            args=(
                job_params,
                q,
            ),
        )
        p.start()
        process_handles.append(p)

        hostname = q.get()
        hostname_list.append(hostname)

    # broadcast the hostname list back to each node

    for q in queue_list:
        q.put(hostname_list)

    # finally allocate and send a pid base to each nid

    hostname_and_q_list = zip(hostname_list, queue_list)

    for hostname, q in hostname_and_q_list:
        if hostname in pid_dict:
            allocated_bases = pid_dict[hostname]
        else:
            allocated_bases = []
            pid_dict[hostname] = allocated_bases

        pid_base = allocate_mpi_pid_base(allocated_bases, ppn)
        assert pid_base != -1, "unable to allocate PIDs for MPI job"

        mpi_pid_bases.append((hostname, pid_base, ppn))

        q.put(pid_base)

    return process_handles, queue_list, mpi_pid_bases


def wait_for_job(process_handles, queue_list, mpi_pid_bases):

    passed = True

    proc_and_q_list = zip(process_handles, queue_list)

    for p, q in proc_and_q_list:
        node_passed = q.get()
        passed = passed and node_passed
        p.join()

    for hostname, base, size in mpi_pid_bases:
        allocated_bases = pid_dict[hostname]
        free_mpi_pid_base(allocated_bases, base, size)

        if not allocated_bases:
            del pid_dict[hostname]

    return passed


class TestPMODMultiNode(unittest.TestCase):
    def test_four_jobs(self):
        job_id = 0
        ppn = 7
        nnodes = 3

        process_handles_0, q_list_0, mpi_pid_bases_0 = start_mpi_job(job_id, ppn, nnodes)

        job_id = 1
        ppn = 4
        nnodes = 2

        process_handles_1, q_list_1, mpi_pid_bases_1 = start_mpi_job(job_id, ppn, nnodes)

        job_id = 2
        ppn = 2
        nnodes = 3

        process_handles_2, q_list_2, mpi_pid_bases_2 = start_mpi_job(job_id, ppn, nnodes)

        job_id = 3
        ppn = 1
        nnodes = 4

        process_handles_3, q_list_3, mpi_pid_bases_3 = start_mpi_job(job_id, ppn, nnodes)

        passed = True

        job_passed = wait_for_job(process_handles_0, q_list_0, mpi_pid_bases_0)
        passed = passed and job_passed

        job_passed = wait_for_job(process_handles_1, q_list_1, mpi_pid_bases_1)
        passed = passed and job_passed

        job_passed = wait_for_job(process_handles_2, q_list_2, mpi_pid_bases_2)
        passed = passed and job_passed

        job_passed = wait_for_job(process_handles_3, q_list_3, mpi_pid_bases_3)
        passed = passed and job_passed

        os.system("./cleanup.sh")

        if not job_passed:
            exit(1)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
