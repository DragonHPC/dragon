"""This file contains Dragon multi-node acceptance tests for the
PMOD MPI job launcher. The test runs 4 MPI jobs simulataneously and
waits for their completion.

The test is run with `dragon test_mpi_hello_world.py -f -v`
"""

import unittest
import os
import sys
import json
from dragon.native.process import ProcessTemplate, Popen, Process
from dragon.native.process_group import ProcessGroup
from dragon.infrastructure.facts import PMIBackend, CONFIG_FILE_PATH
from dragon.native.machine import System, Node
from dragon.infrastructure.policy import Policy

from dragon.dtypes import DragonError
import multiprocessing as mp


def iterative_execution(exe, pmi, nnodes, ppn, niter=1, uneven=False, env=None):
    args = []
    cwd = os.getcwd()

    pg1 = ProcessGroup(restart=False, pmi=pmi)

    if uneven:
        pg1.add_process(
            nproc=nnodes * ppn + 1, template=ProcessTemplate(target=exe, args=args, cwd=cwd, env=env, stdout=Popen.PIPE)
        )
    else:
        pg1.add_process(
            nproc=nnodes * ppn, template=ProcessTemplate(target=exe, args=args, cwd=cwd, env=env, stdout=Popen.PIPE)
        )

    pg1.init()

    stdouts = []
    exits = []
    for idx in range(niter):
        pg1.start()
        puids = pg1.puids
        group_procs = [Process(None, ident=puid) for puid in puids]
        for proc in group_procs:
            if proc.stdout_conn:
                stdouts.append(proc.stdout_conn.recv())
        pg1.join()
        exits += pg1.exit_status
    pg1.close()

    return exits, stdouts


def concurrent_execution(exe, pmi, nnodes, ppn, env=None):
    stdouts = []
    exits = []

    args = []
    cwd = os.getcwd()

    pg1 = ProcessGroup(restart=False, pmi=pmi)
    pg2 = ProcessGroup(restart=False, pmi=pmi)

    pg1.add_process(
        nproc=nnodes * ppn + 1, template=ProcessTemplate(target=exe, args=args, cwd=cwd, env=env, stdout=Popen.PIPE)
    )

    pg2.add_process(
        nproc=nnodes * ppn, template=ProcessTemplate(target=exe, args=args, cwd=cwd, env=env, stdout=Popen.PIPE)
    )

    pg1.init()
    pg2.init()

    pg1.start()
    pg2.start()

    group_procs1 = [Process(None, ident=puid) for puid in pg1.puids]
    group_procs2 = [Process(None, ident=puid) for puid in pg2.puids]

    for group in [group_procs1, group_procs2]:
        lstdout = []
        for proc in group:
            if proc.stdout_conn:
                lstdout.append(proc.stdout_conn.recv())
        stdouts.append(lstdout)

    pg1.join()
    pg2.join()

    exits = pg1.exit_status + pg2.exit_status

    pg1.close()
    pg2.close()

    return exits, stdouts


def policy_execution(exe, pmi, ppn, hostnames, env=None):
    policies = [Policy(placement=Policy.Placement.HOST_NAME, host_name=hostname) for hostname in hostnames]

    args = []
    cwd = os.getcwd()

    pg1 = ProcessGroup(restart=False, pmi=pmi)
    for policy in policies:
        pg1.add_process(
            nproc=ppn,
            template=ProcessTemplate(target=exe, args=args, cwd=cwd, env=env, policy=policy, stdout=Popen.PIPE),
        )

    pg1.init()

    stdouts = []
    exits = []
    pg1.start()
    puids = pg1.puids

    group_procs = [Process(None, ident=puid) for puid in puids]
    for proc in group_procs:
        if proc.stdout_conn:
            # get info printed to stdout from rank 0
            stdouts.append(proc.stdout_conn.recv())
    pg1.join()

    exits = pg1.exit_status
    pg1.close()

    return exits, stdouts


class TestPMIBackends(unittest.TestCase):
    def setUp(self):
        my_alloc = System()
        node_list = my_alloc.nodes
        self.nnodes = len(node_list)

        self.ppn = 4

        self.cray_mpich_exe = "./mpi_hello_cray"
        if not os.path.isfile(self.cray_mpich_exe):
            self.cray_mpich_exe = None

        self.anl_mpich_exe = "./mpi_hello_mpich"
        if not os.path.isfile(self.anl_mpich_exe):
            self.anl_mpich_exe = None

        self.open_mpi_exe = "./mpi_hello_ompi"
        if not os.path.isfile(self.open_mpi_exe):
            self.open_mpi_exe = None

    def _check_exit_codes(self, exits):
        # Confirm all exit codes are zero
        puids, ecodes = zip(*exits)

        self.assertTrue(all(ec == DragonError.SUCCESS for ec in ecodes))

    def _get_ranks_tuples(self, stdouts):
        """Parse Hello World stdouts to get list of (rank, world size, hostname)"""

        # Since this was a round-robin distribution by default, confirm we did a round robin
        # The easiest way to do this is to split stdout by knowing stdout is
        # 'Hello, world, I am <rank> of <size> on host <hostname>'
        ranks = [None] * len(stdouts)
        for s in stdouts:
            split_s = s.split()
            rank = int(split_s[4])
            world_size = int(split_s[6])
            hostname = split_s[9]
            ranks[rank] = (rank, world_size, hostname)
        return ranks

    def _check_round_robin_results(self, stdouts):
        """Check ranks were placed correctly with a round-robin distribution"""

        ranks = self._get_ranks_tuples(stdouts)
        # Check to confirm the ranks were distributed according to the default round-robin distribution
        for lid in range(self.nnodes):
            hosts = [ranks[x + lid][2] for x in range(0, len(ranks), self.nnodes) if x + lid < len(ranks)]
            self.assertTrue(len(set(hosts)) == 1)

    def _check_policy_results(self, stdouts, hostnames, ppn=None):
        """Check that the correct hostnames were used in placing the job"""

        if ppn is None:
            ppn = self.ppn

        ranks = self._get_ranks_tuples(stdouts)

        # Make sure we used a block distribution which is how our policy test runner does things
        for nid in range(self.nnodes - 1):
            base = nid * ppn
            hosts = [ranks[x][2] for x in range(base, base + ppn, 1)]
            self.assertTrue(len(set(hosts)) == 1)

        # Make sure we used all the names in our hostlist
        hosts = set([rank[2] for rank in ranks])

        for host in hosts:
            self.assertTrue(host in hostnames)
            hostnames.remove(host)

        self.assertTrue(len(hostnames) == 0)

    def _get_mpi_lib_path(self, key):
        config_file_path = CONFIG_FILE_PATH

        if config_file_path.exists():
            with open(config_file_path) as config_file:
                config_dict = json.load(config_file)

        return config_dict[key]

    def test_cray_mpich_even_dist(self):
        """Run a simple bring-up and exit of a cray mpich app with nnodes % ppn == 0"""

        if self.cray_mpich_exe is not None:
            the_env = dict(os.environ)
            try:
                cray_mpich_dir = self._get_mpi_lib_path("cray_mpich_runtime_lib")
            except KeyError:
                print("SKIPPING because Cray MPICH library was not found", flush=True, file=sys.stderr)
                return

            the_env["LD_LIBRARY_PATH"] = cray_mpich_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = iterative_execution(
                self.cray_mpich_exe, PMIBackend.CRAY, self.nnodes, self.ppn, uneven=True, env=the_env
            )
        else:
            print("SKIPPING because Cray MPICH exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        self._check_round_robin_results(stdouts)

    def test_cray_mpich_uneven_dist(self):
        """Run a simple bring-up and exit of a cray mpich app with nnodes % ppn != 0"""

        if self.cray_mpich_exe is not None:
            the_env = dict(os.environ)
            try:
                cray_mpich_dir = self._get_mpi_lib_path("cray_mpich_runtime_lib")
            except KeyError:
                print("SKIPPING because Cray MPICH library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = cray_mpich_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = iterative_execution(
                self.cray_mpich_exe, PMIBackend.CRAY, self.nnodes, self.ppn, uneven=True, env=the_env
            )
        else:
            print("SKIPPING because Cray MPICH exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        self._check_round_robin_results(stdouts)

    def test_anl_mpich_even_dist(self):
        """Run a simple bring-up and exit of an ANL MPICH app with nnodes % ppn == 0"""

        if self.anl_mpich_exe is not None:
            the_env = dict(os.environ)
            try:
                anl_mpich_dir = self._get_mpi_lib_path("anl_mpich_runtime_lib")
            except KeyError:
                print("SKIPPING because ANL MPICH library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = anl_mpich_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = iterative_execution(
                self.anl_mpich_exe, PMIBackend.PMIX, self.nnodes, self.ppn, uneven=False, env=the_env
            )
        else:
            print("SKIPPING because ANL MPICH exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        self._check_round_robin_results(stdouts)

    def test_anl_mpich_uneven_dist(self):
        """Run a simple bring-up and exit of a ANL MPICH app with nnodes % ppn != 0"""

        if self.anl_mpich_exe is not None:
            the_env = dict(os.environ)
            try:
                anl_mpich_dir = self._get_mpi_lib_path("anl_mpich_runtime_lib")
            except KeyError:
                print("SKIPPING because ANL mpich library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = anl_mpich_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = iterative_execution(
                self.anl_mpich_exe, PMIBackend.PMIX, self.nnodes, self.ppn, uneven=True, env=the_env
            )
        else:
            print("SKIPPING because ANL MPICH exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        self._check_round_robin_results(stdouts)

    def test_open_mpi_even_dist(self):
        """Run a simple bring-up and exit of an Open MPI app with nnodes % ppn == 0"""

        if self.open_mpi_exe is not None:
            the_env = dict(os.environ)
            try:
                open_mpi_dir = self._get_mpi_lib_path("open_mpi_runtime_lib")
            except KeyError:
                print("SKIPPING because Open MPI library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = open_mpi_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = iterative_execution(
                self.open_mpi_exe, PMIBackend.PMIX, self.nnodes, self.ppn, uneven=False, env=the_env
            )
        else:
            print("SKIPPING because Open MPI exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        self._check_round_robin_results(stdouts)

    def test_open_mpi_uneven_dist(self):
        """Run a simple bring-up and exit of an Open MPI app with nnodes % ppn != 0"""

        if self.open_mpi_exe is not None:
            the_env = dict(os.environ)
            try:
                open_mpi_dir = self._get_mpi_lib_path("open_mpi_runtime_lib")
            except KeyError:
                print("SKIPPING because Open MPI library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = open_mpi_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = iterative_execution(
                self.open_mpi_exe, PMIBackend.PMIX, self.nnodes, self.ppn, uneven=True, env=the_env
            )
        else:
            print("SKIPPING because Open MPI exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        self._check_round_robin_results(stdouts)

    def test_pmix_no_local_mgr(self):
        """Make sure PMIx backend can operate when a ddict manager isn't co-located with server"""

        my_alloc = System()
        node_list = my_alloc.nodes
        self.nnodes = 2
        ppn = 1

        if self.nnodes < 2:
            print("Unable to run the policy placment test")
            return
        hostnames = [Node(node_list[nid]).hostname for nid in range(self.nnodes - 1)]

        hostnames = [Node(node_list[nid]).hostname for nid in range(self.nnodes - 1)]

        if self.open_mpi_exe is not None:
            the_env = dict(os.environ)
            try:
                open_mpi_dir = self._get_mpi_lib_path("open_mpi_runtime_lib")
            except KeyError:
                print("SKIPPING because Open MPI library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = open_mpi_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = policy_execution(self.open_mpi_exe, PMIBackend.PMIX, ppn, hostnames, env=the_env)
        else:
            print("SKIPPING because ANL MPICH exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        self._check_policy_results(stdouts, hostnames, ppn=ppn)

    def test_pmod_pg_replay(self):
        """Confirm PMOD backend allows replaying of Process Group"""
        if self.cray_mpich_exe is not None:
            the_env = dict(os.environ)
            try:
                cray_mpich_dir = self._get_mpi_lib_path("cray_mpich_runtime_lib")
            except KeyError:
                print("SKIPPING because Cray MPICH library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = cray_mpich_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, _ = iterative_execution(
                self.cray_mpich_exe, PMIBackend.CRAY, self.nnodes, self.ppn, niter=4, uneven=True, env=the_env
            )
        else:
            print("SKIPPING because Cray MPICH exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)

    def test_pmix_pg_replay(self):
        """Confirm PMIx backend allows replaying of Process Group"""

        if self.anl_mpich_exe is not None:
            the_env = dict(os.environ)
            try:
                anl_mpich_dir = self._get_mpi_lib_path("anl_mpich_runtime_lib")
            except KeyError:
                print("SKIPPING because ANL MPICH library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = anl_mpich_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = iterative_execution(
                self.anl_mpich_exe, PMIBackend.PMIX, self.nnodes, self.ppn, niter=4, uneven=True, env=the_env
            )
        else:
            print("SKIPPING because ANL MPICH exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)

    def test_pmod_policy(self):
        """Confirm PMOD backend correctly places jobs via policy"""

        my_alloc = System()
        node_list = my_alloc.nodes

        if self.nnodes < 2:
            print("Unable to run the policy placment test")
            return
        hostnames = [Node(node_list[nid]).hostname for nid in range(self.nnodes - 1)]

        if self.cray_mpich_exe is not None:
            the_env = dict(os.environ)
            try:
                cray_mpich_dir = self._get_mpi_lib_path("cray_mpich_runtime_lib")
            except KeyError:
                print("SKIPPING because Cray MPICH library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = cray_mpich_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = policy_execution(self.cray_mpich_exe, PMIBackend.CRAY, self.ppn, hostnames, env=the_env)
        else:
            print("SKIPPING because Cray MPICH exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        self._check_policy_results(stdouts, hostnames)

    def test_pmix_policy(self):
        """Confirm PMIx backend correctly places jobs via policy"""

        my_alloc = System()
        node_list = my_alloc.nodes

        if self.nnodes < 2:
            print("Unable to run the policy placment test")
            return
        hostnames = [Node(node_list[nid]).hostname for nid in range(self.nnodes - 1)]

        if self.anl_mpich_exe is not None:
            the_env = dict(os.environ)
            try:
                anl_mpich_dir = self._get_mpi_lib_path("anl_mpich_runtime_lib")
            except KeyError:
                print("SKIPPING because ANL MPICH library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = anl_mpich_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = policy_execution(self.anl_mpich_exe, PMIBackend.PMIX, self.ppn, hostnames, env=the_env)
        else:
            print("SKIPPING because ANL MPICH exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        self._check_policy_results(stdouts, hostnames)

    def test_pmod_concurrent_process_groups(self):
        """Confirm PMOD backend can handle excecution of concurrent and colocated process groups"""

        if self.cray_mpich_exe is not None:
            the_env = dict(os.environ)
            try:
                cray_mpich_dir = self._get_mpi_lib_path("cray_mpich_runtime_lib")
            except KeyError:
                print("SKIPPING because Cray MPICH library was not found", flush=True, file=sys.stderr)
                return

            the_env["LD_LIBRARY_PATH"] = cray_mpich_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = concurrent_execution(
                self.cray_mpich_exe, PMIBackend.CRAY, self.nnodes, self.ppn, env=the_env
            )
        else:
            print("SKIPPING because Cray MPICH exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        for stdout in stdouts:
            self._check_round_robin_results(stdout)

    def test_pmix_conccurent_process_groups(self):
        """Confirm PMIx backend can handle excecution of concurrent and colocated process groups"""

        if self.open_mpi_exe is not None:
            the_env = dict(os.environ)
            try:
                open_mpi_dir = self._get_mpi_lib_path("open_mpi_runtime_lib")
            except KeyError:
                print("SKIPPING because Open MPI library was not found", flush=True, file=sys.stderr)
                return
            the_env["LD_LIBRARY_PATH"] = open_mpi_dir + ":" + the_env["LD_LIBRARY_PATH"]
            exits, stdouts = concurrent_execution(
                self.open_mpi_exe, PMIBackend.PMIX, self.nnodes, self.ppn, env=the_env
            )
        else:
            print("SKIPPING because Open MPI exe not found", flush=True, file=sys.stderr)
            return

        self._check_exit_codes(exits)
        for stdout in stdouts:
            self._check_round_robin_results(stdout)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
