import argparse
import errno
import getpass
import os
import re
import socket
import subprocess
import sys

from functools import partial
from pathlib import Path
from psutil import Process, process_iter, TimeoutExpired, ZombieProcess, NoSuchProcess, AccessDenied

from dragon.infrastructure.config import hugepages_cleanup
from dragon.infrastructure.facts import (
    ATTACH_DIR_ENV,
    ATTACH_INFO_FILENAME,
    DEFAULT_DICT_POOL_NAME_BASE,
    DEFAULT_POOL_SUFFIX,
    INFRASTRUCTURE_POOL_SUFFIX,
    LOGGING_POOL_SUFFIX,
    default_attach_runtime_dir,
)

import logging

logger = logging.getLogger(__name__)


def _ancestor_pids():
    """Return the pids of this process and every one of its ancestors.

    dragon-cleanup launches itself on each node over its own ssh tree, so the
    drbe process hosting this cleanup is an ancestor and must never be killed.
    """
    pids = {os.getpid()}
    try:
        pids.update(parent.pid for parent in Process(os.getpid()).parents())
    except (NoSuchProcess, AccessDenied):
        pass
    return frozenset(pids)


class DragonCleanup:

    DRAGON_FE_PROCESS_REGEX = r"/dragon$"
    DRAGON_BE_PROCESS_REGEX = r"/dragon-backend$"
    DRAGON_LS_PROCESS_REGEX = r"/dragon-localservices$"
    DRAGON_OVERLAY_PROCESS_REGEX = r"dragon.cli"
    # Match a whole path component so a user's "mydrbe" is never killed. Both
    # are normally absolute, but tolerate a bare name.
    DRAGON_DRBE_PROCESS_REGEX = r"(^|/)drbe$"
    DRAGON_HSTA_PROCESS_REGEX = r"(^|/)dragon-hsta$"

    NVIDIA_CUDA_MPS_REGEX = r"/nvidia-cuda-mps$"

    DATABASE_FILES = "*.db"
    GUNICORN_LOG_FILES = "*_gunicorn.log"
    DDICT_ORCHESTRATOR_FILES = "*ddict_orc*"

    DDICT_DEV_SHM_POOLS = DEFAULT_DICT_POOL_NAME_BASE + "*"

    TMP_DIR = "/tmp"
    DEV_SHM_DIR = "/dev/shm"

    # Darwin has no /dev/shm and POSIX shared memory objects cannot be
    # enumerated, so leaked pools are reclaimed by rebuilding the names Dragon
    # could have used and calling shm_unlink() on each. See cleanup_darwin_shm.
    DARWIN_FILE_PREFIX = "D"  # DRAGON_MEMORY_DEFAULT_FILE_PREFIX
    DARWIN_NAME_SUFFIXES = ("M", "D")  # manifest, data
    DARWIN_PID_RANGE = 1000  # names embed only the last 3 digits of the pid
    DARWIN_RETRY_RANGE = 10  # MemoryPool.create_with_retry default attempts

    MY_PID = os.getpid()
    MY_PPID = os.getppid()
    MY_ANCESTOR_PIDS = _ancestor_pids()
    MY_USERNAME = getpass.getuser()
    MY_HOSTNAME = socket.gethostname()

    def __init__(self, resilient=False, dry_run=False, timeout=2, keep_ddict_files=False):

        self.resilient = resilient
        self.dry_run = dry_run
        self.timeout = timeout
        self.keep_ddict_files = keep_ddict_files

        self.my_rank = int(os.environ.get("DRAGON_RUN_RANK", "0"))
        self.num_ranks = int(os.environ.get("DRAGON_RUN_NUM_RANKS", 0))

        print(
            f"dragon-cleanup my-pid={DragonCleanup.MY_PID} my_rank={self.my_rank} num_ranks={self.num_ranks}",
            flush=True,
        )

        self.nvidia_gpus = Path("/dev/nvidiactl").exists()

        # find all processes owned by the current user
        self.user_processes = list(
            filter(
                DragonCleanup._user_process_filter,
                process_iter(),
            )
        )

    @property
    def is_single_node(self):
        """Helper utility to identify if we're running in single-node mode"""
        return all([self.my_rank == 0, self.num_ranks == 1])

    @property
    def is_multi_node(self):
        """Helper utility to identify if we're running in multi-node mode"""
        return not self.is_single_node

    @property
    def is_fe(self):
        """Helper utility to identify if we're running on the dragon FE node"""
        return self.my_rank == 0

    @property
    def is_be(self):
        """Helper utility to identify if we're running on a dragon BE node"""
        return not self.is_fe or self.is_single_node

    @staticmethod
    def _user_process_filter(process: Process):
        with process.oneshot():

            try:
                if not process.cmdline():
                    return False

                cmdline = process.cmdline()[0]
                return all(
                    [
                        # we don't want to kill ourselves, nor the drbe that is
                        # hosting this cleanup on a multi-node run
                        process.pid not in DragonCleanup.MY_ANCESTOR_PIDS,
                        # only look for processes started by this user
                        process.username() == DragonCleanup.MY_USERNAME,
                        # exclude these commonly started processes
                        "grep" not in cmdline,
                        "bash" not in cmdline,
                        "mpiexec" not in cmdline,
                    ]
                )
            except (NoSuchProcess, ZombieProcess, AccessDenied, PermissionError):
                pass

    @staticmethod
    def _find_dragon_launcher_process(process: Process, expression):
        """
        This is a utility function used to search for our dragon launcher process.
        Since the filter keyword only expects a function with a single parameter,
        the expression argument is set using python partial.
        """
        try:
            # argv[0] must be included: dragon-hsta is exec'd as a bare binary
            # with no arguments, so it only ever appears there.
            return any(expression.search(arg) for arg in process.cmdline()[:3])
        except (IndexError, NoSuchProcess):
            return False

    def _kill_process_tree(self, process_list):
        for process in process_list:
            try:
                children = process.children()

                if children:
                    self._kill_process_tree(children)

            except NoSuchProcess:
                pass

            if self.dry_run:
                print(
                    f"DRY_RUN: Terminating {process.pid} on {DragonCleanup.MY_HOSTNAME}",
                    flush=True,
                )
                continue

            try:
                print(f"Terminating {process.pid} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                process.terminate()
                process.wait(self.timeout)
            except NoSuchProcess:
                pass
            except TimeoutExpired:
                print(f"Killing {process.pid} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                process.kill()

    def kill_dragon_process_tree(self):
        """
        This function searches the users' processes for either the 'dragon' or 'dragon-backend' process,
        depending upon if it is running on the FrontEnd or BackEnd nodes. If it finds a matching process,
        it will call the _kill_process_tree function, which will recursively kill the launcher process and
        any of its child-processes.
        """

        print("Looking for and killing Dragon launcher processes and their child processes.", flush=True)

        dragon_regex = DragonCleanup.DRAGON_FE_PROCESS_REGEX if self.is_fe else DragonCleanup.DRAGON_BE_PROCESS_REGEX

        filter_proc = partial(
            DragonCleanup._find_dragon_launcher_process,
            expression=re.compile(dragon_regex),
        )
        filter_proc_ls = partial(
            DragonCleanup._find_dragon_launcher_process,
            expression=re.compile(DragonCleanup.DRAGON_LS_PROCESS_REGEX),
        )
        filter_proc_overlay = partial(
            DragonCleanup._find_dragon_launcher_process,
            expression=re.compile(DragonCleanup.DRAGON_OVERLAY_PROCESS_REGEX),
        )
        filter_proc_hsta = partial(
            DragonCleanup._find_dragon_launcher_process,
            expression=re.compile(DragonCleanup.DRAGON_HSTA_PROCESS_REGEX),
        )
        filter_proc_drbe = partial(
            DragonCleanup._find_dragon_launcher_process,
            expression=re.compile(DragonCleanup.DRAGON_DRBE_PROCESS_REGEX),
        )

        dragon_processes = list(filter(filter_proc, self.user_processes))

        dragon_ls_processes = list(filter(filter_proc_ls, self.user_processes))
        dragon_processes.extend(dragon_ls_processes)

        dragon_overlay_processes = list(filter(filter_proc_overlay, self.user_processes))
        dragon_processes.extend(dragon_overlay_processes)

        # HSTA outlives its dragon.cli wrapper when a run dies abnormally, and
        # drbe is started over ssh so it is never reached by a parent's tree.
        dragon_hsta_processes = list(filter(filter_proc_hsta, self.user_processes))
        dragon_processes.extend(dragon_hsta_processes)

        # Only a backend rank may sweep drbe. The multi-node coordinator reaches
        # every node through its own drbe tree, and on a host that is both
        # frontend and compute node that tree is a sibling of this process, not
        # an ancestor, so MY_ANCESTOR_PIDS cannot protect it. The backend rank
        # running on that same host clears any stale drbe there.
        if self.is_be:
            dragon_drbe_processes = list(filter(filter_proc_drbe, self.user_processes))
            dragon_processes.extend(dragon_drbe_processes)

        for process in dragon_processes:
            try:
                print(
                    f"Found {process.exe()} with pid {process.pid}. Attempting to cleanup it's process tree.",
                    flush=True,
                )
                self._kill_process_tree([process])
            except NoSuchProcess:
                pass
        else:
            print("There are no Dragon PIDS to kill", flush=True)

    @staticmethod
    def _find_nvidia_cuda_mps_proc(process: Process, expression):
        try:
            if expression.search(process.cmdline()[0]):
                return True
            return False
        except (IndexError, NoSuchProcess):
            return False

    def restart_nvidia_cuda_mps(self):
        """When on a backend node, restart the nvidia-cuda-mps-control process if there are nvidia gpus."""
        if not self.is_be or not self.nvidia_gpus:
            return

        print(f"Restarting nvidia-cuda-mps processes.", flush=True)

        filter_proc = partial(
            DragonCleanup._find_nvidia_cuda_mps_proc,
            expression=re.compile(DragonCleanup.NVIDIA_CUDA_MPS_REGEX),
        )

        nvidia_cuda_mps_procs = list(filter(filter_proc, self.user_processes))
        for nvidia_cuda_mps_proc in nvidia_cuda_mps_procs:
            if self.dry_run:
                print(
                    f"DRY_RUN: Terminating {nvidia_cuda_mps_proc.pid} on {DragonCleanup.MY_HOSTNAME}",
                    flush=True,
                )
                return

            try:
                print(f"Terminating {nvidia_cuda_mps_proc.pid} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                nvidia_cuda_mps_proc.terminate()
                nvidia_cuda_mps_proc.wait(self.timeout)
            except TimeoutExpired:
                print(f"Killing {nvidia_cuda_mps_proc.pid} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                nvidia_cuda_mps_proc.kill()

        try:
            print(f"Restarting nvidia-cuda-mps-control daemon on {DragonCleanup.MY_HOSTNAME}", flush=True)
            subprocess.run(
                ["nvidia-cuda-mps-control", "-d"],
                check=True,
                capture_output=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError as e:
            print(f"Error calling nvidia-cuda-mps-control command on {DragonCleanup.MY_HOSTNAME}: {e}", flush=True)
            print(e.stderr, flush=True)

    def cleanup_dev_shm(self):
        dev_shm_dir = Path(DragonCleanup.DEV_SHM_DIR)
        if not all([dev_shm_dir.exists(), dev_shm_dir.is_dir()]):
            # No /dev/shm to enumerate (Darwin); sweep pool names instead.
            self.cleanup_darwin_shm()
            return

        if self.is_be and self.resilient:
            print(
                f"Cleaning up pools in {DragonCleanup.DEV_SHM_DIR}, but avoiding DDict pools named {DragonCleanup.DDICT_DEV_SHM_POOLS}.",
                flush=True,
            )
        else:
            print(f"Cleaning up pools in {DragonCleanup.DEV_SHM_DIR}.", flush=True)

        for dev_shm_file in [item for item in dev_shm_dir.iterdir() if item.is_file()]:
            try:
                if dev_shm_file.owner() == DragonCleanup.MY_USERNAME:
                    if self.is_be and self.resilient and dev_shm_file.match(DragonCleanup.DDICT_DEV_SHM_POOLS):
                        continue

                    if self.dry_run:
                        print(f"DRY_RUN: Removing {dev_shm_file} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                        continue

                    print(f"Removing {dev_shm_file} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                    dev_shm_file.unlink(missing_ok=True)
            except KeyError:
                # A KeyError can be thrown when the file is owned by
                # a user that is not listed in the /etc/passwd file.
                pass

    def cleanup_darwin_shm(self):
        """Reclaim POSIX shared memory on platforms that have no /dev/shm.

        Darwin cannot enumerate shm objects, so instead every name a run on
        this host could have produced is rebuilt and shm_unlink()ed. Pool names
        embed only the last three digits of the creating pid plus the
        create_with_retry counter, so the space is small enough to sweep.
        DDict manager pools are not covered: their names embed a full puid and
        are not reconstructable from outside the run.
        """
        import ctypes
        import ctypes.util

        libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
        libc.shm_unlink.argtypes = [ctypes.c_char_p]
        libc.shm_unlink.restype = ctypes.c_int
        libc.shm_open.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_uint16]
        libc.shm_open.restype = ctypes.c_int

        uid3 = str(os.getuid())[-3:]
        user3 = DragonCleanup.MY_USERNAME[-3:]
        try:
            from dragon.utils import host_id

            host3 = str(host_id())[-3:]
        except Exception:
            host3 = None

        print(f"Cleaning up POSIX shared memory pools on {DragonCleanup.MY_HOSTNAME}.", flush=True)

        removed = 0
        for pid in range(DragonCleanup.DARWIN_PID_RANGE):
            pid3 = f"{pid:03d}"
            for i in range(DragonCleanup.DARWIN_RETRY_RANGE):
                bases = [
                    f"D{uid3}{pid3}{INFRASTRUCTURE_POOL_SUFFIX}{i}",
                    f"D{uid3}{pid3}{DEFAULT_POOL_SUFFIX}{i}",
                    f"D{user3}{pid3}{LOGGING_POOL_SUFFIX}_{i}",
                ]
                if host3 is not None:
                    bases.append(f"D{uid3}{pid3}{host3}{DEFAULT_POOL_SUFFIX}{i}")

                for base in bases:
                    for suffix in DragonCleanup.DARWIN_NAME_SUFFIXES:
                        name = f"/{DragonCleanup.DARWIN_FILE_PREFIX}{base}{suffix}"
                        if self.dry_run:
                            # Darwin returns EACCES, not a usable fd, when an shm
                            # object exists; only ENOENT means it is absent.
                            ctypes.set_errno(0)
                            fd = libc.shm_open(name.encode(), os.O_RDONLY, 0)
                            if fd >= 0:
                                os.close(fd)
                            elif ctypes.get_errno() == errno.ENOENT:
                                continue
                            print(
                                f"DRY_RUN: Removing {name} on {DragonCleanup.MY_HOSTNAME}",
                                flush=True,
                            )
                            removed += 1
                            continue

                        if libc.shm_unlink(name.encode()) == 0:
                            print(f"Removing {name} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                            removed += 1

        if removed == 0:
            print(f"No leaked pools found on {DragonCleanup.MY_HOSTNAME}.", flush=True)

    def cleanup_tmp(self):
        tmp_dir = Path(DragonCleanup.TMP_DIR)
        if not tmp_dir.exists() or not tmp_dir.is_dir():
            return

        print(
            f"Removing DB files, Gunicorn logfiles, and DDICT orchestrator files from {DragonCleanup.TMP_DIR}",
            flush=True,
        )

        for tmp_file in tmp_dir.iterdir():
            try:
                if not tmp_file.is_file():
                    continue

                if tmp_file.owner() == DragonCleanup.MY_USERNAME and any(
                    [
                        tmp_file.match(DragonCleanup.DATABASE_FILES),
                        tmp_file.match(DragonCleanup.GUNICORN_LOG_FILES),
                        tmp_file.match(DragonCleanup.DDICT_ORCHESTRATOR_FILES)
                        and not self.resilient
                        and not self.keep_ddict_files,
                    ]
                ):
                    if self.dry_run:
                        print(f"DRY_RUN: Removing {tmp_file} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                        continue

                    print(f"Removing {tmp_file} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                    tmp_file.unlink(missing_ok=True)
            except KeyError:
                # A KeyError can be thrown when the file is owned by
                # a user that is not listed in the /etc/passwd file.
                pass
            except PermissionError as e:
                pass

    def cleanup_ddict_orchestrator_files(self):
        """Remove DDict orchestrator files that were not written under /tmp.

        A DDict whose destroy() never ran leaves an orchestrator file in the
        directory the job ran from, which cleanup_tmp does not search. Only the
        orchestrator pattern is swept here; unlike /tmp, these directories hold
        user data and must not be matched more broadly.
        """
        if self.resilient or self.keep_ddict_files:
            return

        roots = []
        for root in (Path.cwd(), Path.home()):
            if root not in roots and root != Path(DragonCleanup.TMP_DIR):
                roots.append(root)

        for root in roots:
            if not root.is_dir():
                continue

            print(
                f"Removing DDICT orchestrator files from {root} on {DragonCleanup.MY_HOSTNAME}",
                flush=True,
            )
            # Matches the depth kclean.sh searched; deep trees are not worth walking.
            for depth in ("", "*/"):
                for orc_file in root.glob(f"{depth}{DragonCleanup.DDICT_ORCHESTRATOR_FILES}"):
                    try:
                        if not orc_file.is_file():
                            continue
                        if orc_file.owner() != DragonCleanup.MY_USERNAME:
                            continue

                        if self.dry_run:
                            print(
                                f"DRY_RUN: Removing {orc_file} on {DragonCleanup.MY_HOSTNAME}",
                                flush=True,
                            )
                            continue

                        print(f"Removing {orc_file} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                        orc_file.unlink(missing_ok=True)
                    except KeyError:
                        # Owner is absent from /etc/passwd.
                        pass
                    except (PermissionError, OSError):
                        pass

    def cleanup_attach_info(self):
        attach_dirs = []
        for attach_dir in (os.environ.get(ATTACH_DIR_ENV), default_attach_runtime_dir()):
            if attach_dir and attach_dir not in attach_dirs:
                attach_dirs.append(attach_dir)

        for attach_dir in attach_dirs:
            self._cleanup_attach_dir(Path(attach_dir))

    def _cleanup_attach_dir(self, attach_dir):
        if not attach_dir.is_dir():
            return

        attach_file = attach_dir / ATTACH_INFO_FILENAME
        if self.dry_run:
            print(
                f"DRY_RUN: Removing {attach_file} and {attach_dir} (if empty) on {DragonCleanup.MY_HOSTNAME}",
                flush=True,
            )
            return

        print(f"Removing {attach_file} and {attach_dir} (if empty) on {DragonCleanup.MY_HOSTNAME}", flush=True)
        attach_file.unlink(missing_ok=True)
        try:
            attach_dir.rmdir()
        except OSError:
            pass

    def cleanup(self):

        self.kill_dragon_process_tree()
        self.restart_nvidia_cuda_mps()
        self.cleanup_tmp()
        self.cleanup_ddict_orchestrator_files()
        self.cleanup_attach_info()
        self.cleanup_dev_shm()

        print("Cleaning up huge-pages.", flush=True)
        if self.is_be:
            hugepages_cleanup(self.resilient, self.dry_run)
        else:
            hugepages_cleanup(False, self.dry_run)


def get_cleanup_parser(parser: argparse.ArgumentParser):

    parser.add_argument(
        "--dry-run",
        help="Dry run. Don't actually make changes",
        action="store_true",
    )
    parser.add_argument(
        "--resilient",
        help="Prevent removing resources to enable resilient restart of the Dragon runtime",
        action="store_true",
    )
    parser.add_argument(
        "--keep-ddict-files",
        help="Leave DDict orchestrator files in place instead of removing them",
        action="store_true",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=2,
        help="Time to wait when terminating a process before killing it.",
    )

    return parser


def get_cleanup_args(args_input=None):
    parser = argparse.ArgumentParser(
        prog="dragon-cleanup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Run dragon backend node cleanup",
    )

    parser = get_cleanup_parser(parser)
    args = parser.parse_args(args_input)

    return {key: value for key, value in vars(args).items() if value is not None}


def main():
    args = get_cleanup_args()
    DragonCleanup(
        resilient=args["resilient"],
        dry_run=args["dry_run"],
        timeout=args["timeout"],
        keep_ddict_files=args["keep_ddict_files"],
    ).cleanup()
    return 0


if __name__ == "__main__":
    sys.exit(main())

DRAGON_CLEANUP_DESCRIPTION = """
The dragon-cleanup tool identifies and/or removes residual dragon
runtime services and user applications from previous runs in either a single
or multi-node environment. This is particularly useful when an execution of Dragon
fails to exit cleanly, leaving behind orphaned processes or resources that could
interfere with subsequent runs.

The tool automatically detects if a Workload Manager (WLM)—such as Slurm or PBS—was
used for node allocation. If a WLM is present, dragon-cleanup targets those
active nodes. Nodes can also be manually specified via the --hostlist or
--hostfile arguments, or leverage the dhosts utility to set the
DRAGON_RUN_NODEFILE environment variable.

To ensure efficiency across large multi-node environments, dragon-cleanup utilizes
an SSH-tree to launch cleanup processes on each node. This requires that all nodes
are configured for password-less SSH and maintain mutual routability.

Example usage:

dragon-cleanup --hostlist host1,host2,host3
    Manually specify that dragon-cleanup should run on host1, host2 and host3.

dragon-cleanup --hostfile my_hostfile.txt --dry-run
    Specify that dragon-cleanup should run in dry-run mode on the hosts specified
    in my_hostfile.txt. In dry-run mode, dragon-cleanup will print the processes and
    resources it would clean up, but won't actually make any changes.

dragon-cleanup --wlm slurm
    Force dragon-cleanup to look for an active Slurm allocation and run on the nodes
    from that allocation.
"""


def get_drun_parser():
    from dragon.tools.dragon_run.src.common_args import add_common_args

    parser = argparse.ArgumentParser(
        prog="dragon-cleanup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=DRAGON_CLEANUP_DESCRIPTION,
    )

    add_common_args(parser)
    parser = get_cleanup_parser(parser)
    parser.add_argument(
        "--only-be",
        help="Only teardown Dragon processes on backend compute nodes.",
        action="store_true",
        default=False,
    )
    return parser


def get_drun_args(args_input=None):

    parser = get_drun_parser()
    args = parser.parse_args(args_input)

    return {key: value for key, value in vars(args).items()}


def drun():
    from .dragon_run import src as drun
    from .dragon_run.src.exceptions import (
        DragonRunMissingAllocation,
        DragonRunNoSupportedWLM,
        DragonRunSingleNodeUnsupported,
    )

    args = get_drun_args()
    user_command = ["python", __file__]
    if "resilient" in args and args["resilient"]:
        user_command.append("--resilient")
    if "dry_run" in args and args["dry_run"]:
        user_command.append("--dry-run")
    user_command.extend(["--timeout", str(args["timeout"])])

    try:
        drun.run_wrapper(
            user_command=user_command,
            host_list=args["host_list"],
            force_single_node=args["force_single_node"],
            force_multi_node=args["force_multi_node"],
            force_wlm=args["force_wlm"],
            ssh_config_path=args.get("ssh_config_path"),
            private_key=args.get("private_key"),
            passphrase=args.get("passphrase"),
            env=dict(os.environ),
            exec_on_fe=not args.get("only_be"),
        )
    except (DragonRunMissingAllocation, DragonRunNoSupportedWLM, DragonRunSingleNodeUnsupported) as exc:
        print(exc, flush=True)
