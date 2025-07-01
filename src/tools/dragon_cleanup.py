#! /usr/bin/env python3

import argparse
import getpass
import os
import re
import socket
import subprocess
import sys

from functools import partial
from pathlib import Path
from typing import Dict
from psutil import Process, process_iter, TimeoutExpired, ZombieProcess, NoSuchProcess

from dragon.infrastructure.config import hugepages_cleanup

import logging

logger = logging.getLogger(__name__)


class DragonCleanup:

    DRAGON_FE_PROCESS_REGEX = r"/dragon$"
    DRAGON_BE_PROCESS_REGEX = r"/dragon-backend$"

    NVIDIA_CUDA_MPS_REGEX = r"/nvidia-cuda-mps$"

    DATABASE_FILES = "*.db"
    GUNICORN_LOG_FILES = "*_gunicorn.log"
    DDICT_ORCHESTRATOR_FILES = "*ddict_orc*"

    DDICT_DEV_SHM_POOLS = "*dict_pool*"

    TMP_DIR = "/tmp"
    DEV_SHM_DIR = "/dev/shm"

    MY_PID = os.getpid()
    MY_PPID = os.getppid()
    MY_USERNAME = getpass.getuser()
    MY_HOSTNAME = socket.gethostname()

    def __init__(self, resilient=False, dry_run=False, timeout=2):

        self.resilient = resilient
        self.dry_run = dry_run
        self.timeout = timeout

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
                        # we don't want to kill ourselves
                        process.pid != DragonCleanup.MY_PID,
                        process.pid != DragonCleanup.MY_PPID,
                        # only look for processes started by this user
                        process.username() == DragonCleanup.MY_USERNAME,
                        # exclude these commonly started processes
                        "grep" not in cmdline,
                        "bash" not in cmdline,
                        "mpiexec" not in cmdline,
                    ]
                )
            except ZombieProcess:
                pass

    @staticmethod
    def _find_dragon_launcher_process(process: Process, expression):
        """
        This is a utility function used to search for our dragon launcher process.
        Since the filter keyword only expects a function with a single parameter,
        the expression argument is set using python partial.
        """
        try:
            if expression.search(process.cmdline()[1]):
                return True
            return False
        except (IndexError, NoSuchProcess):
            return False

    def _kill_process_tree(self, process_list):
        for process in process_list:
            children = process.children()
            if children:
                self._kill_process_tree(children)

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
        dragon_launcher = "dragon" if self.is_fe else "dragon-backend"

        filter_proc = partial(
            DragonCleanup._find_dragon_launcher_process,
            expression=re.compile(dragon_regex),
        )

        dragon_processes = list(filter(filter_proc, self.user_processes))
        for process in dragon_processes:
            print(
                f"Found {dragon_launcher} with pid {process.pid}. Attempting to cleanup it's process tree.", flush=True
            )
            self._kill_process_tree([process])
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
            return

        if self.is_be and self.resilient:
            print(
                f"Cleaning up pools in {DragonCleanup.DEV_SHM_DIR}, but avoiding DDict pools named {DragonCleanup.DDICT_DEV_SHM_POOLS}.",
                flush=True,
            )
        else:
            print(f"Cleaning up pools in {DragonCleanup.DEV_SHM_DIR}.", flush=True)

        for dev_shm_file in [item for item in dev_shm_dir.iterdir() if item.is_file()]:
            if dev_shm_file.owner() == DragonCleanup.MY_USERNAME:
                if self.is_be and self.resilient and dev_shm_file.match(DragonCleanup.DDICT_DEV_SHM_POOLS):
                    continue

                if self.dry_run:
                    print(f"DRY_RUN: Removing {dev_shm_file} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                    continue

                print(f"Removing {dev_shm_file} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                dev_shm_file.unlink()

    def cleanup_tmp(self):
        tmp_dir = Path(DragonCleanup.TMP_DIR)
        if not tmp_dir.exists() or not tmp_dir.is_dir():
            return

        print(
            f"Removing DB files, Gunicorn logfiles and DDICT orchestrator files from {DragonCleanup.TMP_DIR}",
            flush=True,
        )

        for tmp_file in [item for item in tmp_dir.iterdir() if item.is_file()]:
            if tmp_file.owner() == DragonCleanup.MY_USERNAME and any(
                [
                    tmp_file.match(DragonCleanup.DATABASE_FILES),
                    tmp_file.match(DragonCleanup.GUNICORN_LOG_FILES),
                    tmp_file.match(DragonCleanup.DDICT_ORCHESTRATOR_FILES) and not self.resilient,
                ]
            ):
                if self.dry_run:
                    print(f"DRY_RUN: Removing {tmp_file} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                    continue

                print(f"Removing {tmp_file} on {DragonCleanup.MY_HOSTNAME}", flush=True)
                tmp_file.unlink()

    def cleanup(self):

        self.kill_dragon_process_tree()
        self.restart_nvidia_cuda_mps()
        self.cleanup_tmp()
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
    ).cleanup()
    return 0


if __name__ == "__main__":
    sys.exit(main())


def get_drun_parser(parser: argparse.ArgumentParser):

    parser = get_cleanup_parser(parser)
    parser.add_argument(
        "--only-be",
        help="Only teardown Dragon processes on backend compute nodes.",
        action="store_true",
        default=False,
    )
    return parser


def get_drun_args(args_input=None):
    parser = argparse.ArgumentParser(
        prog="dragon-cleanup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Run dragon backend node cleanup",
    )

    parser = get_drun_parser(parser)
    args = parser.parse_args(args_input)

    return {key: value for key, value in vars(args).items() if value is not None}


def drun():
    import contextlib
    import tools.dragon_run.src as drun
    from tools.dragon_run.src.exceptions import (
        DragonRunMissingAllocation,
        DragonRunNoSupportedWLM,
        DragonRunSingleNodeUnsupported,
    )

    args = get_drun_args()

    user_command = [__file__]
    command_args = sys.argv[1:]
    with contextlib.suppress(ValueError):
        command_args.remove("--only-be")
    user_command.extend(command_args)

    try:
        drun.run_wrapper(
            user_command=user_command,
            env=dict(os.environ),
            exec_on_fe=not args.get("only_be"),
        )
    except (DragonRunMissingAllocation, DragonRunNoSupportedWLM, DragonRunSingleNodeUnsupported) as exc:
        print(exc, flush=True)
