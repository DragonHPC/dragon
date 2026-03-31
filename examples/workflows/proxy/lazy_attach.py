import dragon
import multiprocessing as mp
import os
import socket
import sys
import time
import argparse

from dragon.native.process import ProcessTemplate, Popen, Process
from dragon.native.process_group import ProcessGroup
import dragon.workflows.runtime as runtime
from dragon.infrastructure.facts import PMIBackend


def cleanup(conn, q, procs, grp):
    conn.close()
    del conn
    del q
    for p in procs:
        del p
    del grp


def howdy(q):
    q.put(
        f"howdy from {socket.gethostname()} - local num cores is {os.cpu_count()}, runtime available cores is {mp.cpu_count()}"
    )


def signal_exit(exit_path):
    file = open(exit_path, "w")
    file.close()


def shutdown_remote_runtime(exit_path, remote_working_dir):
    exit_proc = Process(target=signal_exit, args=(exit_path,), cwd=remote_working_dir)
    exit_proc.start()
    exit_proc.join()


def remote_work(proxy, remote_working_dir):

    if proxy is not None:
        proc_env = proxy.get_env()
    else:
        proc_env = os.environ.copy()

    q = mp.Queue()
    procs = []

    print("Launching remote runtime processes...", flush=True)
    for _ in range(2):
        p = Process(target=howdy, args=(q,), cwd=remote_working_dir)
        procs.append(p)

    for p in procs:
        p.start()
        msg = q.get()
        print(f"Message from remote runtime: {msg}", flush=True)

    for p in procs:
        p.join()

    # launch the mpi job

    grp = ProcessGroup(restart=False, pmi=PMIBackend.CRAY)

    # TODO: it seems like the client tries to verify that mpi_hello exists locally
    num_ranks = 4
    exe = os.path.join(remote_working_dir, "mpi_hello")
    grp.add_process(
        nproc=1, template=ProcessTemplate(target=exe, args=(), env=proc_env, cwd=remote_working_dir, stdout=Popen.PIPE)
    )
    grp.add_process(
        nproc=num_ranks - 1,
        template=ProcessTemplate(target=exe, args=(), env=proc_env, cwd=remote_working_dir, stdout=Popen.DEVNULL),
    )
    grp.init()
    grp.start()

    while None in grp.puids:
        time.sleep(0.1)

    # get remote runtime's stdout
    child_resources = [Process(None, ident=puid) for puid in grp.puids]
    conn = child_resources[0].stdout_conn
    try:
        while True:
            print(f"{conn.recv()}", flush=True)
    except EOFError:
        pass

    # wait for MPI job to complete

    grp.join()
    grp.close()

    cleanup(conn, q, procs, grp)


def main(args):
    mp.set_start_method("dragon")
    runtime_sdesc = runtime.lookup(args.system, args.runtime_name, 30, publish_dir=args.remote_working_dir)
    proxy = runtime.attach(runtime_sdesc, remote_cwd=args.remote_working_dir)

    print("\n")

    proxy.enable()
    remote_work(proxy, args.remote_working_dir)
    # signal client's exit
    shutdown_remote_runtime(args.exit_path, args.remote_working_dir)
    proxy.disable()


def main_proxy_on_off(args):
    mp.set_start_method("dragon")
    runtime_sdesc = runtime.lookup(args.system, args.runtime_name, 30, publish_dir=args.remote_working_dir)
    proxy = runtime.attach(runtime_sdesc, remote_cwd=args.remote_working_dir)

    print("\n")

    proxy.enable()
    remote_work(proxy, args.remote_working_dir)
    proxy.disable()

    time.sleep(2)
    remote_work(None, os.getcwd())
    time.sleep(2)

    proxy.enable()
    remote_work(proxy, args.remote_working_dir)
    # signal client's exit
    shutdown_remote_runtime(args.exit_path, args.remote_working_dir)
    proxy.disable()


def parse_args():
    parser = argparse.ArgumentParser(description="Dragon Proxy Client Example")
    parser.add_argument(
        "--exit-path",
        type=str,
        default=os.path.join(os.getcwd(), "client_exit"),
        help="Path to the exit file to monitor",
    )
    parser.add_argument(
        "--remote-working-dir",
        "-rwd",
        type=str,
        default=os.getcwd(),
        help="Remote working directory",
    )
    parser.add_argument(
        "--runtime-name",
        type=str,
        default="my-runtime",
        help="Name of the runtime to attach",
    )
    parser.add_argument(
        "--system",
        type=str,
        help="Remote system to connect to",
    )
    parser.add_argument(
        "--proxy-on-off",
        type=bool,
        help="Remote system to connect to",
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    if args.proxy_on_off:
        main_proxy_on_off(args)
    else:
        main(args)
