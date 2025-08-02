import dragon
import multiprocessing as mp
import os
import socket
import sys
import time

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


def signal_exit():
    path = "/home/users/nradclif/hpc-pe-dragon-dragon/examples/dragon_workflows/client_exit"
    file = open(path, "w")


def main():
    mp.set_start_method("dragon")
    username = os.environ["USER"]
    if len(sys.argv) > 1:
        system = sys.argv[1]
    else:
        system = "hotlum-login"

    runtime_sdesc = runtime.lookup(system, "my-runtime", 30)
    proxy = runtime.attach(runtime_sdesc)

    print("\n")

    # test process and queue

    proxy.enable()

    q = mp.Queue()
    procs = []

    for _ in range(2):
        p = mp.Process(target=howdy, args=(q,))
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
    exe = "./mpi_hello"
    grp.add_process(
        nproc=1, template=ProcessTemplate(target=exe, args=[], env=proxy.get_env(), cwd=os.getcwd(), stdout=Popen.PIPE)
    )
    grp.add_process(
        nproc=num_ranks - 1,
        template=ProcessTemplate(target=exe, args=[], env=proxy.get_env(), cwd=os.getcwd(), stdout=Popen.DEVNULL),
    )
    grp.init()
    grp.start()

    while None in grp.puids:
        time.sleep(2)

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
    grp.stop()
    grp.close()

    # signal client's exit

    exit_proc = mp.Process(target=signal_exit, args=())
    exit_proc.start()
    exit_proc.join()

    cleanup(conn, q, procs, grp)

    proxy.disable()


if __name__ == "__main__":
    main()
