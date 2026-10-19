import dragon
import multiprocessing as mp
import os
import socket

from dragon.native.process import Process

def signal_exit(exit_path):
    file = open(exit_path, "w")
    file.close()

def shutdown_remote_runtime(proxy, exit_path, remote_working_dir):
    #exit_proc = Process(target=signal_exit, args=(exit_path,), cwd=remote_working_dir, env=proxy.get_env())
    exit_proc = Process(target=signal_exit, args=(exit_path,), cwd=remote_working_dir)
    exit_proc.start()
    exit_proc.join()

def howdy(q):
    q.put(
        f"howdy from {socket.gethostname()} - local num cores is {os.cpu_count()}, runtime available cores is {mp.cpu_count()}"
    )

def remote_work(proxy, remote_working_dir):

    if proxy is not None:
        proc_env = proxy.get_env()
    else:
        proc_env = None

    q = mp.Queue()
    procs = []

    print("Launching remote runtime processes...", flush=True)
    for _ in range(2):
        # using native process so we can set cwd
        #p = Process(target=howdy, args=(q,), cwd=remote_working_dir, env=proc_env)
        p = Process(target=howdy, args=(q,))
        p.start()
        procs.append(p)

    for p in procs:
        msg = q.get()
        print(f"Message from remote runtime: {msg}", flush=True)

    for p in procs:
        p.join()

    # when running in proxy mode, explicitly delete processes and queue can be helpful
    for p in procs:
        del p
    del q
