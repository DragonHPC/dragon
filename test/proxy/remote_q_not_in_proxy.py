import dragon
import multiprocessing as mp
import os
import socket

from dragon.native.process import Process
import dragon.workflows.runtime as runtime

def signal_exit(exit_path):
    file = open(exit_path, "w")
    file.close()

def shutdown_remote_runtime(exit_path):
    exit_proc = Process(target=signal_exit, args=(exit_path,))
    exit_proc.start()
    exit_proc.join()

def howdy(q):
    q.put(
        f"howdy from {socket.gethostname()} - local num cores is {os.cpu_count()}, runtime available cores is {mp.cpu_count()}"
    )


if __name__ == '__main__':

    # paths to find remote runtime sdesc and signal exit
    #system = "pippin"
    system = "portage"
    #runtime_name = "proxy_runtime"
    runtime_name = "my-runtime"
    #publish_dir = "/lus/scratch/wahlc/dragon/dev/"
    publish_dir = "/lus/lustre1/wahlc/dragon/dev/proxy-dev/examples/workflows/proxy"
    exit_path = "/lus/lustre1/wahlc/dragon/dev/proxy-dev/examples/workflows/proxy/client_exit"

    remote_working_dir = "/lus/lustre1/wahlc/dragon/dev/proxy-dev/examples/workflows/proxy"

    mp.set_start_method("dragon")
    runtime_sdesc = runtime.lookup(system, runtime_name, 30, publish_dir=publish_dir)
    proxy = runtime.attach(runtime_sdesc, remote_cwd=remote_working_dir)

    print("\n")

    # run remote work with proxy
    proxy.enable()
    q = mp.Queue()
    procs = []

    proxy.disable()

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

    proxy.enable()
    shutdown_remote_runtime(exit_path)
    proxy.disable()
