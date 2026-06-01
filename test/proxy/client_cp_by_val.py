import dragon
import multiprocessing as mp
import os

import dragon.workflows.runtime as runtime
import cloudpickle
import my_local_module  # module only on local system
# this may have performance impacts and recursive imports may not work properly, see cloudpickle docs.
cloudpickle.register_pickle_by_value(my_local_module)


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
    my_local_module.remote_work(proxy, publish_dir)
    proxy.disable()

    # run remote work without proxy
    my_local_module.remote_work(None, os.getcwd())
    # run remote work with proxy again
    proxy.enable()
    my_local_module.remote_work(proxy, publish_dir)
    # signal client's exit
    my_local_module.shutdown_remote_runtime(proxy, exit_path, publish_dir)
    proxy.disable()