.. _proxy:

Proxy
+++++

Dragon supports the ability to start a runtime on a remote system and connect to it from a local system via a proxy. This allows users to run Dragon applications that burst across system boundaries. A prototypical example would be using a jupyter notebook on a laptop and connecting to a high-performance computing cluster to run expensive computations. The Dragon runtime would be started on the remote HPC system, and the local jupyter notebook would connect to it via a proxy. As long as the remote runtime is not shutdown the local client can enable and disable the proxy as many times as needed.

Setup
=====

1. Ensure that passwordless SSH access is set up from the local system to the remote system. The proxy mechanism currently uses SSH to connect to the remote system and forward communication between the proxy client and proxy server.
2. Ensure that the Dragon infrastructure is installed and configured on both the local and remote systems.
3. Ensure that the remote python environment has access to the same code and dependencies as the local system. It can be in a different path than where the server was started but must be accessible.

Hello World Example
===================

The following code snippets illustrate a simple example of starting a proxy server on a remote system and connecting to it from a local system via a proxy client.
The proxy server can be re-used with almost no change for any proxy client.
The proxy client code will need to be modified to specify the remote system and paths used for publishing and shutdown on the remote system.

.. code-block:: python
    :linenos:
    :caption: **Proxy server ran on remote system**

    import os
    import time
    import dragon.infrastructure.parameters as dparm
    import dragon.workflows.runtime as runtime

    def wait_for_exit(exit_file_path):
        while not os.path.exists(exit_file_path):
            time.sleep(1)
        time.sleep(1)
        if dparm.this_process.index == 0:
            os.remove(exit_file_path)

    if __name__ == '__main__':
        name='proxy_runtime'
        publish_path=os.getcwd() # path where sdesc is published. needs to be accessible by client
        exit_path=os.path.join(os.getcwd(), 'exit_client') # path where client exit file is created. needs to be writable by client

        sdesc = runtime.publish(name, publish_path)
        print(f'Runtime serialized descriptor: {sdesc}', flush=True)
        wait_for_exit(exit_path)

.. code-block:: python
    :linenos:
    :caption: **Proxy client ran on local system**

    import dragon
    import multiprocessing as mp
    import os
    import socket

    from dragon.native.process import Process
    import dragon.workflows.runtime as runtime

    def signal_exit(exit_path):
        file = open(exit_path, "w")
        file.close()

    def shutdown_remote_runtime(exit_path, remote_working_dir):
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
            proc_env = os.environ.copy()

        q = mp.Queue()
        procs = []

        print("Launching remote runtime processes...", flush=True)
        for _ in range(2):
            # using native process so we can set cwd
            p = Process(target=howdy, args=(q,), cwd=remote_working_dir)
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


    if __name__ == '__main__':

        # paths to find remote runtime sdesc and signal exit
        system = "my.remote.system"
        runtime_name = "proxy_runtime"
        publish_dir = "/my/remote/publish/dir"
        exit_path = "/my/remote/publish/dir/exit_client"

        # paths to files used during remote runtime execution
        remote_working_dir = "/my/remote/working/dir"


        mp.set_start_method("dragon")
        runtime_sdesc = runtime.lookup(system, runtime_name, 30, publish_dir=publish_dir)
        proxy = runtime.attach(runtime_sdesc, remote_cwd=remote_working_dir)

        print("\n")

        # run remote work with proxy
        proxy.enable()
        remote_work(proxy, remote_working_dir)
        proxy.disable()

        # run remote work without proxy
        remote_work(None, os.getcwd())

        # run remote work with proxy again
        proxy.enable()
        remote_work(proxy, remote_working_dir)
        # signal client's exit
        shutdown_remote_runtime(exit_path, remote_working_dir)
        proxy.disable()

Using Pickle by Value
=====================

Cloudpickle is used to serialize functions and objects sent to the remote runtime. Cloudpickle documents an experimental feature to support serializing modules by value rather than the default of by reference. This feature may be helpful when using proxies to ensure that the remote runtime has access to the same code as the client. See the [Cloudpickle documentation](https://github.com/cloudpipe/cloudpickle?tab=readme-ov-file#overriding-pickles-serialization-mechanism-for-importable-constructs) for more information.
The following code snippet shows how a local module can be organized to utilize
cloudpickle's `register_pickle_by_value` to avoid needing module paths to be available on the remote system.

.. code-block:: python
    :linenos:
    :caption: **My module that is only on the local system**

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

    def remote_work():

        q = mp.Queue()
        procs = []

        print("Launching remote runtime processes...", flush=True)
        for _ in range(2):
            # using native process so we can set cwd
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

In this example, the `my_local_module.py` contains code that is only available on the local system. By registering the module with `cloudpickle.register_pickle_by_value`, we can ensure that it is serialized and sent to the remote runtime when needed, allowing the remote runtime to execute code from the module even though it is not available on the remote system.

.. code-block:: python
    :linenos:
    :caption: **Proxy client ran on local system**

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
        system = "my.remote.system"
        runtime_name = "proxy_runtime"
        publish_dir = "/my/remote/publish/dir"
        exit_path = "/my/remote/publish/dir/exit_client"


        mp.set_start_method("dragon")
        runtime_sdesc = runtime.lookup(system, runtime_name, 30, publish_dir=publish_dir)
        proxy = runtime.attach(runtime_sdesc, remote_cwd=publish_dir)

        print("\n")

        # run remote work with proxy
        proxy.enable()
        my_local_module.remote_work()
        proxy.disable()

        # run remote work without proxy
        my_local_module.remote_work()
        # run remote work with proxy again
        proxy.enable()
        my_local_module.remote_work()
        # signal client's exit
        my_local_module.shutdown_remote_runtime(exit_path)
        proxy.disable()



Tips and Tricks
================

Remote:
-------
* Ensure that the named file used to signal exit is not present on the remote system before starting the proxy server.
* Ensure that the remote working directory is accessible and writable by the client process.
* When using a proxy, the remote runtime will continue to run even if the local client disconnects. Be sure to signal the remote runtime to shutdown when finished to avoid leaving stray runtimes running on the remote system.
* Multiple runtimes can attach to the same remote runtime via proxies. Each client will need to lookup and attach to the remote runtime separately. The remote runtime will manage resources for all connected clients. The clients will share the resources of the remote runtime. If a client disconnects, the other clients will still be able to use the remote runtime as long as it is not shutdown. If a client shuts down the remote runtime, all clients will lose access to it and any resources within it. Clients are likely to hang on remote operations if the remote runtime is shutdown while they are connected.

Local:
-------
* Ensure that the descriptor is correct and can be found by the client when looking up the runtime. If the client cannot find the runtime, check that the publish path is correct and accessible by the client, and that the descriptor is being published to the correct place by the server.
* When using a proxy, it is helpful to explicitly delete any Dragon processes and queues created within the remote runtime to ensure proper cleanup.
* When attaching to a remote runtime via a proxy, you can specify a different working directory for the remote processes using the `remote_cwd` parameter.
* Use the `get_env()` method of the proxy to obtain the correct environment variables for processes running in the remote runtime.
* If local runtime appears hung, check the remote system for any error messages or issues with the Dragon runtime. Many errors will be returned to the local runtime when using a proxy; however, some issues may only be visible on the remote system. Specifically, missing dependencies or code on the remote system may cause errors that are not visible on the local system.
* When using a memory object that was created in the remote runtime on the local system, the object needs to be instantiated within the proxy enable/disable block but can be used outside of it until the remote runtime is shutdown.
* Process and ProcessGroup objects created within the remote runtime cannot be used on the local system when the proxy is disabled, but can be used when the proxy is enabled. Be sure to only use these objects when the proxy is enabled to avoid errors. You can disable and re-enable the proxy as needed to check the status of remote processes. In the future, we may add functionality to automatically route process management calls through the proxy when a process was created in the remote runtime to avoid this issue.


Related Cookbook Examples
=========================

* :ref:`cbook_ai_in_the_loop`
* :ref:`batch`
