.. _data_processing:

Data Processing
+++++++++++++++

Dragon supports processing data sets through a number of its APIs, including with the
:external+python:doc:`Python multiprocessing API <library/multiprocessing>`. Let's go through a few common
data processing use cases and how to implement them with Dragon.


Processing Files
================

Processing static data that sits on a filesystem is a very common use case. Since Dragon's primary value is easily
distributing an application or workflow across many nodes, we do need to consider two versions of this use case. One
is where the data is resident in a shared filesystem accessible from any of the nodes Dragon is running on. The other
case is where the data is only visible to one of the compute nodes.


Filesystem Visible Only on One Node
-----------------------------------

Consider the case where files are created in node-local storage and now we need to initiate processing from a Python
process on that node. In this case we cannot directly access the data from a process running on any compute node.
We must instead read the data in from the current process and send that data out to each node we want to parallelize
the processing across. The simplest way to program that is with :py:meth:`~dragon.mpbridge.context.DragonContext.Pool`.
Let's assume to start that there is enough memory on the initial node to read all the data in
and hold two copies of it in process memory.

.. code-block:: python
    :linenos:
    :caption: **Read data from one process and process it in parallel across all nodes**

    import dragon
    from multiprocessing import set_start_method, Pool, cpu_count
    from pathlib import Path

    def process_data(fdata):
        # perform some operations on fdata and place into variable called result
        return result


    if __name__ == '__main__':
        set_start_method("dragon")

        dir_to_process = "."
        data_to_process = []

        num_cores = cpu_count() // 2  # all cores across all nodes, but let's not count hyperthreads

        for f in Path(dir_to_process).glob('*')):
            with open(str(f.resolve()), mode='r') as fo:
                data_to_process.append(fo.read())

        with Pool(num_cores) as p:
            processed_data = p.map(process_data, data_to_process)


One drawback to this implementation is the sequential nature of reading all the data in before any processing can begin.
Depending on the nature of the processing it may be much better to use a iterator along with
:py:class:`Pool.imap() <dragon.mpbridge.pool.DragonPool.imap>`, which will also save us some memory overhead.

.. code-block:: python
    :linenos:
    :caption: **Lazily read data from one process and process it in parallel across all nodes**

    import dragon
    from multiprocessing import set_start_method, Pool, cpu_count
    from pathlib import Path

    def process_data(fdata):
        # perform some operations on fdata and place into variable called result
        return result


    class DataReader:

        def __init__(self, dir):
            self.dir = dir
            self.filenames = [str(f.resolve()) for f in Path(dir_to_process).glob('*')]
            self.idx = 0

        def __iter__(self):
            return self

        def __next__(self):
            with open(self.filenames[self.idx], mode='r') as f:
                data_to_process = f.read()
            self.idx += 1
            return data_to_process


    if __name__ == '__main__':
        set_start_method("dragon")

        dir_to_process = "."
        dr = DataReader(dir_to_process)

        num_cores = cpu_count() // 2  # all cores across all nodes, but let's not count hyperthreads

        with Pool(num_cores) as p:
            processed_data = p.imap(process_data, dr, chunk_size=4)


The nice thing about this style of code is it makes few assumptions about the type of system you are running on, in
particluar there is no assumption files are accessible on all nodes. The drawback is all input and output data flows
through the initial process. If there is a shared filesystem, it may be more efficient to make use of it.


Shared Filesystem
-----------------

For very large datasets that have no hope of fitting in a single node's memory or local storage, it can be better to
leverage a systems shared filesystem (if it has one). On supercomputers, this is often a Lustre filesystem or something
similar. In this case, a process on any node can access any file, and we don't need to flow all input data through the
initial process. Instead we just pass filenames to the workers.

.. code-block:: python
    :linenos:
    :caption: **Read and process data in parallel across all nodes**

    import dragon
    from multiprocessing import set_start_method, Pool, cpu_count
    from pathlib import Path

    def process_data(filename):
        with open(filename, mode='r') as f:
            fdata = f.read()

        # perform some operations on fdata and place into variable called result
        return result


    if __name__ == '__main__':
        set_start_method("dragon")

        dir_to_process = "."
        files_to_process = []

        num_cores = cpu_count() // 2  # all cores across all nodes, but let's not count hyperthreads

        files_to_process = [str(f.resolve()) for f in Path(dir_to_process).glob('*')]

        with Pool(num_cores) as p:
            processed_data = p.map(process_data, files_to_process)


We could also write the processed data back to the shared filesystem, but if we intend to do more work with the processed
data, we're introducing a potential bottleneck into our workflow with the filesystem. One approach with Dragon to keep
data closer to new and existing processes is to use the in-memory distributed dictionary, :py:class:`~dragon.data.DDict`.

.. code-block:: python
    :linenos:
    :caption: **Read and process data in parallel across all nodes and store results in a DDict**

    import dragon
    import multiprocessing as mp
    from multiprocessing import set_start_method, Pool, cpu_count, current_process
    from pathlib import Path

    from dragon.data.ddict import DDict
    from dragon.native.machine import System, Node


    def initialize_worker(the_ddict):
        # Since we want each worker to maintain a persistent handle to the DDict,
        # attach it to the current/local process instance. Done this way, workers attach only
        # once and can reuse it between processing work items
        me = current_process()
        me.stash = {}
        me.stash["ddict"] = the_ddict


    def process_data(filename):
        the_ddict = current_process().stash["ddict"]
        try:
            with open(filename, mode='r') as f:
                fdata = f.read()

            # perform some operations on fdata and place into variable called result
            the_ddict[filename] = result
            return True
        except:
            return False


    def setup_ddict():

        # let's place the DDict across all nodes Dragon is running on
        my_system = System()
        num_nodes = my_system.nnodes

        total_mem = 0
        for huid in my_system.nodes:
            anode = Node(huid)
            total_mem += anode.physical_mem
        dict_mem = 0.1 * total_mem  # use 10% of the mem

        return DDict(
            2,  # two managers per node
            num_nodes,
            int(dict_mem),
            )


    if __name__ == '__main__':
        set_start_method("dragon")

        dir_to_process = "."

        num_cores = cpu_count() // 2  # all cores across all nodes, but let's not count hyperthreads

        files_to_process = [str(f.resolve()) for f in Path(dir_to_process).glob('*')]

        the_ddict = setup_ddict()

        # use the standard initializer argument to Pool to pass the DDict to each worker
        with Pool(num_cores, initializer=initialize_worker, initargs=(the_ddict,)) as p:
            files_were_processed = p.map(process_data, files_to_process)

        # peek at data from one file
        print(the_ddict[files_to_process[2]], flush=True)


Streaming Data Processing Pipeline
==================================

Processing streaming data, where it is likely the volume of data is not known, is also well supported by the Python
:external+python:doc:`multiprocessing <library/multiprocessing>` API and across many nodes with Dragon. Here is an
example processing pipeline with three phases: a ingest phase listening on a socket, a raw data processing phase, and
a second processing phase. Each phase may have different amounts of computation required. This example handles that by
giving different amounts of CPU cores to the two processing phases.

.. code-block:: python
    :linenos:
    :caption: **Process data through a pipeline coming from a socket through two phases**

    import dragon
    from multiprocessing import set_start_method, Pool, cpu_count
    import socket


    def generate_data(data_blk):
        # perform operations on data_blk in the first phase of a pipeline
        return result


    def process_data(data_item):
        # perform post processing operations
        return result


    class StreamDataReader:

        def __init__(self, host='0.0.0.0', port=9000, chunk_size=1024):
            self.chunk_size = chunk_size
            self.socket = socket.socket()
            s.connect((host, port))

        def __iter__(self):
            return self

        def __next__(self):
            data = self.socket.recv(self.chunk_size)
            if data:
                return data_to_process
            else:
                raise(StopIteration)


    if __name__ == '__main__':
        set_start_method("dragon")

        num_cores = cpu_count() // 2  # all cores across all nodes, but let's not count hyperthreads
        num_producers = num_cores // 4
        num_consumers = num_cores - num_producers

        data_stream = StreamDataReader()

        producers = Pool(num_producers)
        consumers = Pool(num_consumers)

        # note that imap() returns an iterable itself, which allows a pipeline like this to get overlap between phases
        gen_data = producers.imap(generate_data, data_stream, 4)
        proc_data = consumers.imap(process_data, gen_data, 2)
        for i, item in enumerate(proc_data):
            print(f"Pipeline product {i}={item}")

        producers.close()
        consumers.close()
        producers.join()
        consumers.join()


The implementation uses :py:class:`Pool.imap() <dragon.mpbridge.pool.DragonPool.imap>` to pull work through an iterator
class `StreamDataReader`. As blocks of data come in through the socket, they are fed to a pool of

Related Cookbook Examples
=========================

* :ref:`merge_sort`
* :ref:`pipeline`
