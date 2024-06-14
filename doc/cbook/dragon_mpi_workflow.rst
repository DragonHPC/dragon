An example MPI workflow using Dragon ProcessGroup
=================================================

This file establishes an example workflow of multiple coordinating
processes:

* Producers: Up to two concurrent producer processes are started
  at any time, with a maximum of 5 producer processes run in total.
  Each Producer process starts a MPI based job of up to half the
  available cores within the multinode allocation. The Dragon
  ProcessGroup API is used to efficiently start and manage the MPI
  processes. When starting the MPI Processes, the stdout of the head MPI
  process is redirected and made available via a Dragon Connection
  object.

  In this example the `osu_alltoall` application is used to generate
  pseudo random data that is fed to the Parser process.

* Parser: Each producer process starts a single Parser process used
  to transform the Producer output. The parser process reads from
  the given Dragon Connection object to receive the stdout from its producer.
  Received data is transformed until all data has been processed.
  Transformed data is placed into a shared Results Queue and sent to the
  Consumer process for post processing.

* Consumer: The consumer process reads the transformed data from the
  shared Results Queue and logs the values. Alternatively, the results
  could be forwarded to another workflow for further processing.

.. figure:: images/dragon_mpi_workflow.svg
    :align: center
    :scale: 75%
    :name: mpiworkflowpic

.. code-block:: python
    :linenos:
    :caption: hpc_workflow_demo_highlevel.py: Example MPI based workflow example

    import os
    import re
    import logging
    import queue

    import dragon
    import multiprocessing as mp

    from dragon.globalservices import node
    from dragon.globalservices.process import multi_join
    from dragon.infrastructure.connection import Connection
    from dragon.native.process import MSG_PIPE, MSG_DEVNULL, Process, ProcessTemplate
    from dragon.native.process_group import ProcessGroup

    logging.basicConfig(level=logging.INFO)


    def producer_proc(producer_id: int, num_ranks: int, result_queue: mp.Queue) -> None:
        """
        The producer process will start `num_ranks` copies of the osu_alltoall
        MPI application. The stdout output from the head process will be piped via
        Dragon to a separate process responsible for parsing the osu_alltoall
        results and putting the results onto the results queue.

        :param producer_id: Numerical id identifying this producer
        :type producer_id: int
        :param num_ranks: The number of MPI ranks to start
        :type num_ranks: int
        :param result_queue: Handle to a queue where the results should be placed
        :type result_queue: mp.Queue
        """

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger(f"producer {producer_id}")
        log.info("Starting producer (num_ranks=%d)", num_ranks)

        exe = os.path.join(os.getcwd(), "osu_alltoall")
        args = ["--warmup", "10", "-m", "4096"]
        run_dir = os.getcwd()

        grp = ProcessGroup(restart=False, pmi_enabled=True)

        # Pipe the stdout output from the head process to a Dragon connection
        grp.add_process(
            nproc=1,
            template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=MSG_PIPE)
        )

        # All other ranks should have their output go to DEVNULL
        grp.add_process(
            nproc=num_ranks-1,
            template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=MSG_DEVNULL)
        )

        grp.init()
        grp.start()
        child_resources = [Process(None, ident=puid) for puid in grp.puids]
        parser_proc = None
        ranks_per_node = {n: 0 for n in range(len(node.get_list()))}
        for child_resource in child_resources:
            ranks_per_node[child_resource.node] = ranks_per_node[child_resource.node] + 1
            if child_resource.stdout_conn:
                log.info("Starting parse_results process for puid=%d", child_resource.puid)
                parser_proc = Process(
                    target=parse_results_proc,
                    args=(producer_id, child_resource.stdout_conn, result_queue),
                )
                parser_proc.start()

        log.info(", ".join([f"node {n} has {r} ranks" for n, r in ranks_per_node.items()]))
        log.info("Waiting for group to finish")
        if len(child_resources) > 0:
            grp.join()

        if parser_proc:
            parser_proc.join()

        grp.stop()

        log.info("Done")


    def parse_results_proc(producer_id: int, stdout_conn: Connection, result_queue: mp.Queue) -> None:
        """
        Read stdout from the Dragon connection. Parse statistical data
        and put onto result queue.

        :param producer_id: Numerical id identifying this producer
        :type producer_id: int
        :param stdout_conn: Dragon Connection object to read stdout data from
        :type stdout_conn: Connection
        :param result_queue: Handle to a queue where the results should be placed
        :type result_queue: mp.Queue
        """

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger(f"parse_results {producer_id}")
        log.info("Parsing stdout from stdout connection")

        try:
            result_matcher = re.compile(r"^(\d+)\s+([\d.]+)")
            while True:
                line = stdout_conn.recv()
                result = result_matcher.search(line)
                if result:
                    result_queue.put(
                        {
                            producer_id: (
                                result[1],
                                result[2],
                            )
                        }
                    )
        except EOFError:
            pass

        log.info("Done")


    def consumer_proc(result_queue: mp.Queue, shutdown_event: mp.Event) -> None:
        """
        Read the values out of the result queue and
        just print them to the log

        :param result_queue: Handle to a queue where the results should be placed
        :type result_queue: mp.Queue
        :param shutdown_event: Event used to signal that the consumer process should exit
        :type shutdown_event: mp.Event
        """

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger("consumer")
        log.info("reading from result_queue")

        while not shutdown_event.is_set():
            try:
                values = result_queue.get(timeout=0.1)
                log.info(values)
            except queue.Empty:
                pass

        log.info("Done")


    def main() -> None:
        mp.set_start_method("dragon")

        log = logging.getLogger("main")

        result_queue = mp.Queue()

        total_runs = 5
        current_runs = 0
        simultaneous_producers = 2
        producer_num = 0

        num_nodes = len(node.get_list())
        reserved_cores = (
            num_nodes * 2
        )  # Reserve a couple of cores for Dragon infrastructure
        num_real_cores = mp.cpu_count() // 2
        ranks_per_job = (num_real_cores - reserved_cores) // simultaneous_producers

        shutdown_event = mp.Event()
        log.info("Starting consumer process")
        consumer = Process(
            target=consumer_proc,
            args=(
                result_queue,
                shutdown_event,
            ),
        )
        consumer.start()

        producers = set()
        active_producers = 0
        while current_runs < total_runs:
            while active_producers < min(simultaneous_producers, total_runs - current_runs):
                log.info("Starting a new producer")
                producer = Process(
                    target=producer_proc, args=(producer_num, ranks_per_job, result_queue)
                )
                producer.start()
                producers.add(producer.puid)
                active_producers += 1
                producer_num += 1

            exited_list, _ = multi_join(producers, join_all=False)
            log.info("at least one producer has exited")
            exited_puids = [] if exited_list is None else [puid for puid, _ in exited_list]
            current_runs = current_runs + len(exited_puids)
            active_producers = active_producers - len(exited_puids)
            producers -= set(exited_puids)

        log.info("Shutting down")
        shutdown_event.set()
        consumer.join()
        log.info("Done")


    if __name__ == "__main__":
        main()

The program output can be seen below:

.. code-block:: console
    :linenos:
    :caption: **Output when running hpc_workflow_demo_highlevel.py**

    >$dragon hpc_workflow_demo_highlevel.py
    INFO:api_setup:We are registering gateways for this process. dp.this_process.num_gw_channels_per_node=1
    INFO:api_setup:connecting to infrastructure from 117921
    INFO:api_setup:debug entry hooked
    INFO:main:Starting consumer process
    INFO:main:Starting a new producer
    INFO:main:Starting a new producer
    INFO:producer 0:Starting producer (num_ranks=252)
    INFO:consumer:reading from result_queue
    INFO:producer 1:Starting producer (num_ranks=252)
    INFO:producer 0:Starting parse_results process for puid=4294967302
    INFO:producer 0:node 0 has 63 ranks, node 1 has 63 ranks, node 2 has 63 ranks, node 3 has 63 ranks
    INFO:producer 0:Waiting for group to finish
    INFO:parse_results 0:Parsing stdout from stdout connection
    INFO:producer 1:Starting parse_results process for puid=4294967554
    INFO:producer 1:node 0 has 63 ranks, node 1 has 63 ranks, node 2 has 63 ranks, node 3 has 63 ranks
    INFO:producer 1:Waiting for group to finish
    INFO:parse_results 1:Parsing stdout from stdout connection
    INFO:consumer:{0: ('1', '65.12')}
    INFO:consumer:{0: ('2', '62.65')}
    INFO:consumer:{0: ('4', '62.30')}
    INFO:consumer:{0: ('8', '68.07')}
    INFO:consumer:{1: ('1', '63.97')}
    INFO:consumer:{0: ('16', '77.03')}
    INFO:consumer:{1: ('2', '68.60')}
    INFO:consumer:{0: ('32', '93.42')}
    INFO:consumer:{1: ('4', '74.10')}
    INFO:consumer:{0: ('64', '137.70')}
    INFO:consumer:{1: ('8', '81.51')}
    INFO:consumer:{1: ('16', '86.40')}
    INFO:consumer:{1: ('32', '101.93')}
    INFO:consumer:{0: ('128', '322.11')}
    INFO:consumer:{1: ('64', '176.49')}
    INFO:consumer:{1: ('128', '415.66')}
    INFO:consumer:{0: ('256', '662.86')}
    INFO:consumer:{1: ('256', '815.32')}
    INFO:consumer:{0: ('512', '1437.74')}
    INFO:consumer:{1: ('512', '1306.46')}
    INFO:consumer:{0: ('1024', '1288.51')}
    INFO:consumer:{1: ('1024', '1400.14')}
    INFO:consumer:{0: ('2048', '2137.02')}
    INFO:consumer:{1: ('2048', '2839.61')}
    INFO:consumer:{0: ('4096', '4095.24')}
    INFO:parse_results 0:Done
    INFO:consumer:{1: ('4096', '3611.41')}
    INFO:producer 0:Done
    INFO:main:at least one producer has exited
    INFO:main:Starting a new producer
    INFO:parse_results 1:Done
    INFO:producer 2:Starting producer (num_ranks=252)
    INFO:producer 1:Done
    INFO:main:at least one producer has exited
    INFO:main:Starting a new producer
    INFO:producer 3:Starting producer (num_ranks=252)
    INFO:producer 2:Starting parse_results process for puid=4294967811
    INFO:producer 2:node 0 has 63 ranks, node 1 has 63 ranks, node 2 has 63 ranks, node 3 has 63 ranks
    INFO:producer 2:Waiting for group to finish
    INFO:parse_results 2:Parsing stdout from stdout connection
    INFO:consumer:{2: ('1', '48.48')}
    INFO:consumer:{2: ('2', '48.84')}
    INFO:consumer:{2: ('4', '50.06')}
    INFO:consumer:{2: ('8', '54.24')}
    INFO:consumer:{2: ('16', '63.57')}
    INFO:consumer:{2: ('32', '80.13')}
    INFO:consumer:{2: ('64', '122.75')}
    INFO:consumer:{2: ('128', '248.06')}
    INFO:consumer:{2: ('256', '478.18')}
    INFO:consumer:{2: ('512', '937.72')}
    INFO:consumer:{2: ('1024', '675.31')}
    INFO:consumer:{2: ('2048', '1259.17')}
    INFO:producer 3:Starting parse_results process for puid=4294968065
    INFO:producer 3:node 0 has 63 ranks, node 1 has 63 ranks, node 2 has 63 ranks, node 3 has 63 ranks
    INFO:producer 3:Waiting for group to finish
    INFO:parse_results 3:Parsing stdout from stdout connection
    INFO:consumer:{3: ('1', '280.64')}
    INFO:consumer:{3: ('2', '281.76')}
    INFO:consumer:{3: ('4', '282.04')}
    INFO:consumer:{2: ('4096', '2412.99')}
    INFO:consumer:{3: ('8', '265.82')}
    INFO:consumer:{3: ('16', '64.42')}
    INFO:parse_results 2:Done
    INFO:consumer:{3: ('32', '83.40')}
    INFO:consumer:{3: ('64', '122.75')}
    INFO:consumer:{3: ('128', '262.51')}
    INFO:producer 2:Done
    INFO:main:at least one producer has exited
    INFO:main:Starting a new producer
    INFO:consumer:{3: ('256', '487.71')}
    INFO:producer 4:Starting producer (num_ranks=252)
    INFO:consumer:{3: ('512', '951.84')}
    INFO:consumer:{3: ('1024', '662.71')}
    INFO:consumer:{3: ('2048', '1246.95')}
    INFO:consumer:{3: ('4096', '2343.83')}
    INFO:parse_results 3:Done
    INFO:producer 4:Starting parse_results process for puid=4294968320
    INFO:producer 4:node 0 has 63 ranks, node 1 has 63 ranks, node 2 has 63 ranks, node 3 has 63 ranks
    INFO:producer 4:Waiting for group to finish
    INFO:producer 3:Done
    INFO:main:at least one producer has exited
    INFO:parse_results 4:Parsing stdout from stdout connection
    INFO:consumer:{4: ('1', '48.31')}
    INFO:consumer:{4: ('2', '48.77')}
    INFO:consumer:{4: ('4', '50.00')}
    INFO:consumer:{4: ('8', '56.37')}
    INFO:consumer:{4: ('16', '64.84')}
    INFO:consumer:{4: ('32', '80.25')}
    INFO:consumer:{4: ('64', '121.91')}
    INFO:consumer:{4: ('128', '260.55')}
    INFO:consumer:{4: ('256', '497.78')}
    INFO:consumer:{4: ('512', '971.32')}
    INFO:consumer:{4: ('1024', '694.80')}
    INFO:consumer:{4: ('2048', '1281.18')}
    INFO:consumer:{4: ('4096', '2374.38')}
    INFO:parse_results 4:Done
    INFO:producer 4:Done
    INFO:main:at least one producer has exited
    INFO:main:Shutting down
    INFO:consumer:Done
    INFO:main:Done