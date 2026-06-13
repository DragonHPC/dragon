import os
import re
import logging
import queue

import dragon
import multiprocessing as mp

from dragon.globalservices import node
from dragon.globalservices.process import multi_join
from dragon.infrastructure.connection import Connection
from dragon.infrastructure.facts import PMIBackend
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

    grp = ProcessGroup(restart=False, pmi=PMIBackend.CRAY)

    # Pipe the stdout output from the head process to a Dragon connection
    grp.add_process(nproc=1, template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=MSG_PIPE))

    # All other ranks should have their output go to DEVNULL
    grp.add_process(
        nproc=num_ranks - 1, template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=MSG_DEVNULL)
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
                target=parse_results_proc, args=(producer_id, child_resource.stdout_conn, result_queue)
            )
            parser_proc.start()

    log.info(", ".join([f"node {n} has {r} ranks" for n, r in ranks_per_node.items()]))
    log.info("Waiting for group to finish")
    if len(child_resources) > 0:
        grp.join()

    if parser_proc:
        parser_proc.join()

    grp.stop()
    grp.close()

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
                result_queue.put({producer_id: (result[1], result[2])})
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
    reserved_cores = num_nodes * 2  # Reserve a couple of cores for Dragon infrastructure
    num_real_cores = mp.cpu_count() // 2
    ranks_per_job = (num_real_cores - reserved_cores) // simultaneous_producers

    shutdown_event = mp.Event()
    log.info("Starting consumer process")
    consumer = Process(target=consumer_proc, args=(result_queue, shutdown_event))
    consumer.start()

    producers = set()
    active_producers = 0
    while current_runs < total_runs:
        while active_producers < min(simultaneous_producers, total_runs - current_runs):
            log.info("Starting a new producer")
            producer = Process(target=producer_proc, args=(producer_num, ranks_per_job, result_queue))
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
