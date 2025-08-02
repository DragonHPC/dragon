import os
import re
import logging
import queue

import dragon
import multiprocessing as mp

from dragon.channels import Channel
from dragon.infrastructure.connection import Connection
from dragon.globalservices import node
from dragon.globalservices import group
from dragon.globalservices import process
from dragon.globalservices import policy_eval
from dragon.infrastructure import process_desc
from dragon.infrastructure.facts import PMIBackend
from dragon.utils import B64

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

    log = logging.getLogger(f"producer {producer_id}")
    log.info("Starting producer (num_ranks=%d)", num_ranks)

    exe = os.path.join(os.getcwd(), "osu_alltoall")
    args = ["--warmup", "10", "-m", "4096"]
    run_dir = os.getcwd()

    # Pipe the stdout output from the head process to a Dragon connection
    rank_that_prints_template = process.get_create_message(
        exe=exe, run_dir=run_dir, args=args, pmi=PMIBackend.CRAY, env=None, stdout=process.PIPE
    )

    # All other ranks should have their output go to DEVNULL
    ranks_that_dont_print_template = process.get_create_message(
        exe=exe, run_dir=run_dir, args=args, pmi=PMIBackend.CRAY, env=None, stdout=process.DEVNULL
    )

    # Establish the list and number of process ranks that should be started
    group_items = [
        (1, rank_that_prints_template.serialize()),
        (num_ranks - 1, ranks_that_dont_print_template.serialize()),
    ]

    # Ask Dragon to create the process group
    grp = group.create(items=group_items, policy=policy_eval.Policy(), soft=False)

    # Inpect the returned group descriptor
    group_puids = []
    parser_proc = None
    ranks_per_node = {n: 0 for n in range(len(node.get_list()))}
    for resources in grp.sets:
        group_puids.extend(
            [
                resource.desc.p_uid
                for resource in resources
                if resource.desc.state == process_desc.ProcessDescriptor.State.ACTIVE
            ]
        )

        for resource in resources:
            ranks_per_node[resource.placement] = ranks_per_node[resource.placement] + 1
            if resource.desc.stdout_sdesc:
                log.info("Starting parse_results process for p_uid=%d", resource.desc.p_uid)
                parser_proc = mp.Process(
                    target=parse_results_proc, args=(producer_id, resource.desc.stdout_sdesc, result_queue)
                )
                parser_proc.start()

    log.info(", ".join([f"node {n} has {r} ranks" for n, r in ranks_per_node.items()]))
    log.info("Waiting for group to finish")
    if len(group_puids) > 0:
        process.multi_join(group_puids, join_all=True)

    if parser_proc:
        parser_proc.join()

    grp = group.destroy(grp.g_uid)

    log.info("Done")


def parse_results_proc(producer_id: int, stdout_sdesc: str, result_queue: mp.Queue) -> None:
    """
    Read stdout from the Dragon connection. Parse statistical data
    and put onto result queue.

    :param producer_id: Numerical id identifying this producer
    :type producer_id: int
    :param stdout_conn: Dragon serialized Channel descriptor used to read stdout data
    :type stdout_conn: str
    :param result_queue: Handle to a queue where the results should be placed
    :type result_queue: mp.Queue
    """

    log = logging.getLogger(f"parse_results {producer_id}")
    log.info("Parsing stdout from stdout connection")

    ch = Channel.attach(B64.from_str(stdout_sdesc).decode())
    conn = Connection(inbound_initializer=ch)

    try:
        result_matcher = re.compile(r"^(\d+)\s+([\d.]+)")
        while True:
            line = conn.recv()
            result = result_matcher.search(line)
            if result:
                result_queue.put({producer_id: (result[1], result[2])})
    except EOFError:
        pass

    ch.detach()
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
    consumer = mp.Process(target=consumer_proc, args=(result_queue, shutdown_event))
    consumer.start()

    producers = set()
    active_producers = 0
    while current_runs < total_runs:
        while active_producers < min(simultaneous_producers, total_runs - current_runs):
            log.info("Starting a new producer")
            producer = mp.Process(target=producer_proc, args=(producer_num, ranks_per_job, result_queue))
            producer.start()
            producers.add(producer.pid)
            active_producers += 1
            producer_num += 1

        exited_list, _ = process.multi_join(producers, join_all=False)
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
