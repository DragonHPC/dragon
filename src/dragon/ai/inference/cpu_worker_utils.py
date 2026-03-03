import dragon
import multiprocessing as mp
import time
import os
from queue import Empty
import socket
import logging

from ...infrastructure.policy import Policy
from ...native.process_group import ProcessGroup
from ...native.process import Process, ProcessTemplate, Popen
from ...dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from ...infrastructure.parameters import this_process
from .config import (
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)
from .inference_worker_utils import InferenceWorker


def setup_logging(type: str) -> logging.Logger:
    """Auxiliary function to initialize and return a logger.

    :param type: Custom identifier for the log file.
    :type type: str
    :returns: Configured logger instance.
    :rtype: logging.Logger
    """
    fname = f"INFERENCE_CPUWorker_{socket.gethostname()}_client_{str(this_process.my_puid)}_{str(type)}.log"
    setup_BE_logging(service=dls.PG, fname=fname)
    return logging.getLogger(str(dls.PG))


class CPUWorker:
    """The CPU worker class monitors prompt concurrency to dynamically spin-up and spin-down of inference workers assigned to it."""

    @staticmethod
    def entry_point(worker_args):
        """Static entry point used as the process target.

        :param worker_args: Dictionary containing all runtime args. Expected keys are ``hostname`` (str), ``inf_wrkr_config`` (list) and
            ``worker_kwargs`` (dict of kwargs for the :class:`CPUWorker` constructor).
        :type worker_args: dict
        """

        hostname = worker_args["hostname"]
        inf_wrkr_config = worker_args["inf_wrkr_config"]
        worker_kwargs = worker_args["worker_kwargs"]

        cpu_worker = CPUWorker(**worker_kwargs)
        cpu_worker.initialize(hostname, inf_wrkr_config)

    def __init__(
        self,
        input_queue,
        model_config: ModelConfig,
        batching_config: BatchingConfig,
        guardrails_config: GuardrailsConfig,
        dynamic_worker_config: DynamicWorkerConfig,
        num_inf_workers_per_cpu: int,
        end_event,
        cpu_barrier,
        dt,
    ) -> None:
        """Initialize a CPU worker.

        :param input_queue: Input queue to feed user-prompts into the backend service.
        :type input_queue: mp.Queue
        :param model_config: Model configuration including name, dtype, tokens, etc.
        :type model_config: ModelConfig
        :param batching_config: Batching configuration.
        :type batching_config: BatchingConfig
        :param guardrails_config: Guardrails/safety configuration.
        :type guardrails_config: GuardrailsConfig
        :param dynamic_worker_config: Dynamic worker spin up/down configuration.
        :type dynamic_worker_config: DynamicWorkerConfig
        :param num_inf_workers_per_cpu: Number of inference workers to be assigned per CPU head.
        :type num_inf_workers_per_cpu: int
        :param end_event: Primary event that terminates all processes.
        :type end_event: mp.Event
        :param cpu_barrier: Barrier used to wait until each CPU process spins up.
        :type cpu_barrier: mp.Barrier
        :param dt: Dragon telemetry object.
        :type dt: dragon.telemetry.telemetry.Telemetry
        """

        self.input_queue = input_queue
        self.end_event = end_event
        self.cpu_barrier = cpu_barrier
        self.dt = dt

        # Store config objects for access
        self.model_config = model_config
        self.batching_config = batching_config
        self.guardrails_config = guardrails_config
        self.dynamic_worker_config = dynamic_worker_config

        # Extract frequently used values for convenience
        self.dynamic_inf_wrkr_toggle = dynamic_worker_config.enabled
        self.spin_down_threshold = dynamic_worker_config.spin_down_threshold_seconds
        self.min_active_inf_workers_per_cpu_head = dynamic_worker_config.min_active_workers_per_cpu
        self.spin_up_threshold_seconds = dynamic_worker_config.spin_up_threshold_seconds
        self.spin_up_prompt_threshold = dynamic_worker_config.spin_up_prompt_threshold

        self.batch_toggle = batching_config.enabled
        self.batch_wait_time = batching_config.batch_wait_seconds
        self.batch_limit_max = batching_config.max_batch_size
        self.batch_type = batching_config.batch_type

        self.model_name = model_config.model_name
        self.dtype = model_config.dtype
        self.hf_token = model_config.hf_token
        self.kv_cache_max_tokens = model_config.max_tokens
        self.max_new_tokens = model_config.max_tokens
        self.num_inf_workers_for_each_cpu_head = num_inf_workers_per_cpu
        self.tp_size = model_config.tp_size

        self.prompt_guard_sensitivity = guardrails_config.prompt_guard_sensitivity
        self.prompt_guard_model = guardrails_config.prompt_guard_model
        self.prompt_guard_toggle = guardrails_config.enabled

        self.padding_side = model_config.padding_side
        self.truncation_side = model_config.truncation_side
        self.top_k = model_config.top_k
        self.top_p = model_config.top_p

        # Determine if preprocessing worker is needed: either guardrails or dynamic batching is enabled
        self.preprocessing_needed = self.prompt_guard_toggle or (self.batch_toggle and self.batch_type == "dynamic")

    def start_inf_workers(
        self,
        my_inf_workers,
        cpu_worker_pid,
        inf_wrkr_input_queue,
        inf_wrkr_manager_q,
        hostname,
        inf_wrkr_config,
    ):
        """Start inference workers and register them with this CPU worker.

        :param my_inf_workers: List of tuples of the form ``(inf_wrkr_down_ev, inf_wrkr_pg, inf_wrkr_id)`` that track
            existing inference workers.
        :type my_inf_workers: list
        :param cpu_worker_pid: Current CPU worker PID.
        :type cpu_worker_pid: int
        :param inf_wrkr_input_queue: Input queue from which inference workers pull user prompts.
        :type inf_wrkr_input_queue: mp.Queue
        :param inf_wrkr_manager_q: Queue of tuples of the form ``(hostname, devices, master_port, inf_wrkr_id)`` that describe
            available inference worker slots.
        :type inf_wrkr_manager_q: mp.Queue
        :param hostname: Hostname of the current CPU worker process.
        :type hostname: str
        :param inf_wrkr_config: List of tuples of the form ``(devices, master_port)`` defining each inference worker.
        :type inf_wrkr_config: list
        :returns: Updated list of inference worker tuples.
        :rtype: list
        """

        # Define barrier to initialize all inf-workers before starting infrencing (block cpu process)
        # breakdown of num_barriers as follows:
        # 1 = Barrier for this CPU head.
        # len(inf_wrkr_config) * 2 = Each inference worker is comprised of a pre-processing worker and gpu worker.
        num_barriers = 1 + len(inf_wrkr_config) * 2 if self.preprocessing_needed else 1 + len(inf_wrkr_config)
        self.log.info(f"\nCPU Head Module: Number of barriers created: {num_barriers=}")

        self.init_inf_wrkr_barrier = mp.Barrier(num_barriers)
        self.inf_wrkr_down_events = []

        # Create inference worker.
        inf_wrkr_id = 1
        for devices, master_port in inf_wrkr_config:
            llm_proc_end_ev = mp.Event()
            self.llm_proc_end_events.append(llm_proc_end_ev)

            output_queue = mp.Queue(maxsize=2)
            self.inf_wrkr_output_queues.append(output_queue)

            inf_wrkr_down_ev = mp.Event()
            self.inf_wrkr_down_events.append(
                inf_wrkr_down_ev
            )  # make sure that the refcount to each event is maintained

            inf_wrkr_pg = self.create_inf_worker(
                hostname,
                cpu_worker_pid,
                devices,
                master_port,
                inf_wrkr_input_queue,
                self.init_inf_wrkr_barrier,
                inf_wrkr_down_ev,
                inf_wrkr_id,
                inf_wrkr_manager_q,
                output_queue,
                llm_proc_end_ev,
            )

            self.log.info(
                f"\nCPU Head Module: Created new inf-wrkr. {hostname=} {cpu_worker_pid=} {devices=} {master_port=} {inf_wrkr_id=}"
            )
            my_inf_workers.append((inf_wrkr_down_ev, inf_wrkr_pg, inf_wrkr_id))
            inf_wrkr_id += 1

        # Wait for inf-worker to initialize before proceeding.
        self.init_inf_wrkr_barrier.wait()
        self.log.info(f"\nCPU Head Module: All infr-wrkrs have spun-up!")

        return my_inf_workers

    def dynamic_inf_workers(
        self,
        my_inf_workers,
        cpu_worker_pid,
        inf_wrkr_input_queue,
        inf_wrkr_manager_q,
        num_input_prompts_since_last_idle,
        idle_time_seconds,
    ):
        """Dynamically manage spinning up inference workers.

        :param my_inf_workers: List of tuples of the form ``(inf_wrkr_down_ev, inf_wrkr_pg, inf_wrkr_id)`` describing
            active inference workers.
        :type my_inf_workers: list
        :param cpu_worker_pid: Current CPU worker PID.
        :type cpu_worker_pid: int
        :param inf_wrkr_input_queue: Input queue from which inference workers pull user prompts.
        :type inf_wrkr_input_queue: mp.Queue
        :param inf_wrkr_manager_q: Queue of tuples of the form ``(hostname, devices, master_port, inf_wrkr_id)`` describing
            potential inference workers.
        :type inf_wrkr_manager_q: mp.Queue
        :param num_input_prompts_since_last_idle: Number of input prompts received since the last idle period.
        :type num_input_prompts_since_last_idle: int
        :param idle_time_seconds: Number of seconds since the last prompt.
        :type idle_time_seconds: float
        :returns: Tuple of the updated list of inference workers and the updated prompt counter since last idle time.
        :rtype: tuple[list, int]
        """
        # Close inf-worker pg if it has already spun-down.
        for ev, pg, inf_wrkr_id in my_inf_workers:
            if ev.is_set():
                pg.join()
                pg.stop()
                pg.close()

        # Remove inf-workers from list if it has spun-down.
        my_inf_workers = [(ev, pg, inf_worker_id) for ev, pg, inf_worker_id in my_inf_workers if not ev.is_set()]

        # If more user-prompts have been received in the last 'n' seconds than the defaults defined,
        # spin up a new inf-wrkr.

        if idle_time_seconds < self.spin_up_threshold_seconds:
            if num_input_prompts_since_last_idle >= self.spin_up_prompt_threshold:
                try:
                    hostname, devices, master_port, inf_wrkr_id = inf_wrkr_manager_q.get(timeout=0.1)
                    # Define barrier to initialize all inf-workers before starting infrencing.
                    # 3: One each for head cpu-worker, pre-processing worker, and llm worker.
                    num_barriers = 3 if self.preprocessing_needed else 2
                    dynamic_inf_wrkr_barrier = mp.Barrier(num_barriers)
                    inf_wrkr_down_ev = mp.Event()

                    llm_proc_end_ev = mp.Event()
                    self.llm_proc_end_events.append(llm_proc_end_ev)

                    output_queue = mp.Queue(maxsize=2)
                    self.inf_wrkr_output_queues.append(output_queue)

                    # Create inference worker.
                    inf_wrkr_pg = self.create_inf_worker(
                        hostname,
                        cpu_worker_pid,
                        devices,
                        master_port,
                        inf_wrkr_input_queue,
                        dynamic_inf_wrkr_barrier,
                        inf_wrkr_down_ev,
                        inf_wrkr_id,
                        inf_wrkr_manager_q,
                        output_queue,
                        llm_proc_end_ev,
                    )
                    my_inf_workers.append((inf_wrkr_down_ev, inf_wrkr_pg, inf_wrkr_id))

                    # Wait for inf-worker to initialize before proceeding.
                    dynamic_inf_wrkr_barrier.wait()
                except Empty:
                    pass
        else:
            num_input_prompts_since_last_idle = 1
        return my_inf_workers, num_input_prompts_since_last_idle

    def initialize(self, hostname, inf_wrkr_config):
        """Initialize the CPU-head worker and corresponding inference workers.

        :param hostname: Current process hostname.
        :type hostname: str
        :param inf_wrkr_config: List of tuples of the form ``(devices, master_port)`` for each inference worker.
        :type inf_wrkr_config: list
        """
        # Re-initialize mp dragon within process.
        mp.set_start_method("dragon")
        self.inf_wrkr_input_queue = mp.Queue()
        cpu_worker_pid = os.getpid()
        self.inf_wrkr_manager_q = mp.Queue()
        my_inf_workers = []

        # Maintain a list of all output queues for each inference worker.
        self.inf_wrkr_output_queues = []
        # Create per module events to ensure no module is running before breaking out of while loop.
        self.llm_proc_end_events = []

        # Initialize logger
        self.log = setup_logging(type=f"cpu-pid_{cpu_worker_pid}_part1")

        self.log.info("CPU Head: Logger initialized")
        self.log.info(f"CPU Head: {hostname=} {inf_wrkr_config=}")

        # Spin up all inference workers on start.
        my_inf_workers = self.start_inf_workers(
            my_inf_workers,
            cpu_worker_pid,
            self.inf_wrkr_input_queue,
            self.inf_wrkr_manager_q,
            hostname,
            inf_wrkr_config,
        )
        self.cpu_barrier.wait()
        self.log.info("CPU Head: All inf-wrkrs have been initialized.")

        num_input_prompts_since_last_idle = 0
        idle_start_time = time.time()

        # Wait for user response
        while True:
            try:
                # gets the prompt from the queue
                q_item = self.input_queue.get(timeout=1)
                # self.log.info(f'CPU Head: Collected New Item {q_item=}')

                idle_end_time = time.time()
                user_prompt = q_item[0]
                formatted_input = q_item[1]
                q = q_item[2]
                input_entry_timestamp = q_item[3]

                # Latency metrics.
                cpu_head_network_latency = round(time.time() - input_entry_timestamp, 2)
                tuple_latency_metric = (
                    input_entry_timestamp,
                    cpu_head_network_latency,
                    time.time(),
                )

                # Forward pass user-prompt to inference worker(s).
                self.inf_wrkr_input_queue.put((user_prompt, formatted_input, q, tuple_latency_metric))

                if self.dynamic_inf_wrkr_toggle:
                    num_input_prompts_since_last_idle += 1
                    cur_idle_time = idle_end_time - idle_start_time

                    # Dynamically manage spinning-up new inf workers (if available)
                    my_inf_workers, num_input_prompts_since_last_idle = self.dynamic_inf_workers(
                        my_inf_workers,
                        cpu_worker_pid,
                        self.inf_wrkr_input_queue,
                        self.inf_wrkr_manager_q,
                        num_input_prompts_since_last_idle,
                        cur_idle_time,
                    )
                    idle_start_time = time.time()

            except Empty:
                if self.end_event.is_set():
                    # if the queue is empty and the end event is set then we shut down
                    self.log.info("CPU Head Module: Shutting down cpu-head module")
                    self.destroy(my_inf_workers)
                    break
                else:
                    continue
            except Exception as e:
                self.log.info(f"CPU Head: Exception caught {e=}")

    def destroy(self, my_inf_workers):
        """Terminate all active inference workers assigned to this CPU head.

        :param my_inf_workers: List of tuples of the form
            ``(inf_wrkr_down_ev, inf_wrkr_pg, inf_wrkr_id)`` describing
            active inference workers.
        :type my_inf_workers: list
        """
        for ev, pg, inf_wrkr_id in my_inf_workers:
            pg.join()
            pg.stop()
            pg.close()

    def create_inf_worker(
        self,
        hostname,
        cpu_worker_pid,
        devices,
        master_port,
        inf_worker_queue,
        inf_wrkr_barrier,
        inf_wrkr_down_ev,
        inf_wrkr_id,
        inf_wrkr_manager_q,
        output_queue,
        llm_proc_end_ev,
    ):
        """Spin up an inference worker process group.

        :param hostname: Current process hostname.
        :type hostname: str
        :param cpu_worker_pid: CPU worker PID.
        :type cpu_worker_pid: int
        :param devices: List of GPU ranks for the current inference worker.
        :type devices: list[int]
        :param master_port: Master port assigned to the inference worker.
        :type master_port: str
        :param inf_worker_queue: Input queue from which the inference worker pulls user prompts.
        :type inf_worker_queue: mp.Queue
        :param inf_wrkr_barrier: Barrier used to wait until all inference worker modules are spun up and ready.
        :type inf_wrkr_barrier: mp.Barrier
        :param inf_wrkr_down_ev: Event used to signal that the inference worker should spin down.
        :type inf_wrkr_down_ev: mp.Event
        :param inf_wrkr_id: Unique identifier for the current inference worker.
        :type inf_wrkr_id: int
        :param inf_wrkr_manager_q: Queue of tuples of the form ``(hostname, devices, master_port, inf_wrkr_id)`` used to manage
            available inference workers.
        :type inf_wrkr_manager_q: mp.Queue
        :param output_queue: Output queue where the inference worker pushes LLM responses.
        :type output_queue: mp.Queue
        :param llm_proc_end_ev: Event used to denote that the LLM process has ended.
        :type llm_proc_end_ev: mp.Event
        :returns: Process group comprising the inference worker (pre-processing process and tensor-parallel GPU process(es)).
        :rtype: ProcessGroup
        """

        # Create inference-worker process group
        placement_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=hostname)
        inf_worker_grp = ProcessGroup(restart=False, policy=placement_policy)
        run_dir = os.getcwd()

        # Build complete InferenceWorker args including all runtime parameters.
        # Both preprocessing and LLM workers use the same args structure.
        # Preprocessing worker ignores LLM-specific fields (master_port, inf_wrkr_down_ev, inf_wrkr_manager_q).
        inference_worker_args = {
            # Config objects
            "end_event": self.end_event,
            "model_config": self.model_config,
            "batching_config": self.batching_config,
            "guardrails_config": self.guardrails_config,
            "dynamic_worker_config": self.dynamic_worker_config,
            "dt": self.dt,
            # Runtime parameters
            "hostname": hostname,
            "devices": devices,
            "head_cpu_pid": cpu_worker_pid,
            "inf_wrkr_id": inf_wrkr_id,
            "preprocessing_input_queue": inf_worker_queue,
            "preprocessing_output_queue": output_queue,
            "inf_wrkr_barrier": inf_wrkr_barrier,
            "llm_proc_end_ev": llm_proc_end_ev,
            "master_port": master_port,
            "inf_wrkr_down_ev": inf_wrkr_down_ev,
            "inf_wrkr_manager_q": inf_wrkr_manager_q,
        }

        # Add pre-processing worker process - only if guardrails module is enabled
        # or dynamic batching is enabled.
        if self.preprocessing_needed:
            inf_worker_grp.add_process(
                nproc=1,
                template=ProcessTemplate(
                    target=InferenceWorker.preprocessing_entry_point,
                    args=(inference_worker_args,),
                    cwd=run_dir,
                    stdout=Popen.PIPE,
                ),
            )

        # Add vllm worker process.
        local_policy = Policy(gpu_affinity=devices)

        inf_worker_grp.add_process(
            nproc=1,
            template=ProcessTemplate(
                target=InferenceWorker.llm_inference_entry_point,
                args=(inference_worker_args,),
                cwd=run_dir,
                stdout=Popen.DEVNULL,
                policy=local_policy,
            ),
        )

        # Start process-group
        inf_worker_grp.init()
        inf_worker_grp.start()
        return inf_worker_grp
