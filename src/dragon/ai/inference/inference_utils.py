"""Top-level orchestration for the Dragon inference service.

The :class:`Inference` class discovers the Dragon allocation, validates the
requested model and hardware configuration, partitions nodes and GPUs into
CPU-head workers and tensor-parallel inference workers, and owns the backend
process-group lifecycle.  It exposes a small lifecycle API:

* construct with an :class:`~dragon.ai.inference.InferenceConfig` and a
    shared input queue,
* call :meth:`Inference.initialize` to start workers,
* submit low-level requests with :meth:`Inference.query`, and
* call :meth:`Inference.destroy` to stop workers and close the queue.
"""

import os
from ...infrastructure.policy import Policy
from ...native.process_group import ProcessGroup
from ...native.process import Process, ProcessTemplate, Popen
from ...native.machine import System, Node
from ...native.event import Event
from ...native.barrier import Barrier
from ...telemetry.telemetry import Telemetry
import sys
import time
from .cpu_worker_utils import CPUWorker
from .config import InferenceConfig
from pprint import pprint


class Inference:
    """
    This class is the starting point for initializing the inference pipeline.
    It provides distributed multi-gpu & multi-node inference capabilities for low-latency
    high throughput GenAI inference. Furthermore, inference batching feature for optimizing performance
    is enabled, along with dynamic inference worker spin-up and spin-down capabilities to reduce
    power consumption and carbon emissions is also enabled.
    """

    def __init__(self, config: InferenceConfig, input_queue) -> None:
        """Initialize a :class:`Inference` instance.

        :param config: Type-safe configuration object.
        :type config: InferenceConfig
        :param input_queue: Input queue that feeds user prompts into the
            backend service.
        :type input_queue: dragon.native.Queue
        """

        # Store config for access by other methods
        self.config = config

        # Required parameters - extracted from ModelConfig
        self.model_name = config.model.model_name
        self.hf_token = config.model.hf_token
        self.tp_size = config.model.tp_size
        self.run_type = config.run_type

        # Initialize user-prompt input queue
        self.end_event = Event()
        self.input_queue = input_queue

        # Input batching args - extracted from BatchingConfig
        self.batch_toggle = config.batching.enabled
        self.batch_type = config.batching.batch_type
        self.batch_wait_time = (
            config.batching.batch_wait_seconds if self.batch_toggle else 0.1
        )
        self.batch_limit_max = (
            config.batching.max_batch_size if self.batch_toggle else 1
        )

        # Prompt Guard Module parameters - extracted from GuardrailsConfig
        self.prompt_guard_toggle = config.guardrails.enabled
        self.prompt_guard_sensitivity = (
            config.guardrails.prompt_guard_sensitivity
            if self.prompt_guard_toggle
            else None
        )
        self.prompt_guard_model = (
            config.guardrails.prompt_guard_model if self.prompt_guard_toggle else None
        )

        # Dynamic inf-worker args - extracted from DynamicWorkerConfig
        self.dynamic_inf_wrkr_toggle = config.dynamic_worker.enabled
        self.min_active_inf_workers_per_cpu_head = (
            config.dynamic_worker.min_active_workers_per_cpu
            if self.dynamic_inf_wrkr_toggle
            else None
        )
        self.spin_down_threshold = (
            config.dynamic_worker.spin_down_threshold_seconds
            if self.dynamic_inf_wrkr_toggle
            else None
        )
        self.spin_up_threshold_seconds = (
            config.dynamic_worker.spin_up_threshold_seconds
            if self.dynamic_inf_wrkr_toggle
            else None
        )
        self.spin_up_prompt_threshold = (
            config.dynamic_worker.spin_up_prompt_threshold
            if self.dynamic_inf_wrkr_toggle
            else None
        )

        # LLM Module parameters - extracted from ModelConfig
        self.dtype = config.model.dtype
        self.kv_cache_max_tokens = config.model.max_tokens
        self.max_new_tokens = config.model.max_tokens
        self.run_dir = os.getcwd()
        self.padding_side = config.model.padding_side
        self.truncation_side = config.model.truncation_side
        self.top_k = config.model.top_k
        self.top_p = config.model.top_p
        self.system_prompt = config.model.system_prompt

        # Infra/Setup parameters - extracted from HardwareConfig
        self.nnodes = config.hardware.num_nodes
        self.node_offset = config.hardware.node_offset
        self.ngpus = config.hardware.num_gpus

        # Get all nodes in allocation
        self.all_nodes = self.get_nodes_in_alloc()

        # Validate configuration against available resources
        config.validate_all(self.all_nodes)

        # Subset nodes & gpus if user-specified.
        self.nodes, resolved_gpus = self.maybe_subset_nodes_gpus()

        # Calculate num_inf_workers_per_cpu after GPU count is resolved
        self.num_inf_workers_for_each_cpu_head = config.hardware.calculate_inf_workers_per_cpu(
            resolved_gpus, self.tp_size
        )
        if config.hardware.num_inf_workers_per_cpu == -1:
            print(
                f"Auto-calculated num_inf_workers_per_cpu: {self.num_inf_workers_for_each_cpu_head} "
                f"(gpus={resolved_gpus}, tp_size={self.tp_size})",
                flush=True,
            )

        # Create cpu and device dict by hostname affinity.
        self.cpu_and_device_proc_by_hostname, self.cpu_nprocs = (
            self.create_cpu_device_workers_by_node()
        )

        # Create cpu workers process-group
        self.cpu_wrks_pg = ProcessGroup(restart=False)

        # Define barrier to initialize all cpu-workers before starting inferencing
        num_barriers = 1 + self.cpu_nprocs
        self.cpu_barrier = Barrier(num_barriers)

        # Create dragon telemetry procs.
        self.dt = Telemetry()

        # Calculate inf_wrkr_queue_maxsize (default: num_inf_workers * 2)
        if config.hardware.inf_wrkr_queue_maxsize == -1:
            self.inf_wrkr_queue_maxsize = self.num_inf_workers_for_each_cpu_head * 2
        else:
            self.inf_wrkr_queue_maxsize = config.hardware.inf_wrkr_queue_maxsize

        # Store CPU worker constructor kwargs (shared across CPU worker processes)
        self.cpu_worker_kwargs = {
            "input_queue": self.input_queue,
            "model_config": config.model,
            "batching_config": config.batching,
            "guardrails_config": config.guardrails,
            "dynamic_worker_config": config.dynamic_worker,
            "num_inf_workers_per_cpu": self.num_inf_workers_for_each_cpu_head,
            "inf_wrkr_queue_maxsize": self.inf_wrkr_queue_maxsize,
            "end_event": self.end_event,
            "cpu_barrier": self.cpu_barrier,
            "dt": self.dt,
        }

    def get_nodes_in_alloc(self):
        """Get all nodes in current dragon enabled allocation.

        :returns: Dictionary of all available nodes in the allocation.
            Keys are hostnames, values are ``dragon.native.machine.Node``
            objects.
        :rtype: dict
        """
        my_alloc = System()
        node_list = my_alloc.nodes
        all_nodes = {}
        for node_id in node_list:
            node = Node(node_id)
            all_nodes[node.hostname] = node
        return all_nodes

    def maybe_subset_nodes_gpus(self):
        """
        If the number of Node(s) and GPU(s) specified in config.yaml are
        different than default (full utilization), subset the nodes and
        GPUs accordingly.

        :returns: Tuple of (nodes_dict, gpus_per_node) where nodes_dict maps
            (hostname, Dragon Node) tuples to GPU ranks, and gpus_per_node
            is the number of GPUs per node after subsetting.
        :rtype: tuple[dict, int]
        """
        # Maybe subset nodes
        all_nodes = self.all_nodes
        num_nodes_in_alloc = len(all_nodes)
        if self.nnodes == -1:
            print(f"Using all {num_nodes_in_alloc} nodes in allocation.", flush=True)
        else:
            print(
                f"Using {self.nnodes} of {num_nodes_in_alloc} node(s) in allocation.",
                flush=True,
            )
            start = self.node_offset
            end = self.node_offset + self.nnodes
            all_nodes = {
                key: all_nodes[key] for key in list(all_nodes.keys())[start:end]
            }

        # Maybe subset gpus
        my_nodes = {}
        for hostname, node in all_nodes.items():
            if self.ngpus == -1:
                devices = node.gpus
            else:
                devices = node.gpus[0 : self.ngpus]

            # Validate tensor-parallel input arg by node
            self.tp_args_validator(len(devices))
            my_nodes[(hostname, node)] = devices

        # Return nodes dict and GPU count (assumes homogeneous cluster)
        gpus_per_node = len(devices) if my_nodes else 0
        return my_nodes, gpus_per_node

    def tp_args_validator(self, app_gpus):
        """Validate tensor-parallel size against GPUs available per node.

        :param app_gpus: Number of GPUs available in each node.
        :type app_gpus: int
        """
        # Validate tensor parallelism user arg
        if self.tp_size > app_gpus:
            print(
                f"Tensor Parallelism ({self.tp_size}) cannot be greater than the number of GPUs requested/available ({app_gpus}).",
                flush=True,
            )
            sys.exit(1)

    def create_cpu_device_workers_by_node(self):
        """Create CPU-head workers and associated inference workers by node.

        :returns: Tuple ``(cpu_and_device_proc_by_hostname, num_cpu_procs)``
            where ``cpu_and_device_proc_by_hostname`` maps hostnames to
            dictionaries of CPU-worker IDs and their inference worker
            configurations, and ``num_cpu_procs`` is the total number of CPU
            processes.
        :rtype: tuple[dict, int]
        """
        # Create inference workers by node.
        cpu_and_device_proc_by_hostname = {}
        num_cpu_procs = 0

        for (hostname, node), devices in self.nodes.items():
            num_inf_wrkrs = 0
            cpu_wrkr_id = 0
            cpu_and_device_proc_by_hostname[hostname] = {}
            inf_wrkr_config = []
            for i in range(
                0, len(devices) - (len(devices) % self.tp_size), self.tp_size
            ):

                if num_inf_wrkrs >= self.num_inf_workers_for_each_cpu_head:
                    cpu_wrkr_id += 1
                    my_dict = cpu_and_device_proc_by_hostname[hostname]
                    my_dict[f"{cpu_wrkr_id=}"] = inf_wrkr_config
                    cpu_and_device_proc_by_hostname[hostname] = my_dict
                    num_cpu_procs += 1
                    inf_wrkr_config = []
                    num_inf_wrkrs = 0

                cur_inf_worker = devices[i : i + self.tp_size]
                num_inf_wrkrs += 1
                inf_wrkr_config.append(cur_inf_worker)

            cpu_wrkr_id += 1
            num_cpu_procs += 1
            my_dict = cpu_and_device_proc_by_hostname[hostname]
            my_dict[f"{cpu_wrkr_id=}"] = inf_wrkr_config
            cpu_and_device_proc_by_hostname[hostname] = my_dict

        pprint(cpu_and_device_proc_by_hostname)
        sys.stdout.flush()
        print(f"\nThe total number of CPU procs: {num_cpu_procs=}", flush=True)
        return cpu_and_device_proc_by_hostname, num_cpu_procs

    def query(self, q_item):
        """Queries the dragon-inference application to generate a response from the
        GenAI model.

        :param q_item: Tuple of the form ``(user_input, response_queue)``
            where ``user_input`` is a string or list of strings and
            ``response_queue`` is an ``dragon.native.Queue`` used to receive responses.
        :type q_item: tuple
        """
        start_time = time.time()

        prompt = q_item[0]
        response_q = q_item[1]

        def _build_messages(user_prompt: str) -> list:
            """Build OpenAI-format message list for a single prompt.

            The worker will apply the model's chat template using vLLM's
            already-loaded tokenizer, avoiding redundant tokenizer loads.
            """
            sp = (
                " ".join(self.system_prompt)
                if isinstance(self.system_prompt, list)
                else self.system_prompt
            )
            return [
                {"role": "system", "content": sp},
                {"role": "user", "content": user_prompt},
            ]

        # Build message dicts — formatting happens on the worker using vLLM's tokenizer.
        # No batching or single inputs that are dynamically batched in the backend.
        if self.batch_toggle is False or self.batch_type == "dynamic":
            formatted_prompt = _build_messages(prompt)
        elif self.batch_type == "pre-batch":  # Already pre-batched list of inputs.
            formatted_prompt = [_build_messages(item) for item in prompt]

        item = (prompt, formatted_prompt, response_q, start_time)
        self.input_queue.put(item)

    def initialize(self):
        """Initializes the backend services to spin up the GenAI inference application."""
        for hostname, my_dict in self.cpu_and_device_proc_by_hostname.items():
            num_cpu_procs_per_hostname = len(my_dict)

            # Assign each cpu-head process to its localized node for faster communication with GPU devices.
            cpu_hostname_policy = Policy(
                placement=Policy.Placement.HOST_NAME, host_name=hostname
            )

            for cpu_wrkr_id in range(1, num_cpu_procs_per_hostname + 1):

                inf_wrkr_config = my_dict[f"{cpu_wrkr_id=}"]
                cpu_args = {
                    "hostname": hostname,
                    "inf_wrkr_config": inf_wrkr_config,
                    "worker_kwargs": self.cpu_worker_kwargs,
                }

                # Add head cpu worker(s)
                self.cpu_wrks_pg.add_process(
                    nproc=1,
                    template=ProcessTemplate(
                        target=CPUWorker.entry_point,
                        args=(cpu_args,),
                        cwd=self.run_dir,
                        policy=cpu_hostname_policy,
                    ),
                )

        # Start process-group
        self.cpu_wrks_pg.init()
        self.cpu_wrks_pg.start()

        # Wait until all processes are in ready state.
        self.cpu_barrier.wait()
        print("\n\n\nPipeline Initialized!!\n\n\n", flush=True)

    def destroy(self):
        """Destroys all spun-up processes and terminates the application."""
        self.end_event.set()
        self.cpu_wrks_pg.join()
        self.cpu_wrks_pg.stop()
        self.cpu_wrks_pg.close()
        self.input_queue.close()
