import dragon
import multiprocessing as mp
import socket
import os
import sys
import math
import time
from pprint import pprint

from ...infrastructure.policy import Policy
from ...native.process_group import ProcessGroup
from ...native.process import ProcessTemplate, Popen
from ...native.machine import System, Node
from ...telemetry.telemetry import Telemetry
from .cpu_worker_utils import CPUWorker
from .config import InferenceConfig
from .llm_engine import chat_template_formatter


class Inference:
    """
    This class is the starting point for initializing the inference pipeline.
    It provides distributed multi-gpu & multi-node inference capabilities for low-latency
    high throughput GenAI inference. Furthermore, inference batching feature for optimizing performance
    is enabled, along with dynamic inference worker spin-up and spin-down capabilities to reduce
    power consumption and carbon emissions is also enabled.
    """

    def __init__(self, config: InferenceConfig, num_nodes, offset, input_queue) -> None:
        """Initialize a :class:`Inference` instance.

        :param config: Type-safe configuration object.
        :type config: InferenceConfig
        :param num_nodes: Number of nodes to use for inference.
        :type num_nodes: int
        :param offset: Offset used to subset nodes from the allocation.
        :type offset: int
        :param input_queue: Input queue that feeds user prompts into the
            backend service.
        :type input_queue: mp.Queue
        """

        # Store config for access by other methods
        self.config = config

        # Required parameters - extracted from ModelConfig
        self.model_name = config.model.model_name
        self.hf_token = config.model.hf_token
        self.tp_size = config.model.tp_size

        # Initialize user-prompt input queue
        self.end_event = mp.Event()
        self.input_queue = input_queue

        # Input batching args - extracted from BatchingConfig
        self.batch_toggle = config.batching.enabled
        self.batch_type = config.batching.batch_type
        self.batch_wait_time = config.batching.batch_wait_seconds if self.batch_toggle else 0.1
        self.batch_limit_max = config.batching.max_batch_size if self.batch_toggle else 1

        # Prompt Guard Module parameters - extracted from GuardrailsConfig
        self.prompt_guard_toggle = config.guardrails.enabled
        self.prompt_guard_sensitivity = config.guardrails.prompt_guard_sensitivity if self.prompt_guard_toggle else None
        self.prompt_guard_model = config.guardrails.prompt_guard_model if self.prompt_guard_toggle else None

        # Dynamic inf-worker args - extracted from DynamicWorkerConfig
        self.dynamic_inf_wrkr_toggle = config.dynamic_worker.enabled
        self.min_active_inf_workers_per_cpu_head = (
            config.dynamic_worker.min_active_workers_per_cpu if self.dynamic_inf_wrkr_toggle else None
        )
        self.spin_down_threshold = (
            config.dynamic_worker.spin_down_threshold_seconds if self.dynamic_inf_wrkr_toggle else None
        )
        self.spin_up_threshold_seconds = (
            config.dynamic_worker.spin_up_threshold_seconds if self.dynamic_inf_wrkr_toggle else None
        )
        self.spin_up_prompt_threshold = (
            config.dynamic_worker.spin_up_prompt_threshold if self.dynamic_inf_wrkr_toggle else None
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
        self.nnodes = num_nodes
        self.offset = offset
        self.ngpus = config.hardware.num_gpus
        self.num_inf_workers_for_each_cpu_head = config.hardware.num_inf_workers_per_cpu

        self.in_use_ports = []

        # Get all nodes in allocation
        self.all_nodes = self.get_nodes_in_alloc()

        # Validate num_nodes parameter
        available_nodes = len(self.all_nodes)
        if self.nnodes != -1:
            if self.nnodes <= 0:
                raise ValueError(f"num_nodes must be >= 1, got {self.nnodes}")
            if self.nnodes > available_nodes:
                raise ValueError(f"Requested {self.nnodes} nodes but only {available_nodes} available")

        # Validate configuration against available resources
        config.validate_all(self.all_nodes)

        # Subset nodes & gpus if user-specified.
        self.nodes = self.maybe_subset_nodes_gpus()

        # Create cpu and device dict by hostname affinity.
        self.cpu_and_device_proc_by_hostname, self.cpu_nprocs = self.create_cpu_device_workers_by_node()

        # Create cpu workers process-group
        self.cpu_wrks_pg = ProcessGroup(restart=False)

        # Define barrier to initialize all cpu-workers before starting inferencing
        num_barriers = 1 + self.cpu_nprocs
        self.cpu_barrier = mp.Barrier(num_barriers)

        # Create dragon telemetry procs.
        self.dt = Telemetry()

        # Store CPU worker constructor kwargs (shared across CPU worker processes)
        self.cpu_worker_kwargs = {
            "input_queue": self.input_queue,
            "model_config": config.model,
            "batching_config": config.batching,
            "guardrails_config": config.guardrails,
            "dynamic_worker_config": config.dynamic_worker,
            "num_inf_workers_per_cpu": self.num_inf_workers_for_each_cpu_head,
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

        :returns: Dictionary mapping (hostname, Dragon Node) tuples to GPU ranks.
        :rtype: dict
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
            start = self.offset
            end = self.offset + self.nnodes
            all_nodes = {key: all_nodes[key] for key in list(all_nodes.keys())[start:end]}

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
        return my_nodes

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

    def get_master_port(self, base_port=29500, port_range_size=1000) -> str:
        """Return the first available TCP port in a given range.

        :param base_port: Starting port of the search range.
        :type base_port: int
        :param port_range_size: Size of the port range to scan.
        :type port_range_size: int
        :raises IOError: If no free port is found in the range.
        :returns: A free port as a string.
        :rtype: str
        """
        port = base_port
        max_port = base_port + port_range_size
        sock = socket.socket()
        while port < max_port:
            try:
                if str(port) not in self.in_use_ports:
                    sock.bind(("", port))
                    sock.close()
                    self.in_use_ports.append(str(port))
                    return str(port)
                else:
                    port += 1
            except OSError:
                port += 1
        raise IOError("no free ports")

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
            for i in range(0, len(devices) - (len(devices) % self.tp_size), self.tp_size):

                if num_inf_wrkrs >= self.num_inf_workers_for_each_cpu_head:
                    cpu_wrkr_id += 1
                    my_dict = cpu_and_device_proc_by_hostname[hostname]
                    my_dict[f"{cpu_wrkr_id=}"] = inf_wrkr_config
                    cpu_and_device_proc_by_hostname[hostname] = my_dict
                    num_cpu_procs += 1
                    inf_wrkr_config = []
                    num_inf_wrkrs = 0

                cur_inf_worker = devices[i : i + self.tp_size]
                ports_list = self.get_master_port()
                num_inf_wrkrs += 1
                inf_wrkr_config.append((cur_inf_worker, ports_list))

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
            ``response_queue`` is an ``mp.Queue`` used to receive responses.
        :type q_item: tuple
        """
        start_time = time.time()

        prompt = q_item[0]
        response_q = q_item[1]

        # No batching or single inputs that are dynamically batched in the backend.
        # Format them individually.
        if self.batch_toggle is False or self.batch_type == "dynamic":
            formatted_prompt = chat_template_formatter(self.system_prompt, prompt, [], self.model_name)
        elif self.batch_type == "pre-batch":  # Already pre-batched list of inputs.
            formatted_prompt = []
            for item in prompt:
                formatted_prompt.append(chat_template_formatter(self.system_prompt, item, [], self.model_name))
        item = (prompt, formatted_prompt, response_q, start_time)
        self.input_queue.put(item)

    def initialize(self):
        """Initializes the backend services to spin up the GenAI inference application."""
        for hostname, my_dict in self.cpu_and_device_proc_by_hostname.items():
            num_cpu_procs_per_hostname = len(my_dict)

            # Assign each cpu-head process to its localized node for faster communication with GPU devices.
            cpu_hostname_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=hostname)

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
                        stdout=Popen.PIPE,
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
