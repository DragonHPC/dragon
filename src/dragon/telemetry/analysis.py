"""Dragon's API to access telemetry data from user space"""

from dragon.infrastructure.parameters import this_process
from abc import ABC, abstractmethod
import time
import logging
import cloudpickle
import socket
import os
from dragon.dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from dragon.utils import get_local_kv, B64
from dragon.native.queue import Queue
from dragon.globalservices.process import runtime_reboot
import queue

from dragon.native.process_group import ProcessGroup
from dragon.native.process import Process, ProcessTemplate
from dragon.native.event import Event
from dragon.native.machine import System, current
from dragon.infrastructure.policy import Policy
from dragon.telemetry.telemetry import Telemetry

import dragon.infrastructure.parameters as di_param

LS_TAS_KEY = "telemetry_analysis_queue"
log = None


def _setup_be_logging(fname):
    # This block turns on a client log for each client
    global log
    if log is None:
        setup_BE_logging(service=dls.TELEM, fname=fname)
        log = logging.getLogger(str(dls.TELEM))


class AnalysisClient:
    """
    This is the main user interface to access telmetery data. The client requests data from a server that is started when the telemetry component of the runtime is brought up. Multiple clients can request data from the server. The client API also has a method to reboot the runtime. This is useful for when a user wants to remove a node from the runtime. The reboot will cause the runtime to tear down and restart with the specified nodes removed.

    Example usage:

    .. highlight:: python
    .. code-block:: python

        import dragon
        from dragon.telemetry import AnalysisClient, Telemetry

        if __name__ == "__main__":
            dt = Telemetry()
            dac = AnalysisClient()
            dac.connect()

            # while doing some GPU work

            metrics = dac.get_metrics()

            if "DeviceUtilization" not in metrics:
                return

            data = dac.get_data("DeviceUtilization")

            averages = []
            for data_dict in data:
                avg = sum(data_dict["dps"].values())/len(data_dict["dps"]):
                averages.append(avg)

            worst = min(averages)
            worst_node_idx = averages.index(worst)
            node_to_remove = data_dict[worst_node_idx]["tags"]["host"]

            print(f"Worst Node: {node_to_remove} with minimum average value = {worst}",flush=True)

            # restart the program with the worst performing node removed
            dac.reboot(exclude_hostnames=node_to_remove)

            dt.finalize()
    """

    def __init__(self):
        self._data_queue = Queue()
        self._request_conn = None

    def _setup_logging(self):
        # This block turns on a client log for each client
        fname = f"{dls.TELEM}_{socket.gethostname()}_client_{str(this_process.my_puid)}.log"
        _setup_be_logging(fname)

    def connect(self, timeout: int = None) -> None:
        """A user is required to connect to the server before requesting data. By connecting, a user can add requests to the server's request queue. A timeout can be provided to wait for the connection.

        :param timeout: user provided timeout for getting server request queue. Without a timeout this is a blocking call, defaults to None
        :type timeout: int, optional
        :raises RuntimeError: if the connection request cannot be completed in the alotted time.
        """
        self._setup_logging()

        try:
            analysis_comm_sdesc = get_local_kv(key=LS_TAS_KEY, timeout=timeout)

            self._request_conn = cloudpickle.loads(B64.str_to_bytes(analysis_comm_sdesc))
        except Exception as e:
            raise RuntimeError(f"Unable to get requeuest queue due to the following exception:\n {e}\n")

    def get_data(self, metrics: str or list, tags: str or list = None, start_time: int = None) -> list:
        """Gathers telmetery data from every node in the allocation for the given metric(s) after the specified start time.

        :param metrics: a metric or list of metrics to gather data for
        :type metrics: str or list
        :param start_time: the time after which the user wants data collected. By default we will return the last five minutes of data, defaults to None
        :type start_time: int, optional
        :raises RuntimeError: raised if the user hasn't connected to the server
        :raises AttributeError: raised if the metric is neither a string nor list
        :return: a list containing dictionaries with the response from each node.
        :rtype: list
        """
        if self._request_conn is None:
            raise RuntimeError("Unable to make a request without first connecting to the server.")
        # if a start time isn't given get data from the last 5 minutes
        if start_time is None:
            start_time = int(time.time()) - 5 * 60

        temp_request = {"type": "query", "start_time": start_time}

        tags_list = None
        if tags is None:
            tags_list = ["host"]
        elif isinstance(tags, str):
            tags_list = [tags]
        elif isinstance(tags, list):
            tags_list = tags
        else:
            raise AttributeError("Request tags is neither a string nor a list.")

        temp_request["tags"] = tags_list

        if isinstance(metrics, str):
            temp_request["metrics"] = [metrics]
        elif isinstance(metrics, list):
            temp_request["metrics"] = metrics
        else:
            raise AttributeError("Request metrics is neither a string nor a list.")

        data = self._send_recv_req(temp_request)

        simplified_data = [result for dat in data for result in dat["result"]]

        return simplified_data

    def get_metrics(self) -> list:
        """Returns all of the metrics that have been collected on any node

        :raises RuntimeError: raised if the user hasn't connected to the server
        :return: a list of all metrics that were found
        :rtype: list
        """

        if self._request_conn is None:
            raise RuntimeError("Unable to make a request without first connecting to the server.")

        start_time = int(time.time())

        temp_request = {"type": "suggest", "start_time": start_time, "metrics": None}

        data = self._send_recv_req(temp_request)

        metrics = set()
        for dat in data:
            log.debug(dat["result"])
            metrics.update(dat["result"])
            log.debug(f"{metrics=}")

        return metrics

    def _send_recv_req(self, request):
        self._request_conn.put((request, self._data_queue))
        data = self._data_queue.get()
        return data

    def reboot(self, *, exclude_huids: list = None, exclude_hostnames: list = None) -> None:
        """Calling this will reboot the entire runtime and cause the Dragon runtime to begin tearing down immediately. Any methods called after this, whether they interact with Dragon infrastructure or not, should not be expected to complete in an uncorrupted state.

        :param exclude_huids: List of huids to exclude when restarting the runtime, defaults to None
        :type exclude_huids: list of ints, optional
        :param exclude_hostnames: List of hostnames to exclude when restarting the runtime, defaults to None
        :type exclude_hostnames: list of strings, optional
        """
        runtime_reboot(huids=exclude_huids, hostnames=exclude_hostnames)


class AnalysisServer:
    def __init__(self, queue_dict, return_queue, channel_discovery, shutdown_event, nnodes):
        self._queue_dict = queue_dict
        self._return_queue = return_queue
        self._shutdown_event = shutdown_event
        self._channel_discovery = channel_discovery
        self._nnodes = nnodes

    def _setup_logging(self):
        # This block turns on a client log for each client
        fname = f"{dls.TELEM}_{socket.gethostname()}_analysis_server_{str(this_process.my_puid)}.log"
        _setup_be_logging(fname)

    def run(self):
        self._setup_logging()

        log.debug("running analysis server")

        req_channel = Queue()
        analysis_comm_sdesc = B64.bytes_to_str(cloudpickle.dumps(req_channel))

        for _ in range(self._nnodes):
            self._channel_discovery.put(analysis_comm_sdesc)

        while not self._shutdown_event.is_set():
            try:
                log.debug("In blocking receive")
                request_msg, data_queue = req_channel.get(timeout=1)
            except (queue.Empty, TimeoutError):
                log.debug("Nothing in the req queue")
                continue

            log.debug("got request msg: %s", request_msg)
            results = self._request_dragon_server(request_msg)

            resp_msg = {}
            resp_msg = results
            log.debug(f"sending results: {resp_msg}")
            data_queue.put(resp_msg)

    def _transform_request(self, request_msg):
        request_body = {}

        # type is either query or suggest at the moment
        request_body["type"] = request_msg["type"]

        # grafana uses milliseconds so we use milliseconds as well
        request_body["start"] = str(request_msg["start_time"] * 1000)

        # a somewhat unique req_id. mostly useful for debugging
        request_body["req_id"] = str(request_body["start"]) + "_" + str(request_msg["type"])

        # a list of the metrics that we want to query
        if request_msg["metrics"] is not None:
            request_body["queries"] = [
                {
                    "metric": metric,
                    "filters": [
                        {"filter": "*", "groupBy": False, "tagk": tag, "type": "wildcard"}
                        for tag in request_msg["tags"]
                    ],
                }
                for metric in request_msg["metrics"]
            ]
        else:
            request_body["queries"] = None
        # queue to return data to
        request_body["return_queue"] = "analysis-server"

        # shows db query. useful for debugging
        request_body["showQuery"] = False

        return request_body

    def _request_dragon_server(self, request_msg):
        request_body = self._transform_request(request_msg)

        log.debug("putting %s into queue", request_body)
        for q in self._queue_dict.values():
            # Convert query request to a dragon-server structure and put in queue
            q.put(request_body)

        remaining_hosts = len(self._queue_dict)
        results = []
        log.debug("waiting for results from dragon servers")
        while len(results) != remaining_hosts:
            try:
                res = self._return_queue.get(timeout=1)
                log.debug(f"got result: {res}")
                results.append(res)
            except queue.Empty as e:
                continue
            except Exception as e:
                log.warn(f"Telemetry Analysis Error: {e}")

        return results


class MetricCollector:
    """This class is used to collect telemetry data from a user defined function and add it to the telemetry database."""

    def __init__(self, callable, callable_args, end_event, metric_name, per_gpu, collection_rate):
        self.callable = callable
        self.callable_args = callable_args
        self.per_gpu = per_gpu
        self.metric_name = metric_name
        self.collection_rate = collection_rate
        self._end_event = end_event

    def collect(self):
        """This method is used to collect telemetry data. It will call the user function and log the data to the telemetry database. The user function can take any number of arguments and keyword arguments. The arguments and keyword arguments are passed to the user function when it is called. The collection rate is the rate at which the user function is called. The default is 3 seconds."""
        dt = Telemetry()
        node = current()
        if node.gpu_env_str is not None:
            gpu_num = os.getenv(node.gpu_env_str, None)
        else:
            gpu_num = None
        while not self._end_event.is_set():
            # call the user function
            result = self.callable(self.callable_args)

            # log the data
            if self.per_gpu:
                dt.add_data(ts_metric_name=self.metric_name, ts_data=result, tagk="gpu", tagv=str(gpu_num))
            else:
                dt.add_data(ts_metric_name=self.metric_name, ts_data=result)

            # sleep for the collection rate
            time.sleep(self.collection_rate)


class AnalysisGroupBase(ABC):
    def __init__(self, metric_name: str = "user-cg", collection_rate: float = 3, **kwargs):
        self.metric_name = metric_name
        self.collection_rate = collection_rate

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass


class CollectorGroup(AnalysisGroupBase):
    """This class is used to collect telemetry data from a group of nodes. It is used by the AnalysisServer to collect data from all nodes in the allocation."""

    def __init__(
        self,
        metric_name: str = "user-cg",
        per_gpu: bool = False,
        collection_rate: float = 3,
        collector_args=None,
        **kwargs,
    ):
        self.collector_fn = self.collector_test
        self.collector_args = collector_args
        self.per_gpu = per_gpu
        self.metric_name = metric_name
        self._end_event = Event()
        self.collection_rate = collection_rate
        self._grp = None
        self._collector = MetricCollector(
            callable=self.collector_fn,
            callable_args=self.collector_args,
            end_event=self._end_event,
            metric_name=self.metric_name,
            per_gpu=self.per_gpu,
            collection_rate=self.collection_rate,
        )
        super().__init__(metric_name=self.metric_name, collection_rate=self.collection_rate, **kwargs)

    def _setup_logging(self):
        # This block turns on a client log for each client
        fname = f"{dls.TELEM}_{socket.gethostname()}_user_collectorgroup_{str(this_process.my_puid)}.log"
        _setup_be_logging(fname)

    def generate_policies(self) -> list[Policy]:
        """Generate the list of policies to use for the collector group. If per_gpu is true, then the policies will be per GPU. If per_gpu is false, then the policies will be per node.

        :return: list of policies with one policy per hostname or GPU in the allocation
        :rtype: list[Policy]
        """
        sys = System()
        return sys.gpu_policies() if self.per_gpu else sys.hostname_policies()

    def start(self):
        """This method is used to start the collection of telemetry data. It will create a process group and start the collection on all nodes in the allocation."""
        self._setup_logging()
        policies = self.generate_policies()
        self._grp = ProcessGroup(restart=False, pmi=None)
        for policy in policies:
            self._grp.add_process(nproc=1, template=ProcessTemplate(target=self._collector.collect, policy=policy))
        self._grp.init()
        self._grp.start()
        super().start()

    def stop(self):
        """This method is used to stop the collection of telemetry data. It will set the end event and join the process group."""
        self._end_event.set()
        self._grp.join()
        self._grp.close()
        super().stop()

    @staticmethod
    @abstractmethod
    def collector_test(collector_args: tuple) -> float:
        """This function will be run on each Node (or GPU) in the allocation. It will call the user function and the returned float will be written to the telemetry database.

        :raises NotImplementedError: if the method is not implemented in the subclass
        :return: value to be added to the telemetry database
        :rtype: float
        """
        raise NotImplementedError("This method should be implemented in the subclass.")


class Detector(AnalysisGroupBase):
    def __init__(
        self,
        metric_name: str = "user-cg",
        collection_rate: float = 3,
        sacred_nodes: list = None,
        restartable_ddicts: list = None,
        tags: list = None,
        analysis_args: tuple = None,
        **kwargs,
    ):
        self.metric_name = metric_name
        self.collection_rate = collection_rate
        self._watcher_end_event = Event()
        self.sacred_nodes = sacred_nodes
        self.restartable_ddicts = restartable_ddicts
        self.tags = tags
        self.analysis_fn = self.analyze_data
        self.analysis_args = analysis_args
        self.watcher_args = (
            self._watcher_end_event,
            self.metric_name,
            self.collection_rate,
            self.sacred_nodes,
            self.restartable_ddicts,
            self.tags,
            self.analysis_fn,
            self.analysis_args,
        )
        self._analysis_process = Process(target=self.watcher, args=self.watcher_args)
        super().__init__(metric_name=self.metric_name, collection_rate=self.collection_rate, **kwargs)

    def start(self) -> None:
        self._analysis_process.start()
        super().start()

    def stop(self) -> None:
        self._watcher_end_event.set()
        self._analysis_process.join()
        super().stop()

    @classmethod
    def watcher(
        cls, end_event, metric_name, collection_rate, sacred_nodes, restartable_ddicts, tags, analysis_fn, analysis_args
    ) -> None:
        """Process launched to watch the GPU utilization, analyze the data, and trigger a restart.

        :param stop: event to indicate that main computational work is done
        :type stop: mp.Event
        :param metric_name: name of the metric to watch
        :type metric_name: str
        :param collection_rate: rate at which to collect data, defaults to 1
        :type collection_rate: float, optional
        :param sacred_nodes: hostname of nodes that we place the dictionary on and don't want to remove
        :type sacred_nodes: list
        :param restartable_ddicts: list of ddicts that need to be destroyed with allow_restart=True
        :type restartable_ddicts: list
        :param tags: list of tags to use when getting data from telemetry, typically ["host", "gpu"]
        :type tags: list
        """
        fname = f"{dls.TELEM}_{socket.gethostname()}_user_watcher_{str(this_process.my_puid)}.log"
        _setup_be_logging(fname)
        # initialize analysis client to retrieve telemetry data
        tac = AnalysisClient()
        tac.connect(timeout=60)
        # ignore data before this process has started
        start_time = int(time.time())

        # loop until told that main computational work is done
        while not end_event.is_set():
            # get gpu utilization and analyze data
            time.sleep(collection_rate)
            data = tac.get_data(metric_name, tags=tags, start_time=start_time)
            try:
                restart, node_to_remove = analysis_fn(data, analysis_args)
            except Exception as e:
                log.error(f"Unable to analyze data due to the following exception:\n {e}\n")
                continue

            # start restart logic
            if restart and not end_event.is_set():
                cls.trigger_restart(node_to_remove, sacred_nodes, restartable_ddicts)

    @staticmethod
    def trigger_restart(nodes_to_remove: list, sacred_nodes: list, restartable_ddicts: list) -> None:
        """Trigger a restart of the runtime and remove nodes that are not sacred nodes. The restartable_ddicts are destroyed with `allow_restart=True`.

        :param node_to_ignore: list of nodes to remove on restart
        :type node_to_ignore: list
        :param sacred_nodes: hostname of the sacred node
        :type sacred_nodes: list
        :param restartable_ddicts: list of ddicts that need to be destroyed with allow_restart=True
        :type restartable_ddicts: list
        """

        tac = AnalysisClient()
        # we're going to restart so we need to destroy the ddict and allow it to be restarted
        # if we have a node to exclude, restart with that node excluded
        for dd in restartable_ddicts:
            dd.destroy(allow_restart=True)
        set_to_remove = set(nodes_to_remove)
        set_sacred_nodes = set(sacred_nodes)
        removing = list(set_to_remove - set_sacred_nodes)
        tac.reboot(exclude_hostnames=removing)

    @staticmethod
    @abstractmethod
    def analyze_data(data: list, analysis_args: tuple) -> tuple:
        """This will analyze the data returned from the analysis clients `get_data` method. The data is a list of dictionaries with the following keys: tags, dps. The tags are the tags provide information about the node and GPU/process where the data was written from. The dps are the data points that were collected. The function will return a tuple with two values: a boolean indicating if a restart is needed and a list of nodes to remove when restarting.

        :param data: data returned from analysis clients `get_data` method
        :type data: list
        :raises NotImplementedError: if the method is not implemented in the subclass
        :return: flag for restart and list of nodes to remove when restarting
        :rtype: tuple
        """
        raise NotImplementedError("This method should be implemented in the subclass.")


class CollectorDetectorGroup(Detector, CollectorGroup):
    """This class is a base class for Collector-Detector implementations that utilize Dragon's telemetry infrastructure. The CollectorGroup implements a flexible framework for collecting telemetry data from a user-defined function and adding it to the telemetry database. The CollectorDetectorGroup extends this functionality by providing a mechanism to monitor the collected data and trigger an event if certain conditions are met. The class is designed to be subclassed, and the user must implement the `collector_test`, `trigger_restart`, and `analyze_data` methods to define the specific behavior of the detector. The `SlowGPUDetector` class is an example of a subclass that implements a specific detector for slow GPUs. The `watcher` method is responsible for monitoring the collected data and triggering the restart logic if necessary. The `start` and `stop` methods are used to control the lifecycle of the CollectorGroup and the watcher process. The `sacred_nodes`, `restartable_ddicts`, and `tags` parameters are used to specify the nodes that cannot be removed, the dictionaries to be restarted that need to be destroyed with `allow_restart=True`, and the tags to be used when getting data from telemetry, respectively."""

    def __init__(
        self,
        metric_name: str = "user-cg",
        per_gpu: bool = False,
        collection_rate: float = 3,
        sacred_nodes: list = None,
        restartable_ddicts: list = None,
        tags: list = None,
        collector_args: tuple = None,
        analysis_args: tuple = None,
    ):
        super().__init__(
            metric_name=metric_name,
            per_gpu=per_gpu,
            collection_rate=collection_rate,
            collector_fn=self.collector_test,
            collector_args=collector_args,
            sacred_nodes=sacred_nodes,
            restartable_ddicts=restartable_ddicts,
            tags=tags,
            analysis_args=analysis_args,
        )

    @staticmethod
    def collector_test(collector_args: tuple) -> float:
        """This function will be run on each Node (or GPU) in the allocation. It will call the user function and the returned float will be written to the telemetry database.

        :raises NotImplementedError: if the method is not implemented in the subclass
        :return: value to be added to the telemetry database
        :rtype: float
        """
        raise NotImplementedError("This method should be implemented in the subclass.")

    @staticmethod
    def analyze_data(data: list, analysis_args: tuple) -> tuple:
        """This will analyze the data returned from the analysis clients `get_data` method. The data is a list of dictionaries with the following keys: tags, dps. The tags are the tags provide information about the node and GPU/process where the data was written from. The dps are the data points that were collected. The function will return a tuple with two values: a boolean indicating if a restart is needed and a list of nodes to remove when restarting.

        :param data: data returned from analysis clients `get_data` method
        :type data: list
        :raises NotImplementedError: if the method is not implemented in the subclass
        :return: flag for restart and list of nodes to remove when restarting
        :rtype: tuple
        """
        raise NotImplementedError("This method should be implemented in the subclass.")


class SlowGPUDetector(CollectorDetectorGroup):
    """This class is used to detect slow GPUs. It will call the user function and log the data to the telemetry database. The user function can take any number of arguments and keyword arguments. The arguments and keyword arguments are passed to the user function when it is called. The collection rate is the rate at which the user function is called. The default is 3 seconds.

    Example usage:

    .. highlight:: python
    .. code-block:: python

        import dragon
        from dragon.telemetry import SlowGPUDetector

        if __name__ == "__main__":

            # get list of nodes
            alloc = System()
            nodes = [Node(id) for id in alloc.nodes]

            # find the primary node and define it as sacred
            sacred_node = alloc.sacred_node.hostname
            print(f"Sacred node: {sacred_node}", flush=True)

            # determine if we're using cpus or gpus
            a_node = nodes[0]
            num_gpus = a_node.num_gpus
            gpu_vendor = a_node.gpu_vendor

            policies = alloc.gpu_policies()
            num_workers = len(policies)

            # define ddict placement policy so that we can restart if sacred node is in set of nodes we restart on
            ddict_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=sacred_node)
            orc_ddict_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=sacred_node)

            # check if this is a restart and set args to allow restart if it is
            restarted = alloc.restarted
            n_nodes = 1
            managers_per_node = 1
            if restarted:
                ddict_policy = None

            # define ddict
            dd = DDict(
                n_nodes=n_nodes,
                managers_per_node=managers_per_node,
                total_mem=50 * 1024 * 1024 * 1024,
                managers_per_policy=1,
                policy=ddict_policy,
                orc_policy=orc_ddict_policy,
                name="coordination_dict",
                restart=restarted,
            )

            heimdallr = SlowGPUDetector(restartable_ddicts=[dd])
            heimdallr.start()

            # do some work on the GPUs and write some data to the DDict. This will be monitored by the SlowGPUDetector and the runtime may be restarted if a GPU is determined to be slow. The DDict will be destroyed with the allow_restart=True flag. This allows the DDict to be reconstituted when the runtime is restarted and data within the DDict to be used to define the restart logic.

            heimdallr.stop()
            dd.destroy()
    """

    def __init__(
        self,
        collection_rate: float = 3,
        restartable_ddicts: list = None,
        analysis_args: tuple = None,
        collector_args: tuple = None,
    ):
        self._sacred_hostname = None
        super().__init__(
            metric_name="slow-gpu",
            per_gpu=True,
            collection_rate=collection_rate,
            tags=["host", "gpu"],
            restartable_ddicts=restartable_ddicts,
            sacred_nodes=[self.sacred_hostname],
            collector_args=collector_args,
            analysis_args=analysis_args,
        )

    @staticmethod
    def collector_test(collector_args) -> float:
        """This function is used to test the GPU. It will create low-rank matrices and perform a tensor contraction. The time taken to perform the tensor contraction is returned.

        :return: the elapsed time for the main kernel
        :rtype: float
        """
        import torch

        # Define the size of the tensors
        N = 20  # takes 2s
        r = 10  # Low-rank approximation (r << N)

        # Move tensors to GPU
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Generate low-rank matrices taking 4*N*r*size(float) bytes
        U = torch.rand(N, r, device=device)
        V = torch.rand(N, r, device=device)
        P = torch.rand(N, r, device=device)
        Q = torch.rand(N, r, device=device)

        # Initialize the output tensor
        C = torch.zeros(N, N, device=device)  # Resulting tensor of size (N, N)

        start = time.perf_counter()
        # Perform the tensor contraction
        for i in range(N):
            for j in range(N):
                for p in range(r):
                    for q in range(r):
                        C[i, j] += torch.sum(V[:, p] * P[:, q]) * U[i, p] * Q[j, q]
        end = time.perf_counter()
        elapsed = end - start

        return elapsed

    @property
    def sacred_hostname(self) -> str:
        """Return the sacred hostname of the node.

        :return: sacred hostname
        :rtype: str
        """
        if self._sacred_hostname is None:
            sys = System()
            self._sacred_hostname = sys.primary_node.hostname
        return self._sacred_hostname

    @staticmethod
    def analyze_data(gpu_data: list, analysis_args: tuple) -> tuple:
        """Analyze the slow gpu kernel data collected by the above and determine if a gpu is lagging sufficiently to require a restart of the runtime and exclusion of that node.

        :param gpu_data: list of dicts containing tags and data points
        :type gpu_data: list
        :return: flag for restart and list of nodes to exclude
        :rtype: tuple
        """
        import pandas as pd

        # return values
        restart = False
        take_out = []

        # number of data points to include in computation
        window = 4
        # used for the computation of the z-score
        all_data_array = []
        mean_dict = {}

        # iterate through the list of data
        for data_dict in gpu_data:
            # get tags and data points
            tags = data_dict["tags"]
            data = data_dict["dps"]
            # if there aren't any data points don't do anything
            if data == {}:
                return restart, take_out
            # generate the time series
            time_array = [int(time) for time in data.keys()]
            time_array.sort()
            if len(time_array) < window:
                return restart, take_out
            num_points = min(window, len(time_array))
            # generate data array for relevant time points
            data_array = [data[str(time)] for time in time_array[-1 * num_points :]]
            # extend copy of total data
            all_data_array.extend(data_array)
            # compute the mean utilization for the specific host and gpu
            mean_data = sum(data_array) / len(data_array)
            try:
                # response structure seems to have changed so that the gpu tag doesn't come with now? is that true?
                mean_dict[tags["host"] + "_" + tags["gpu"]] = mean_data
            except KeyError:
                raise KeyError(f"Unable to find host and gpu in tags: {tags}, {data=}, {gpu_data=}")

        # compute the mean and std gpu utilization for all gpus
        all_series = pd.Series(all_data_array)
        all_mean = all_series.mean()
        all_std = all_series.std()

        # if the std is small, everything is tightly coupled so don't restart
        if all_std < 10 ** (-9):
            return restart, take_out

        # for each gpu, compute the z-score of the utilization over the window
        for tags, mean in mean_dict.items():
            zscore = (mean - all_mean) / all_std
            if zscore > 2.5:
                # if more than two stds below performance, restart without that node
                restart = True
                hostname = tags.split("_")[0]
                take_out.append(hostname)

        return restart, take_out
