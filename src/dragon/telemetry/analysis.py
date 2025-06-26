"""Dragon's API to access telemetry data from user space"""
from dragon.infrastructure.parameters import this_process
import time
import logging
import cloudpickle
import socket
from dragon.dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from dragon.utils import get_local_kv, B64
from dragon.native.queue import Queue
from dragon.globalservices.process import runtime_reboot
import queue


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

    def get_data(self, metrics: str or list, start_time: int = None) -> list:
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
            request_body["queries"] = [{"metric": metric, "filters": []} for metric in request_msg["metrics"]]
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
