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

import dragon.infrastructure.parameters as di_param

LS_TAS_KEY = "telemetry_analysis_queue"
log = None


def _setup_be_logging(fname):
    # This block turns on a client log for each client
    global log
    if log is None:
        setup_BE_logging(service=dls.TELEM, fname=fname)
        log = logging.getLogger(str(dls.TELEM))


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
