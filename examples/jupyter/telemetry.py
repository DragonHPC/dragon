import dragon
import multiprocessing as mp
from dragon.globalservices.node import get_list
from py3nvml.py3nvml import *
import py3nvml.nvidia_smi as smi
from prometheus_client import Gauge, Histogram, start_http_server
import time
import os
import socket


class TelemWorkers:
    def __init__(self):
        self.procs = []
        self.end_ev = mp.Event()

    def start(self):
        """Launches a telemetry worker on each node"""

        node_list = get_list()
        nnodes = len(node_list)
        # Create a process on each node for monitoring
        for _ in range(nnodes):
            proc = mp.Process(target=self.telem_work, args=(self.end_ev,))
            proc.start()
            self.procs.append(proc)

    def close(self):
        """Stops the collection of data and closes the procs"""
        # wait on the monitoring processes
        self.end_ev.set()
        for proc in self.procs:
            proc.join()

    def telem_work(self, end_ev):
        """Updates a prometheus server with telemetry data from cpus and gpus on each node

        :param end_ev: the event used to signal the end of the telemetry data collection
        :type end_ev: mp.Event
        """
        print(f"This is a telemetry process on node {os.uname().nodename}.", flush=True)
        # Create Prometheus metrics
        gpu_utilization = Gauge(
            "gpu_utilization", "GPU utilization percentage", ["hostname", "gpu_index", "uuid"]
        )
        gpu_memory_utilization = Gauge(
            "gpu_memory_utilization", "GPU memory utilization percentage", ["hostname", "gpu_index", "uuid"]
        )
        gpu_memory_used = Gauge("gpu_memory_used", "GPU memory used ", ["hostname", "gpu_index", "uuid"])
        gpu_memory_free = Gauge("gpu_memory_free", "GPU memory free ", ["hostname", "gpu_index", "uuid"])
        gpu_memory_total = Gauge("gpu_memory_total", "GPU memory total ", ["hostname", "gpu_index", "uuid"])
        system_load_average = Gauge("system_load_average", "System load average over 1 minute")
        request_latency = Histogram("request_latency_seconds", "Request latency in seconds")

        # Start the Prometheus metrics server
        start_http_server(8000)

        while True:
            # TELEMETRY WITH PROMETHEUS
            # Initialize NVML
            nvmlInit()

            # Process requests and update metrics
            # Record the start time of the request
            start_time = time.time()

            # Get the system load averages
            load1, _, _ = os.getloadavg()

            # Update the system_load_average gauge with the new value
            system_load_average.set(load1)

            # Get the GPU utilization and memory utilization for each device
            device_count = nvmlDeviceGetCount()
            for i in range(device_count):
                handle = nvmlDeviceGetHandleByIndex(i)
                uuid = nvmlDeviceGetUUID(handle)
                utilization = nvmlDeviceGetUtilizationRates(handle)
                memory = nvmlDeviceGetMemoryInfo(handle)
                gpu_utilization.labels(socket.gethostname(), i, uuid).set(utilization.gpu)
                gpu_memory_utilization.labels(socket.gethostname(), i, uuid).set(utilization.memory)
                gpu_memory_used.labels(socket.gethostname(), i, uuid).set(memory.used >> 20)
                gpu_memory_free.labels(socket.gethostname(), i, uuid).set(memory.free >> 20)
                gpu_memory_total.labels(socket.gethostname(), i, uuid).set(memory.total >> 20)

            # Record the end time of the request and update the request_latency histogram
            end_time = time.time()
            request_latency.observe(end_time - start_time)

            # Shut down NVML
            nvmlShutdown()
            # END

            time.sleep(1)

            # check if the end event is set. If yes, exit.
            if end_ev.is_set():
                print(f"Telemetry process on node {os.uname().nodename} exiting ...", flush=True)
                break
