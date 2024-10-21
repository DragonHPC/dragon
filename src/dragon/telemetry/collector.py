import dragon
from dragon.globalservices.node import get_list
from dragon.channels import Channel
import dragon.managed_memory as dmem
import multiprocessing as mp
import psutil
import os
import sys
import time
import requests
import logging
import getpass

LOG = logging.getLogger(__name__)

class Collector:
    """Collects telemetry data from compute node
        Current default metrics -
        - load_average
        - used_RAM
        - cpu_percent
    """
    def __init__(self):
        """Iniitialize data structure to store metrics
        """
        # Instance of telemetry data collected by each collector process
        self.data = {}
        self.data["dps"] = {}
        self.url = "http://localhost:4243/api/metrics"
        self.telemetry_level = int(os.getenv('DRAGON_TELEMETRY_LEVEL', 0))
        self.cleanup_url = "http://localhost:4243/api/tsdb_cleanup"
        self.tmdb_window = int(os.getenv("DRAGON_DEFAULT_TMDB_WNDW", 300))
        self.start_time = ""
        self._default_pool = None

    def collect(self, start_event: object, shutdown_event: object):
        """ Collect telemetry metrics and send POST and cleanup requests to TSDB Server

        Args:
            start_event (object): Event signaling TSDBServer has started, and Collector can send POST/Cleanup requests to it.
            shutdown_event (object): Event signaling it is time to shutdown collector processes
        """

        try:
            start_event.wait(timeout=30)
        except TimeoutError:
            LOG.debug(f"Collector timed out waiting for TSDB server on {os.uname().nodename} and start event to be set")


        LOG.debug(f"Collecting telemetry data from {os.uname().nodename}")
        self.start_time = int(time.time())
        self._default_pool = dmem.MemoryPool.attach_default()
        while True:
            load1, _, _ = os.getloadavg()

            self.data["timestamp"] = int(time.time())
            self.data["dps"]["load_average"] = load1
            self.data["dps"]["used_RAM"] = psutil.virtual_memory().percent
            self.data["dps"]["cpu_percent"] = psutil.cpu_percent()
            self.data["dps"]["num_running_processes"] = len([(p.pid, p.info['name']) for p in psutil.process_iter(['name', 'username']) if p.info['username'] == getpass.getuser()])
            def_pool_utilization = self._default_pool.utilization
            self.data["dps"]["def_pool_utilization"] = def_pool_utilization
            try:
                api_resp = requests.post(self.url, json=self.data)
                time.sleep(0.1)
            except requests.exceptions.RequestException as e:
                if shutdown_event.is_set():
                    LOG.debug(f"Shutting down collector on {os.uname().nodename}")
                    break
                else:
                    LOG.debug(f"{os.uname().nodename}: Waiting on TSDB Server")
                    time.sleep(0.1)

            if shutdown_event.is_set():
                LOG.debug(f"Shutting down collector on {os.uname().nodename}")
                break

            if (self.data["timestamp"] - self.start_time) > self.tmdb_window:
                try:
                    start_time = self.data["timestamp"] - self.tmdb_window
                    cleanup_req = {"start_time": start_time }
                    api_resp = requests.post(self.cleanup_url, json=cleanup_req)
                    self.start_time = start_time
                except requests.exceptions.RequestException as e:
                    if shutdown_event.is_set():
                        LOG.debug(f"Shutting down collector on {os.uname().nodename}")
                        break
                    else:
                        LOG.debug(f"{os.uname().nodename}: Waiting on TSDB Server")
                        time.sleep(10)
                time.sleep(0.1)
