import dragon
import dragon.managed_memory as dmem
import multiprocessing as mp
import psutil
import os
import socket
import time
import requests
import logging
from ..infrastructure.gpu_desc import AccVendor
from ..infrastructure.gpu_desc import find_accelerators
from dragon.dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from dragon.infrastructure.parameters import this_process
import getpass
import sys
import subprocess

log = None


def setup_logging():
    # This block turns on a client log for each client
    global log
    if log is None:
        fname = f"{dls.TELEM}_{socket.gethostname()}_collector_{str(this_process.my_puid)}.log"
        setup_BE_logging(service=dls.TELEM, fname=fname)
        log = logging.getLogger(str(dls.TELEM))


accelerator = find_accelerators()

if accelerator is not None:
    if accelerator.vendor == AccVendor.NVIDIA:
        import pynvml
    if accelerator.vendor == AccVendor.AMD:
        rocm_path = os.path.join(os.environ.get("ROCM_PATH", "/opt/rocm/"), "libexec/rocm_smi")
        sys.path.append(rocm_path)
        import rocm_smi
    # Intel devices don't currently require any special packages


def identify_gpu():
    gpu_count = 0
    if accelerator is not None:
        if accelerator.vendor == AccVendor.NVIDIA:
            nvml_instance = pynvml.nvmlInit()
            gpu_count = pynvml.nvmlDeviceGetCount()

        if accelerator.vendor == AccVendor.AMD:
            rocm_instance = rocm_smi.initializeRsmi()
            gpu_count = rocm_smi.listDevices()

        if accelerator.vendor == AccVendor.INTEL:
            # strip out tiling
            gpu_count = len(set(map(int, accelerator.device_list)))

        return accelerator.vendor, gpu_count
    return None, gpu_count


def get_nvidia_metrics(gpu_count, telemetry_level):
    try:
        handle = pynvml.nvmlDeviceGetHandleByIndex(gpu_count)
    except pynvml.NVMLError_GpuIsLost:
        return None
    memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
    memory_utilization = round(float(memory.used) / float(memory.total) * 100, 2)
    results_dict = [
        {
            "metric": "DeviceUtilization",
            "value": pynvml.nvmlDeviceGetUtilizationRates(handle).gpu,
            "tags": {"gpu": gpu_count},
        },
        {
            "metric": "DeviceMemoryUtilization", 
            "value": memory_utilization, 
            "tags": {"gpu": gpu_count}
        },
        {
            "metric": "DevicePowerUsage",
            "value": pynvml.nvmlDeviceGetPowerUsage(handle)/1000, # Unit mW to W
            "tags": {"gpu": gpu_count},
        },
    ]

    return results_dict


def get_amd_metrics(gpu_count, telemetry_level):
    utilization_gpu = rocm_smi.getGpuUse(gpu_count)
    mem_used, mem_total = rocm_smi.getMemInfo(gpu_count, "vram")
    mem = round(float(mem_used) / float(mem_total) * 100, 2)
    power = (rocm_smi.getPower(gpu_count)).get("power")  # Type: Average, Unit W
    if power == "N/A":
        power = 0
    else:
        power = float(power)
    results_dict = [
        {"metric": "DevicePowerUsage", "value": power, "tags": {"gpu": gpu_count}},
        {"metric": "DeviceUtilization", "value": utilization_gpu, "tags": {"gpu": gpu_count}},
        {"metric": "DeviceMemoryUtilization", "value": mem, "tags": {"gpu": gpu_count}},
    ]

    return results_dict


def get_intel_metrics(telemetry_level):
    """Use xpu-smi to get gpu metrics for intel devices. Opening one process and dumping metrics for all GPUs was more performant than doing each individually. This is different from the AMD and Nvidia implementations.

    :param telemetry_level: telemetry level for splitting up GPU metrics.
    :type telemetry_level: int
    :return: dictionary of metrics for all devices on the node
    :rtype: dict
    """

    # prints out the three specified metrics for every GPU on the node
    output = subprocess.run(["xpu-smi", "dump", "-d", "-1", "-m", "0,1,5", "-n", "1"], text=True, capture_output=True)
    output_string = output.stdout
    output_string = output_string.splitlines()
    all_gpu_results_dict = {}

    # parse the output and create a dictionary per device
    for device_stats in output_string[1:]:
        device_info = device_stats.split(",")
        device_id = int(device_info[1].strip())
        if device_info[2].strip() == "N/A":
            utilization_gpu = 0
        else:
            utilization_gpu = float(device_info[2])
        if device_info[4].strip() == "N/A":
            mem = 0
        else:
            mem = float(device_info[4])
        if device_info[3].strip() == "N/A":
            power = 0
        else:
            power = float(device_info[3]) # Unit: W

        all_gpu_results_dict[device_id] = [
            {"metric": "DevicePowerUsage", "value": power, "tags": {"gpu": device_id}},
            {"metric": "DeviceUtilization", "value": utilization_gpu, "tags": {"gpu": device_id}},
            {"metric": "DeviceMemoryUtilization", "value": mem, "tags": {"gpu": device_id}},
        ]

    return all_gpu_results_dict


class Collector:
    """Collects telemetry data from compute node
    Current default metrics -
    - load_average
    - used_RAM
    - cpu_percent
    """

    def __init__(self, telemetry_config: object):
        """Iniitialize data structure to store metrics"""
        # Instance of telemetry data collected by each collector process
        self.metrics = {}
        self.data = {}
        self.data["dps"] = []
        self.telem_cfg = telemetry_config
        tsdb_port = self.telem_cfg.get("tsdb_server_port", "4243")
        self.url = f"http://localhost:{tsdb_port}/api/metrics"
        self.telemetry_level = int(os.getenv("DRAGON_TELEMETRY_LEVEL", 0))
        self.cleanup_url = f"http://localhost:{tsdb_port}/api/tsdb_cleanup"
        self.tmdb_window = int(self.telem_cfg.get("default_tmdb_window", 300))
        self.collection_rate = float(self.telem_cfg.get("collector_rate", 0.5))
        self.start_time = ""
        self._default_pool = None

    def collect(self, start_event: object, shutdown_event: object):
        """Collect telemetry metrics and send POST and cleanup requests to TSDB Server

        Args:
            start_event (object): Event signaling TSDBServer has started, and Collector can send POST/Cleanup requests to it.
            shutdown_event (object): Event signaling it is time to shutdown collector processes
        """

        setup_logging()

        try:
            start_event.wait(timeout=30)
        except TimeoutError:
            log.debug(f"Collector timed out waiting for TSDB server on {os.uname().nodename} and start event to be set")

        log.debug(f"Collecting telemetry data from {os.uname().nodename}")
        self.start_time = int(time.time())
        self._default_pool = dmem.MemoryPool.attach_default()

        if self.telemetry_level > 2:
            gpu, gpu_count = identify_gpu()

        if self.telemetry_level > 3:
            tmdb_directory = self.telem_cfg.get("default_tmdb_dir", "/tmp")
            user = os.environ.get("USER", str(os.getuid()))
            db_path = os.path.join(tmdb_directory, "ts_" + user + "_" + os.uname().nodename + ".db")

        while not shutdown_event.is_set():
            load1, _, _ = os.getloadavg()

            self.data["timestamp"] = int(time.time())

            self.data["dps"].append({"metric": "load_average", "value": load1})
            self.data["dps"].append({"metric": "used_RAM", "value": psutil.virtual_memory().percent})
            self.data["dps"].append({"metric": "cpu_percent", "value": psutil.cpu_percent()})
            self.data["dps"].append(
                {
                    "metric": "num_running_processes",
                    "value": len(
                        [
                            (p.pid, p.info["name"])
                            for p in psutil.process_iter(["name", "username"])
                            if p.info["username"] == getpass.getuser()
                        ]
                    ),
                }
            )
            def_pool_utilization = self._default_pool.utilization
            self.data["dps"].append({"metric": "def_pool_utilization", "value": def_pool_utilization})

            if self.telemetry_level > 2:
                if gpu is not None:
                    if gpu == AccVendor.NVIDIA:
                        for i in range(gpu_count):
                            metrics_dict_list = get_nvidia_metrics(i, self.telemetry_level)
                            self.data["dps"].extend(metrics_dict_list)

                    if gpu == AccVendor.AMD:
                        for i in gpu_count:
                            metrics_dict_list = get_amd_metrics(i, self.telemetry_level)
                            self.data["dps"].extend(metrics_dict_list)

                    if gpu == AccVendor.INTEL:
                        all_metrics_dict_list = get_intel_metrics(self.telemetry_level)
                        for metrics_dict_list in all_metrics_dict_list.values():
                            self.data["dps"].extend(metrics_dict_list)

            if self.telemetry_level > 3:
                # convert to MB
                self.data["dps"].append({"metric": "db_size", "value": (os.path.getsize(db_path))/(1024 * 1024)})

            try:
                api_resp = requests.post(self.url, json=self.data)
                self.data["dps"] = []
            except requests.exceptions.RequestException as e:
                pass

            if (self.data["timestamp"] - self.start_time) > self.tmdb_window:
                try:
                    start_time = self.data["timestamp"] - self.tmdb_window
                    cleanup_req = {"start_time": start_time}
                    api_resp = requests.post(self.cleanup_url, json=cleanup_req)
                    self.start_time = start_time
                except requests.exceptions.RequestException as e:
                    pass

            time.sleep(self.collection_rate)
