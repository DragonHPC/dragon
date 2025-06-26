"""Dragon's API to generate and visualize time series metrics in grafana utilizing the telemetry infrastructure"""
import requests
import time
import socket
import os
from yaml import safe_load


class Telemetry:
    """
    This class is used to generate and visualize time series metrics in Grafana utilizing the Dragon telemetry infrastructure. The telemetry infrastructure is started when the runtime is started with `--telemetry-level` set to greater than zero. A `--telemetry-level=1` is reserved for user metrics and will not collect any default metrics. The user can add data to the local node database using the `add_data` method. The data can then be visualized in Grafana or retrieved and analyzed utilizing the `AnalysisClient`.

    Example usage:

    .. highlight:: python
    .. code-block:: python

        import dragon
        import multiprocessing as mp
        import time
        from dragon.telemetry import Telemetry

        if __name__ == "__main__":
            dt = Telemetry()

            mp.set_start_method("dragon")
            pool = mp.Pool(10)

            for _ in range(10)
                start = time.time()
                pool.map(f, list(range(100)))
                dt.add_data("map_time", time.time()-start, telemetry_level=2)

            pool.close()
            pool.join()

            dt.finalize()
    """

    def __init__(self, metrics_url="http://localhost:4243/api/metrics", timeout= None):
        telem_cfg = os.getenv("DRAGON_TELEMETRY_CONFIG", None)

        if telem_cfg is None:
            telemetry_cfg = {}
        else:
            with open(telem_cfg, "r") as file:
                telemetry_cfg = safe_load(file)
        tsdb_port = telemetry_cfg.get("tsdb_server_port", "4243")
        self.metrics_url = f"http://localhost:{tsdb_port}/api/metrics"
        self._shutdown_url = f"http://localhost:{tsdb_port}/api/set_shutdown"
        self._ready_url = f"http://localhost:{tsdb_port}/api/ready"
        self.formatted_data = {"dps": {}}
        self._telemetry_level = int(os.getenv("DRAGON_TELEMETRY_LEVEL", 0))
        # Check if TSDB Server is up
        # If telemetry level is 0, telemetry infrastructure isn't requested
        if self._telemetry_level > 0:
            while True:
                try:
                    api_resp = requests.get(self._ready_url, timeout=(timeout, timeout))
                    break
                except requests.exceptions.ConnectionError as e:
                    time.sleep(0.1)
                    pass
                except requests.exceptions.ReadTimeout as e:
                    raise TimeoutError("Telemetry took longer than expected to start.")

    @property
    def level(self):
        return self._telemetry_level

    def add_data(self, ts_metric_name: str, ts_data: float, timestamp: int = None, telemetry_level: int = 1, tagk: str = None, tagv: int | str = None) -> None:
        """Adds user defined metric data to node local database that can then be retrieved via Grafana

        :param ts_metric_name: Metric name used to store data and retrieve it in Grafana. This should be consistent across nodes. Grafana's retrieval will add the hostname to the metric.
        :type ts_metric_name: str
        :param ts_data: Time-series data point
        :type ts_data: float
        :param timestamp: time stamp for time-series data point, defaults to int(time.time())
        :type timestamp: int, optional
        :param telemetry_level: telemetry data level for metric. if the data_level is greater than the launch specified telemetry level the data will not be added to the data base, defaults to 1
        :type telemetry_level: int, optional
        :param tagk: tag key for the datapoint
        :type tagk: str, optional
        :param tagv: tag value
        :type tagv: int | str, optional
        """

        if telemetry_level <= self._telemetry_level:
            data_name = ts_metric_name
            if timestamp is None:
                timestamp = int(time.time())
            self.formatted_data["timestamp"] = timestamp
            if tagk is None or tagv is None:
                self.formatted_data["dps"]=[{"metric":data_name, "value": ts_data}]
            else:
                self.formatted_data["dps"]=[{"metric":data_name, "value": ts_data, "tags": {tagk: tagv}}]

            api_resp = requests.post(self.metrics_url, json=self.formatted_data)

    def finalize(self) -> None:
        """Finalize shuts down the telemetry service if it was started. It can be called when the user is done with telemetry. If it is not called the user will have to Ctrl-C from the terminal to shutdown the telemetry service."""
        # if telemetry_level is 0 then the telemetry infrastructure wasn't started
        if self._telemetry_level > 0:
            _ = requests.get(self._shutdown_url)
