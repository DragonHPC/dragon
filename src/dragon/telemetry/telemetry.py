import requests
import time
import socket
import os


class Telemetry():
    def __init__(self, metrics_url="http://localhost:4243/api/metrics"):
        self.metrics_url = metrics_url
        self._shutdown_url = "http://localhost:4243/api/set_shutdown"
        self._ready_url  = "http://localhost:4243/api/ready"
        self.formatted_data = {"dps":{}}
        self._telemetry_level = int(os.getenv('DRAGON_TELEMETRY_LEVEL', 0))
        # Check if TSDB Server is up
        # If telemetry level is 0, telemetry infrastructure isn't requested
        if self._telemetry_level > 0:
            while True:
                try:
                    api_resp = requests.get(self._ready_url)
                    break
                except requests.exceptions.RequestException as e:
                    time.sleep(0.1)
                    pass

    def add_data(self, ts_metric_name: str, ts_data: float, timestamp: int = None, telemetry_level: int = 1) -> None:
        """Adds user defined metric data to node local database that can then be retrieved via Grafana

        :param ts_metric_name: Metric name used to store data and retrieve it in Grafana. This should be consistent across nodes. Grafana's retrieval will add the hostname to the metric.
        :type ts_metric_name: str
        :param ts_data: Time-series data point
        :type ts_data: float
        :param timestamp: time stamp for time-series data point, defaults to int(time.time())
        :type timestamp: int, optional
        :param telemetry_level: telemetry data level for metric. if the data_level is greater than the launch specified telemetry level the data will not be added to the data base, defaults to 1
        :type telemetry_level: int, optional
        """

        if telemetry_level <= self._telemetry_level:
            data_name = ts_metric_name
            if timestamp is None:
                timestamp = int(time.time())
            self.formatted_data["timestamp"] = timestamp
            self.formatted_data["dps"][data_name] = ts_data
            api_resp = requests.post(self.metrics_url, json=self.formatted_data)


    def finalize(self) -> None:
        """Finalize shuts down the telemetry service if it was started. It can be called when the user is done with telemetry. If it is not called the user will have to Ctrl-C from the terminal to shutdown the telemetry service.
        """
        # if telemetry_level is 0 then the telemetry infrastructure wasn't started
        if self._telemetry_level > 0:
            _ = requests.get(self._shutdown_url)

