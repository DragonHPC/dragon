from dragon.telemetry.telemetry_head import start_telemetry
from dragon.telemetry.analysis import AnalysisClient
from dragon.native.process import Process
import ipywidgets
import random
import time
import os
import matplotlib.pyplot as plt
import requests
import matplotlib.dates as mdates
from datetime import datetime


class DragonTelemetryWidget:
    """
    A widget for the Dragon telemetry system.
    This widget is used to start the telemetry system and manage its lifecycle.
    """

    def __init__(self):
        print(f'{os.getenv("DRAGON_TELEMETRY_LEVEL", None)=}')
        if int(os.getenv("DRAGON_TELEMETRY_LEVEL", None)) > 0:
            self._telemetry_started = True
        else:
            self._telemetry_started = False
        self._telemetry_head_proc = None
        self._start_time = int(time.time())
        self.tac = None

    def connect(self):
        try:
            self.tac = AnalysisClient()
            self.tac.connect(timeout=60)
            self._telemetry_started = True
        except Exception as e:
            raise RuntimeError("Could not connect to telemetry analysis server.") from e

    def get_start_time(self):
        """ Get the start time of the telemetry system. """
        if not self._telemetry_started:
            raise RuntimeError("Telemetry has not been started. Call start_telemetry() first.")
        return self._start_time

    def get_metrics(self):
        """ Get the list of available metrics from the Dragon telemetry system. """
        if not self._telemetry_started:
            raise RuntimeError("Telemetry has not been started. Call start_telemetry() first.")
        if self.tac is None:
            self.connect()
        metrics = self.tac.get_metrics()
        return metrics

    def get_data(self, metric_name: str = None, start_time: int = None):
        if metric_name is None:
            metric_name = "num_running_processes"
        if start_time is None:
            start_time = self._start_time
        data = self.tac.get_data(metric_name, start_time=start_time)
        return data

    def _render(self, metric_name: str = None, start_time: int = None):
        if metric_name is None:
            metric_name = "num_running_processes"
        if start_time is None:
            start_time = self._start_time
        data_array = self.tac.get_data(metric_name, start_time=start_time)
        plt.figure()

        for data in data_array:
            non_int_times = [datetime.fromtimestamp(int(int_time)) for int_time in data["dps"].keys()]
            times = data["dps"].keys()
            values = data["dps"].values()
            host = data["tags"]["host"]
            try:
                gpu = data["tags"]["gpu"]
            except KeyError:
                gpu = None
            if gpu is not None:
                label = f"{host} (gpu:{gpu})"
            else:
                label = f"{host}"
            plt.plot(
                non_int_times,
                values,
                label=label,
            )
            plt.title(f"Metric: {metric_name}")
            plt.xlabel("time")
            plt.legend()
            plt.locator_params(axis="x", nbins=10)
            ax = plt.gca()  # Get the current axes
            date_format = mdates.DateFormatter("%H:%M:%S")
            ax.xaxis.set_major_formatter(date_format)
            # ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=30))
            plt.gcf().autofmt_xdate()
        return plt

    def render(self, metric_name, percentage_window):
        start_time_init = self.get_start_time()
        current_time = int(time.time())
        start_time = int(start_time_init + (current_time - start_time_init) * percentage_window)
        print(start_time)
        plt = self._render(metric_name, start_time)
        return plt.gcf()

    def custom_slider(self):
        return ipywidgets.FloatSlider(min=0, max=1, step=0.01, description="Time Zoom:")
