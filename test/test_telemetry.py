#!/usr/bin/env python3
import shim_dragon_paths
import unittest
import multiprocessing as mp
import os

from telemetry.test_aggregator_app import TestDragonTelemetryAggregatorApp
from telemetry.test_collector import TestDragonTelemetryCollector
from telemetry.test_dragon_server import TestDragonTelemetryDragonServer
from telemetry.test_tsdb_app import TestDragonTelemetryTSDBApp, TestDragonTelemetryTSDBAppErrors
from telemetry.test_tsdb_server import TestDragonTelemetryTSDBServer
from telemetry.test_analysis import TestDragonTelemetryAnalysisClient, TestDragonTelemetryAnalysisServer


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
