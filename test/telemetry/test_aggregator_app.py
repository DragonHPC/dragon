import unittest
from unittest.mock import patch
import dragon
import multiprocessing as mp
import time
import json
import os
from threading import get_ident

from dragon.globalservices.node import get_list, query
from dragon.telemetry.aggregator_app import app


class TestDragonTelemetryAggregatorApp(unittest.TestCase):

    def setUp(self):
        node_list = get_list()
        node_descr_list = [query(h_uid) for h_uid in node_list]
        node_list = [descr.host_name for descr in node_descr_list]
        queue_dict = {}
        for host in node_list:
            queue_dict[host] = mp.Queue()
        self.context = app.app_context()
        self.context.push()
        self.client = app.test_client()
        app.config["queue_dict"] = queue_dict
        app.config["return_queue"] = mp.Queue()
        app.config["result_dict"] = {}

    def test_get_aggregators(self):
        response = self.client.get("/api/aggregators")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.get_data(as_text=True)), ["min", "sum", "max", "avg", "dev"])

    @patch("dragon.telemetry.aggregator_app.time")
    def test_post_query(self, mock_time):

        fix_time = time.time()
        mock_time.time.return_value = fix_time
        query_from_grafana = {
            "start": 1708966449000,
            "queries": [
                {"metric": "load_average", "aggregator": "sum", "downsample": "100ms-avg", "tags": {"host": "*"}}
            ],
            "msResolution": False,
            "globalAnnotations": True,
            "showQuery": True,
            "end": 1701789807277,
        }
        req_uid = str(int(fix_time * 100)) + "_" + str(get_ident())

        # Assume collectors and dragon servers do their work and send data to return queue
        dragon_server_response = {
            "start": 1708966449000,
            "queries": [
                {
                    "metric": "cpu_percent",
                    "aggregator": "sum",
                    "downsample": "100ms-avg",
                    "tags": {"host": "*"},
                    "index": 0,
                }
            ],
            "msResolution": False,
            "globalAnnotations": True,
            "showQuery": True,
            "end": 1701789807277,
            "req_id": req_uid,
            "type": "query",
            "host": "pinoak0034",
            "result": [
                {
                    "query": {
                        "metric": "cpu_percent",
                        "aggregator": "sum",
                        "downsample": "100ms-avg",
                        "tags": {"host": "*"},
                        "index": 0,
                    },
                    "metric": "cpu_percent",
                    "tags": {"host": "pinoak0034"},
                    "aggregateTags": [],
                    "dps": {
                        "1721860573": 0.5,
                        "1721860574": 0.0,
                        "1721860575": 0.0,
                        "1721860576": 0.8,
                        "1721860577": 0.8,
                        "1721860578": 0.8,
                        "1721860579": 0.8,
                        "1721860580": 0.8,
                        "1721860581": 0.8,
                        "1721860582": 0.8,
                        "1721860583": 0.7,
                        "1721860584": 0.8,
                        "1721860585": 0.8,
                        "1721860586": 0.8,
                        "1721860587": 0.8,
                        "1721860588": 0.8,
                        "1721860589": 0.8,
                        "1721860590": 0.8,
                        "1721860591": 1.0,
                        "1721860592": 0.8,
                        "1721860593": 0.8,
                        "1721860594": 0.8,
                        "1721860595": 0.8,
                        "1721860596": 56.7,
                        "1721860597": 99.5,
                        "1721860598": 15.0,
                        "1721860599": 0.8,
                        "1721860600": 0.8,
                        "1721860601": 0.8,
                        "1721860602": 0.8,
                        "1721860603": 0.9,
                        "1721860604": 0.8,
                        "1721860605": 0.8,
                        "1721860606": 0.8,
                        "1721860607": 0.8,
                        "1721860608": 0.8,
                        "1721860609": 0.8,
                        "1721860610": 0.9,
                        "1721860611": 0.8,
                        "1721860612": 0.8,
                        "1721860613": 0.8,
                        "1721860614": 0.8,
                        "1721860615": 0.8,
                        "1721860616": 0.9,
                        "1721860617": 0.8,
                        "1721860618": 0.8,
                        "1721860619": 0.8,
                        "1721860620": 0.8,
                        "1721860621": 0.8,
                        "1721860622": 0.8,
                        "1721860623": 0.8,
                        "1721860624": 0.8,
                        "1721860625": 0.9,
                        "1721860626": 0.8,
                        "1721860627": 0.8,
                        "1721860628": 0.8,
                        "1721860629": 0.8,
                        "1721860630": 0.8,
                        "1721860631": 0.8,
                        "1721860632": 0.8,
                        "1721860633": 0.8,
                        "1721860634": 0.8,
                        "1721860635": 0.9,
                        "1721860636": 0.8,
                        "1721860637": 0.8,
                        "1721860638": 0.9,
                        "1721860639": 0.8,
                        "1721860640": 0.8,
                        "1721860641": 0.8,
                        "1721860642": 0.8,
                        "1721860643": 0.8,
                        "1721860644": 0.8,
                        "1721860645": 0.8,
                        "1721860646": 0.8,
                        "1721860647": 0.8,
                        "1721860648": 0.8,
                        "1721860649": 0.8,
                        "1721860650": 0.8,
                        "1721860651": 0.8,
                        "1721860652": 0.8,
                        "1721860653": 0.8,
                    },
                }
            ],
        }
        app.config["return_queue"].put(dragon_server_response)

        response = self.client.post("/api/query", json=query_from_grafana)

        self.assertEqual(response.status_code, 200)

    @patch("dragon.telemetry.aggregator_app.time")
    def test_get_suggest(self, mock_time):
        dragon_server_response = {
            "type": "suggest",
            "request": [],
            "host": "pinoak0011",
            "result": ["load_average", "used_RAM", "cpu_percent", "dragon_telemetry_test_data"],
        }
        fix_time = time.time()
        mock_time.time.return_value = fix_time
        req_uid = str(int(fix_time * 100)) + "_" + str(os.getpid())
        dragon_server_response["req_id"] = req_uid
        app.config["return_queue"].put(dragon_server_response)
        response = self.client.get("/api/suggest")

        response_data = json.loads(response.get_data(as_text=True))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response_data), 4)
        assert isinstance(response_data, list)

    @patch("dragon.telemetry.aggregator_app.time")
    def test_get_suggest_with_query(self, mock_time):
        fix_time = time.time()
        mock_time.time.return_value = fix_time
        uid = str(int(fix_time * 100)) + "_" + str(os.getpid())
        dragon_server_response = {
            "type": "suggest",
            "req_id": uid,
            "request": [],
            "host": "pinoak0011",
            "result": ["load_average", "used_RAM", "cpu_percent", "dragon_telemetry_test_data"],
        }
        app.config["return_queue"].put(dragon_server_response)
        response = self.client.get("/api/suggest?q=test")
        response_data = json.loads(response.get_data())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response_data, ["dragon_telemetry_test_data"])
        self.assertEqual(len(response_data), 1)

        assert isinstance(response_data, list)

    def test_get_telemetery_shutdown(self):
        response = self.client.get("/api/telemetry_shutdown")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(json.loads(response.get_data())["shutdown_begin"])

    def tearDown(self):
        self.context.pop()


if __name__ == "__main__":
    unittest.main()
